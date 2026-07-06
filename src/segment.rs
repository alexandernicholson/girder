//! Immutable, key-sorted segments with zone maps.
//!
//! **Format v2 (block-structured columnar).** Row order inside a segment is
//! key-sorted (preserves `get` binary search, prefix pruning, and merge logic).
//! Data is stored as independently-crc32'd *sections* — a key column, a
//! timestamp column, one dictionary-coded (or plain) column per label, a
//! presence-bitmap + dense-values column per numeric, a payload blob, and an
//! rmp block index (per ~4096-row zone maps). A footer holds the section
//! directory. Filters run over the typed columns and the payload is sliced out
//! only for the rows that survive:
//!
//! ```text
//! [u32 magic "gird"][u32 version=2]
//! -- sections, each: [u32 crc32(body)][body] --
//!   keys      : u32 count · u64 offsets[count+1] · utf8 bytes   (sorted)
//!   timestamps: i64[count]
//!   label <n> : mode 0 dict (u32 n · (u32 len,utf8)×n · u16 codes[count], 0=absent)
//!               mode 1 plain (presence bitmap · strings)  [dict overflow > u16]
//!   numeric<n>: presence bitmap(count bits) · f64[dense present values]
//!   payload   : u64 offsets[count+1] · raw bytes
//!   blocks    : rmp(Vec<BlockMeta>)  (per ~4096 rows: ts/key/numeric/label bounds)
//! -- footer --
//!   rmp(SectionDir{count, entries}) · u64 footer_off · u32 footer_crc · u32 magic
//! ```
//!
//! **WS4 — section-granular targeted I/O.** A scan reads only the sections it
//! touches, one `read_exact_at` per section, and *never* faults in the payload
//! blob wholesale — payload bytes are sliced per surviving row. The footer's
//! section directory ([`SegDir`]) is parsed from a tiny tail read; each column
//! ([`Section`]) is then read + crc-verified individually and cached per
//! `(segment_id, SectionId)`, so a cold `selective` query reads tens of MB of
//! columns instead of the ~GB of payloads. Every positioned read funnels
//! through [`read_at`], which tallies a `bytes_read` counter so per-query I/O is
//! observable. The payload offset table is read on its own (the first
//! `8·(count+1)` bytes of the payload section) and structurally validated
//! without reading — or crc-checking — the raw payload bytes.
//!
//! **Format v1 (legacy)** was `[magic][version=1][crc32(body)][rmp(Vec<Record>)]`.
//! It stays fully readable via a compat shim (`read_columns` / `read_all_records`
//! decode it into the same in-memory column set); the version word dispatches.
//! v1 files are rewritten to v2 by the first compaction that touches them (WS3).
//!
//! The zone map is stored in the manifest (not the file), so the manifest
//! format does not migrate.
use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::{GirderError, Result};
use crate::record::{QuerySpec, Record};

const MAGIC: u32 = 0x6769_7264; // "gird"
const VERSION_V1: u32 = 1;
const VERSION_V2: u32 = 2;
/// Cap on tracked distinct values per label; beyond it the label is treated
/// as unprunable (high cardinality).
const LABEL_VALUES_CAP: usize = 64;
/// Rows per block-index entry.
const BLOCK_ROWS: usize = 4096;
/// Max distinct dictionary values before a label falls back to a plain column.
const DICT_MAX: usize = u16::MAX as usize;
/// Max label dict codes representable in a per-block u64 prune bitset.
const BLOCK_BITSET_CODES: u16 = 64;

// Section kind tags.
/// The format every new segment is written in (see docs/COMPAT.md).
pub const CURRENT_SEGMENT_VERSION: u32 = VERSION_V2;

const K_KEYS: u8 = 0;
const K_TS: u8 = 1;
const K_LABEL: u8 = 2;
const K_NUMERIC: u8 = 3;
const K_PAYLOAD: u8 = 4;
const K_BLOCKS: u8 = 5;
/// Searchable text column (`Record.text`): presence bitmap + offset table +
/// utf8 blob, sliced per row at materialize time like the payload. Emitted
/// only when at least one record carries text, so text-less segments stay
/// byte-identical to the pre-text format.
const K_TEXT: u8 = 6;
/// Token postings index over `Record.text` (the FTS index): sorted token
/// dictionary + per-token ascending row ids (LEB128 delta-varint). Written
/// alongside K_TEXT; rebuilt from merged rows at every compaction by
/// construction (the encoder derives it from the records it is given).
const K_TOKENS: u8 = 7;

fn corrupt(detail: impl Into<String>) -> GirderError {
    GirderError::Corrupt {
        what: "segment",
        detail: detail.into(),
    }
}

// ---------------------------------------------------------------------------
// Zone map (per segment, stored in the manifest) — unchanged from v1.
// ---------------------------------------------------------------------------

/// Per-segment pruning metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ZoneMap {
    pub min_ts: i64,
    pub max_ts: i64,
    pub min_key: String,
    pub max_key: String,
    /// label -> distinct values (None = too many to track, can't prune).
    pub labels: BTreeMap<String, Option<BTreeSet<String>>>,
    /// numeric -> (min, max)
    pub numerics: BTreeMap<String, (f64, f64)>,
    pub count: usize,
}

impl ZoneMap {
    pub fn build<R: Borrow<Record>>(records: &[R]) -> ZoneMap {
        let mut zone = ZoneMap {
            min_ts: i64::MAX,
            max_ts: i64::MIN,
            min_key: String::new(),
            max_key: String::new(),
            labels: BTreeMap::new(),
            numerics: BTreeMap::new(),
            count: records.len(),
        };
        for (i, r) in records.iter().enumerate() {
            let record = r.borrow();
            zone.min_ts = zone.min_ts.min(record.timestamp);
            zone.max_ts = zone.max_ts.max(record.timestamp);
            if i == 0 || record.key < zone.min_key {
                zone.min_key = record.key.clone();
            }
            if i == 0 || record.key > zone.max_key {
                zone.max_key = record.key.clone();
            }
            for (k, v) in &record.labels {
                let entry = zone
                    .labels
                    .entry(k.clone())
                    .or_insert_with(|| Some(BTreeSet::new()));
                if let Some(set) = entry {
                    set.insert(v.clone());
                    if set.len() > LABEL_VALUES_CAP {
                        *entry = None; // high cardinality: stop tracking
                    }
                }
            }
            for (k, v) in &record.numerics {
                let entry = zone.numerics.entry(k.clone()).or_insert((*v, *v));
                entry.0 = entry.0.min(*v);
                entry.1 = entry.1.max(*v);
            }
        }
        zone
    }

    /// Could this segment contain a record matching `spec`? False = safe skip.
    pub fn may_match(&self, spec: &QuerySpec) -> bool {
        if let Some((lo, hi)) = spec.time {
            if self.max_ts < lo || self.min_ts > hi {
                return false;
            }
        }
        if let Some(prefix) = &spec.key_prefix {
            if !key_range_overlaps_prefix(&self.min_key, &self.max_key, prefix) {
                return false;
            }
        }
        for (name, value) in &spec.labels {
            match self.labels.get(name) {
                None => return false,
                Some(Some(values)) if !values.contains(value) => return false,
                _ => {}
            }
        }
        for (name, lo, hi) in &spec.numeric_ranges {
            match self.numerics.get(name) {
                None => return false,
                Some((min, max)) if max < lo || min > hi => return false,
                _ => {}
            }
        }
        true
    }

    /// Key-range-only face of [`ZoneMap::may_match`]: could this segment hold
    /// any key under the spec's prefix? Time/label/numeric zones deliberately
    /// NOT consulted — this is the shadow-candidate test: a newer version of
    /// a key shadows older versions whether or not IT matches the spec, so a
    /// segment may be excused from the newest-wins walk only when it cannot
    /// share a key with it (see `Girder::walk_plan`).
    pub fn may_overlap_prefix(&self, spec: &QuerySpec) -> bool {
        match &spec.key_prefix {
            None => true,
            Some(p) => key_range_overlaps_prefix(&self.min_key, &self.max_key, p),
        }
    }

    /// Do two segments' key ranges intersect (i.e. could they share a key)?
    pub fn key_range_overlaps(&self, other: &ZoneMap) -> bool {
        !(self.max_key < other.min_key || other.max_key < self.min_key)
    }
}

/// Does the key range `[min_key, max_key]` overlap the set of keys starting
/// with `prefix`? Shared by segment- and block-level pruning.
fn key_range_overlaps_prefix(min_key: &str, max_key: &str, prefix: &str) -> bool {
    if max_key < prefix {
        return false;
    }
    if !min_key.starts_with(prefix) && min_key > prefix {
        let mut upper = prefix.to_string();
        upper.push(char::MAX);
        if min_key > upper.as_str() {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Block index
// ---------------------------------------------------------------------------

/// Per-block (≈`BLOCK_ROWS` rows) zone map, embedded in the v2 footer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMeta {
    start: u32,
    end: u32, // exclusive
    min_ts: i64,
    max_ts: i64,
    first_key: String,
    last_key: String,
    /// numeric name -> (min, max) over present values (NaN excluded).
    numerics: BTreeMap<String, (f64, f64)>,
    /// small-dict label name -> bitset of dict codes present (bit c-1 for code c).
    label_bitsets: BTreeMap<String, u64>,
}

// ---------------------------------------------------------------------------
// Decoded columns (the cacheable sections; payload bytes excluded for v2)
// ---------------------------------------------------------------------------

/// A decoded label column.
pub enum LabelColumn {
    Dict {
        dict: Vec<String>,
        code_of: HashMap<String, u16>,
        codes: Vec<u16>, // 0 = absent, else dict index + 1
    },
    Plain {
        values: Vec<Option<String>>,
    },
}

/// A decoded numeric column: `present[i]` gates `values[i]` (absent → NaN).
pub struct NumericColumn {
    present: Vec<bool>,
    values: Vec<f64>,
}

/// The decoded key column: concatenated key-sorted utf8 + byte offsets.
pub struct KeysSection {
    blob: String,
    offsets: Vec<u64>, // len count+1
}

impl KeysSection {
    #[inline]
    pub(crate) fn key_at(&self, i: usize) -> &str {
        &self.blob[self.offsets[i] as usize..self.offsets[i + 1] as usize]
    }
    /// Row count (named like [`SegmentColumns::count`], not `len`, on purpose).
    pub(crate) fn count(&self) -> usize {
        self.offsets.len() - 1
    }
    /// Binary search the (sorted, unique) key column — the keys-only twin of
    /// [`SegmentColumns::find_key`], for callers that audit a segment without
    /// assembling full columns (the seal-reclaim audit).
    pub(crate) fn find(&self, key: &str) -> Option<usize> {
        let mut lo = 0usize;
        let mut hi = self.count();
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.key_at(mid).cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }
    fn bytes(&self) -> u64 {
        self.blob.len() as u64 + (self.offsets.len() as u64) * 8 + 32
    }
}

/// The payload offset table (v2). `rel[i]..rel[i+1]` are byte offsets into the
/// payload blob whose absolute file offset is `abs_base`. Note the raw payload
/// bytes are *not* held here — they are `read_exact_at` per surviving row.
pub struct PayloadIndex {
    abs_base: u64,
    rel: Vec<u64>,
}

impl PayloadIndex {
    fn bytes(&self) -> u64 {
        (self.rel.len() as u64) * 8 + 32
    }
}

/// The text offset table (K_TEXT): which rows have text, and where each
/// row's utf8 slice lives in the file. Blob bytes are read per surviving
/// row, mirroring [`PayloadIndex`] — a scan that never materializes a row
/// never reads its text. `rel` offsets are in STORED space; a v2 section
/// (D8) additionally marks which rows are individually deflated.
pub struct TextIndex {
    abs_base: u64,
    present: Vec<bool>,
    rel: Vec<u64>,
    /// v2 (D8): which rows' stored bytes are deflate-compressed (rows above
    /// [`TEXT_COMPRESS_MIN`] raw bytes at encode). `None` = legacy v1 blob.
    /// Per-ROW compression keeps every point/scattered read one bounded
    /// read (+ one inflate amortized by the row's own size) — the first
    /// D8 cut compressed 64 KiB CHUNKS and regressed scattered reads 12×
    /// (every materialized row inflated a whole chunk); do not reintroduce
    /// chunks without beating that number.
    compressed: Option<Vec<bool>>,
}

/// Raw bytes above which a row's text is stored deflated (D8, ruling
/// D8-1′). Below it, deflate's overhead eats the win on small documents
/// and the row stays raw — a small-text corpus reads byte-identically to
/// v1 by construction. Rivet's span-text documents (name + every string
/// attribute) are the KB-scale payoff this targets.
const TEXT_COMPRESS_MIN: usize = 512;

impl TextIndex {
    fn bytes(&self) -> u64 {
        let comp = self.compressed.as_ref().map_or(0, |c| c.len() as u64 + 24);
        self.present.len() as u64 + (self.rel.len() as u64) * 8 + 32 + comp
    }

    /// Decode one row's stored text bytes (inflating if the row is marked
    /// compressed) — shared by the per-row file read path and the
    /// whole-file record reader.
    fn decode_row_text(&self, i: usize, stored: Vec<u8>) -> Result<String> {
        let bytes = if self.compressed.as_ref().is_some_and(|c| c[i]) {
            use std::io::Read as _;
            let mut raw = Vec::new();
            flate2::bufread::DeflateDecoder::new(stored.as_slice())
                .read_to_end(&mut raw)
                .map_err(|_| corrupt("text row inflate failed"))?;
            raw
        } else {
            stored
        };
        String::from_utf8(bytes).map_err(|_| corrupt("text not utf8"))
    }
}

/// The token postings index of one segment (K_TOKENS, decoded): for each
/// distinct text token (sorted), the ascending row ids whose text contains
/// it. Lookup is a binary search; a `text_match` is the intersection of its
/// query tokens' postings — exact rows, no post-verification needed, because
/// the tokenizer at encode time IS the query tokenizer (`text::fts_tokens`).
pub struct TokenIndex {
    tokens: Vec<String>,
    postings: Postings,
}

/// How a token's postings are held (D7-b). The build path and legacy v1
/// bodies are fully decoded; a v2 body keeps the varint blob and decodes a
/// token's list the FIRST time a query asks for it — `OnceLock` per token,
/// so warm queries borrow the decoded list exactly like the eager form
/// (decode once per token per cache residency, not once per query; the
/// first bench cut decoded per-query and REGRESSED the warm FTS legs).
/// The whole blob was validated at section decode, so lazy decoding cannot
/// fail.
enum Postings {
    Eager(Vec<Vec<u32>>),
    Lazy {
        /// The concatenated per-token varint-delta lists, validated at
        /// decode time (ascending, in-range, spans exact).
        blob: Vec<u8>,
        /// Per token, parallel to `tokens`: (byte offset, byte len, nrows).
        spans: Vec<(u32, u32, u32)>,
        /// Decode-once cells, parallel to `tokens` (empty until queried).
        /// `bytes()` accounts the blob + directory only: the decoded lists
        /// are query working set, bounded above by ~4× the blob.
        decoded: Vec<std::sync::OnceLock<Vec<u32>>>,
    },
}

/// A LIKE `Prefix` constraint's resolution against one segment's dictionary
/// (F2, ruling 4).
enum PrefixUnion {
    /// The dictionary range exceeded the budget — constraint dropped, the
    /// caller narrows by whatever else it has (never an error).
    OverBudget,
    /// No token starts with the prefix — provably no row matches.
    Empty,
    /// The ascending, deduped union of the range's postings.
    Rows(Vec<u32>),
}

/// Ruling 4: a LIKE prefix constraint may expand to at most this many
/// dictionary tokens per segment; beyond it the constraint is dropped
/// (fall back, never error). Compile-time on purpose, like the seal
/// constants — a knob nobody should tune per-deployment.
const LIKE_PREFIX_BUDGET: usize = 64;

impl TokenIndex {
    /// Decoded postings of the token at dictionary index `i`.
    fn postings_at(&self, i: usize) -> &[u32] {
        match &self.postings {
            Postings::Eager(lists) => lists[i].as_slice(),
            Postings::Lazy {
                blob,
                spans,
                decoded,
            } => decoded[i]
                .get_or_init(|| {
                    let (off, len, nrows) = spans[i];
                    decode_posting_list(&blob[off as usize..(off + len) as usize], nrows as usize)
                })
                .as_slice(),
        }
    }

    fn postings_of(&self, token: &str) -> Option<&[u32]> {
        let i = self
            .tokens
            .binary_search_by(|t| t.as_str().cmp(token))
            .ok()?;
        Some(self.postings_at(i))
    }

    /// Union of postings over the dictionary range of tokens starting with
    /// `prefix` (tokens are sorted, so the range is contiguous), bounded by
    /// `budget` dictionary tokens. Lazy postings decode only inside an
    /// in-budget range — an over-budget range decodes NOTHING.
    fn postings_union_of_prefix(&self, prefix: &str, budget: usize) -> PrefixUnion {
        let start = self.tokens.partition_point(|t| t.as_str() < prefix);
        let mut end = start;
        while end < self.tokens.len() && self.tokens[end].starts_with(prefix) {
            end += 1;
            if end - start > budget {
                return PrefixUnion::OverBudget;
            }
        }
        if end == start {
            return PrefixUnion::Empty;
        }
        let mut rows: Vec<u32> = Vec::new();
        for i in start..end {
            rows.extend_from_slice(self.postings_at(i));
        }
        rows.sort_unstable();
        rows.dedup();
        PrefixUnion::Rows(rows)
    }

    /// Rows whose text contains ALL of `want` (AND-of-tokens), ascending.
    /// `want` empty ⇒ empty (no tokens = no match); any unknown token ⇒ empty.
    pub fn rows_matching_all(&self, want: &[String]) -> Vec<u32> {
        if want.is_empty() {
            return Vec::new();
        }
        let mut lists: Vec<&[u32]> = Vec::with_capacity(want.len());
        for t in want {
            match self.postings_of(t) {
                Some(l) => lists.push(l),
                None => return Vec::new(),
            }
        }
        lists.sort_by_key(|l| l.len());
        let (first, rest) = lists.split_first().expect("want is non-empty");
        first
            .iter()
            .copied()
            .filter(|row| rest.iter().all(|l| l.binary_search(row).is_ok()))
            .collect()
    }

    fn bytes(&self) -> u64 {
        let mut n = 64u64;
        for t in &self.tokens {
            n += t.len() as u64 + 24;
        }
        match &self.postings {
            Postings::Eager(lists) => {
                for p in lists {
                    n += (p.len() as u64) * 4 + 24;
                }
            }
            Postings::Lazy { blob, spans, .. } => {
                n += blob.len() as u64 + (spans.len() as u64) * 12 + 48;
            }
        }
        n
    }

    /// Build from records (encode path + the v1/in-memory column path).
    fn build<R: Borrow<Record>>(records: &[R]) -> TokenIndex {
        let mut map: BTreeMap<String, Vec<u32>> = BTreeMap::new();
        for (i, r) in records.iter().enumerate() {
            if let Some(text) = &r.borrow().text {
                let mut distinct = crate::text::fts_tokens(text);
                distinct.sort_unstable();
                distinct.dedup();
                for t in distinct {
                    map.entry(t).or_default().push(i as u32);
                }
            }
        }
        let mut tokens = Vec::with_capacity(map.len());
        let mut postings = Vec::with_capacity(map.len());
        for (t, p) in map {
            tokens.push(t);
            postings.push(p); // ascending by construction (row order)
        }
        TokenIndex {
            tokens,
            postings: Postings::Eager(postings),
        }
    }
}

/// Intersection of ascending row lists — the owned-list twin of
/// [`TokenIndex::rows_matching_all`]'s strategy (walk the smallest, binary
/// search the rest). Empty input ⇒ empty (callers never build a zero-list
/// intersection meaning "everything").
fn intersect_ascending(mut lists: Vec<Vec<u32>>) -> Vec<u32> {
    let Some(min_idx) = (0..lists.len()).min_by_key(|&i| lists[i].len()) else {
        return Vec::new();
    };
    let first = lists.swap_remove(min_idx);
    first
        .into_iter()
        .filter(|row| lists.iter().all(|l| l.binary_search(row).is_ok()))
        .collect()
}

/// Decode one validated varint-delta posting list (the lazy read path). The
/// blob was fully validated at section decode, so this cannot fail — the
/// asserts are debug belts, not error paths.
fn decode_posting_list(span: &[u8], nrows: usize) -> Vec<u32> {
    let mut c = Cur::new(span);
    let mut rows = Vec::with_capacity(nrows);
    let mut prev = 0u32;
    for i in 0..nrows {
        let delta = varint_read(&mut c).expect("validated at decode");
        let row = if i == 0 { delta } else { prev + delta };
        rows.push(row);
        prev = row;
    }
    debug_assert!(c.at_end(), "validated span length");
    rows
}

/// How row texts are sourced when materializing a `Record`. `None` on the
/// containing [`SegmentColumns`] means the segment carries no text at all
/// (pre-text file, or no record had text) — honest absence, not empty.
enum Texts {
    /// v2: slice `rel[i]..rel[i+1]` at `abs_base` when `present[i]`.
    File(Arc<TextIndex>),
    /// v1 compat / in-memory columns.
    Mem(Arc<Vec<Option<String>>>),
}

/// How row payloads are sourced when materializing a `Record`.
enum Payloads {
    /// v2: raw bytes live in the file; slice `rel[i]..rel[i+1]` at `abs_base`.
    File(Arc<PayloadIndex>),
    /// v1 compat: owned payload bytes in memory.
    Mem(Arc<Vec<Vec<u8>>>),
}

/// A segment decoded into typed columns. Filters run over these; payloads are
/// sliced only for surviving rows. Each column is an `Arc` shared with the
/// section cache (WS4), so assembling a view for a scan is cheap and the same
/// decoded bytes are never held twice.
pub struct SegmentColumns {
    count: usize,
    keys: Arc<KeysSection>,
    timestamps: Arc<Vec<i64>>,
    labels: BTreeMap<String, Arc<LabelColumn>>,
    numerics: BTreeMap<String, Arc<NumericColumn>>,
    blocks: Arc<Vec<BlockMeta>>,
    payloads: Payloads,
    texts: Option<Texts>,
    tokens: Option<Arc<TokenIndex>>,
}

impl SegmentColumns {
    /// Row count. Production reads now go through `matching_rows` /
    /// `find_key`; retained for the unit tests below.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn count(&self) -> usize {
        self.count
    }

    #[inline]
    pub fn key_at(&self, i: usize) -> &str {
        self.keys.key_at(i)
    }

    /// Timestamp of row `i` (for order-by / early-termination bounds).
    #[inline]
    pub fn timestamp_at(&self, i: usize) -> i64 {
        self.timestamps[i]
    }

    /// Value of numeric `name` at row `i`, or `None` if absent (used as the
    /// sort key for numeric `order_by`). No payload touch.
    #[inline]
    pub fn numeric_at(&self, name: &str, i: usize) -> Option<f64> {
        match self.numerics.get(name) {
            Some(nc) if nc.present[i] => Some(nc.values[i]),
            _ => None,
        }
    }

    /// True if materializing a row needs the segment file open (v2 payloads).
    pub fn payload_needs_file(&self) -> bool {
        matches!(self.payloads, Payloads::File(_))
    }

    /// Value of label `name` at row `i`, or `None` if absent. No payload touch.
    pub fn label_value_at(&self, name: &str, i: usize) -> Option<&str> {
        match self.labels.get(name)?.as_ref() {
            LabelColumn::Dict { dict, codes, .. } => match codes[i] {
                0 => None,
                c => Some(dict[(c - 1) as usize].as_str()),
            },
            LabelColumn::Plain { values } => values[i].as_deref(),
        }
    }

    /// Is row `i` a tombstone (delete marker)? The column-side face of
    /// [`crate::Record::is_tombstone`] — reads exclude such rows from
    /// results while their keys keep shadowing older versions.
    #[inline]
    pub fn is_tombstone_at(&self, i: usize) -> bool {
        self.label_value_at(crate::record::TOMBSTONE_LABEL, i) == Some("1")
    }

    /// Binary search the (sorted) key column.
    pub fn find_key(&self, key: &str) -> Option<usize> {
        let mut lo = 0usize;
        let mut hi = self.count;
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.key_at(mid).cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    /// Estimated in-memory footprint of the whole column set. Used only for v1
    /// (whole-bundle) cache accounting; v2 accounts each `Section` separately.
    pub fn heap_bytes(&self) -> u64 {
        let mut n = self.keys.bytes() + (self.timestamps.len() as u64) * 8;
        for col in self.labels.values() {
            n += label_bytes(col);
        }
        for col in self.numerics.values() {
            n += numeric_bytes(col);
        }
        match &self.payloads {
            Payloads::File(pi) => n += pi.bytes(),
            Payloads::Mem(v) => {
                for p in v.iter() {
                    n += p.len() as u64 + 24;
                }
            }
        }
        n + 256
    }

    /// Row indices matching `spec` — the segment-side predicate oracle: every
    /// scan/count walk trusts these rows as exact matches. Uses the block
    /// index to skip whole blocks. Returns empty if any required
    /// label/numeric column is absent from the segment (mirrors
    /// `QuerySpec::matches` semantics: absent ⇒ no match).
    ///
    /// `file` must be `Some` (the open segment file) when the spec carries
    /// `text_like` and this segment's texts live in the file (v2) — LIKE is
    /// verified against the raw text of every candidate, so this is the one
    /// row-selection path that can touch the file (tallied into
    /// `bytes_read`). Specs without `text_like` never read it.
    pub fn matching_rows(
        &self,
        spec: &QuerySpec,
        file: Option<&File>,
        bytes_read: &AtomicU64,
    ) -> Result<Vec<u32>> {
        let Some(pattern) = &spec.text_like else {
            return Ok(self.rows_by_columns(spec));
        };
        if self.texts.is_none() {
            return Ok(Vec::new()); // no record here has text — LIKE can't match
        }
        let mut out = Vec::new();
        for row in self.rows_by_columns(spec) {
            if let Some(text) = self.text_at(row as usize, file, bytes_read)? {
                if crate::text::like_match(pattern, &text) {
                    out.push(row);
                }
            }
        }
        Ok(out)
    }

    /// The column-resolvable predicates (everything except `text_like`):
    /// fields via the block walk, `text_match` via the token postings index.
    fn rows_by_columns(&self, spec: &QuerySpec) -> Vec<u32> {
        // Resolve label predicates against the columns.
        enum LReq<'a> {
            Code(&'a [u16], u16),
            Plain(&'a [Option<String>], &'a str),
        }
        let mut lreqs: Vec<LReq> = Vec::with_capacity(spec.labels.len());
        // (name, code) pairs usable for per-block bitset pruning.
        let mut dict_prune: Vec<(&str, u16)> = Vec::new();
        for (name, val) in &spec.labels {
            match self.labels.get(name) {
                None => return Vec::new(),
                Some(col) => match col.as_ref() {
                    LabelColumn::Dict { code_of, codes, .. } => match code_of.get(val) {
                        Some(&c) => {
                            lreqs.push(LReq::Code(codes.as_slice(), c));
                            if c <= BLOCK_BITSET_CODES {
                                dict_prune.push((name.as_str(), c));
                            }
                        }
                        None => return Vec::new(),
                    },
                    LabelColumn::Plain { values } => {
                        lreqs.push(LReq::Plain(values.as_slice(), val.as_str()))
                    }
                },
            }
        }
        let mut nreqs: Vec<(&NumericColumn, f64, f64)> =
            Vec::with_capacity(spec.numeric_ranges.len());
        for (name, lo, hi) in &spec.numeric_ranges {
            match self.numerics.get(name) {
                None => return Vec::new(),
                Some(nc) => nreqs.push((nc.as_ref(), *lo, *hi)),
            }
        }
        let prefix = spec.key_prefix.as_deref();
        let time = spec.time;

        // Text predicates: resolve through the token postings index. The
        // candidate rows are EXACT for `text_match` (encode-time tokenizer
        // == query tokenizer) and a sound SUPERSET for `text_like` (F2: the
        // pattern's token/prefix constraints narrow; the caller's verifier
        // supplies exactness), so only the other predicates need checking
        // per candidate. D7-a: candidates first merge against the blocks
        // surviving the OTHER predicates' zone tests (both sides are
        // ascending, so it is one linear walk) — a COMMON token composed
        // with a selective time/label predicate no longer pays per-row
        // checks inside dead blocks.
        let like_lists = spec
            .text_like
            .as_deref()
            .and_then(|p| self.like_candidate_lists(p));
        if spec.text_match.is_some() || like_lists.is_some() {
            let Some(idx) = &self.tokens else {
                return Vec::new(); // segment has no text at all
            };
            let cand_rows: Vec<u32> = match (&spec.text_match, like_lists) {
                // text_match alone: the historical path, byte-identical.
                (Some(q), None) => idx.rows_matching_all(&crate::text::fts_tokens(q)),
                // like constraints, optionally AND text_match tokens: one
                // generalized intersection over all the sorted lists.
                (tm, Some(mut lists)) => {
                    if let Some(q) = tm {
                        let want = crate::text::fts_tokens(q);
                        if want.is_empty() {
                            return Vec::new(); // no tokens = no match
                        }
                        for t in &want {
                            match idx.postings_of(t) {
                                Some(l) => lists.push(l.to_vec()),
                                None => return Vec::new(),
                            }
                        }
                    }
                    intersect_ascending(lists)
                }
                (None, None) => unreachable!("guarded by the arm condition"),
            };
            let live_blocks: Vec<&BlockMeta> = self
                .blocks
                .iter()
                .filter(|b| self.block_may_match(b, spec, &dict_prune))
                .collect();
            let mut block_iter = live_blocks.iter().peekable();
            let mut out: Vec<u32> = Vec::new();
            'cand: for row in cand_rows {
                // Advance to the block that could hold `row`; a candidate
                // before the next live block is inside a pruned one.
                while let Some(b) = block_iter.peek() {
                    if b.end <= row {
                        block_iter.next();
                    } else {
                        break;
                    }
                }
                match block_iter.peek() {
                    None => break 'cand, // no live blocks remain
                    Some(b) if b.start > row => continue 'cand,
                    Some(_) => {}
                }
                let i = row as usize;
                if let Some((lo, hi)) = time {
                    let t = self.timestamps[i];
                    if t < lo || t > hi {
                        continue;
                    }
                }
                if let Some(p) = prefix {
                    if !self.key_at(i).starts_with(p) {
                        continue;
                    }
                }
                for lr in &lreqs {
                    let hit = match lr {
                        LReq::Code(codes, c) => codes[i] == *c,
                        LReq::Plain(values, want) => values[i].as_deref() == Some(*want),
                    };
                    if !hit {
                        continue 'cand;
                    }
                }
                for (nc, lo, hi) in &nreqs {
                    if !(nc.present[i] && nc.values[i] >= *lo && nc.values[i] <= *hi) {
                        continue 'cand;
                    }
                }
                out.push(row);
            }
            return out;
        }

        let mut out: Vec<u32> = Vec::new();
        for block in self.blocks.iter() {
            if !self.block_may_match(block, spec, &dict_prune) {
                continue;
            }
            for i in block.start as usize..block.end as usize {
                if let Some((lo, hi)) = time {
                    let t = self.timestamps[i];
                    if t < lo || t > hi {
                        continue;
                    }
                }
                if let Some(p) = prefix {
                    if !self.key_at(i).starts_with(p) {
                        continue;
                    }
                }
                let mut ok = true;
                for lr in &lreqs {
                    let hit = match lr {
                        LReq::Code(codes, c) => codes[i] == *c,
                        LReq::Plain(values, want) => values[i].as_deref() == Some(*want),
                    };
                    if !hit {
                        ok = false;
                        break;
                    }
                }
                if ok {
                    for (nc, lo, hi) in &nreqs {
                        if !(nc.present[i] && nc.values[i] >= *lo && nc.values[i] <= *hi) {
                            ok = false;
                            break;
                        }
                    }
                }
                if ok {
                    out.push(i as u32);
                }
            }
        }
        out
    }

    /// Resolve a LIKE pattern's sound token constraints (F2, rulings 2–4)
    /// against this segment's dictionary into candidate row lists:
    ///
    /// - `None` — nothing usable (unanalyzable pattern, every prefix over
    ///   budget, or no token index): the caller falls back to the full walk,
    ///   which is exact via the verifier. Never an error.
    /// - `Some(lists)` — intersect them; a provably-impossible constraint
    ///   (required token absent, prefix range empty) is an empty list, which
    ///   empties the intersection.
    fn like_candidate_lists(&self, pattern: &str) -> Option<Vec<Vec<u32>>> {
        let idx = self.tokens.as_ref()?;
        let constraints = crate::text::like_constraints(pattern);
        if constraints.is_empty() {
            return None;
        }
        let mut lists: Vec<Vec<u32>> = Vec::with_capacity(constraints.len());
        for c in &constraints {
            match c {
                crate::text::LikeConstraint::Token(t) => match idx.postings_of(t) {
                    Some(l) => lists.push(l.to_vec()),
                    // A required token the segment lacks: no row can match.
                    None => return Some(vec![Vec::new()]),
                },
                crate::text::LikeConstraint::Prefix(p) => {
                    match idx.postings_union_of_prefix(p, LIKE_PREFIX_BUDGET) {
                        PrefixUnion::OverBudget => {} // dropped (ruling 4)
                        // No token starts with it: no row can match.
                        PrefixUnion::Empty => return Some(vec![Vec::new()]),
                        PrefixUnion::Rows(rows) => lists.push(rows),
                    }
                }
            }
        }
        // Every constraint was budget-dropped → nothing narrows.
        (!lists.is_empty()).then_some(lists)
    }

    fn block_may_match(&self, b: &BlockMeta, spec: &QuerySpec, dict_prune: &[(&str, u16)]) -> bool {
        if let Some((lo, hi)) = spec.time {
            if b.max_ts < lo || b.min_ts > hi {
                return false;
            }
        }
        if let Some(prefix) = &spec.key_prefix {
            if !key_range_overlaps_prefix(&b.first_key, &b.last_key, prefix) {
                return false;
            }
        }
        for (name, lo, hi) in &spec.numeric_ranges {
            match b.numerics.get(name) {
                None => return false,
                Some((min, max)) if max < lo || min > hi => return false,
                _ => {}
            }
        }
        for (name, code) in dict_prune {
            if let Some(bits) = b.label_bitsets.get(*name) {
                if bits & (1u64 << (code - 1)) == 0 {
                    return false;
                }
            }
        }
        true
    }

    /// Reconstruct a full `Record` for row `i`. `file` must be `Some` (the open
    /// segment file) when `payload_needs_file()` is true. Any payload read is
    /// tallied into `bytes_read` (WS4 per-query I/O accounting).
    /// `want_text: false` is the `QuerySpec::omit_text` projection: the text
    /// column is not touched (no read, no inflate) and the record carries
    /// `text: None` by explicit caller contract. Write paths and `get()`
    /// always pass `true`.
    pub fn materialize(
        &self,
        i: usize,
        file: Option<&File>,
        bytes_read: &AtomicU64,
        want_text: bool,
    ) -> Result<Record> {
        let payload = match &self.payloads {
            Payloads::Mem(v) => v[i].clone(),
            Payloads::File(pi) => {
                let f = file.ok_or_else(|| corrupt("payload file handle missing"))?;
                let off = pi.abs_base + pi.rel[i];
                let len = (pi.rel[i + 1] - pi.rel[i]) as usize;
                read_at(f, off, len, bytes_read)?
            }
        };
        let text = if want_text {
            self.text_at(i, file, bytes_read)?
        } else {
            None
        };
        Ok(self.build_record(i, payload, text))
    }

    /// Row `i`'s text (`None` = record has no text — honest absence). The
    /// single text accessor: `materialize` and the `text_like` verifier both
    /// resolve through it. `file` must be `Some` for file-sourced texts (v2).
    fn text_at(
        &self,
        i: usize,
        file: Option<&File>,
        bytes_read: &AtomicU64,
    ) -> Result<Option<String>> {
        match &self.texts {
            None => Ok(None),
            Some(Texts::Mem(v)) => Ok(v[i].clone()),
            Some(Texts::File(ti)) => {
                if !ti.present[i] {
                    return Ok(None);
                }
                let raw_start = ti.rel[i];
                let raw_len = (ti.rel[i + 1] - raw_start) as usize;
                if raw_len == 0 {
                    return Ok(Some(String::new())); // present-but-empty text
                }
                let f = file.ok_or_else(|| corrupt("text file handle missing"))?;
                // One bounded read either way; v2 rows marked compressed
                // additionally inflate their own bytes (D8, per-row).
                let stored = read_at(f, ti.abs_base + raw_start, raw_len, bytes_read)?;
                Ok(Some(ti.decode_row_text(i, stored)?))
            }
        }
    }

    fn build_record(&self, i: usize, payload: Vec<u8>, text: Option<String>) -> Record {
        let mut labels = BTreeMap::new();
        for (name, col) in &self.labels {
            match col.as_ref() {
                LabelColumn::Dict { dict, codes, .. } => {
                    let c = codes[i];
                    if c != 0 {
                        labels.insert(name.clone(), dict[(c - 1) as usize].clone());
                    }
                }
                LabelColumn::Plain { values } => {
                    if let Some(v) = &values[i] {
                        labels.insert(name.clone(), v.clone());
                    }
                }
            }
        }
        let mut numerics = BTreeMap::new();
        for (name, nc) in &self.numerics {
            if nc.present[i] {
                numerics.insert(name.clone(), nc.values[i]);
            }
        }
        Record {
            key: self.key_at(i).to_string(),
            timestamp: self.timestamps[i],
            labels,
            numerics,
            payload,
            text,
        }
    }

    /// v1-compat / in-memory constructor: build columns from decoded records.
    pub fn from_records(records: Vec<Record>) -> SegmentColumns {
        let count = records.len();
        let mut blob = String::new();
        let mut offsets = Vec::with_capacity(count + 1);
        offsets.push(0u64);
        for r in &records {
            blob.push_str(&r.key);
            offsets.push(blob.len() as u64);
        }
        let timestamps: Vec<i64> = records.iter().map(|r| r.timestamp).collect();

        let plans = plan_labels(&records);
        let mut labels = BTreeMap::new();
        for plan in &plans {
            let col = match &plan.mode {
                LabelMode::Dict { dict, codes } => {
                    let code_of = dict
                        .iter()
                        .enumerate()
                        .map(|(i, s)| (s.clone(), (i + 1) as u16))
                        .collect();
                    LabelColumn::Dict {
                        dict: dict.clone(),
                        code_of,
                        codes: codes.clone(),
                    }
                }
                LabelMode::Plain => {
                    let values = records
                        .iter()
                        .map(|r| r.labels.get(&plan.name).cloned())
                        .collect();
                    LabelColumn::Plain { values }
                }
            };
            labels.insert(plan.name.clone(), Arc::new(col));
        }

        let mut numerics = BTreeMap::new();
        for name in numeric_names(&records) {
            let mut present = vec![false; count];
            let mut values = vec![f64::NAN; count];
            for (i, r) in records.iter().enumerate() {
                if let Some(v) = r.numerics.get(&name) {
                    present[i] = true;
                    values[i] = *v;
                }
            }
            numerics.insert(name, Arc::new(NumericColumn { present, values }));
        }

        let blocks = build_blocks(&records, &plans);
        let any_text = records.iter().any(|r| r.text.is_some());
        let tokens = any_text.then(|| Arc::new(TokenIndex::build(&records)));
        let mut texts_vec: Vec<Option<String>> = Vec::with_capacity(count);
        let payloads = Payloads::Mem(Arc::new(
            records
                .into_iter()
                .map(|mut r| {
                    texts_vec.push(r.text.take());
                    r.payload
                })
                .collect(),
        ));
        let texts = any_text.then(|| Texts::Mem(Arc::new(texts_vec)));
        SegmentColumns {
            count,
            keys: Arc::new(KeysSection { blob, offsets }),
            timestamps: Arc::new(timestamps),
            labels,
            numerics,
            blocks: Arc::new(blocks),
            payloads,
            texts,
            tokens,
        }
    }

    /// Assemble a v2 column view from already-decoded (cached) section `Arc`s.
    /// Used by the engine's section loader once every needed section is in hand.
    #[allow(clippy::too_many_arguments)]
    pub fn assemble(
        count: usize,
        keys: Arc<KeysSection>,
        timestamps: Arc<Vec<i64>>,
        labels: BTreeMap<String, Arc<LabelColumn>>,
        numerics: BTreeMap<String, Arc<NumericColumn>>,
        blocks: Arc<Vec<BlockMeta>>,
        payload_index: Arc<PayloadIndex>,
        text_index: Option<Arc<TextIndex>>,
        tokens: Option<Arc<TokenIndex>>,
    ) -> SegmentColumns {
        SegmentColumns {
            count,
            keys,
            timestamps,
            labels,
            numerics,
            blocks,
            payloads: Payloads::File(payload_index),
            texts: text_index.map(Texts::File),
            tokens,
        }
    }
}

fn label_bytes(col: &LabelColumn) -> u64 {
    match col {
        LabelColumn::Dict { dict, codes, .. } => {
            let mut n = (codes.len() as u64) * 2;
            for s in dict {
                n += s.len() as u64 + 24;
            }
            n
        }
        LabelColumn::Plain { values } => {
            let mut n = 0;
            for v in values {
                n += v.as_ref().map(|s| s.len() as u64 + 24).unwrap_or(8);
            }
            n
        }
    }
}

fn numeric_bytes(col: &NumericColumn) -> u64 {
    col.present.len() as u64 + (col.values.len() as u64) * 8 + 32
}

fn block_bytes(blocks: &[BlockMeta]) -> u64 {
    let mut n = 64;
    for b in blocks {
        n += 96 + b.first_key.len() as u64 + b.last_key.len() as u64;
        n += (b.numerics.len() as u64) * 40;
        n += (b.label_bitsets.len() as u64) * 40;
    }
    n
}

// ---------------------------------------------------------------------------
// Section cache identity (WS4)
// ---------------------------------------------------------------------------

/// Identifies one cacheable section of a segment. The cache key is
/// `(segment_id, SectionId)` — column sections (MBs) are cached individually,
/// so a tiny `cache_bytes` still evicts predictably and RSS stays bounded.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum SectionId {
    /// The parsed footer directory (or version marker).
    Dir,
    Keys,
    Timestamps,
    Blocks,
    PayloadIndex,
    Label(String),
    Numeric(String),
    /// The text offset table (K_TEXT header; blob bytes are never cached).
    TextIndex,
    /// The decoded token postings index (K_TOKENS).
    Tokens,
    /// A whole v1 (legacy) segment decoded in memory — v1 is not
    /// section-structured, so it is cached as one entry.
    V1Whole,
}

/// A decoded, cacheable section. Each variant wraps an `Arc` so a cache hit is
/// a pointer clone shared with the assembled [`SegmentColumns`] view.
#[derive(Clone)]
pub enum Section {
    Dir(Arc<SegDir>),
    Keys(Arc<KeysSection>),
    Timestamps(Arc<Vec<i64>>),
    Blocks(Arc<Vec<BlockMeta>>),
    PayloadIndex(Arc<PayloadIndex>),
    Label(Arc<LabelColumn>),
    Numeric(Arc<NumericColumn>),
    TextIndex(Arc<TextIndex>),
    Tokens(Arc<TokenIndex>),
    V1Whole(Arc<SegmentColumns>),
}

impl Section {
    /// Estimated in-memory footprint, the single source of truth for cache
    /// accounting (sized once, on insert).
    pub fn bytes(&self) -> u64 {
        match self {
            Section::Dir(d) => d.bytes(),
            Section::Keys(k) => k.bytes(),
            Section::Timestamps(t) => (t.len() as u64) * 8 + 32,
            Section::Blocks(b) => block_bytes(b),
            Section::PayloadIndex(p) => p.bytes(),
            Section::Label(l) => label_bytes(l),
            Section::Numeric(n) => numeric_bytes(n),
            Section::TextIndex(t) => t.bytes(),
            Section::Tokens(t) => t.bytes(),
            Section::V1Whole(c) => c.heap_bytes(),
        }
    }
}

// ---------------------------------------------------------------------------
// Encoding (v2)
// ---------------------------------------------------------------------------

enum LabelMode {
    Dict { dict: Vec<String>, codes: Vec<u16> },
    Plain,
}

struct LabelPlan {
    name: String,
    mode: LabelMode,
}

fn label_names<R: Borrow<Record>>(records: &[R]) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for r in records {
        for k in r.borrow().labels.keys() {
            names.insert(k.clone());
        }
    }
    names
}

fn numeric_names<R: Borrow<Record>>(records: &[R]) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for r in records {
        for k in r.borrow().numerics.keys() {
            names.insert(k.clone());
        }
    }
    names
}

fn plan_labels<R: Borrow<Record>>(records: &[R]) -> Vec<LabelPlan> {
    let mut plans = Vec::new();
    for name in label_names(records) {
        let mut distinct: BTreeSet<&str> = BTreeSet::new();
        for r in records {
            if let Some(v) = r.borrow().labels.get(&name) {
                distinct.insert(v.as_str());
            }
        }
        if distinct.len() <= DICT_MAX {
            let dict: Vec<String> = distinct.iter().map(|s| s.to_string()).collect();
            let index: HashMap<&str, u16> = dict
                .iter()
                .enumerate()
                .map(|(i, s)| (s.as_str(), (i + 1) as u16))
                .collect();
            let codes: Vec<u16> = records
                .iter()
                .map(|r| {
                    r.borrow()
                        .labels
                        .get(&name)
                        .map(|v| index[v.as_str()])
                        .unwrap_or(0)
                })
                .collect();
            plans.push(LabelPlan {
                name,
                mode: LabelMode::Dict { dict, codes },
            });
        } else {
            plans.push(LabelPlan {
                name,
                mode: LabelMode::Plain,
            });
        }
    }
    plans
}

fn build_blocks<R: Borrow<Record>>(records: &[R], plans: &[LabelPlan]) -> Vec<BlockMeta> {
    let count = records.len();
    let mut blocks = Vec::new();
    let mut s = 0;
    while s < count {
        let e = (s + BLOCK_ROWS).min(count);
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        let mut numerics: BTreeMap<String, (f64, f64)> = BTreeMap::new();
        for r in &records[s..e] {
            let r = r.borrow();
            min_ts = min_ts.min(r.timestamp);
            max_ts = max_ts.max(r.timestamp);
            for (k, v) in &r.numerics {
                let ent = numerics
                    .entry(k.clone())
                    .or_insert((f64::INFINITY, f64::NEG_INFINITY));
                // f64::min/max ignore NaN → NaN excluded from bounds. A block
                // whose only values are NaN keeps (INF, -INF) → prunes (NaN
                // never matches a range).
                ent.0 = ent.0.min(*v);
                ent.1 = ent.1.max(*v);
            }
        }
        let mut label_bitsets: BTreeMap<String, u64> = BTreeMap::new();
        for plan in plans {
            if let LabelMode::Dict { dict, codes } = &plan.mode {
                if dict.len() <= BLOCK_BITSET_CODES as usize {
                    let mut bits = 0u64;
                    for &c in &codes[s..e] {
                        if (1..=BLOCK_BITSET_CODES).contains(&c) {
                            bits |= 1u64 << (c - 1);
                        }
                    }
                    label_bitsets.insert(plan.name.clone(), bits);
                }
            }
        }
        blocks.push(BlockMeta {
            start: s as u32,
            end: e as u32,
            min_ts,
            max_ts,
            first_key: records[s].borrow().key.clone(),
            last_key: records[e - 1].borrow().key.clone(),
            numerics,
            label_bitsets,
        });
        s = e;
    }
    blocks
}

#[derive(Serialize, Deserialize)]
struct SectionEntry {
    kind: u8,
    name: String,
    offset: u64, // absolute file offset of the section's crc word
    len: u64,    // body length (bytes after the crc word)
    crc: u32,
}

#[derive(Serialize, Deserialize)]
struct SectionDir {
    count: u64,
    entries: Vec<SectionEntry>,
}

fn push_section(out: &mut Vec<u8>, dir: &mut Vec<SectionEntry>, kind: u8, name: &str, body: &[u8]) {
    let crc = crc32fast::hash(body);
    let offset = out.len() as u64;
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(body);
    dir.push(SectionEntry {
        kind,
        name: name.to_string(),
        offset,
        len: body.len() as u64,
        crc,
    });
}

fn encode_strings(items: &[&[u8]]) -> Vec<u8> {
    let count = items.len();
    let total: usize = items.iter().map(|s| s.len()).sum();
    let mut body = Vec::with_capacity(4 + 8 * (count + 1) + total);
    body.extend_from_slice(&(count as u32).to_le_bytes());
    let mut off = 0u64;
    body.extend_from_slice(&off.to_le_bytes());
    for it in items {
        off += it.len() as u64;
        body.extend_from_slice(&off.to_le_bytes());
    }
    for it in items {
        body.extend_from_slice(it);
    }
    body
}

fn encode_label_column<R: Borrow<Record>>(records: &[R], plan: &LabelPlan) -> Vec<u8> {
    match &plan.mode {
        LabelMode::Dict { dict, codes } => {
            let mut body = Vec::new();
            body.push(0u8); // mode: dict
            body.extend_from_slice(&(dict.len() as u32).to_le_bytes());
            for v in dict {
                body.extend_from_slice(&(v.len() as u32).to_le_bytes());
                body.extend_from_slice(v.as_bytes());
            }
            for c in codes {
                body.extend_from_slice(&c.to_le_bytes());
            }
            body
        }
        LabelMode::Plain => {
            let count = records.len();
            let mut body = Vec::new();
            body.push(1u8); // mode: plain
            let mut presence = vec![0u8; count.div_ceil(8)];
            let mut present_vals: Vec<&[u8]> = Vec::new();
            for (i, r) in records.iter().enumerate() {
                if let Some(v) = r.borrow().labels.get(&plan.name) {
                    presence[i / 8] |= 1 << (i % 8);
                    present_vals.push(v.as_bytes());
                }
            }
            body.extend_from_slice(&presence);
            body.extend_from_slice(&encode_strings(&present_vals));
            body
        }
    }
}

fn encode_numeric_column<R: Borrow<Record>>(records: &[R], name: &str) -> Vec<u8> {
    let count = records.len();
    let mut presence = vec![0u8; count.div_ceil(8)];
    let mut vals: Vec<f64> = Vec::new();
    for (i, r) in records.iter().enumerate() {
        if let Some(v) = r.borrow().numerics.get(name) {
            presence[i / 8] |= 1 << (i % 8);
            vals.push(*v);
        }
    }
    let mut body = Vec::with_capacity(presence.len() + vals.len() * 8);
    body.extend_from_slice(&presence);
    for v in &vals {
        body.extend_from_slice(&v.to_le_bytes());
    }
    body
}

fn encode_payloads<R: Borrow<Record>>(records: &[R]) -> Vec<u8> {
    let count = records.len();
    let total: usize = records.iter().map(|r| r.borrow().payload.len()).sum();
    let mut body = Vec::with_capacity(8 * (count + 1) + total);
    let mut off = 0u64;
    body.extend_from_slice(&off.to_le_bytes());
    for r in records {
        off += r.borrow().payload.len() as u64;
        body.extend_from_slice(&off.to_le_bytes());
    }
    for r in records {
        body.extend_from_slice(&r.borrow().payload);
    }
    body
}

/// The K_TEXT poison body (D8): written at the OLD directory key
/// `(K_TEXT, "")` so a pre-D8 binary reading a v2 store fails LOUDLY —
/// the v1 decoder requires at least `ceil(count/8) + 8·(count+1)` bytes
/// (≥ 17 for any non-empty segment), so 2 bytes are a guaranteed "text
/// section shorter than header" corrupt error — instead of silently
/// reading every text as absent. The real v2 body lives under
/// [`TEXT_V2_NAME`]; rationale in `docs/COMPAT.md`.
const TEXT_POISON: &[u8] = b"!2";
/// Directory name of the v2 (per-row-compressed) K_TEXT body.
const TEXT_V2_NAME: &str = "z2";
/// In-body version word of the "z2" body (future-proofing: the NAME gates
/// v1-vs-v2, the word gates v2-vs-later; unknown future = loud Corrupt).
const TEXT_V2_VERSION: u32 = 2;

/// Encode the K_TEXT v2 section (D8, ruling D8-1′): `[u32 version=2]`, the
/// presence bitmap, the COMPRESSED-rows bitmap, the zero-based STORED-space
/// offset table (count+1 over ALL rows), then the stored blob — each row's
/// text raw when < [`TEXT_COMPRESS_MIN`] bytes, individually deflated
/// otherwise. Per-row storage keeps every point read one bounded
/// `read_exact_at` (+ an inflate amortized by the row's own size).
fn encode_texts<R: Borrow<Record>>(records: &[R]) -> Vec<u8> {
    use std::io::Write as _;
    let count = records.len();
    let bitmap_len = count.div_ceil(8);
    let mut presence = vec![0u8; bitmap_len];
    let mut compressed = vec![0u8; bitmap_len];
    let mut blob = Vec::new();
    let mut offsets = Vec::with_capacity(count + 1);
    offsets.push(0u64);
    for (i, r) in records.iter().enumerate() {
        if let Some(t) = &r.borrow().text {
            presence[i / 8] |= 1 << (i % 8);
            if t.len() >= TEXT_COMPRESS_MIN {
                let mut enc =
                    flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
                enc.write_all(t.as_bytes()).expect("in-memory write");
                let comp = enc.finish().expect("in-memory finish");
                // Store compressed only when it actually shrinks — an
                // incompressible row stays raw (honesty at the byte level).
                if comp.len() < t.len() {
                    compressed[i / 8] |= 1 << (i % 8);
                    blob.extend_from_slice(&comp);
                } else {
                    blob.extend_from_slice(t.as_bytes());
                }
            } else {
                blob.extend_from_slice(t.as_bytes());
            }
        }
        offsets.push(blob.len() as u64);
    }
    let mut body = Vec::with_capacity(4 + bitmap_len * 2 + 8 * (count + 1) + blob.len());
    body.extend_from_slice(&TEXT_V2_VERSION.to_le_bytes());
    body.extend_from_slice(&presence);
    body.extend_from_slice(&compressed);
    for o in &offsets {
        body.extend_from_slice(&o.to_le_bytes());
    }
    body.extend_from_slice(&blob);
    body
}

fn varint_push(out: &mut Vec<u8>, mut v: u32) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

fn varint_read(c: &mut Cur) -> Result<u32> {
    let mut v: u32 = 0;
    let mut shift = 0u32;
    loop {
        let byte = c.u8()?;
        v |= ((byte & 0x7f) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok(v);
        }
        shift += 7;
        if shift >= 32 {
            return Err(corrupt("varint too long"));
        }
    }
}

/// The K_TOKENS v2 body sentinel (D7-b). A v1 body starts with `ntokens`,
/// which can never be `u32::MAX` (4 billion distinct tokens exceeds every
/// section bound), so the sentinel is unambiguous version detection —
/// headerless bodies stay readable as v1 forever, an unknown FUTURE version
/// is a LOUD corrupt error (never a silent "segment has no text index":
/// silently-empty text matches would be data loss dressed as no-results).
const TOKENS_V2_SENTINEL: u32 = u32::MAX;
const TOKENS_VERSION: u32 = 2;

/// Encode the K_TOKENS section (v2, D7-b):
/// `[u32 SENTINEL][u32 version][u32 ntokens]`, the token DIRECTORY — per
/// token `[u32 len][utf8][u32 nrows][u32 off][u32 len]` (offsets into the
/// postings blob) — then `[u32 blob_len][blob]` where each token's span is
/// its LEB128 delta list (first row as-is, then gaps). Splitting directory
/// from blob is what lets a reader resolve query tokens without decoding
/// anyone else's postings.
fn encode_tokens(idx: &TokenIndex) -> Vec<u8> {
    let Postings::Eager(lists) = &idx.postings else {
        unreachable!("encode always runs on a freshly built (eager) index");
    };
    let mut blob = Vec::new();
    let mut spans: Vec<(u32, u32)> = Vec::with_capacity(lists.len());
    for rows in lists {
        let start = blob.len();
        let mut prev = 0u32;
        for (i, &row) in rows.iter().enumerate() {
            let delta = if i == 0 { row } else { row - prev };
            varint_push(&mut blob, delta);
            prev = row;
        }
        spans.push((start as u32, (blob.len() - start) as u32));
    }
    let mut body = Vec::new();
    body.extend_from_slice(&TOKENS_V2_SENTINEL.to_le_bytes());
    body.extend_from_slice(&TOKENS_VERSION.to_le_bytes());
    body.extend_from_slice(&(idx.tokens.len() as u32).to_le_bytes());
    for ((t, rows), (off, len)) in idx.tokens.iter().zip(lists).zip(&spans) {
        body.extend_from_slice(&(t.len() as u32).to_le_bytes());
        body.extend_from_slice(t.as_bytes());
        body.extend_from_slice(&(rows.len() as u32).to_le_bytes());
        body.extend_from_slice(&off.to_le_bytes());
        body.extend_from_slice(&len.to_le_bytes());
    }
    body.extend_from_slice(&(blob.len() as u32).to_le_bytes());
    body.extend_from_slice(&blob);
    body
}

/// Decode a K_TOKENS body. Validates tokens sorted-ascending and row ids
/// strictly ascending within `count`.
fn decode_tokens(body: &[u8], count: usize) -> Result<TokenIndex> {
    let mut c = Cur::new(body);
    let first = c.u32()?;
    if first == TOKENS_V2_SENTINEL {
        let version = c.u32()?;
        if version != TOKENS_VERSION {
            // Unknown FUTURE layout: fail CLOSED. Reading it as "no text
            // index" would serve silently-empty matches.
            return Err(corrupt("unsupported K_TOKENS version"));
        }
        return decode_tokens_v2(c, count);
    }
    // Headerless = legacy v1 (`first` is ntokens): eager decode, forever.
    let n = first as usize;
    let mut tokens: Vec<String> = Vec::with_capacity(n.min(1 << 20));
    let mut postings: Vec<Vec<u32>> = Vec::with_capacity(n.min(1 << 20));
    for _ in 0..n {
        let tlen = c.u32()? as usize;
        let tok =
            String::from_utf8(c.take(tlen)?.to_vec()).map_err(|_| corrupt("token not utf8"))?;
        if let Some(last) = tokens.last() {
            if *last >= tok {
                return Err(corrupt("token dictionary not sorted"));
            }
        }
        let nrows = c.u32()? as usize;
        let mut rows = Vec::with_capacity(nrows.min(count + 1));
        let mut prev = 0u32;
        for i in 0..nrows {
            let delta = varint_read(&mut c)?;
            let row = if i == 0 {
                delta
            } else {
                prev.checked_add(delta)
                    .ok_or_else(|| corrupt("posting overflow"))?
            };
            if i > 0 && delta == 0 {
                return Err(corrupt("postings not strictly ascending"));
            }
            if row as usize >= count {
                return Err(corrupt("posting row out of range"));
            }
            rows.push(row);
            prev = row;
        }
        tokens.push(tok);
        postings.push(rows);
    }
    Ok(TokenIndex {
        tokens,
        postings: Postings::Eager(postings),
    })
}

/// Decode a v2 K_TOKENS body (cursor past sentinel+version): parse the
/// directory, keep the postings BLOB undecoded, and validate every span
/// completely — ascending in-range rows, exact byte lengths — so the lazy
/// per-query reads (`decode_posting_list`) are infallible by construction.
fn decode_tokens_v2(mut c: Cur<'_>, count: usize) -> Result<TokenIndex> {
    let n = c.u32()? as usize;
    let mut tokens: Vec<String> = Vec::with_capacity(n.min(1 << 20));
    let mut spans: Vec<(u32, u32, u32)> = Vec::with_capacity(n.min(1 << 20));
    for _ in 0..n {
        let tlen = c.u32()? as usize;
        let tok =
            String::from_utf8(c.take(tlen)?.to_vec()).map_err(|_| corrupt("token not utf8"))?;
        if let Some(last) = tokens.last() {
            if *last >= tok {
                return Err(corrupt("token dictionary not sorted"));
            }
        }
        let nrows = c.u32()?;
        let off = c.u32()?;
        let len = c.u32()?;
        tokens.push(tok);
        spans.push((off, len, nrows));
    }
    let blob_len = c.u32()? as usize;
    let blob = c.take(blob_len)?.to_vec();
    if !c.at_end() {
        return Err(corrupt("trailing bytes after postings blob"));
    }
    // Validate every span now: the lazy read path never re-checks.
    for &(off, len, nrows) in &spans {
        let end = (off as usize)
            .checked_add(len as usize)
            .ok_or_else(|| corrupt("span overflow"))?;
        if end > blob.len() {
            return Err(corrupt("posting span out of blob"));
        }
        let mut sc = Cur::new(&blob[off as usize..end]);
        let mut prev = 0u32;
        for i in 0..nrows {
            let delta = varint_read(&mut sc)?;
            let row = if i == 0 {
                delta
            } else {
                if delta == 0 {
                    return Err(corrupt("postings not strictly ascending"));
                }
                prev.checked_add(delta)
                    .ok_or_else(|| corrupt("posting overflow"))?
            };
            if row as usize >= count {
                return Err(corrupt("posting row out of range"));
            }
            prev = row;
        }
        if !sc.at_end() {
            return Err(corrupt("posting span length mismatch"));
        }
    }
    let decoded = spans.iter().map(|_| std::sync::OnceLock::new()).collect();
    Ok(TokenIndex {
        tokens,
        postings: Postings::Lazy {
            blob,
            spans,
            decoded,
        },
    })
}

fn encode_v2<R: Borrow<Record>>(records: &[R]) -> Result<Vec<u8>> {
    let count = records.len();
    let plans = plan_labels(records);
    let mut out = Vec::new();
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.extend_from_slice(&VERSION_V2.to_le_bytes());
    let mut dir: Vec<SectionEntry> = Vec::new();

    let key_items: Vec<&[u8]> = records.iter().map(|r| r.borrow().key.as_bytes()).collect();
    push_section(&mut out, &mut dir, K_KEYS, "", &encode_strings(&key_items));

    let mut ts = Vec::with_capacity(count * 8);
    for r in records {
        ts.extend_from_slice(&r.borrow().timestamp.to_le_bytes());
    }
    push_section(&mut out, &mut dir, K_TS, "", &ts);

    for plan in &plans {
        push_section(
            &mut out,
            &mut dir,
            K_LABEL,
            &plan.name,
            &encode_label_column(records, plan),
        );
    }
    for name in numeric_names(records) {
        push_section(
            &mut out,
            &mut dir,
            K_NUMERIC,
            &name,
            &encode_numeric_column(records, &name),
        );
    }

    push_section(&mut out, &mut dir, K_PAYLOAD, "", &encode_payloads(records));

    if records.iter().any(|r| r.borrow().text.is_some()) {
        // D8: the poison at the old key makes a pre-D8 binary fail LOUDLY;
        // the real v2 body rides the "z2" name (docs/COMPAT.md).
        push_section(&mut out, &mut dir, K_TEXT, "", TEXT_POISON);
        push_section(
            &mut out,
            &mut dir,
            K_TEXT,
            TEXT_V2_NAME,
            &encode_texts(records),
        );
        push_section(
            &mut out,
            &mut dir,
            K_TOKENS,
            "",
            &encode_tokens(&TokenIndex::build(records)),
        );
    }

    let blocks = build_blocks(records, &plans);
    let blocks_body = rmp_serde::to_vec(&blocks).map_err(|e| GirderError::Encode(e.to_string()))?;
    push_section(&mut out, &mut dir, K_BLOCKS, "", &blocks_body);

    let dir_body = rmp_serde::to_vec(&SectionDir {
        count: count as u64,
        entries: dir,
    })
    .map_err(|e| GirderError::Encode(e.to_string()))?;
    let footer_off = out.len() as u64;
    let footer_crc = crc32fast::hash(&dir_body);
    out.extend_from_slice(&dir_body);
    out.extend_from_slice(&footer_off.to_le_bytes());
    out.extend_from_slice(&footer_crc.to_le_bytes());
    out.extend_from_slice(&MAGIC.to_le_bytes());
    Ok(out)
}

/// Write records to a v2 segment file, sorting them by key first. Returns the
/// zone map. (Production writes go through the presorted/borrowed variants;
/// this sort-then-write helper is retained for tests and fixtures.)
#[cfg(test)]
pub fn write_segment(path: &Path, records: &mut [Record]) -> Result<ZoneMap> {
    records.sort_by(|a, b| a.key.cmp(&b.key));
    write_sorted(path, records)
}

/// Write already-key-sorted owned records to a v2 segment (compaction split
/// output — the merged stream is BTreeMap-ordered, so no re-sort is needed).
pub fn write_segment_presorted(path: &Path, records: &[Record]) -> Result<ZoneMap> {
    write_sorted(path, records)
}

/// Zero-clone flush path: write a segment straight from `&Record` borrows that
/// are already key-sorted (a frozen memtable's `BTreeMap::values()`), without
/// cloning payloads into an intermediate `Vec` or re-sorting.
pub fn write_segment_refs(path: &Path, records: &[&Record]) -> Result<ZoneMap> {
    write_sorted(path, records)
}

/// Encode a key-sorted record slice (owned or borrowed) and write it atomically
/// (tmp → fsync → rename). Callers guarantee the input is sorted by key.
fn write_sorted<R: Borrow<Record>>(path: &Path, records: &[R]) -> Result<ZoneMap> {
    debug_assert!(
        records
            .windows(2)
            .all(|w| w[0].borrow().key <= w[1].borrow().key),
        "write_sorted requires key-sorted input"
    );
    let zone = ZoneMap::build(records);
    let out = encode_v2(records)?;
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, &out)?;
    let file = std::fs::File::open(&tmp)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, path)?;
    Ok(zone)
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// A checked little-endian cursor over a section body.
struct Cur<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> Cur<'a> {
    fn new(b: &'a [u8]) -> Self {
        Cur { b, p: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .p
            .checked_add(n)
            .ok_or_else(|| corrupt("length overflow"))?;
        if end > self.b.len() {
            return Err(corrupt("section truncated"));
        }
        let s = &self.b[self.p..end];
        self.p = end;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn rest(&self) -> &'a [u8] {
        &self.b[self.p..]
    }
    fn at_end(&self) -> bool {
        self.p == self.b.len()
    }
}

fn decode_strings(body: &[u8]) -> Result<(String, Vec<u64>)> {
    let mut c = Cur::new(body);
    let count = c.u32()? as usize;
    let mut offsets = Vec::with_capacity(count + 1);
    for _ in 0..count + 1 {
        offsets.push(c.u64()?);
    }
    let blob = c.rest();
    if *offsets.last().unwrap() as usize != blob.len() {
        return Err(corrupt("string blob length mismatch"));
    }
    let s = String::from_utf8(blob.to_vec()).map_err(|_| corrupt("non-utf8 string column"))?;
    Ok((s, offsets))
}

fn decode_label_column(body: &[u8], count: usize) -> Result<LabelColumn> {
    let mode = *body.first().ok_or_else(|| corrupt("empty label section"))?;
    let rest = &body[1..];
    match mode {
        0 => {
            let mut c = Cur::new(rest);
            let n = c.u32()? as usize;
            let mut dict = Vec::with_capacity(n);
            for _ in 0..n {
                let len = c.u32()? as usize;
                let s = String::from_utf8(c.take(len)?.to_vec())
                    .map_err(|_| corrupt("non-utf8 dict value"))?;
                dict.push(s);
            }
            let mut codes = Vec::with_capacity(count);
            for _ in 0..count {
                codes.push(c.u16()?);
            }
            let code_of = dict
                .iter()
                .enumerate()
                .map(|(i, s)| (s.clone(), (i + 1) as u16))
                .collect();
            Ok(LabelColumn::Dict {
                dict,
                code_of,
                codes,
            })
        }
        1 => {
            let bitmap_len = count.div_ceil(8);
            let mut c = Cur::new(rest);
            let presence = c.take(bitmap_len)?.to_vec();
            let (concat, offsets) = decode_strings(c.rest())?;
            let mut values = vec![None; count];
            let mut pi = 0usize;
            for (i, slot) in values.iter_mut().enumerate() {
                if presence[i / 8] >> (i % 8) & 1 == 1 {
                    *slot =
                        Some(concat[offsets[pi] as usize..offsets[pi + 1] as usize].to_string());
                    pi += 1;
                }
            }
            if pi + 1 != offsets.len() {
                return Err(corrupt("plain label presence/value count mismatch"));
            }
            Ok(LabelColumn::Plain { values })
        }
        other => Err(corrupt(format!("bad label mode {other}"))),
    }
}

fn decode_numeric_column(body: &[u8], count: usize) -> Result<NumericColumn> {
    let bitmap_len = count.div_ceil(8);
    let mut c = Cur::new(body);
    let presence = c.take(bitmap_len)?.to_vec();
    let present: Vec<bool> = (0..count)
        .map(|i| presence[i / 8] >> (i % 8) & 1 == 1)
        .collect();
    let mut values = vec![f64::NAN; count];
    for (i, p) in present.iter().enumerate() {
        if *p {
            values[i] = c.f64()?;
        }
    }
    Ok(NumericColumn { present, values })
}

fn decode_timestamps(body: &[u8], count: usize) -> Result<Vec<i64>> {
    if body.len() != count * 8 {
        return Err(corrupt("timestamp column size mismatch"));
    }
    let mut c = Cur::new(body);
    let mut v = Vec::with_capacity(count);
    for _ in 0..count {
        v.push(c.i64()?);
    }
    Ok(v)
}

fn slice(bytes: &[u8], from: usize, len: usize) -> Result<&[u8]> {
    let end = from
        .checked_add(len)
        .ok_or_else(|| corrupt("offset overflow"))?;
    if end > bytes.len() {
        return Err(corrupt("section out of bounds"));
    }
    Ok(&bytes[from..end])
}

// ---------------------------------------------------------------------------
// Whole-file decoders (compaction / recovery / tests). These read every byte;
// scans use the targeted section readers below (WS4).
// ---------------------------------------------------------------------------

fn decode_v2_columns(bytes: &[u8]) -> Result<SegmentColumns> {
    let len = bytes.len();
    if len < 24 {
        return Err(corrupt("v2 file too short"));
    }
    let end_magic = u32::from_le_bytes(bytes[len - 4..len].try_into().unwrap());
    if end_magic != MAGIC {
        return Err(corrupt("bad trailing magic"));
    }
    let footer_crc = u32::from_le_bytes(bytes[len - 8..len - 4].try_into().unwrap());
    let footer_off = u64::from_le_bytes(bytes[len - 16..len - 8].try_into().unwrap()) as usize;
    if footer_off > len - 16 {
        return Err(corrupt("footer offset out of bounds"));
    }
    let dir_body = &bytes[footer_off..len - 16];
    if crc32fast::hash(dir_body) != footer_crc {
        return Err(corrupt("footer crc mismatch"));
    }
    let dir: SectionDir =
        rmp_serde::from_slice(dir_body).map_err(|e| corrupt(format!("footer: {e}")))?;
    let count = dir.count as usize;

    let mut keys: Option<KeysSection> = None;
    let mut timestamps: Option<Vec<i64>> = None;
    let mut labels: BTreeMap<String, Arc<LabelColumn>> = BTreeMap::new();
    let mut numerics: BTreeMap<String, Arc<NumericColumn>> = BTreeMap::new();
    let mut blocks: Option<Vec<BlockMeta>> = None;
    let mut payloads: Option<Arc<PayloadIndex>> = None;
    let mut texts: Option<Arc<TextIndex>> = None;
    let mut text_v1_pending: Option<(&[u8], usize)> = None;
    let mut tokens: Option<Arc<TokenIndex>> = None;

    for e in &dir.entries {
        let start = e.offset as usize;
        // inline crc word precedes the body
        let crc_bytes = slice(bytes, start, 4)?;
        let body = slice(bytes, start + 4, e.len as usize)?;
        let inline_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        if inline_crc != e.crc || crc32fast::hash(body) != e.crc {
            return Err(corrupt(format!("section crc mismatch (kind {})", e.kind)));
        }
        match e.kind {
            K_KEYS => {
                let (blob, offsets) = decode_strings(body)?;
                keys = Some(KeysSection { blob, offsets });
            }
            K_TS => timestamps = Some(decode_timestamps(body, count)?),
            K_LABEL => {
                labels.insert(e.name.clone(), Arc::new(decode_label_column(body, count)?));
            }
            K_NUMERIC => {
                numerics.insert(
                    e.name.clone(),
                    Arc::new(decode_numeric_column(body, count)?),
                );
            }
            K_PAYLOAD => {
                let mut c = Cur::new(body);
                let mut rel = Vec::with_capacity(count + 1);
                for _ in 0..count + 1 {
                    rel.push(c.u64()?);
                }
                let abs_base = (start + 4 + 8 * (count + 1)) as u64;
                payloads = Some(Arc::new(PayloadIndex { abs_base, rel }));
            }
            K_BLOCKS => {
                blocks =
                    Some(rmp_serde::from_slice(body).map_err(|e| corrupt(format!("blocks: {e}")))?);
            }
            K_TEXT => {
                // D8: "z2" = the v2 chunked body; "" is EITHER a legacy v1
                // body (old file) or the 2-byte poison beside a "z2" entry
                // (new file, aimed at pre-D8 binaries). Defer the "" decode
                // until the whole directory is walked so the poison is
                // never parsed when v2 is present.
                if e.name == TEXT_V2_NAME {
                    texts = Some(Arc::new(decode_text_v2(body, count, (start + 4) as u64)?));
                } else {
                    text_v1_pending = Some((body, start));
                }
            }
            K_TOKENS => {
                tokens = Some(Arc::new(decode_tokens(body, count)?));
            }
            _ => {} // unknown kind: ignore (forward-compat)
        }
    }

    if texts.is_none() {
        if let Some((body, start)) = text_v1_pending {
            // No v2 entry: the "" body is a real legacy v1 section.
            texts = Some(Arc::new(decode_text_index(
                body,
                count,
                (start + 4) as u64,
            )?));
        }
    }
    let keys = keys.ok_or_else(|| corrupt("missing key section"))?;
    let timestamps = timestamps.ok_or_else(|| corrupt("missing timestamp section"))?;
    let payloads = payloads.ok_or_else(|| corrupt("missing payload section"))?;
    let blocks = blocks.ok_or_else(|| corrupt("missing block index"))?;
    if keys.offsets.len() != count + 1 || timestamps.len() != count {
        return Err(corrupt("column count mismatch"));
    }
    Ok(SegmentColumns {
        count,
        keys: Arc::new(keys),
        timestamps: Arc::new(timestamps),
        labels,
        numerics,
        blocks: Arc::new(blocks),
        payloads: Payloads::File(payloads),
        texts: texts.map(Texts::File),
        tokens,
    })
}

/// Decode a K_TEXT body given the absolute file offset of the body start.
/// Validates the offset table like the payload table: zero-based, monotonic,
/// terminating exactly at the blob length.
fn decode_text_index(body: &[u8], count: usize, body_abs: u64) -> Result<TextIndex> {
    let presence_len = count.div_ceil(8);
    let table_len = 8usize
        .checked_mul(count + 1)
        .ok_or_else(|| corrupt("text table overflow"))?;
    if body.len() < presence_len + table_len {
        return Err(corrupt("text section shorter than header"));
    }
    let blob_len = (body.len() - presence_len - table_len) as u64;
    decode_text_header(&body[..presence_len + table_len], count, body_abs, blob_len)
}

/// Shared K_TEXT header decoder: presence bitmap + offset table, validated
/// (zero-based, monotonic, terminating exactly at `blob_len`).
fn decode_text_header(
    header: &[u8],
    count: usize,
    body_abs: u64,
    blob_len: u64,
) -> Result<TextIndex> {
    let presence_len = count.div_ceil(8);
    let table_len = 8 * (count + 1);
    let mut present = Vec::with_capacity(count);
    for i in 0..count {
        present.push(header[i / 8] & (1 << (i % 8)) != 0);
    }
    let mut c = Cur::new(&header[presence_len..presence_len + table_len]);
    let mut rel = Vec::with_capacity(count + 1);
    for _ in 0..count + 1 {
        rel.push(c.u64()?);
    }
    if rel[0] != 0 {
        return Err(corrupt("text offsets not zero-based"));
    }
    if rel.windows(2).any(|w| w[0] > w[1]) {
        return Err(corrupt("text offsets not monotonic"));
    }
    if *rel.last().unwrap() != blob_len {
        return Err(corrupt(
            "text offset table inconsistent with section length",
        ));
    }
    let abs_base = body_abs + (presence_len + table_len) as u64;
    Ok(TextIndex {
        abs_base,
        present,
        rel,
        compressed: None,
    })
}

/// Decode a v2 K_TEXT header (D8): `[u32 version]` (already consumed by
/// callers), presence bitmap, compressed-rows bitmap, STORED-space offset
/// table. Validated like v1 (zero-based, monotonic, terminating exactly at
/// the stored blob length) plus: a compressed bit on an absent row is
/// corruption.
fn decode_text_v2_header(
    header: &[u8],
    count: usize,
    blob_abs: u64,
    blob_len: u64,
) -> Result<TextIndex> {
    let bitmap_len = count.div_ceil(8);
    let table_len = 8 * (count + 1);
    if header.len() != bitmap_len * 2 + table_len {
        return Err(corrupt("text v2 header length mismatch"));
    }
    let mut present = Vec::with_capacity(count);
    let mut compressed = Vec::with_capacity(count);
    for i in 0..count {
        present.push(header[i / 8] & (1 << (i % 8)) != 0);
        compressed.push(header[bitmap_len + i / 8] & (1 << (i % 8)) != 0);
    }
    if compressed.iter().zip(&present).any(|(&c, &p)| c && !p) {
        return Err(corrupt("compressed bit on absent text row"));
    }
    let mut c = Cur::new(&header[bitmap_len * 2..]);
    let mut rel = Vec::with_capacity(count + 1);
    for _ in 0..count + 1 {
        rel.push(c.u64()?);
    }
    if rel[0] != 0 {
        return Err(corrupt("text offsets not zero-based"));
    }
    if rel.windows(2).any(|w| w[0] > w[1]) {
        return Err(corrupt("text offsets not monotonic"));
    }
    if *rel.last().unwrap() != blob_len {
        return Err(corrupt(
            "text offset table inconsistent with section length",
        ));
    }
    Ok(TextIndex {
        abs_base: blob_abs,
        present,
        rel,
        compressed: Some(compressed),
    })
}

/// Decode a whole v2 K_TEXT body (the whole-file read path).
fn decode_text_v2(body: &[u8], count: usize, body_abs: u64) -> Result<TextIndex> {
    let mut c = Cur::new(body);
    let version = c.u32()?;
    if version != TEXT_V2_VERSION {
        // Unknown FUTURE layout: fail CLOSED — reading it as no-text would
        // serve silently-absent documents.
        return Err(corrupt("unsupported K_TEXT z2 version"));
    }
    let bitmap_len = count.div_ceil(8);
    let table_len = 8 * (count + 1);
    let header_len = bitmap_len
        .checked_mul(2)
        .and_then(|n| n.checked_add(table_len))
        .ok_or_else(|| corrupt("text v2 header overflow"))?;
    let header = c.take(header_len)?;
    let blob_len = c.rest().len() as u64;
    decode_text_v2_header(header, count, body_abs + 4 + header_len as u64, blob_len)
}

fn decode_v1_records(bytes: &[u8]) -> Result<Vec<Record>> {
    if bytes.len() < 12 {
        return Err(corrupt("v1 file too short"));
    }
    let crc = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let body = &bytes[12..];
    if crc32fast::hash(body) != crc {
        return Err(corrupt("v1 crc mismatch"));
    }
    rmp_serde::from_slice(body).map_err(|e| corrupt(format!("v1 decode: {e}")))
}

fn header(bytes: &[u8]) -> Result<(u32, u32)> {
    if bytes.len() < 8 {
        return Err(corrupt("file too short"));
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if magic != MAGIC {
        return Err(corrupt("bad magic"));
    }
    Ok((magic, version))
}

/// Read + verify a whole segment file into typed columns (payloads for v2 are
/// still sliced lazily from the file). Whole-file convenience used by the
/// segment tests; scans use [`read_footer`] + the per-section readers below.
#[cfg(test)]
pub fn read_columns(path: &Path) -> Result<SegmentColumns> {
    let bytes = std::fs::read(path)?;
    let (_, version) = header(&bytes)?;
    match version {
        VERSION_V1 => Ok(SegmentColumns::from_records(decode_v1_records(&bytes)?)),
        VERSION_V2 => decode_v2_columns(&bytes),
        other => Err(corrupt(format!("unsupported version {other}"))),
    }
}

/// Read + verify a segment file into full `Record`s (payloads included).
/// Used by compaction and any all-rows consumer; handles v1 and v2.
pub fn read_all_records(path: &Path) -> Result<Vec<Record>> {
    let bytes = std::fs::read(path)?;
    let (_, version) = header(&bytes)?;
    match version {
        VERSION_V1 => decode_v1_records(&bytes),
        VERSION_V2 => {
            let cols = decode_v2_columns(&bytes)?;
            let Payloads::File(pi) = &cols.payloads else {
                return Err(corrupt("v2 payloads not file-backed"));
            };
            let mut out = Vec::with_capacity(cols.count);
            for i in 0..cols.count {
                let from = (pi.abs_base + pi.rel[i]) as usize;
                let plen = (pi.rel[i + 1] - pi.rel[i]) as usize;
                let payload = slice(&bytes, from, plen)?.to_vec();
                let text = match &cols.texts {
                    Some(Texts::File(ti)) if ti.present[i] => {
                        let tfrom = (ti.abs_base + ti.rel[i]) as usize;
                        let tlen = (ti.rel[i + 1] - ti.rel[i]) as usize;
                        let stored = slice(&bytes, tfrom, tlen)?.to_vec();
                        Some(ti.decode_row_text(i, stored)?)
                    }
                    _ => None,
                };
                out.push(cols.build_record(i, payload, text));
            }
            Ok(out)
        }
        other => Err(corrupt(format!("unsupported version {other}"))),
    }
}

// ---------------------------------------------------------------------------
// Targeted section reads (WS4): read only the sections a scan touches.
// ---------------------------------------------------------------------------

/// One entry of the parsed footer directory.
struct DirEntry {
    offset: u64, // absolute file offset of the section's crc word
    len: u64,    // body length (bytes after the crc word)
    crc: u32,
}

/// The parsed footer of a v2 segment: what sections exist, where, and their
/// crc. Small (independent of row count); cached per segment so warm scans
/// touch no disk. `version` is 1 for a legacy file (no directory).
pub struct SegDir {
    count: usize,
    entries: HashMap<(u8, String), DirEntry>,
}

impl SegDir {
    pub fn count(&self) -> usize {
        self.count
    }
    fn entry(&self, kind: u8, name: &str) -> Result<&DirEntry> {
        self.entries
            .get(&(kind, name.to_string()))
            .ok_or_else(|| corrupt(format!("missing section (kind {kind})")))
    }
    /// Names of the segment's label columns.
    pub fn label_names(&self) -> Vec<String> {
        self.entries
            .keys()
            .filter(|(k, _)| *k == K_LABEL)
            .map(|(_, n)| n.clone())
            .collect()
    }
    /// Names of the segment's numeric columns.
    pub fn numeric_names(&self) -> Vec<String> {
        self.entries
            .keys()
            .filter(|(k, _)| *k == K_NUMERIC)
            .map(|(_, n)| n.clone())
            .collect()
    }
    fn bytes(&self) -> u64 {
        let mut n = 64;
        for (_, name) in self.entries.keys() {
            n += name.len() as u64 + 48;
        }
        n
    }
}

/// Read `len` bytes at absolute `offset`, tallying `bytes_read`. Safe positioned
/// read (no mmap, no unsafe): `FileExt::read_exact_at` on unix, seek+read
/// fallback elsewhere.
fn read_at(file: &File, offset: u64, len: usize, bytes_read: &AtomicU64) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(&mut buf, offset)?;
    }
    #[cfg(not(unix))]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = file.try_clone()?;
        f.seek(SeekFrom::Start(offset))?;
        f.read_exact(&mut buf)?;
    }
    bytes_read.fetch_add(len as u64, Ordering::Relaxed);
    Ok(buf)
}

/// Read the 8-byte header (magic + version) from an open segment file.
pub fn read_header(file: &File, bytes_read: &AtomicU64) -> Result<u32> {
    let h = read_at(file, 0, 8, bytes_read)?;
    let magic = u32::from_le_bytes(h[0..4].try_into().unwrap());
    let version = u32::from_le_bytes(h[4..8].try_into().unwrap());
    if magic != MAGIC {
        return Err(corrupt("bad magic"));
    }
    Ok(version)
}

/// Parse a v2 segment's footer directory with two small positioned reads (the
/// 16-byte trailer, then the crc-verified directory body). No column bytes are
/// read here.
/// The on-disk format version of a segment file (cheap 8-byte header read).
pub fn file_version(path: &Path) -> Result<u32> {
    let file = File::open(path)?;
    let ignored = AtomicU64::new(0);
    read_header(&file, &ignored)
}

pub fn read_footer(file: &File, bytes_read: &AtomicU64) -> Result<SegDir> {
    let len = file.metadata()?.len();
    if len < 24 {
        return Err(corrupt("v2 file too short"));
    }
    let tail = read_at(file, len - 16, 16, bytes_read)?;
    let footer_off = u64::from_le_bytes(tail[0..8].try_into().unwrap());
    let footer_crc = u32::from_le_bytes(tail[8..12].try_into().unwrap());
    let end_magic = u32::from_le_bytes(tail[12..16].try_into().unwrap());
    if end_magic != MAGIC {
        return Err(corrupt("bad trailing magic"));
    }
    if footer_off > len - 16 {
        return Err(corrupt("footer offset out of bounds"));
    }
    let dir_body = read_at(
        file,
        footer_off,
        (len - 16 - footer_off) as usize,
        bytes_read,
    )?;
    if crc32fast::hash(&dir_body) != footer_crc {
        return Err(corrupt("footer crc mismatch"));
    }
    let dir: SectionDir =
        rmp_serde::from_slice(&dir_body).map_err(|e| corrupt(format!("footer: {e}")))?;
    let mut entries = HashMap::with_capacity(dir.entries.len());
    for e in dir.entries {
        entries.insert(
            (e.kind, e.name),
            DirEntry {
                offset: e.offset,
                len: e.len,
                crc: e.crc,
            },
        );
    }
    Ok(SegDir {
        count: dir.count as usize,
        entries,
    })
}

/// Read + crc-verify one section's body via a single positioned read.
fn read_verified_body(
    file: &File,
    e: &DirEntry,
    kind: u8,
    bytes_read: &AtomicU64,
) -> Result<Vec<u8>> {
    let total = 4usize
        .checked_add(e.len as usize)
        .ok_or_else(|| corrupt("section length overflow"))?;
    let buf = read_at(file, e.offset, total, bytes_read)?;
    let inline_crc = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let body = &buf[4..];
    if inline_crc != e.crc || crc32fast::hash(body) != e.crc {
        return Err(corrupt(format!("section crc mismatch (kind {kind})")));
    }
    Ok(body.to_vec())
}

/// Load the key column.
pub fn load_keys(file: &File, dir: &SegDir, bytes_read: &AtomicU64) -> Result<KeysSection> {
    let e = dir.entry(K_KEYS, "")?;
    let body = read_verified_body(file, e, K_KEYS, bytes_read)?;
    let (blob, offsets) = decode_strings(&body)?;
    if offsets.len() != dir.count + 1 {
        return Err(corrupt("key column count mismatch"));
    }
    Ok(KeysSection { blob, offsets })
}

/// Load the timestamp column.
pub fn load_timestamps(file: &File, dir: &SegDir, bytes_read: &AtomicU64) -> Result<Vec<i64>> {
    let e = dir.entry(K_TS, "")?;
    let body = read_verified_body(file, e, K_TS, bytes_read)?;
    decode_timestamps(&body, dir.count)
}

/// Load the block index.
pub fn load_blocks(file: &File, dir: &SegDir, bytes_read: &AtomicU64) -> Result<Vec<BlockMeta>> {
    let e = dir.entry(K_BLOCKS, "")?;
    let body = read_verified_body(file, e, K_BLOCKS, bytes_read)?;
    rmp_serde::from_slice(&body).map_err(|e| corrupt(format!("blocks: {e}")))
}

/// Load one label column.
pub fn load_label(
    file: &File,
    dir: &SegDir,
    name: &str,
    bytes_read: &AtomicU64,
) -> Result<LabelColumn> {
    let e = dir.entry(K_LABEL, name)?;
    let body = read_verified_body(file, e, K_LABEL, bytes_read)?;
    decode_label_column(&body, dir.count)
}

/// Load one numeric column.
pub fn load_numeric(
    file: &File,
    dir: &SegDir,
    name: &str,
    bytes_read: &AtomicU64,
) -> Result<NumericColumn> {
    let e = dir.entry(K_NUMERIC, name)?;
    let body = read_verified_body(file, e, K_NUMERIC, bytes_read)?;
    decode_numeric_column(&body, dir.count)
}

/// Load the text offset table (K_TEXT header: presence + offsets; blob bytes
/// are sliced per row at materialize time). Returns `None` when the segment
/// has no text section (pre-text file, or no record carried text).
pub fn load_text_index(
    file: &File,
    dir: &SegDir,
    bytes_read: &AtomicU64,
) -> Result<Option<TextIndex>> {
    let count = dir.count;
    // D8: the v2 chunked body rides the "z2" name; the old "" key holds
    // the poison a pre-D8 binary trips over. New readers resolve v2 FIRST
    // and never touch the poison.
    if let Some(e) = dir.entries.get(&(K_TEXT, TEXT_V2_NAME.to_string())) {
        let version_word = read_at(file, e.offset + 4, 4, bytes_read)?;
        let version = u32::from_le_bytes(version_word.as_slice().try_into().unwrap());
        if version != TEXT_V2_VERSION {
            return Err(corrupt("unsupported K_TEXT z2 version"));
        }
        let bitmap_len = count.div_ceil(8);
        let table_len = 8usize
            .checked_mul(count + 1)
            .ok_or_else(|| corrupt("text table overflow"))?;
        let header_len = bitmap_len
            .checked_mul(2)
            .and_then(|n| n.checked_add(table_len))
            .ok_or_else(|| corrupt("text v2 header overflow"))?;
        if (e.len as usize) < 4 + header_len {
            return Err(corrupt("text v2 section shorter than header"));
        }
        let header = read_at(file, e.offset + 8, header_len, bytes_read)?;
        let blob_abs = e.offset + 8 + header_len as u64;
        let blob_len = e.len - 4 - header_len as u64;
        return Ok(Some(decode_text_v2_header(
            &header, count, blob_abs, blob_len,
        )?));
    }
    let Some(e) = dir.entries.get(&(K_TEXT, String::new())) else {
        return Ok(None);
    };
    let presence_len = count.div_ceil(8);
    let table_len = 8usize
        .checked_mul(count + 1)
        .ok_or_else(|| corrupt("text table overflow"))?;
    if (e.len as usize) < presence_len + table_len {
        return Err(corrupt("text section shorter than header"));
    }
    let header = read_at(file, e.offset + 4, presence_len + table_len, bytes_read)?;
    let blob_len = e.len - (presence_len + table_len) as u64;
    // Reuse the shared decoder by handing it just the header + declared blob
    // length (it validates zero-based/monotonic/terminating).
    let idx = decode_text_header(&header, count, e.offset + 4, blob_len)?;
    Ok(Some(idx))
}

/// Load the decoded token postings index, or `None` when the segment has no
/// K_TOKENS section. Loaded only for text queries (the engine passes
/// `need_tokens` through), so field-only scans never pay for it.
pub fn load_tokens(
    file: &File,
    dir: &SegDir,
    bytes_read: &AtomicU64,
) -> Result<Option<TokenIndex>> {
    let Some(e) = dir.entries.get(&(K_TOKENS, String::new())) else {
        return Ok(None);
    };
    let body = read_verified_body(file, e, K_TOKENS, bytes_read)?;
    Ok(Some(decode_tokens(&body, dir.count)?))
}

/// Load *only* the payload offset table (the first `8·(count+1)` bytes of the
/// payload section). The raw payload bytes — the ~GB — are never read here and
/// their crc is not verified (per-row slices are read on demand at materialize
/// time). The table is instead validated structurally: zero-based, monotonic,
/// and terminating exactly at the section's raw-byte length.
pub fn load_payload_index(
    file: &File,
    dir: &SegDir,
    bytes_read: &AtomicU64,
) -> Result<PayloadIndex> {
    let e = dir.entry(K_PAYLOAD, "")?;
    let count = dir.count;
    let table_len = 8usize
        .checked_mul(count + 1)
        .ok_or_else(|| corrupt("payload table overflow"))?;
    if (e.len as usize) < table_len {
        return Err(corrupt("payload section shorter than offset table"));
    }
    // Offsets start right after the crc word at `e.offset`.
    let buf = read_at(file, e.offset + 4, table_len, bytes_read)?;
    let mut c = Cur::new(&buf);
    let mut rel = Vec::with_capacity(count + 1);
    for _ in 0..count + 1 {
        rel.push(c.u64()?);
    }
    if rel[0] != 0 {
        return Err(corrupt("payload offsets not zero-based"));
    }
    if rel.windows(2).any(|w| w[0] > w[1]) {
        return Err(corrupt("payload offsets not monotonic"));
    }
    let raw_len = e.len - table_len as u64;
    if *rel.last().unwrap() != raw_len {
        return Err(corrupt(
            "payload offset table inconsistent with section length",
        ));
    }
    let abs_base = e.offset + 4 + table_len as u64;
    Ok(PayloadIndex { abs_base, rel })
}

/// Decode a whole v1 (legacy) file into columns from an open handle. v1 is not
/// section-structured, so the whole file is read (legacy compat only).
pub fn load_v1_whole(file: &File, bytes_read: &AtomicU64) -> Result<SegmentColumns> {
    let len = file.metadata()?.len();
    let bytes = read_at(file, 0, len as usize, bytes_read)?;
    Ok(SegmentColumns::from_records(decode_v1_records(&bytes)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(key: &str, ts: i64, model: &str, latency: f64) -> Record {
        Record {
            key: key.into(),
            timestamp: ts,
            labels: BTreeMap::from([("model".to_string(), model.to_string())]),
            numerics: BTreeMap::from([("latency_ms".to_string(), latency)]),
            payload: format!("p-{key}").into_bytes(),
            text: None,
        }
    }

    /// Write a legacy v1 segment (for the compat-reader fixture test).
    fn write_v1(path: &Path, records: &mut Vec<Record>) {
        records.sort_by(|a, b| a.key.cmp(&b.key));
        let body = rmp_serde::to_vec(&records).unwrap();
        let crc = crc32fast::hash(&body);
        let mut out = Vec::new();
        out.extend_from_slice(&MAGIC.to_le_bytes());
        out.extend_from_slice(&VERSION_V1.to_le_bytes());
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&body);
        std::fs::write(path, out).unwrap();
    }

    #[test]
    fn v2_roundtrip_and_crc_guard() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s1.gird");
        let mut records = vec![record("b", 2, "gpt", 5.0), record("a", 1, "claude", 9.0)];
        let zone = write_segment(&path, &mut records).unwrap();
        assert_eq!((zone.min_ts, zone.max_ts), (1, 2));
        assert_eq!(zone.min_key, "a");

        let loaded = read_all_records(&path).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].key, "a"); // sorted
        assert_eq!(loaded[0].labels["model"], "claude");
        assert_eq!(loaded[0].numerics["latency_ms"], 9.0);
        assert_eq!(loaded[0].payload, b"p-a");

        // Corrupt one byte in the body → read fails loudly.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[20] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        assert!(read_columns(&path).is_err());
        assert!(read_all_records(&path).is_err());
    }

    /// WS4: the targeted section readers verify each section's crc and produce
    /// the same columns as the whole-file decoder, and a corrupt column byte is
    /// caught on the section read path too.
    #[test]
    fn targeted_section_reads_roundtrip_and_verify() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.gird");
        let mut records = vec![
            record("a", 1, "gpt-4o", 10.0),
            record("b", 2, "claude", 20.0),
            record("c", 3, "gpt-4o", 30.0),
        ];
        write_segment(&path, &mut records).unwrap();

        let br = AtomicU64::new(0);
        let file = File::open(&path).unwrap();
        assert_eq!(read_header(&file, &br).unwrap(), VERSION_V2);
        let seg = read_footer(&file, &br).unwrap();
        assert_eq!(seg.count(), 3);

        let keys = load_keys(&file, &seg, &br).unwrap();
        assert_eq!(keys.key_at(0), "a");
        assert_eq!(keys.key_at(2), "c");
        let ts = load_timestamps(&file, &seg, &br).unwrap();
        assert_eq!(ts, vec![1, 2, 3]);
        let lat = load_numeric(&file, &seg, "latency_ms", &br).unwrap();
        assert_eq!(lat.values, vec![10.0, 20.0, 30.0]);
        let model = load_label(&file, &seg, "model", &br).unwrap();
        assert!(matches!(model, LabelColumn::Dict { .. }));
        let pi = load_payload_index(&file, &seg, &br).unwrap();
        assert_eq!(pi.rel.len(), 4);
        let _ = load_blocks(&file, &seg, &br).unwrap();

        // Targeted reads never fault the whole file in.
        assert!(
            br.load(Ordering::Relaxed) < std::fs::metadata(&path).unwrap().len(),
            "section reads should be less than the whole file"
        );

        // Corrupt the timestamp section body → its crc check fails.
        let mut bytes = std::fs::read(&path).unwrap();
        let ts_off = seg.entry(K_TS, "").unwrap().offset as usize;
        bytes[ts_off + 4] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        let file2 = File::open(&path).unwrap();
        let seg2 = read_footer(&file2, &br).unwrap();
        assert!(load_timestamps(&file2, &seg2, &br).is_err());
    }

    #[test]
    fn v1_compat_reader() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.gird");
        let mut records = vec![
            record("a", 1, "gpt-4o", 10.0),
            record("b", 2, "claude", 20.0),
            record("c", 3, "gpt-4o", 30.0),
        ];
        write_v1(&path, &mut records);

        // read_all_records handles v1.
        let all = read_all_records(&path).unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[2].payload, b"p-c");

        // read_columns decodes v1 into the same column set; queries work.
        let cols = read_columns(&path).unwrap();
        assert_eq!(cols.count(), 3);
        assert_eq!(cols.find_key("b"), Some(1));
        let rows = cols
            .matching_rows(
                &QuerySpec {
                    labels: vec![("model".into(), "gpt-4o".into())],
                    numeric_ranges: vec![("latency_ms".into(), 25.0, 100.0)],
                    ..Default::default()
                },
                None,
                &AtomicU64::new(0),
            )
            .unwrap();
        assert_eq!(rows, vec![2]); // only "c" (gpt-4o & latency 30)
        let rec = cols.materialize(2, None, &AtomicU64::new(0), true).unwrap();
        assert_eq!(rec.key, "c");
        assert_eq!(rec.payload, b"p-c");

        // load_v1_whole from an open handle agrees.
        let file = File::open(&path).unwrap();
        assert_eq!(read_header(&file, &AtomicU64::new(0)).unwrap(), VERSION_V1);
        let cols2 = load_v1_whole(&file, &AtomicU64::new(0)).unwrap();
        assert_eq!(cols2.count(), 3);
        assert_eq!(cols2.key_at(2), "c");
    }

    /// WS3: compaction reads mixed v1 + v2 inputs (via `read_all_records`),
    /// merges newest-wins, and always writes v2 (`write_segment_presorted`).
    /// This is the exact merge mechanism `actors::compact` uses.
    #[test]
    fn compaction_merges_v1_and_v2_into_v2() {
        let dir = tempfile::tempdir().unwrap();
        let v1_path = dir.path().join("old.gird");
        let v2_path = dir.path().join("new.gird");
        // Older v1 segment: k=old, plus a v1-only key.
        let mut v1 = vec![
            record("k", 1, "v1-model", 10.0),
            record("only_v1", 1, "v1-model", 11.0),
        ];
        write_v1(&v1_path, &mut v1);
        // Newer v2 segment: k rewritten, plus a v2-only key.
        let mut v2 = vec![
            record("k", 2, "v2-model", 20.0),
            record("only_v2", 2, "v2-model", 21.0),
        ];
        write_segment(&v2_path, &mut v2).unwrap();

        // Merge newest-wins: read v1 (older) then v2 (newer); later wins.
        let mut merged: BTreeMap<String, Record> = BTreeMap::new();
        for r in read_all_records(&v1_path).unwrap() {
            merged.insert(r.key.clone(), r);
        }
        for r in read_all_records(&v2_path).unwrap() {
            merged.insert(r.key.clone(), r);
        }
        let out: Vec<Record> = merged.into_values().collect();
        let out_path = dir.path().join("merged.gird");
        write_segment_presorted(&out_path, &out).unwrap();

        // Output is v2.
        let bytes = std::fs::read(&out_path).unwrap();
        assert_eq!(header(&bytes).unwrap().1, VERSION_V2, "output must be v2");

        // No lost records; k resolves to the newer (v2) version.
        let all = read_all_records(&out_path).unwrap();
        assert_eq!(all.len(), 3); // k, only_v1, only_v2
        let by_key: BTreeMap<&str, &Record> = all.iter().map(|r| (r.key.as_str(), r)).collect();
        assert_eq!(by_key["k"].labels["model"], "v2-model");
        assert_eq!(by_key["k"].timestamp, 2);
        assert!(by_key.contains_key("only_v1"));
        assert!(by_key.contains_key("only_v2"));
    }

    #[test]
    fn matching_rows_vs_naive_oracle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.gird");
        // Deterministic pseudo-random corpus with mixed schemas across >1 block.
        let mut records = Vec::new();
        let mut state = 0x1234_5678u64;
        let mut rng = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for i in 0..10_000usize {
            let model = ["gpt-4o", "claude", "llama", "mistral"][(rng() % 4) as usize];
            let mut labels = BTreeMap::from([("model".to_string(), model.to_string())]);
            if rng() % 3 == 0 {
                labels.insert("region".to_string(), format!("r{}", rng() % 5));
            }
            let mut numerics = BTreeMap::new();
            if rng() % 4 != 0 {
                numerics.insert("latency_ms".to_string(), (rng() % 2000) as f64);
            }
            // Text on ~half the rows: a COMMON token ("beta", the D7-a
            // block-merge stressor) plus a sparse one — so composed
            // text+time/label specs cross block boundaries with dead blocks.
            let text = match rng() % 4 {
                0 | 1 => Some(format!(
                    "beta note {}",
                    ["billing", "zebra"][(rng() % 2) as usize]
                )),
                2 => Some("plain filler".to_string()),
                _ => None,
            };
            records.push(Record {
                key: format!("k/{i:06}"),
                // Ascending with the (key-sorted) row order, so per-block
                // timestamp ranges are DISJOINT and a time window really
                // prunes blocks — the shape D7-a's candidate merge exists
                // for (random timestamps would leave every block alive).
                timestamp: (i as i64) * 100,
                labels,
                numerics,
                payload: vec![0u8; 4],
                text,
            });
        }
        let mut sorted = records.clone();
        sorted.sort_by(|a, b| a.key.cmp(&b.key));
        write_segment(&path, &mut records).unwrap();
        let cols = read_columns(&path).unwrap();
        assert!(cols.count() > BLOCK_ROWS, "want multiple blocks");

        let specs = vec![
            QuerySpec {
                numeric_ranges: vec![("latency_ms".into(), 1995.0, f64::MAX)],
                ..Default::default()
            },
            QuerySpec {
                labels: vec![("model".into(), "gpt-4o".into())],
                numeric_ranges: vec![("latency_ms".into(), 1000.0, f64::MAX)],
                ..Default::default()
            },
            QuerySpec {
                labels: vec![("region".into(), "r3".into())],
                ..Default::default()
            },
            QuerySpec {
                time: Some((0, 100_000)),
                ..Default::default()
            },
            QuerySpec {
                key_prefix: Some("k/0001".into()),
                ..Default::default()
            },
            QuerySpec {
                labels: vec![("model".into(), "missing".into())],
                ..Default::default()
            },
            // D7-a shapes: the common token alone, composed with a narrow
            // time window (most blocks dead), with a label, with a prefix,
            // and a two-token AND with a sparse partner.
            QuerySpec {
                text_match: Some("beta".into()),
                ..Default::default()
            },
            QuerySpec {
                text_match: Some("beta".into()),
                time: Some((0, 50_000)),
                ..Default::default()
            },
            QuerySpec {
                text_match: Some("beta".into()),
                labels: vec![("model".into(), "gpt-4o".into())],
                numeric_ranges: vec![("latency_ms".into(), 1500.0, f64::MAX)],
                ..Default::default()
            },
            QuerySpec {
                text_match: Some("beta".into()),
                key_prefix: Some("k/0004".into()),
                ..Default::default()
            },
            QuerySpec {
                text_match: Some("beta zebra".into()),
                time: Some((100_000, 900_000)),
                ..Default::default()
            },
            // Every block dead: the merge's no-live-blocks early exit.
            QuerySpec {
                text_match: Some("beta".into()),
                time: Some((2_000_000, 3_000_000)),
                ..Default::default()
            },
        ];
        for spec in &specs {
            let mut oracle: Vec<String> = sorted
                .iter()
                .filter(|r| spec.matches(r))
                .map(|r| r.key.clone())
                .collect();
            oracle.sort();
            let mut got: Vec<String> = cols
                .matching_rows(spec, None, &AtomicU64::new(0))
                .unwrap()
                .iter()
                .map(|&i| cols.key_at(i as usize).to_string())
                .collect();
            got.sort();
            assert_eq!(got, oracle, "spec {spec:?}");
        }
    }

    /// The pre-D7-b headerless K_TOKENS body (written by every earlier
    /// binary) must decode forever, and must answer identically to the v2
    /// lazy layout for the same data.
    #[test]
    fn tokens_v1_body_reads_forever_and_matches_v2() {
        fn tokened(i: usize, text: &str) -> Record {
            Record {
                key: format!("k/{i:03}"),
                timestamp: i as i64,
                labels: BTreeMap::new(),
                numerics: BTreeMap::new(),
                payload: vec![],
                text: Some(text.to_string()),
            }
        }
        let records: Vec<Record> = (0..200)
            .map(|i| tokened(i, ["alpha beta", "beta gamma", "gamma delta"][i % 3]))
            .collect();
        let idx = TokenIndex::build(&records);

        // Hand-encode the v1 layout: [ntokens] then per token
        // [len][utf8][nrows][LEB deltas] — the exact pre-D7-b writer.
        let Postings::Eager(lists) = &idx.postings else {
            unreachable!("build is eager")
        };
        let mut v1 = Vec::new();
        v1.extend_from_slice(&(idx.tokens.len() as u32).to_le_bytes());
        for (t, rows) in idx.tokens.iter().zip(lists) {
            v1.extend_from_slice(&(t.len() as u32).to_le_bytes());
            v1.extend_from_slice(t.as_bytes());
            v1.extend_from_slice(&(rows.len() as u32).to_le_bytes());
            let mut prev = 0u32;
            for (i, &row) in rows.iter().enumerate() {
                let delta = if i == 0 { row } else { row - prev };
                varint_push(&mut v1, delta);
                prev = row;
            }
        }
        let from_v1 = decode_tokens(&v1, records.len()).unwrap();
        let from_v2 = decode_tokens(&encode_tokens(&idx), records.len()).unwrap();
        assert!(matches!(from_v1.postings, Postings::Eager(_)));
        assert!(matches!(from_v2.postings, Postings::Lazy { .. }));
        for want in [
            vec!["beta".to_string()],
            vec!["beta".to_string(), "gamma".to_string()],
            vec!["alpha".to_string(), "delta".to_string()], // disjoint ⇒ empty
            vec!["nope".to_string()],
        ] {
            let a = from_v1.rows_matching_all(&want);
            let b = from_v2.rows_matching_all(&want);
            let c = idx.rows_matching_all(&want);
            assert_eq!(a, b, "v1 vs v2 lazy: {want:?}");
            assert_eq!(a, c, "v1 vs built eager: {want:?}");
        }
    }

    /// An unknown FUTURE K_TOKENS version is a LOUD corrupt error — never a
    /// silent "segment has no text index" (silently-empty matches would be
    /// data loss dressed as no-results).
    #[test]
    fn tokens_unknown_future_version_fails_closed() {
        let mut body = Vec::new();
        body.extend_from_slice(&TOKENS_V2_SENTINEL.to_le_bytes());
        body.extend_from_slice(&(TOKENS_VERSION + 1).to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());
        let err = match decode_tokens(&body, 10) {
            Err(e) => e,
            Ok(_) => panic!("future version must fail closed"),
        };
        assert!(
            err.to_string().contains("unsupported K_TOKENS version"),
            "loud version error, got: {err}"
        );
    }

    /// v2 span validation happens at decode, exhaustively: a blob whose
    /// postings point out of range fails the SECTION decode, so the lazy
    /// per-query reads never see invalid bytes.
    #[test]
    fn tokens_v2_validates_spans_at_decode() {
        let records: Vec<Record> = (0..8)
            .map(|i| Record {
                key: format!("k/{i}"),
                timestamp: i as i64,
                labels: BTreeMap::new(),
                numerics: BTreeMap::new(),
                payload: vec![],
                text: Some("alpha".to_string()),
            })
            .collect();
        let good = encode_tokens(&TokenIndex::build(&records));
        // Row ids valid for 8 rows are invalid for a 4-row segment.
        assert!(decode_tokens(&good, 4).is_err(), "out-of-range postings");
        assert!(decode_tokens(&good, 8).is_ok());
        // Truncating the blob breaks a span's byte length: loud at decode.
        let truncated = &good[..good.len() - 1];
        assert!(decode_tokens(truncated, 8).is_err(), "truncated blob");
    }

    #[test]
    fn nan_numeric_never_matches_range() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nan.gird");
        let mut records = vec![
            Record {
                key: "a".into(),
                timestamp: 1,
                labels: BTreeMap::new(),
                numerics: BTreeMap::from([("x".to_string(), f64::NAN)]),
                payload: vec![],
                text: None,
            },
            Record {
                key: "b".into(),
                timestamp: 2,
                labels: BTreeMap::new(),
                numerics: BTreeMap::from([("x".to_string(), 5.0)]),
                payload: vec![],
                text: None,
            },
        ];
        write_segment(&path, &mut records).unwrap();
        let cols = read_columns(&path).unwrap();
        let rows = cols
            .matching_rows(
                &QuerySpec {
                    numeric_ranges: vec![("x".into(), f64::MIN, f64::MAX)],
                    ..Default::default()
                },
                None,
                &AtomicU64::new(0),
            )
            .unwrap();
        assert_eq!(rows, vec![1]); // NaN row excluded, 5.0 row included
    }

    #[test]
    fn zone_map_prunes_correctly() {
        let records = vec![record("a", 100, "gpt", 5.0), record("b", 200, "gpt", 50.0)];
        let zone = ZoneMap::build(&records);
        assert!(!zone.may_match(&QuerySpec {
            time: Some((300, 400)),
            ..Default::default()
        }));
        assert!(zone.may_match(&QuerySpec {
            time: Some((150, 250)),
            ..Default::default()
        }));
        assert!(!zone.may_match(&QuerySpec {
            labels: vec![("model".into(), "claude".into())],
            ..Default::default()
        }));
        assert!(!zone.may_match(&QuerySpec {
            labels: vec![("nope".into(), "x".into())],
            ..Default::default()
        }));
        assert!(!zone.may_match(&QuerySpec {
            numeric_ranges: vec![("latency_ms".into(), 100.0, 900.0)],
            ..Default::default()
        }));
        assert!(zone.may_match(&QuerySpec {
            numeric_ranges: vec![("latency_ms".into(), 10.0, 60.0)],
            ..Default::default()
        }));
        assert!(!zone.may_match(&QuerySpec {
            key_prefix: Some("z".into()),
            ..Default::default()
        }));
        assert!(zone.may_match(&QuerySpec {
            key_prefix: Some("a".into()),
            ..Default::default()
        }));
    }

    /// K_TEXT is emitted only when some record carries text: a text-less
    /// record set encodes BYTE-IDENTICAL with or without the text machinery
    /// (the section is simply absent), and text round-trips at the segment
    /// level through both whole-file readers.
    #[test]
    fn text_section_only_when_text_present() {
        let no_text: Vec<Record> = (0..5)
            .map(|i| record(&format!("k{i}"), i, "m", 1.0))
            .collect();
        let bytes = encode_v2(&no_text.iter().collect::<Vec<_>>()).unwrap();
        let dir_has_text = {
            // decode footer directly
            let len = bytes.len();
            let footer_off =
                u64::from_le_bytes(bytes[len - 16..len - 8].try_into().unwrap()) as usize;
            let dir: SectionDir = rmp_serde::from_slice(&bytes[footer_off..len - 16]).unwrap();
            dir.entries.iter().any(|e| e.kind == K_TEXT)
        };
        assert!(
            !dir_has_text,
            "text-less segment must have no K_TEXT section"
        );

        let mut with_text = no_text.clone();
        with_text[1].text = Some("Hello, World!".into());
        with_text[3].text = Some(String::new()); // empty ≠ absent
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("t.gird");
        let mut sorted = with_text.clone();
        write_segment(&path, &mut sorted).unwrap();

        // Whole-file record reader (compaction path).
        let back = read_all_records(&path).unwrap();
        assert_eq!(back[1].text.as_deref(), Some("Hello, World!"));
        assert_eq!(back[3].text.as_deref(), Some(""));
        assert_eq!(back[0].text, None);

        // Column view + per-row materialize (scan path).
        let cols = read_columns(&path).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let br = std::sync::atomic::AtomicU64::new(0);
        let r1 = cols.materialize(1, Some(&file), &br, true).unwrap();
        assert_eq!(r1.text.as_deref(), Some("Hello, World!"));
        let r0 = cols.materialize(0, Some(&file), &br, true).unwrap();
        assert_eq!(r0.text, None);
        let r3 = cols.materialize(3, Some(&file), &br, true).unwrap();
        assert_eq!(r3.text.as_deref(), Some(""));
    }

    /// D8 (ruling D8-1′): a mixed corpus — absent, empty, small-raw and
    /// KB-scale texts — round-trips byte-identically through both text read
    /// paths, in ascending AND scattered order. Small rows must be stored
    /// RAW (the compressed bitmap is the proof), big compressible rows
    /// deflated, and an incompressible big row honestly kept raw.
    #[test]
    fn text_v2_per_row_compression_roundtrip() {
        // Pseudo-random incompressible bytes (printable, so utf8-safe).
        let mut state = 0x9e37_79b9u64;
        let mut noise = String::new();
        for _ in 0..2000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            noise.push(char::from(b' ' + (state % 90) as u8));
        }
        let mut records: Vec<Record> = (0..300)
            .map(|i| {
                let text = match i % 5 {
                    0 => None,
                    1 => Some(String::new()),
                    2 => Some(format!("small doc {i}")), // < threshold: raw
                    3 => Some(format!("big {i} {}", "lorem ipsum dolor ".repeat(300))),
                    _ => Some(noise.clone()), // big, high-entropy (mildly compressible)
                };
                Record {
                    key: format!("k/{i:04}"),
                    timestamp: i as i64,
                    labels: BTreeMap::new(),
                    numerics: BTreeMap::new(),
                    payload: vec![1u8; 8],
                    text,
                }
            })
            .collect();
        let want: Vec<Option<String>> = records.iter().map(|r| r.text.clone()).collect();
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("perrow.gird");
        write_segment(&path, &mut records).unwrap();

        let cols = read_columns(&path).unwrap();
        let Some(Texts::File(ti)) = &cols.texts else {
            panic!("expected file-backed text");
        };
        let comp = ti.compressed.as_ref().expect("v2 bitmap");
        assert!(comp.iter().any(|&c| c), "big compressible rows deflated");
        #[allow(clippy::needless_range_loop)] // i%5 drives the corpus shape
        for i in 0..records.len() {
            match i % 5 {
                2 => assert!(!comp[i], "small row {i} must stay raw"),
                3 => assert!(comp[i], "compressible row {i} must be deflated"),
                // Row 4 (high-entropy printable text) deflates only mildly —
                // whichever way the stored-only-if-smaller valve lands, the
                // roundtrip below is the contract. (Truly incompressible
                // UTF-8 barely exists; the valve is for pathological rows.)
                4 => {}
                _ => assert!(!comp[i]),
            }
        }
        let file = std::fs::File::open(&path).unwrap();
        let br = std::sync::atomic::AtomicU64::new(0);
        for (i, w) in want.iter().enumerate() {
            assert_eq!(cols.text_at(i, Some(&file), &br).unwrap(), *w, "row {i}");
        }
        for i in (0..records.len()).rev().step_by(7) {
            assert_eq!(
                cols.text_at(i, Some(&file), &br).unwrap(),
                want[i],
                "row {i}"
            );
        }
        // Whole-file reader (compaction path).
        let back = read_all_records(&path).unwrap();
        for (i, r) in back.iter().enumerate() {
            assert_eq!(r.text, want[i], "row {i}");
        }
    }

    /// D8 fail-closed at the z2 tier: an unknown FUTURE z2 body version is
    /// a loud corrupt error, never a silent no-text read.
    #[test]
    fn text_z2_unknown_future_version_fails_closed() {
        let mut body = Vec::new();
        body.extend_from_slice(&(TEXT_V2_VERSION + 1).to_le_bytes());
        body.extend_from_slice(&[0u8; 32]);
        assert!(decode_text_v2(&body, 4, 0).is_err());
    }

    /// D8 fail-closed: the poison body at the OLD key is a GUARANTEED loud
    /// error through the v1 decoder (what a pre-D8 binary would run) for
    /// any non-empty segment — never a silent no-text read. And the legacy
    /// v1 body itself stays readable forever.
    #[test]
    fn text_poison_fails_v1_loud_and_v1_reads_forever() {
        // A pre-D8 binary decodes (K_TEXT, "") with decode_text_index:
        assert!(
            decode_text_index(TEXT_POISON, 1, 0).is_err(),
            "poison must fail the v1 decoder for count ≥ 1"
        );
        assert!(decode_text_index(TEXT_POISON, 4096, 0).is_err());

        // Hand-encode the exact pre-D8 v1 layout and read it back.
        let texts = [Some("alpha"), None, Some(""), Some("beta gamma")];
        let count = texts.len();
        let mut presence = vec![0u8; count.div_ceil(8)];
        let mut offsets = vec![0u64];
        let mut blob = Vec::new();
        for (i, t) in texts.iter().enumerate() {
            if let Some(t) = t {
                presence[i / 8] |= 1 << (i % 8);
                blob.extend_from_slice(t.as_bytes());
            }
            offsets.push(blob.len() as u64);
        }
        let mut v1 = presence.clone();
        for o in &offsets {
            v1.extend_from_slice(&o.to_le_bytes());
        }
        v1.extend_from_slice(&blob);
        let ti = decode_text_index(&v1, count, 0).unwrap();
        assert!(ti.compressed.is_none(), "v1 = plain blob");
        assert_eq!(ti.present, vec![true, false, true, true]);
        assert_eq!(*ti.rel.last().unwrap(), blob.len() as u64);
    }
}
