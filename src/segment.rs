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
    fn key_at(&self, i: usize) -> &str {
        &self.blob[self.offsets[i] as usize..self.offsets[i + 1] as usize]
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
/// row's utf8 slice lives in the file. Blob bytes are `read_exact_at` per
/// surviving row, mirroring [`PayloadIndex`] — a scan that never
/// materializes a row never reads its text.
pub struct TextIndex {
    abs_base: u64,
    present: Vec<bool>,
    rel: Vec<u64>,
}

impl TextIndex {
    fn bytes(&self) -> u64 {
        self.present.len() as u64 + (self.rel.len() as u64) * 8 + 32
    }
}

/// The token postings index of one segment (K_TOKENS, decoded): for each
/// distinct text token (sorted), the ascending row ids whose text contains
/// it. Lookup is a binary search; a `text_match` is the intersection of its
/// query tokens' postings — exact rows, no post-verification needed, because
/// the tokenizer at encode time IS the query tokenizer (`text::fts_tokens`).
pub struct TokenIndex {
    tokens: Vec<String>,
    postings: Vec<Vec<u32>>,
}

impl TokenIndex {
    fn postings_of(&self, token: &str) -> Option<&[u32]> {
        self.tokens
            .binary_search_by(|t| t.as_str().cmp(token))
            .ok()
            .map(|i| self.postings[i].as_slice())
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
        for (t, p) in self.tokens.iter().zip(&self.postings) {
            n += t.len() as u64 + 24 + (p.len() as u64) * 4 + 24;
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
        TokenIndex { tokens, postings }
    }
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

    /// Row indices matching `spec`, using the block index to skip whole blocks.
    /// Returns empty if any required label/numeric column is absent from the
    /// segment (mirrors `QuerySpec::matches` semantics: absent ⇒ no match).
    pub fn matching_rows(&self, spec: &QuerySpec) -> Vec<u32> {
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

        // Text predicate: resolve through the token postings index. The
        // candidate rows are EXACT for the text predicate (encode-time
        // tokenizer == query tokenizer), so only the other predicates need
        // checking per candidate — and a text query skips the block walk
        // entirely (text is selective; candidates are already few).
        if let Some(q) = &spec.text_match {
            let want = crate::text::fts_tokens(q);
            let Some(idx) = &self.tokens else {
                return Vec::new(); // segment has no text at all
            };
            let mut out: Vec<u32> = Vec::new();
            'cand: for row in idx.rows_matching_all(&want) {
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
    pub fn materialize(
        &self,
        i: usize,
        file: Option<&File>,
        bytes_read: &AtomicU64,
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
        let text = match &self.texts {
            None => None,
            Some(Texts::Mem(v)) => v[i].clone(),
            Some(Texts::File(ti)) => {
                if ti.present[i] {
                    let f = file.ok_or_else(|| corrupt("text file handle missing"))?;
                    let off = ti.abs_base + ti.rel[i];
                    let len = (ti.rel[i + 1] - ti.rel[i]) as usize;
                    let bytes = read_at(f, off, len, bytes_read)?;
                    Some(String::from_utf8(bytes).map_err(|_| corrupt("text not utf8"))?)
                } else {
                    None
                }
            }
        };
        Ok(self.build_record(i, payload, text))
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

/// Encode the K_TEXT section: presence bitmap + zero-based offset table
/// (count+1 entries over ALL rows; absent rows contribute zero length) +
/// concatenated utf8 blob of present texts.
fn encode_texts<R: Borrow<Record>>(records: &[R]) -> Vec<u8> {
    let count = records.len();
    let mut presence = vec![0u8; count.div_ceil(8)];
    let total: usize = records
        .iter()
        .map(|r| r.borrow().text.as_deref().map_or(0, str::len))
        .sum();
    let mut body = Vec::with_capacity(presence.len() + 8 * (count + 1) + total);
    let mut off = 0u64;
    let mut offsets = Vec::with_capacity(count + 1);
    offsets.push(0u64);
    for (i, r) in records.iter().enumerate() {
        if let Some(t) = &r.borrow().text {
            presence[i / 8] |= 1 << (i % 8);
            off += t.len() as u64;
        }
        offsets.push(off);
    }
    body.extend_from_slice(&presence);
    for o in &offsets {
        body.extend_from_slice(&o.to_le_bytes());
    }
    for r in records {
        if let Some(t) = &r.borrow().text {
            body.extend_from_slice(t.as_bytes());
        }
    }
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

/// Encode the K_TOKENS section from a built [`TokenIndex`]:
/// `[u32 ntokens]` then per token `[u32 len][utf8][u32 nrows][deltas…]`
/// where deltas are LEB128: first row id as-is, then successive gaps.
fn encode_tokens(idx: &TokenIndex) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(idx.tokens.len() as u32).to_le_bytes());
    for (t, rows) in idx.tokens.iter().zip(&idx.postings) {
        body.extend_from_slice(&(t.len() as u32).to_le_bytes());
        body.extend_from_slice(t.as_bytes());
        body.extend_from_slice(&(rows.len() as u32).to_le_bytes());
        let mut prev = 0u32;
        for (i, &row) in rows.iter().enumerate() {
            let delta = if i == 0 { row } else { row - prev };
            varint_push(&mut body, delta);
            prev = row;
        }
    }
    body
}

/// Decode a K_TOKENS body. Validates tokens sorted-ascending and row ids
/// strictly ascending within `count`.
fn decode_tokens(body: &[u8], count: usize) -> Result<TokenIndex> {
    let mut c = Cur::new(body);
    let n = c.u32()? as usize;
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
    Ok(TokenIndex { tokens, postings })
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
        push_section(&mut out, &mut dir, K_TEXT, "", &encode_texts(records));
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
                texts = Some(Arc::new(decode_text_index(
                    body,
                    count,
                    (start + 4) as u64,
                )?));
            }
            K_TOKENS => {
                tokens = Some(Arc::new(decode_tokens(body, count)?));
            }
            _ => {} // unknown kind: ignore (forward-compat)
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
    })
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
                        Some(
                            String::from_utf8(slice(&bytes, tfrom, tlen)?.to_vec())
                                .map_err(|_| corrupt("text not utf8"))?,
                        )
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
    let Some(e) = dir.entries.get(&(K_TEXT, String::new())) else {
        return Ok(None);
    };
    let count = dir.count;
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
        let rows = cols.matching_rows(&QuerySpec {
            labels: vec![("model".into(), "gpt-4o".into())],
            numeric_ranges: vec![("latency_ms".into(), 25.0, 100.0)],
            ..Default::default()
        });
        assert_eq!(rows, vec![2]); // only "c" (gpt-4o & latency 30)
        let rec = cols.materialize(2, None, &AtomicU64::new(0)).unwrap();
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
            records.push(Record {
                key: format!("k/{i:06}"),
                timestamp: (rng() % 1_000_000) as i64,
                labels,
                numerics,
                payload: vec![0u8; 4],
                text: None,
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
        ];
        for spec in &specs {
            let mut oracle: Vec<String> = sorted
                .iter()
                .filter(|r| spec.matches(r))
                .map(|r| r.key.clone())
                .collect();
            oracle.sort();
            let mut got: Vec<String> = cols
                .matching_rows(spec)
                .iter()
                .map(|&i| cols.key_at(i as usize).to_string())
                .collect();
            got.sort();
            assert_eq!(got, oracle, "spec {spec:?}");
        }
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
        let rows = cols.matching_rows(&QuerySpec {
            numeric_ranges: vec![("x".into(), f64::MIN, f64::MAX)],
            ..Default::default()
        });
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
        let r1 = cols.materialize(1, Some(&file), &br).unwrap();
        assert_eq!(r1.text.as_deref(), Some("Hello, World!"));
        let r0 = cols.materialize(0, Some(&file), &br).unwrap();
        assert_eq!(r0.text, None);
        let r3 = cols.materialize(3, Some(&file), &br).unwrap();
        assert_eq!(r3.text.as_deref(), Some(""));
    }
}
