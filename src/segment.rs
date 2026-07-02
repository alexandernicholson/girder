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
//! **Format v1 (legacy)** was `[magic][version=1][crc32(body)][rmp(Vec<Record>)]`.
//! It stays fully readable via a compat shim (`read_columns` decodes it into the
//! same in-memory column set); the version word dispatches. v1 files are
//! rewritten to v2 by the first compaction that touches them (WS3).
//!
//! The zone map is stored in the manifest (not the file), so the manifest
//! format does not migrate.
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::path::Path;

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
const K_KEYS: u8 = 0;
const K_TS: u8 = 1;
const K_LABEL: u8 = 2;
const K_NUMERIC: u8 = 3;
const K_PAYLOAD: u8 = 4;
const K_BLOCKS: u8 = 5;

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
    pub fn build(records: &[Record]) -> ZoneMap {
        let mut zone = ZoneMap {
            min_ts: i64::MAX,
            max_ts: i64::MIN,
            min_key: String::new(),
            max_key: String::new(),
            labels: BTreeMap::new(),
            numerics: BTreeMap::new(),
            count: records.len(),
        };
        for (i, record) in records.iter().enumerate() {
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
struct BlockMeta {
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
// Decoded column set (what the cache holds; payload bytes excluded for v2)
// ---------------------------------------------------------------------------

/// A decoded label column.
enum LabelColumn {
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
struct NumericColumn {
    present: Vec<bool>,
    values: Vec<f64>,
}

/// How row payloads are sourced when materializing a `Record`.
enum Payloads {
    /// v2: raw bytes live in the file; `rel[i]..rel[i+1]` are offsets into the
    /// payload blob whose absolute file offset is `abs_base`.
    File { abs_base: u64, rel: Vec<u64> },
    /// v1 compat: owned payload bytes in memory.
    Mem(Vec<Vec<u8>>),
}

/// A segment decoded into typed columns. Filters run over these; payloads are
/// sliced only for surviving rows. Cheaply cloneable via `Arc` in the cache.
pub struct SegmentColumns {
    count: usize,
    keys: String,          // concatenated, key-sorted
    key_offsets: Vec<u64>, // len count+1, byte offsets into `keys`
    timestamps: Vec<i64>,
    labels: BTreeMap<String, LabelColumn>,
    numerics: BTreeMap<String, NumericColumn>,
    blocks: Vec<BlockMeta>,
    payloads: Payloads,
}

impl SegmentColumns {
    pub fn count(&self) -> usize {
        self.count
    }

    #[inline]
    pub fn key_at(&self, i: usize) -> &str {
        &self.keys[self.key_offsets[i] as usize..self.key_offsets[i + 1] as usize]
    }

    /// True if materializing a row needs the segment file open (v2 payloads).
    pub fn payload_needs_file(&self) -> bool {
        matches!(self.payloads, Payloads::File { .. })
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

    /// Estimated in-memory footprint, for cache accounting.
    pub fn heap_bytes(&self) -> u64 {
        let mut n = self.keys.len() as u64 + (self.key_offsets.len() as u64) * 8;
        n += (self.timestamps.len() as u64) * 8;
        for col in self.labels.values() {
            match col {
                LabelColumn::Dict { dict, codes, .. } => {
                    n += (codes.len() as u64) * 2;
                    for s in dict {
                        n += s.len() as u64 + 24;
                    }
                }
                LabelColumn::Plain { values } => {
                    for v in values {
                        n += v.as_ref().map(|s| s.len() as u64 + 24).unwrap_or(8);
                    }
                }
            }
        }
        for col in self.numerics.values() {
            n += col.present.len() as u64 + (col.values.len() as u64) * 8;
        }
        match &self.payloads {
            Payloads::File { rel, .. } => n += (rel.len() as u64) * 8,
            Payloads::Mem(v) => {
                for p in v {
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
                Some(LabelColumn::Dict { code_of, codes, .. }) => match code_of.get(val) {
                    Some(&c) => {
                        lreqs.push(LReq::Code(codes, c));
                        if c <= BLOCK_BITSET_CODES {
                            dict_prune.push((name.as_str(), c));
                        }
                    }
                    None => return Vec::new(),
                },
                Some(LabelColumn::Plain { values }) => {
                    lreqs.push(LReq::Plain(values, val.as_str()))
                }
            }
        }
        let mut nreqs: Vec<(&NumericColumn, f64, f64)> =
            Vec::with_capacity(spec.numeric_ranges.len());
        for (name, lo, hi) in &spec.numeric_ranges {
            match self.numerics.get(name) {
                None => return Vec::new(),
                Some(nc) => nreqs.push((nc, *lo, *hi)),
            }
        }
        let prefix = spec.key_prefix.as_deref();
        let time = spec.time;

        let mut out: Vec<u32> = Vec::new();
        for block in &self.blocks {
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
    /// segment file) when `payload_needs_file()` is true.
    pub fn materialize(&self, i: usize, file: Option<&File>) -> Result<Record> {
        let payload = match &self.payloads {
            Payloads::Mem(v) => v[i].clone(),
            Payloads::File { abs_base, rel } => {
                let f = file.ok_or_else(|| corrupt("payload file handle missing"))?;
                let off = abs_base + rel[i];
                let len = (rel[i + 1] - rel[i]) as usize;
                read_at(f, off, len)?
            }
        };
        Ok(self.build_record(i, payload))
    }

    fn build_record(&self, i: usize, payload: Vec<u8>) -> Record {
        let mut labels = BTreeMap::new();
        for (name, col) in &self.labels {
            match col {
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
        }
    }

    /// v1-compat / in-memory constructor: build columns from decoded records.
    pub fn from_records(records: Vec<Record>) -> SegmentColumns {
        let count = records.len();
        let mut keys = String::new();
        let mut key_offsets = Vec::with_capacity(count + 1);
        key_offsets.push(0u64);
        for r in &records {
            keys.push_str(&r.key);
            key_offsets.push(keys.len() as u64);
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
            labels.insert(plan.name.clone(), col);
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
            numerics.insert(name, NumericColumn { present, values });
        }

        let blocks = build_blocks(&records, &plans);
        let payloads = Payloads::Mem(records.into_iter().map(|r| r.payload).collect());
        SegmentColumns {
            count,
            keys,
            key_offsets,
            timestamps,
            labels,
            numerics,
            blocks,
            payloads,
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

fn label_names(records: &[Record]) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for r in records {
        for k in r.labels.keys() {
            names.insert(k.clone());
        }
    }
    names
}

fn numeric_names(records: &[Record]) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for r in records {
        for k in r.numerics.keys() {
            names.insert(k.clone());
        }
    }
    names
}

fn plan_labels(records: &[Record]) -> Vec<LabelPlan> {
    let mut plans = Vec::new();
    for name in label_names(records) {
        let mut distinct: BTreeSet<&str> = BTreeSet::new();
        for r in records {
            if let Some(v) = r.labels.get(&name) {
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
                .map(|r| r.labels.get(&name).map(|v| index[v.as_str()]).unwrap_or(0))
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

fn build_blocks(records: &[Record], plans: &[LabelPlan]) -> Vec<BlockMeta> {
    let count = records.len();
    let mut blocks = Vec::new();
    let mut s = 0;
    while s < count {
        let e = (s + BLOCK_ROWS).min(count);
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        let mut numerics: BTreeMap<String, (f64, f64)> = BTreeMap::new();
        for r in &records[s..e] {
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
            first_key: records[s].key.clone(),
            last_key: records[e - 1].key.clone(),
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

fn encode_label_column(records: &[Record], plan: &LabelPlan) -> Vec<u8> {
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
                if let Some(v) = r.labels.get(&plan.name) {
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

fn encode_numeric_column(records: &[Record], name: &str) -> Vec<u8> {
    let count = records.len();
    let mut presence = vec![0u8; count.div_ceil(8)];
    let mut vals: Vec<f64> = Vec::new();
    for (i, r) in records.iter().enumerate() {
        if let Some(v) = r.numerics.get(name) {
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

fn encode_payloads(records: &[Record]) -> Vec<u8> {
    let count = records.len();
    let total: usize = records.iter().map(|r| r.payload.len()).sum();
    let mut body = Vec::with_capacity(8 * (count + 1) + total);
    let mut off = 0u64;
    body.extend_from_slice(&off.to_le_bytes());
    for r in records {
        off += r.payload.len() as u64;
        body.extend_from_slice(&off.to_le_bytes());
    }
    for r in records {
        body.extend_from_slice(&r.payload);
    }
    body
}

fn encode_v2(records: &[Record]) -> Result<Vec<u8>> {
    let count = records.len();
    let plans = plan_labels(records);
    let mut out = Vec::new();
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.extend_from_slice(&VERSION_V2.to_le_bytes());
    let mut dir: Vec<SectionEntry> = Vec::new();

    let key_items: Vec<&[u8]> = records.iter().map(|r| r.key.as_bytes()).collect();
    push_section(&mut out, &mut dir, K_KEYS, "", &encode_strings(&key_items));

    let mut ts = Vec::with_capacity(count * 8);
    for r in records {
        ts.extend_from_slice(&r.timestamp.to_le_bytes());
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

/// Write records (sorted by key) to a v2 segment file. Returns the zone map.
pub fn write_segment(path: &Path, records: &mut [Record]) -> Result<ZoneMap> {
    records.sort_by(|a, b| a.key.cmp(&b.key));
    let zone = ZoneMap::build(records);
    let out = encode_v2(records)?;
    // Atomic-ish: write tmp, fsync, rename.
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

fn slice(bytes: &[u8], from: usize, len: usize) -> Result<&[u8]> {
    let end = from
        .checked_add(len)
        .ok_or_else(|| corrupt("offset overflow"))?;
    if end > bytes.len() {
        return Err(corrupt("section out of bounds"));
    }
    Ok(&bytes[from..end])
}

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

    let mut keys: Option<(String, Vec<u64>)> = None;
    let mut timestamps: Option<Vec<i64>> = None;
    let mut labels: BTreeMap<String, LabelColumn> = BTreeMap::new();
    let mut numerics: BTreeMap<String, NumericColumn> = BTreeMap::new();
    let mut blocks: Option<Vec<BlockMeta>> = None;
    let mut payloads: Option<Payloads> = None;

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
            K_KEYS => keys = Some(decode_strings(body)?),
            K_TS => {
                if body.len() != count * 8 {
                    return Err(corrupt("timestamp column size mismatch"));
                }
                let mut c = Cur::new(body);
                let mut v = Vec::with_capacity(count);
                for _ in 0..count {
                    v.push(c.i64()?);
                }
                timestamps = Some(v);
            }
            K_LABEL => {
                labels.insert(e.name.clone(), decode_label_column(body, count)?);
            }
            K_NUMERIC => {
                numerics.insert(e.name.clone(), decode_numeric_column(body, count)?);
            }
            K_PAYLOAD => {
                let mut c = Cur::new(body);
                let mut rel = Vec::with_capacity(count + 1);
                for _ in 0..count + 1 {
                    rel.push(c.u64()?);
                }
                let abs_base = (start + 4 + 8 * (count + 1)) as u64;
                payloads = Some(Payloads::File { abs_base, rel });
            }
            K_BLOCKS => {
                blocks =
                    Some(rmp_serde::from_slice(body).map_err(|e| corrupt(format!("blocks: {e}")))?);
            }
            _ => {} // unknown kind: ignore (forward-compat)
        }
    }

    let (keys, key_offsets) = keys.ok_or_else(|| corrupt("missing key section"))?;
    let timestamps = timestamps.ok_or_else(|| corrupt("missing timestamp section"))?;
    let payloads = payloads.ok_or_else(|| corrupt("missing payload section"))?;
    let blocks = blocks.ok_or_else(|| corrupt("missing block index"))?;
    if key_offsets.len() != count + 1 || timestamps.len() != count {
        return Err(corrupt("column count mismatch"));
    }
    Ok(SegmentColumns {
        count,
        keys,
        key_offsets,
        timestamps,
        labels,
        numerics,
        blocks,
        payloads,
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

/// Read + verify a segment file into typed columns (payloads for v2 are read
/// lazily from the file). Dispatches on the version word; v1 files are decoded
/// via the compat shim.
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
            let Payloads::File { abs_base, rel } = &cols.payloads else {
                return Err(corrupt("v2 payloads not file-backed"));
            };
            let mut out = Vec::with_capacity(cols.count);
            for i in 0..cols.count {
                let from = (abs_base + rel[i]) as usize;
                let plen = (rel[i + 1] - rel[i]) as usize;
                let payload = slice(&bytes, from, plen)?.to_vec();
                out.push(cols.build_record(i, payload));
            }
            Ok(out)
        }
        other => Err(corrupt(format!("unsupported version {other}"))),
    }
}

/// Read `len` bytes at absolute `offset`. Safe positioned read (no mmap, no
/// unsafe): `FileExt::read_exact_at` on unix, seek+read fallback elsewhere.
fn read_at(file: &File, offset: u64, len: usize) -> Result<Vec<u8>> {
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
    Ok(buf)
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
        let rec = cols.materialize(2, None).unwrap();
        assert_eq!(rec.key, "c");
        assert_eq!(rec.payload, b"p-c");
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
            },
            Record {
                key: "b".into(),
                timestamp: 2,
                labels: BTreeMap::new(),
                numerics: BTreeMap::from([("x".to_string(), 5.0)]),
                payload: vec![],
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
}
