//! Immutable sorted segments with zone maps.
//!
//! File format: `[u32 magic][u32 version][u32 crc32(body)][body: rmp(Vec<Record>)]`.
//! Records inside a segment are sorted by key; the zone map (stored in the
//! manifest, not the file) lets queries skip whole segments without touching
//! disk.
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{GirderError, Result};
use crate::record::{QuerySpec, Record};

const MAGIC: u32 = 0x6769_7264; // "gird"
const VERSION: u32 = 1;
/// Cap on tracked distinct values per label; beyond it the label is treated
/// as unprunable (high cardinality).
const LABEL_VALUES_CAP: usize = 64;

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
            // Segment key range must overlap [prefix, prefix~).
            if self.max_key.as_str() < prefix.as_str() {
                return false;
            }
            // min_key > every key with this prefix iff min_key doesn't start
            // with prefix and is greater than prefix.
            if !self.min_key.starts_with(prefix.as_str()) && self.min_key.as_str() > prefix.as_str()
            {
                // Check the smallest possible prefixed key upper bound.
                let mut upper = prefix.clone();
                upper.push(char::MAX);
                if self.min_key.as_str() > upper.as_str() {
                    return false;
                }
            }
        }
        for (name, value) in &spec.labels {
            match self.labels.get(name) {
                // Label absent from every record in this segment → no match.
                None => return false,
                // Tracked values and the wanted one isn't there → skip.
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

/// Write records (sorted by key) to a segment file. Returns the zone map.
pub fn write_segment(path: &Path, records: &mut Vec<Record>) -> Result<ZoneMap> {
    records.sort_by(|a, b| a.key.cmp(&b.key));
    let body = rmp_serde::to_vec(&records).map_err(|e| GirderError::Encode(e.to_string()))?;
    let crc = crc32fast::hash(&body);
    let mut out = Vec::with_capacity(body.len() + 12);
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&body);
    // Atomic-ish: write tmp, fsync, rename.
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, &out)?;
    let file = std::fs::File::open(&tmp)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, path)?;
    Ok(ZoneMap::build(records))
}

/// Read + verify a segment file.
pub fn read_segment(path: &Path) -> Result<Vec<Record>> {
    let bytes = std::fs::read(path)?;
    if bytes.len() < 12 {
        return Err(GirderError::Corrupt {
            what: "segment",
            detail: format!("{path:?}: too short"),
        });
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let crc = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    if magic != MAGIC {
        return Err(GirderError::Corrupt {
            what: "segment",
            detail: format!("{path:?}: bad magic"),
        });
    }
    if version != VERSION {
        return Err(GirderError::Corrupt {
            what: "segment",
            detail: format!("{path:?}: unsupported version {version}"),
        });
    }
    let body = &bytes[12..];
    if crc32fast::hash(body) != crc {
        return Err(GirderError::Corrupt {
            what: "segment",
            detail: format!("{path:?}: crc mismatch"),
        });
    }
    rmp_serde::from_slice(body).map_err(|e| GirderError::Corrupt {
        what: "segment",
        detail: format!("{path:?}: {e}"),
    })
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
            payload: vec![0; 16],
        }
    }

    #[test]
    fn roundtrip_and_crc_guard() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s1.seg");
        let mut records = vec![record("b", 2, "gpt", 5.0), record("a", 1, "claude", 9.0)];
        let zone = write_segment(&path, &mut records).unwrap();
        assert_eq!((zone.min_ts, zone.max_ts), (1, 2));
        assert_eq!(zone.min_key, "a");
        let loaded = read_segment(&path).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].key, "a"); // sorted

        // Corrupt one byte → read fails loudly.
        let mut bytes = std::fs::read(&path).unwrap();
        let n = bytes.len();
        bytes[n - 1] ^= 0xFF;
        std::fs::write(&path, bytes).unwrap();
        assert!(read_segment(&path).is_err());
    }

    #[test]
    fn zone_map_prunes_correctly() {
        let records = vec![record("a", 100, "gpt", 5.0), record("b", 200, "gpt", 50.0)];
        let zone = ZoneMap::build(&records);

        // Time pruning.
        assert!(!zone.may_match(&QuerySpec { time: Some((300, 400)), ..Default::default() }));
        assert!(zone.may_match(&QuerySpec { time: Some((150, 250)), ..Default::default() }));
        // Label pruning.
        assert!(!zone.may_match(&QuerySpec {
            labels: vec![("model".into(), "claude".into())],
            ..Default::default()
        }));
        assert!(!zone.may_match(&QuerySpec {
            labels: vec![("nope".into(), "x".into())],
            ..Default::default()
        }));
        // Numeric pruning.
        assert!(!zone.may_match(&QuerySpec {
            numeric_ranges: vec![("latency_ms".into(), 100.0, 900.0)],
            ..Default::default()
        }));
        assert!(zone.may_match(&QuerySpec {
            numeric_ranges: vec![("latency_ms".into(), 10.0, 60.0)],
            ..Default::default()
        }));
        // Key-prefix pruning.
        assert!(!zone.may_match(&QuerySpec { key_prefix: Some("z".into()), ..Default::default() }));
        assert!(zone.may_match(&QuerySpec { key_prefix: Some("a".into()), ..Default::default() }));
    }
}
