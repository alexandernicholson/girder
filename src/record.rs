//! The record model and query spec.
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One stored record. `key` is unique (newest write wins); `timestamp`,
/// `labels` and `numerics` are the *indexed* dimensions (zone maps prune on
/// them); `payload` is opaque to the engine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub key: String,
    /// Primary time dimension (unix nanos).
    pub timestamp: i64,
    /// Low-cardinality dimensions (project, kind, model, status…).
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Numeric dimensions (latency_ms, token_total…).
    #[serde(default)]
    pub numerics: BTreeMap<String, f64>,
    /// Opaque document (e.g. serialized span JSON).
    #[serde(with = "serde_bytes_vec", default)]
    pub payload: Vec<u8>,
}

/// Compact byte-array encoding for msgpack (avoids per-element ints).
mod serde_bytes_vec {
    use serde::{Deserializer, Serializer};
    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(v)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        serde_bytes_like(d)
    }
    fn serde_bytes_like<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Vec<u8>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("bytes")
            }
            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Vec<u8>, E> {
                Ok(v.to_vec())
            }
            fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<Vec<u8>, E> {
                Ok(v)
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Vec<u8>, A::Error> {
                let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(b) = seq.next_element::<u8>()? {
                    out.push(b);
                }
                Ok(out)
            }
        }
        d.deserialize_byte_buf(V)
    }
}

/// Result ordering for `scan`. The tiebreak on every variant is key ascending
/// (matching the historical sort). Records missing the ordered numeric (or
/// holding a NaN) rank *after* all present values, for both directions.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderBy {
    /// Newest first — identical ordering to `order_by: None`.
    TimestampDesc,
    /// Oldest first.
    TimestampAsc,
    /// Highest value of the named numeric first.
    NumericDesc(String),
    /// Lowest value of the named numeric first.
    NumericAsc(String),
}

/// A pruning-friendly query. All conditions AND together; empty spec = scan
/// everything (bounded by `limit`).
#[derive(Debug, Clone, Default)]
pub struct QuerySpec {
    /// Inclusive time range on `timestamp`.
    pub time: Option<(i64, i64)>,
    /// Label equality conditions.
    pub labels: Vec<(String, String)>,
    /// Inclusive numeric ranges: (name, min, max).
    pub numeric_ranges: Vec<(String, f64, f64)>,
    /// Key prefix (e.g. all spans of one trace).
    pub key_prefix: Option<String>,
    /// Max records returned (newest-first). 0 = unlimited.
    pub limit: usize,
    /// Result ordering. `None` ⇒ exactly the historical semantics: timestamp
    /// descending (key ascending tiebreak), truncated after a full sort.
    /// `Some(_)` with `limit > 0` engages a bounded top-k heap that never
    /// materializes the full match set (see `Girder::scan`).
    pub order_by: Option<OrderBy>,
}

impl QuerySpec {
    pub fn matches(&self, record: &Record) -> bool {
        if let Some((lo, hi)) = self.time {
            if record.timestamp < lo || record.timestamp > hi {
                return false;
            }
        }
        if let Some(prefix) = &self.key_prefix {
            if !record.key.starts_with(prefix.as_str()) {
                return false;
            }
        }
        for (k, v) in &self.labels {
            if record.labels.get(k) != Some(v) {
                return false;
            }
        }
        for (name, lo, hi) in &self.numeric_ranges {
            match record.numerics.get(name) {
                Some(v) if v >= lo && v <= hi => {}
                _ => return false,
            }
        }
        true
    }
}
