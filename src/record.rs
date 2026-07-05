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
    /// Caller-supplied searchable text (the FTS document for this record —
    /// e.g. extracted message content). The engine tokenizes it per
    /// [`crate::text::fts_tokens`] into the segment token index; the payload
    /// stays opaque. `None` = not searchable (absent ≠ empty string).
    ///
    /// Serde: `skip_serializing_if` keeps text-less records byte-identical
    /// to the pre-text wire format, and `default` keeps every pre-text WAL
    /// frame / v1 segment readable (pinned by `text_field_wire_compat`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
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
    /// Full-text predicate over `Record.text`: every token of this query
    /// (per [`crate::text::fts_tokens`]) must appear among the record's text
    /// tokens — AND semantics, case-insensitive, exact token equality. A
    /// query with no tokens matches nothing; a record without text never
    /// matches. Served from the segment token index; [`QuerySpec::matches`]
    /// is the naive-scan oracle the index must agree with.
    pub text_match: Option<String>,
}

impl QuerySpec {
    /// The full predicate — the naive-scan ORACLE (fields + text). Backends
    /// of the text predicate (segment token index, memtable token map) must
    /// agree with this exactly.
    pub fn matches(&self, record: &Record) -> bool {
        if !self.matches_fields(record) {
            return false;
        }
        match &self.text_match {
            None => true,
            Some(q) => {
                let want = crate::text::fts_tokens(q);
                crate::text::text_contains_all(record.text.as_deref(), &want)
            }
        }
    }

    /// Every predicate EXCEPT text (used by callers that pre-resolve the
    /// text predicate through a token structure).
    pub fn matches_fields(&self, record: &Record) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact pre-text `Record` wire shape (what every WAL frame and v1
    /// segment written before the text field contained).
    #[derive(Serialize)]
    struct RecordV0<'a> {
        key: &'a str,
        timestamp: i64,
        labels: &'a BTreeMap<String, String>,
        numerics: &'a BTreeMap<String, f64>,
        #[serde(with = "ser_bytes")]
        payload: &'a [u8],
    }
    mod ser_bytes {
        use serde::Serializer;
        pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
            s.serialize_bytes(v)
        }
    }

    /// Wire compat, both directions (pins the `Record.text` serde contract):
    /// pre-text bytes decode with `text: None`, and a text-less record
    /// serializes byte-identical to the pre-text format — so introducing the
    /// field changed NOTHING on disk until a caller actually supplies text.
    #[test]
    fn text_field_wire_compat() {
        let labels = BTreeMap::from([("model".to_string(), "gpt-4o".to_string())]);
        let numerics = BTreeMap::from([("latency_ms".to_string(), 42.0)]);
        let old_bytes = rmp_serde::to_vec(&RecordV0 {
            key: "k1",
            timestamp: 123,
            labels: &labels,
            numerics: &numerics,
            payload: b"span-json",
        })
        .unwrap();

        // Old bytes → new struct: text defaults to None.
        let decoded: Record = rmp_serde::from_slice(&old_bytes).unwrap();
        assert_eq!(decoded.text, None);
        assert_eq!(decoded.key, "k1");
        assert_eq!(decoded.payload, b"span-json");

        // New struct without text → byte-identical to the old format.
        let new_bytes = rmp_serde::to_vec(&decoded).unwrap();
        assert_eq!(
            new_bytes, old_bytes,
            "text-less record must not change the wire format"
        );

        // With text → longer form that round-trips.
        let mut with_text = decoded.clone();
        with_text.text = Some("hello world".into());
        let bytes = rmp_serde::to_vec(&with_text).unwrap();
        assert_ne!(bytes, old_bytes);
        let back: Record = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(back, with_text);

        // Empty-string text is preserved as Some("") — absent ≠ empty.
        let mut empty_text = decoded.clone();
        empty_text.text = Some(String::new());
        let back: Record = rmp_serde::from_slice(&rmp_serde::to_vec(&empty_text).unwrap()).unwrap();
        assert_eq!(back.text, Some(String::new()));
    }
}
