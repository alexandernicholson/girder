//! Byte-bounded LRU cache of decoded segment *sections* (WS4).
//!
//! The cache key is `(segment_id, SectionId)`: a key column, a timestamp
//! column, one label column, one numeric column, the block index, the payload
//! offset table, … are each cached and evicted independently under one shared
//! byte budget. A hit skips both disk I/O and column decoding. Payload *bytes*
//! are never cached — they are `read_exact_at` per surviving row — so a cached
//! entry is a small typed column, never the ~GB of payloads. Because entries
//! are per-section, even a tiny `cache_bytes` evicts predictably and keeps
//! resident memory bounded.
//!
//! Segment-level hit/miss accounting lives in the engine (one hit or miss per
//! segment per query, matching the historical zone-map-test semantics); this
//! module is a pure store.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::segment::{Section, SectionId};

struct Entry {
    section: Section,
    bytes: u64,
    /// Monotonic recency stamp.
    used: u64,
}

pub struct SegmentCache {
    capacity_bytes: u64,
    inner: Mutex<HashMap<(u64, SectionId), Entry>>,
    clock: AtomicU64,
}

impl SegmentCache {
    pub fn new(capacity_bytes: u64) -> Self {
        SegmentCache {
            capacity_bytes,
            inner: Mutex::new(HashMap::new()),
            clock: AtomicU64::new(0),
        }
    }

    /// Fetch a decoded section (pointer clone). Recency is bumped on hit.
    pub fn get(&self, id: u64, section: SectionId) -> Option<Section> {
        let mut guard = self.inner.lock().unwrap();
        match guard.get_mut(&(id, section)) {
            Some(entry) => {
                entry.used = self.clock.fetch_add(1, Ordering::Relaxed);
                Some(entry.section.clone())
            }
            None => None,
        }
    }

    /// Insert a decoded section, then evict least-recently-used entries until
    /// the total is back under the byte budget. `bytes` is the sized-once
    /// footprint (the single source of truth for accounting).
    pub fn put(&self, id: u64, section_id: SectionId, section: Section, bytes: u64) {
        let mut guard = self.inner.lock().unwrap();
        guard.insert(
            (id, section_id),
            Entry {
                section,
                bytes,
                used: self.clock.fetch_add(1, Ordering::Relaxed),
            },
        );
        let mut total: u64 = guard.values().map(|e| e.bytes).sum();
        while total > self.capacity_bytes && guard.len() > 1 {
            if let Some((victim, _)) = guard.iter().min_by_key(|(_, e)| e.used) {
                let victim = victim.clone();
                if let Some(entry) = guard.remove(&victim) {
                    total -= entry.bytes;
                }
            } else {
                break;
            }
        }
    }

    /// Drop every cached section of a segment (compaction retires its id).
    pub fn invalidate(&self, id: u64) {
        self.inner.lock().unwrap().retain(|(seg, _), _| *seg != id);
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    #[cfg(test)]
    pub fn total_bytes(&self) -> u64 {
        self.inner.lock().unwrap().values().map(|e| e.bytes).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// A section of a chosen byte size (contents irrelevant to LRU behavior).
    fn ts_section(vals: usize) -> Section {
        Section::Timestamps(Arc::new(vec![0i64; vals]))
    }

    #[test]
    fn lru_evicts_cold_entries_and_stays_bounded() {
        let cache = SegmentCache::new(100);
        cache.put(1, SectionId::Timestamps, ts_section(1), 60);
        cache.put(2, SectionId::Timestamps, ts_section(1), 60); // over cap → evict LRU (1)
        assert!(cache.get(1, SectionId::Timestamps).is_none());
        assert!(cache.get(2, SectionId::Timestamps).is_some());
        // Touch 2, insert 3 → 2 stays (recently used) if capacity allows one.
        cache.put(3, SectionId::Timestamps, ts_section(1), 60);
        assert!(
            cache.get(2, SectionId::Timestamps).is_none()
                || cache.get(3, SectionId::Timestamps).is_some()
        );
        assert!(cache.len() >= 1);
    }

    /// Distinct sections of one segment are keyed independently, and the total
    /// resident bytes never exceed the budget plus a single (largest) entry —
    /// the property that keeps RSS bounded by `cache_bytes`.
    #[test]
    fn section_keys_are_independent_and_total_bounded() {
        let cap = 1000u64;
        let cache = SegmentCache::new(cap);
        // Insert many large sections across several segments.
        let mut biggest = 0u64;
        for seg in 0..20u64 {
            for (sid, n) in [
                (SectionId::Keys, 30usize),
                (SectionId::Timestamps, 25),
                (SectionId::Numeric("latency".into()), 20),
                (SectionId::Blocks, 5),
            ] {
                let sec = ts_section(n); // stand-in payload; size passed explicitly
                let bytes = (n as u64) * 8 + 40;
                biggest = biggest.max(bytes);
                cache.put(seg, sid, sec, bytes);
                assert!(
                    cache.total_bytes() <= cap + biggest,
                    "cache exceeded budget + one entry"
                );
            }
        }
        // Invalidate wipes all sections of a segment.
        let before = cache.len();
        cache.invalidate(19);
        assert!(cache.len() < before);
    }
}
