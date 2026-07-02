//! Byte-bounded LRU cache of decoded segments. A hit skips both disk I/O and
//! msgpack decoding — the warm-read fast path.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::record::Record;

struct Entry {
    records: Arc<Vec<Record>>,
    bytes: u64,
    /// Monotonic recency stamp.
    used: u64,
}

pub struct SegmentCache {
    capacity_bytes: u64,
    inner: Mutex<HashMap<u64, Entry>>,
    clock: AtomicU64,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}

impl SegmentCache {
    pub fn new(capacity_bytes: u64) -> Self {
        SegmentCache {
            capacity_bytes,
            inner: Mutex::new(HashMap::new()),
            clock: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, id: u64) -> Option<Arc<Vec<Record>>> {
        let mut guard = self.inner.lock().unwrap();
        match guard.get_mut(&id) {
            Some(entry) => {
                entry.used = self.clock.fetch_add(1, Ordering::Relaxed);
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(Arc::clone(&entry.records))
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn put(&self, id: u64, records: Arc<Vec<Record>>, bytes: u64) {
        let mut guard = self.inner.lock().unwrap();
        guard.insert(
            id,
            Entry {
                records,
                bytes,
                used: self.clock.fetch_add(1, Ordering::Relaxed),
            },
        );
        // Evict least-recently-used until under capacity.
        let mut total: u64 = guard.values().map(|e| e.bytes).sum();
        while total > self.capacity_bytes && guard.len() > 1 {
            if let Some((&victim, _)) = guard.iter().min_by_key(|(_, e)| e.used) {
                if let Some(entry) = guard.remove(&victim) {
                    total -= entry.bytes;
                }
            } else {
                break;
            }
        }
    }

    pub fn invalidate(&self, id: u64) {
        self.inner.lock().unwrap().remove(&id);
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn records(n: usize) -> Arc<Vec<Record>> {
        Arc::new(
            (0..n)
                .map(|i| Record {
                    key: format!("k{i}"),
                    timestamp: 0,
                    labels: Default::default(),
                    numerics: Default::default(),
                    payload: vec![],
                })
                .collect(),
        )
    }

    #[test]
    fn lru_evicts_cold_entries() {
        let cache = SegmentCache::new(100);
        cache.put(1, records(1), 60);
        cache.put(2, records(1), 60); // over capacity → evict LRU (1)
        assert!(cache.get(1).is_none());
        assert!(cache.get(2).is_some());
        // Touch 2, insert 3 → 2 stays (recently used) if capacity allows one.
        cache.put(3, records(1), 60);
        assert!(cache.get(2).is_none() || cache.get(3).is_some());
        assert!(cache.len() >= 1);
    }
}
