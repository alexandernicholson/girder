//! The remote (object-storage) tier (SCALE-1, docs/SCALE.md). Contract pinned
//! here: a cold segment past `remote_ttl` moves cold→remote (PUT→flip→delete),
//! reads fetch it back transparently through a bounded pull cache, retention
//! deletes the object, and a crash mid-move leaves the segment readable at
//! every step (reconciled at open). A `None` store is byte-identical to a
//! two-tier engine.
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use girder::{FsyncPolicy, Girder, GirderConfig, ObjectStore, QuerySpec, Record, Result};

/// In-memory object store that counts gets (to prove pull-cache hits/misses)
/// and is inspectable by the test.
#[derive(Default)]
struct MemStore {
    objects: Mutex<HashMap<String, Vec<u8>>>,
    gets: AtomicU64,
}
impl MemStore {
    fn keys(&self) -> Vec<String> {
        let mut k: Vec<String> = self.objects.lock().unwrap().keys().cloned().collect();
        k.sort();
        k
    }
    fn contains(&self, key: &str) -> bool {
        self.objects.lock().unwrap().contains_key(key)
    }
}
impl ObjectStore for MemStore {
    fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        self.objects.lock().unwrap().insert(key.to_string(), bytes);
        Ok(())
    }
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }
    fn delete(&self, key: &str) -> Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }
}

fn record(key: &str, ts: i64, text: &str) -> Record {
    Record {
        key: key.to_string(),
        timestamp: ts,
        labels: BTreeMap::from([("m".to_string(), "x".to_string())]),
        numerics: BTreeMap::new(),
        payload: format!("payload-{key}").into_bytes(),
        text: Some(text.to_string()),
    }
}

/// Config with aggressive tiering: hot_ttl=0 (everything ages to cold at once)
/// and remote_ttl as given. Manual ticks via `maintain()`.
fn config(dir: &std::path::Path, remote_ttl_nanos: i64, pull_cache_bytes: u64) -> GirderConfig {
    let mut c = GirderConfig::at(dir);
    c.fsync = FsyncPolicy::EveryN(64);
    c.memtable_max_records = 10_000;
    c.compact_min_segments = 1_000_000; // keep compaction out of the way
    c.tick_interval = Duration::from_secs(3600);
    c.hot_ttl_nanos = 0; // hot→cold on the first tick
    c.remote_ttl_nanos = remote_ttl_nanos;
    c.pull_cache_bytes = pull_cache_bytes;
    c
}

async fn keys_of(engine: &Girder, spec: &QuerySpec) -> Vec<String> {
    let mut v: Vec<String> = engine
        .scan(spec)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.key)
        .collect();
    v.sort();
    v
}

/// A cold segment past remote_ttl moves cold→remote across two ticks (never
/// skipping cold), the object lands in the store, the local file is gone, and
/// scans read it back transparently.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_to_remote_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(MemStore::default());
    let engine = Girder::open_with_object_store(config(dir.path(), 0, 1 << 30), store.clone())
        .await
        .unwrap();
    for i in 0..50 {
        engine
            .put(record(&format!("k/{i:03}"), i, "hello remote tier"))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    let all = keys_of(&engine, &QuerySpec::default()).await;
    assert_eq!(all.len(), 50);

    // Tick 1: hot→cold. Tick 2: cold→remote (never skips the cold hop).
    engine.maintain().await.unwrap();
    assert_eq!(engine.stats().cold_segments, 1, "hot→cold on tick 1");
    assert_eq!(engine.stats().remote_segments, 0, "not remote yet");
    engine.maintain().await.unwrap();
    let s = engine.stats();
    assert_eq!(s.remote_segments, 1, "cold→remote on tick 2");
    assert_eq!(s.cold_segments, 0);

    // The object is in the store; scans read it back identically.
    assert_eq!(store.keys().len(), 1, "one segment object uploaded");
    assert_eq!(keys_of(&engine, &QuerySpec::default()).await, all);
    // A predicate scan through the remote segment is exact too.
    let spec = QuerySpec {
        text_like: Some("%remote%".into()),
        ..Default::default()
    };
    assert_eq!(engine.scan(&spec).await.unwrap().len(), 50);
    assert!(
        store.gets.load(Ordering::Relaxed) >= 1,
        "read fetched via pull"
    );
}

/// `None` store ⇒ segments stop at cold, never go remote (byte-identical to a
/// two-tier engine even with remote_ttl set).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_store_never_reaches_remote() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = config(dir.path(), 0, 1 << 30);
    c.hot_ttl_nanos = 0;
    let engine = Girder::open(c).await.unwrap();
    for i in 0..20 {
        engine
            .put(record(&format!("k/{i:03}"), i, "t"))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    engine.maintain().await.unwrap();
    engine.maintain().await.unwrap();
    let s = engine.stats();
    assert_eq!(s.remote_segments, 0, "no store ⇒ never remote");
    assert_eq!(s.cold_segments, 1, "stops at cold");
    assert_eq!(keys_of(&engine, &QuerySpec::default()).await.len(), 20);
}

/// The pull cache holds the resident set within budget, but an oversized
/// segment (larger than the whole budget) is STILL fetchable — budget-plus-one,
/// never refuse a read (docs/SCALE.md §3.4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pull_cache_oversized_entry_is_still_served() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(MemStore::default());
    // A tiny pull budget — every real segment will exceed it.
    let engine = Girder::open_with_object_store(config(dir.path(), 0, 1024), store.clone())
        .await
        .unwrap();
    let big = "x".repeat(4096);
    for i in 0..30 {
        engine
            .put(record(&format!("k/{i:03}"), i, &big))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    engine.maintain().await.unwrap(); // hot→cold
    engine.maintain().await.unwrap(); // cold→remote
    assert_eq!(engine.stats().remote_segments, 1);

    // The segment far exceeds pull_cache_bytes=1024, yet the read succeeds
    // (budget-plus-one: hold the one oversized entry rather than refuse).
    let got = engine.scan(&QuerySpec::default()).await.unwrap();
    assert_eq!(got.len(), 30, "oversized segment still fully served");
    // The pull dir holds exactly the one (oversized) entry.
    let pull = dir.path().join("pull");
    let resident = std::fs::read_dir(&pull)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| e.path().is_file())
                .count()
        })
        .unwrap_or(0);
    assert!(
        resident <= 1,
        "pull cache holds at most the one entry, got {resident}"
    );
}

/// An object present in the store but named by NO live manifest entry is
/// orphan residue of a crash mid retention-drop — reconciled (deleted) at open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_object_reconciled_at_open() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(MemStore::default());
    // Seed an orphan object that no manifest will reference.
    store.put("seg-999999.gird", vec![1, 2, 3]).unwrap();
    assert!(store.contains("seg-999999.gird"));

    let engine = Girder::open_with_object_store(config(dir.path(), 0, 1 << 30), store.clone())
        .await
        .unwrap();
    // Open reconciled the orphan (no live Remote entry names it).
    assert!(
        !store.contains("seg-999999.gird"),
        "orphan object must be reaped at open"
    );
    drop(engine);
}

/// Crash after the manifest flip but before the local delete: a stale cold
/// file coexists with the remote object. Reads still work (prefer the store),
/// and the stale local file is reaped at the next open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flip_before_delete_residue_reaped_and_readable() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(MemStore::default());
    let engine = Girder::open_with_object_store(config(dir.path(), 0, 1 << 30), store.clone())
        .await
        .unwrap();
    for i in 0..20 {
        engine
            .put(record(&format!("k/{i:03}"), i, "t"))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    engine.maintain().await.unwrap();
    engine.maintain().await.unwrap();
    assert_eq!(engine.stats().remote_segments, 1);
    let object_key = store.keys()[0].clone();
    engine.close().await.unwrap();

    // Simulate the flip-before-delete crash: recreate a stale cold copy.
    let cold_file = dir.path().join("cold").join(&object_key);
    std::fs::write(&cold_file, b"stale-residue").unwrap();
    assert!(cold_file.exists());

    // Reopen: reconcile reaps the stale local file; reads still return all rows.
    let engine = Girder::open_with_object_store(config(dir.path(), 0, 1 << 30), store.clone())
        .await
        .unwrap();
    assert!(!cold_file.exists(), "stale cold residue reaped at open");
    assert_eq!(keys_of(&engine, &QuerySpec::default()).await.len(), 20);
}
