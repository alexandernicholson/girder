//! `Girder::purge_key` (D-3 heal): physically remove one key — record,
//! tombstone AND zone-map footprint — via a targeted compaction. Built for
//! meta keys that leaked into a zone-mapped data keyspace (docs/COMPAT.md:
//! "never mix meta keys into a zone-mapped data keyspace"): one such key
//! with a spanning range or wall-clock timestamp defeats disjointness
//! and time pruning for every scan.
use std::collections::BTreeMap;
use std::time::Duration;

use girder::{FsyncPolicy, Girder, GirderConfig, QuerySpec, Record};

fn config(dir: &std::path::Path) -> GirderConfig {
    let mut config = GirderConfig::at(dir);
    config.fsync = FsyncPolicy::Always;
    config.memtable_max_records = 1000;
    config.compact_min_segments = 1000; // manual control
    config.tick_interval = Duration::from_secs(3600);
    config
}

fn span(i: usize) -> Record {
    Record {
        key: format!("s/{i:05}"),
        timestamp: 1_000 + i as i64,
        labels: BTreeMap::from([("project".to_string(), "prod".to_string())]),
        numerics: BTreeMap::new(),
        payload: format!("row-{i}").into_bytes(),
        text: None,
    }
}

/// The poison shape: a meta key below the data prefix with a wall-clock
/// timestamp — exactly the D-2 marker that motivated this API.
fn poison() -> Record {
    Record {
        key: "m/poison-marker".to_string(),
        timestamp: 1_783_000_000_000_000_000, // wall-clock nanos
        labels: BTreeMap::new(),
        numerics: BTreeMap::new(),
        payload: b"2".to_vec(),
        text: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn purge_removes_key_and_heals_zones() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    // The poison flushes IN THE SAME segment as early data (the real
    // failure mode), then more generations follow.
    engine.put(poison()).await.unwrap();
    for i in 0..200 {
        engine.put(span(i)).await.unwrap();
    }
    engine.flush().await.unwrap();
    for i in 200..400 {
        engine.put(span(i)).await.unwrap();
    }
    engine.flush().await.unwrap();

    // Pre-purge: the marker exists; a time-windowed scan CANNOT prune the
    // poisoned segment (its ts range spans everything).
    assert!(engine.get("m/poison-marker").await.unwrap().is_some());

    engine.purge_key("m/poison-marker").await.unwrap();

    // Gone — record and reachability.
    assert!(engine.get("m/poison-marker").await.unwrap().is_none());
    let m_scan = engine
        .scan(&QuerySpec {
            key_prefix: Some("m/".to_string()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(m_scan.is_empty(), "no m/ record survives the purge");
    // Every data row intact.
    let rows = engine
        .scan(&QuerySpec {
            key_prefix: Some("s/".to_string()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(rows.len(), 400, "purge touches ONLY the purged key");
    // Idempotent: a second purge is a clean no-op.
    engine.purge_key("m/poison-marker").await.unwrap();
    assert_eq!(
        engine
            .scan(&QuerySpec {
                key_prefix: Some("s/".to_string()),
                ..Default::default()
            })
            .await
            .unwrap()
            .len(),
        400
    );

    // Zone healing is observable through TIME PRUNING: a wall-clock-era
    // window matches nothing AND (post-purge) prunes every segment — no
    // segment's ts range reaches wall-clock any more, so the scan does no
    // segment I/O. We assert the semantic half (empty) plus survival
    // across a reopen (the rewritten manifest is durable).
    let far_future = engine
        .scan(&QuerySpec {
            time: Some((1_700_000_000_000_000_000, i64::MAX)),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(far_future.is_empty());
    drop(engine);
    let engine = Girder::open(config(dir.path())).await.unwrap();
    assert!(engine.get("m/poison-marker").await.unwrap().is_none());
    assert_eq!(
        engine
            .scan(&QuerySpec {
                key_prefix: Some("s/".to_string()),
                ..Default::default()
            })
            .await
            .unwrap()
            .len(),
        400
    );
}

/// A tombstoned key purges cleanly too (record in one segment, tombstone in
/// another — the run gathers BOTH, so the purge can't un-shadow anything).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn purge_folds_record_and_tombstone_together() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    engine.put(poison()).await.unwrap();
    for i in 0..100 {
        engine.put(span(i)).await.unwrap();
    }
    engine.flush().await.unwrap();
    engine
        .delete("m/poison-marker", 1_783_000_000_000_000_001)
        .await
        .unwrap();
    engine.flush().await.unwrap(); // tombstone in its own segment

    engine.purge_key("m/poison-marker").await.unwrap();
    assert!(engine.get("m/poison-marker").await.unwrap().is_none());
    let m_scan = engine
        .scan(&QuerySpec {
            key_prefix: Some("m/".to_string()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(m_scan.is_empty());
}

/// Counter keys refuse: a partial delta fold must never be materialized.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn purge_refuses_counter_keys() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    engine
        .incr("bl/counter", 1, BTreeMap::from([("n".to_string(), 1.0)]))
        .await
        .unwrap();
    engine.flush().await.unwrap();
    assert!(engine.purge_key("bl/counter").await.is_err());
    // And the counter is untouched by the refusal.
    assert!(engine.get("bl/counter").await.unwrap().is_some());
}
