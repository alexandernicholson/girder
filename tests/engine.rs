//! End-to-end engine tests: durability, recovery, flush, dedupe, compaction,
//! tiering, retention, pruning, cache.
use std::collections::BTreeMap;
use std::time::Duration;

use girder::{FsyncPolicy, Girder, GirderConfig, QuerySpec, Record};

fn record(key: &str, ts: i64, model: &str, latency: f64) -> Record {
    Record {
        key: key.to_string(),
        timestamp: ts,
        labels: BTreeMap::from([
            ("model".to_string(), model.to_string()),
            ("project".to_string(), "prod".to_string()),
        ]),
        numerics: BTreeMap::from([("latency_ms".to_string(), latency)]),
        payload: format!("payload-{key}").into_bytes(),
    }
}

fn config(dir: &std::path::Path) -> GirderConfig {
    let mut config = GirderConfig::at(dir);
    config.fsync = FsyncPolicy::Always;
    config.memtable_max_records = 100;
    config.compact_min_segments = 3;
    config.tick_interval = Duration::from_secs(3600); // manual ticks in tests
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_get_scan_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    for i in 0..50 {
        engine
            .put(record(&format!("s/{i:03}"), i, if i % 2 == 0 { "gpt-4o" } else { "claude" }, i as f64))
            .await
            .unwrap();
    }
    // Point get.
    let got = engine.get("s/007").await.unwrap().unwrap();
    assert_eq!(got.payload, b"payload-s/007");
    assert!(engine.get("s/999").await.unwrap().is_none());

    // Scan with label + numeric + time predicates.
    let hits = engine
        .scan(&QuerySpec {
            labels: vec![("model".into(), "gpt-4o".into())],
            numeric_ranges: vec![("latency_ms".into(), 10.0, 20.0)],
            time: Some((0, 100)),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(hits.len(), 6); // even i in 10..=20
    assert!(hits.windows(2).all(|w| w[0].timestamp >= w[1].timestamp)); // newest first

    // Key prefix scan.
    let all = engine
        .scan(&QuerySpec { key_prefix: Some("s/".into()), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(all.len(), 50);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn newest_write_wins_across_tiers() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    engine.put(record("k", 1, "old", 1.0)).await.unwrap();
    engine.flush().await.unwrap(); // old version now in a segment
    engine.put(record("k", 2, "new", 2.0)).await.unwrap(); // memtable
    let got = engine.get("k").await.unwrap().unwrap();
    assert_eq!(got.labels["model"], "new");
    let scanned = engine.scan(&QuerySpec::default()).await.unwrap();
    assert_eq!(scanned.len(), 1, "dedupe across memtable + segment");
    assert_eq!(scanned[0].labels["model"], "new");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_recovery_replays_wal() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Girder::open(config(dir.path())).await.unwrap();
        for i in 0..10 {
            engine.put(record(&format!("k{i}"), i, "gpt-4o", 1.0)).await.unwrap();
        }
        // NO flush, NO close — simulate a crash by dropping.
        drop(engine);
    }
    let engine = Girder::open(config(dir.path())).await.unwrap();
    let all = engine.scan(&QuerySpec::default()).await.unwrap();
    assert_eq!(all.len(), 10, "WAL tail recovered");
    assert!(engine.get("k7").await.unwrap().is_some());
    // Recovery checkpointed into a segment; WALs cleaned.
    assert!(engine.stats().total_records_in_segments >= 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn freeze_flush_and_cache() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    // 250 records with threshold 100 → at least 2 automatic freezes.
    let records: Vec<Record> = (0..250).map(|i| record(&format!("k{i:04}"), i, "gpt-4o", 1.0)).collect();
    for chunk in records.chunks(50) {
        engine.put_batch(chunk.to_vec()).await.unwrap();
    }
    engine.flush().await.unwrap();
    let stats = engine.stats();
    assert!(stats.hot_segments >= 2, "{stats:?}");
    assert_eq!(stats.total_records_in_segments + stats.memtable_records, 250);

    // First scan loads segments (misses), second scan hits the cache.
    let spec = QuerySpec { labels: vec![("model".into(), "gpt-4o".into())], ..Default::default() };
    engine.scan(&spec).await.unwrap();
    let misses_after_first = engine.stats().cache_misses;
    engine.scan(&spec).await.unwrap();
    let stats = engine.stats();
    assert_eq!(stats.cache_misses, misses_after_first, "second scan fully cached");
    assert!(stats.cache_hits > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_merges_and_dedupes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    // Three segments, overlapping keys (k0..k59, updated in later segments).
    for round in 0..3 {
        for i in 0..60 {
            engine
                .put(record(&format!("k{i:02}"), round * 100 + i, &format!("v{round}"), 1.0))
                .await
                .unwrap();
        }
        engine.flush().await.unwrap();
    }
    assert!(engine.stats().hot_segments >= 3);
    engine.maintain().await.unwrap(); // compaction pass
    let stats = engine.stats();
    assert_eq!(stats.hot_segments, 1, "merged into one: {stats:?}");
    assert_eq!(stats.total_records_in_segments, 60, "deduped");
    assert!(stats.compactions >= 1);
    // Every key resolves to the newest round.
    let got = engine.get("k07").await.unwrap().unwrap();
    assert_eq!(got.labels["model"], "v2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiering_moves_old_segments_to_cold() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.hot_ttl_nanos = 0; // everything is instantly "old"
    config.compact_min_segments = 100; // don't compact in this test
    let engine = Girder::open(config).await.unwrap();
    engine.put(record("k", 1, "gpt-4o", 1.0)).await.unwrap();
    engine.flush().await.unwrap();
    engine.maintain().await.unwrap(); // tiering pass
    let stats = engine.stats();
    assert_eq!((stats.hot_segments, stats.cold_segments), (0, 1), "{stats:?}");
    // Cold data still readable.
    assert!(engine.get("k").await.unwrap().is_some());
    // File physically lives in the cold dir.
    let cold_files = std::fs::read_dir(dir.path().join("cold")).unwrap().count();
    assert_eq!(cold_files, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retention_drops_expired_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.retention_nanos = Some(0); // everything already expired
    config.compact_min_segments = 1;
    let engine = Girder::open(config).await.unwrap();
    engine.put(record("k", 1, "gpt-4o", 1.0)).await.unwrap();
    engine.flush().await.unwrap();
    engine.maintain().await.unwrap(); // compaction applies retention
    let all = engine.scan(&QuerySpec::default()).await.unwrap();
    assert!(all.is_empty(), "expired records dropped at compaction");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zone_maps_prune_segment_loads() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    // Segment A: model=alpha; Segment B: model=beta.
    for i in 0..50 {
        engine.put(record(&format!("a{i}"), i, "alpha", 1.0)).await.unwrap();
    }
    engine.flush().await.unwrap();
    for i in 0..50 {
        engine.put(record(&format!("b{i}"), i, "beta", 1.0)).await.unwrap();
    }
    engine.flush().await.unwrap();

    // Query for gamma: zone maps exclude BOTH segments → zero disk loads.
    let before = engine.stats().cache_misses;
    let none = engine
        .scan(&QuerySpec { labels: vec![("model".into(), "gamma".into())], ..Default::default() })
        .await
        .unwrap();
    assert!(none.is_empty());
    assert_eq!(engine.stats().cache_misses, before, "no segment was loaded");

    // Query for alpha: only segment A loads.
    engine
        .scan(&QuerySpec { labels: vec![("model".into(), "alpha".into())], ..Default::default() })
        .await
        .unwrap();
    assert_eq!(engine.stats().cache_misses, before + 1, "exactly one segment loaded");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_are_serialized_safely() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(Girder::open(config(dir.path())).await.unwrap());
    let mut handles = Vec::new();
    for w in 0..8 {
        let engine = engine.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..100 {
                engine
                    .put(record(&format!("w{w}/k{i}"), i, "gpt-4o", 1.0))
                    .await
                    .unwrap();
            }
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }
    let all = engine
        .scan(&QuerySpec { key_prefix: Some("w".into()), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(all.len(), 800);
}
