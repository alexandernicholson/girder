//! Counter primitive acceptance (`Girder::incr`): atomic increments folded
//! via the single `merge_delta` oracle across memtable, flush, compaction,
//! WAL replay — with ordinary `put` last-write-wins preserved, and the
//! concurrent-accrual lost-update race closed by construction.
use std::collections::BTreeMap;
use std::time::Duration;

use girder::{FsyncPolicy, Girder, GirderConfig, OrderBy, QuerySpec, Record};

fn config(dir: &std::path::Path) -> GirderConfig {
    let mut config = GirderConfig::at(dir);
    config.fsync = FsyncPolicy::EveryN(64);
    config.memtable_max_records = 10_000;
    config.compact_min_segments = 2;
    config.tick_interval = Duration::from_secs(3600);
    config
}

fn deltas(pairs: &[(&str, f64)]) -> BTreeMap<String, f64> {
    pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

fn full_record(key: &str, ts: i64, cost: f64) -> Record {
    Record {
        key: key.to_string(),
        timestamp: ts,
        labels: BTreeMap::from([("kind".to_string(), "ledger".to_string())]),
        numerics: BTreeMap::from([("cost".to_string(), cost)]),
        payload: b"row".to_vec(),
        text: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incr_creates_accumulates_and_returns_post_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    // Create-on-first-increment.
    let v = engine
        .incr("bl/p1", 1, deltas(&[("cost", 0.5), ("requests", 1.0)]))
        .await
        .unwrap();
    assert_eq!(v["cost"], 0.5);
    assert_eq!(v["requests"], 1.0);

    // Accumulate, including a numeric absent from the first increment.
    let v = engine
        .incr("bl/p1", 2, deltas(&[("cost", 0.25), ("tokens", 100.0)]))
        .await
        .unwrap();
    assert_eq!(v["cost"], 0.75);
    assert_eq!(v["requests"], 1.0);
    assert_eq!(v["tokens"], 100.0);

    // get() sees the same folded row, without the internal delta label.
    let row = engine.get("bl/p1").await.unwrap().unwrap();
    assert_eq!(row.numerics["cost"], 0.75);
    assert!(!row.labels.contains_key("girder.delta"));
    // Folded timestamp = latest activity.
    assert_eq!(row.timestamp, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incr_folds_across_memtable_segments_and_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    // Base as a full put, flushed to a segment.
    engine.put(full_record("bl/p1", 1, 10.0)).await.unwrap();
    engine.flush().await.unwrap();
    // Delta in the memtable over the segment base.
    engine
        .incr("bl/p1", 2, deltas(&[("cost", 1.0)]))
        .await
        .unwrap();
    let row = engine.get("bl/p1").await.unwrap().unwrap();
    assert_eq!(
        row.numerics["cost"], 11.0,
        "memtable delta over segment base"
    );
    assert_eq!(row.labels["kind"], "ledger", "identity comes from the base");
    assert_eq!(row.payload, b"row");

    // Delta flushed to its own segment: base and delta both on disk.
    engine.flush().await.unwrap();
    let row = engine.get("bl/p1").await.unwrap().unwrap();
    assert_eq!(row.numerics["cost"], 11.0, "cross-segment fold");

    // Another delta + flush → three segments; scan folds too.
    engine
        .incr("bl/p1", 3, deltas(&[("cost", 0.5)]))
        .await
        .unwrap();
    engine.flush().await.unwrap();
    let hits = engine
        .scan(&QuerySpec {
            key_prefix: Some("bl/".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "one folded row, never partial versions");
    assert_eq!(hits[0].numerics["cost"], 11.5);

    // Compaction collapses the chain and the value is unchanged.
    engine.maintain().await.unwrap();
    assert!(engine.stats().compactions >= 1);
    let row = engine.get("bl/p1").await.unwrap().unwrap();
    assert_eq!(row.numerics["cost"], 11.5, "post-compaction");
    // And further increments keep working on the collapsed row.
    engine
        .incr("bl/p1", 4, deltas(&[("cost", 0.5)]))
        .await
        .unwrap();
    assert_eq!(
        engine.get("bl/p1").await.unwrap().unwrap().numerics["cost"],
        12.0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_replay_folds_deltas_exactly() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Girder::open(config(dir.path())).await.unwrap();
        engine.put(full_record("bl/p1", 1, 10.0)).await.unwrap();
        engine.flush().await.unwrap(); // base durable in a segment
        for i in 0..10 {
            engine
                .incr("bl/p1", 2 + i, deltas(&[("cost", 1.0)]))
                .await
                .unwrap();
        }
        drop(engine); // crash: deltas live only in the WAL
    }
    let engine = Girder::open(config(dir.path())).await.unwrap();
    assert_eq!(
        engine.get("bl/p1").await.unwrap().unwrap().numerics["cost"],
        20.0,
        "WAL replay reapplies increments via the same fold oracle"
    );
    // Close/reopen (checkpoint path) keeps it stable.
    engine.close().await.unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    assert_eq!(
        engine.get("bl/p1").await.unwrap().unwrap().numerics["cost"],
        20.0
    );
}

/// The upsert guarantee holds unchanged: a full `put` REPLACES the
/// accumulated value (last write wins), and increments then build on the
/// new base.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_replaces_accumulated_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    engine.incr("k", 1, deltas(&[("cost", 5.0)])).await.unwrap();
    engine.flush().await.unwrap();
    engine.incr("k", 2, deltas(&[("cost", 5.0)])).await.unwrap();
    engine.put(full_record("k", 3, 1.0)).await.unwrap(); // reset
    assert_eq!(
        engine.get("k").await.unwrap().unwrap().numerics["cost"],
        1.0
    );
    engine.incr("k", 4, deltas(&[("cost", 2.0)])).await.unwrap();
    assert_eq!(
        engine.get("k").await.unwrap().unwrap().numerics["cost"],
        3.0
    );
}

/// THE race-closure acceptance: concurrent increments from many tasks, with
/// flushes and compactions racing them, sum EXACTLY — the lost-update window
/// of read-modify-write is structurally gone (single-writer fold).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_incrs_never_lose_updates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(Girder::open(config(dir.path())).await.unwrap());

    let mut tasks = Vec::new();
    for t in 0..8 {
        let e = engine.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..50i64 {
                e.incr(
                    "bl/hot",
                    t * 100 + i,
                    deltas(&[("cost", 0.25), ("requests", 1.0)]),
                )
                .await
                .unwrap();
            }
        }));
    }
    // Maintenance races the increments (flush + compaction mid-stream).
    {
        let e = engine.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..10 {
                e.flush().await.unwrap();
                e.maintain().await.unwrap();
                tokio::task::yield_now().await;
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let row = engine.get("bl/hot").await.unwrap().unwrap();
    assert_eq!(row.numerics["requests"], 400.0, "every increment counted");
    assert_eq!(row.numerics["cost"], 100.0);

    // And it survives a restart.
    std::sync::Arc::into_inner(engine)
        .expect("all tasks joined")
        .close()
        .await
        .unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    assert_eq!(
        engine.get("bl/hot").await.unwrap().unwrap().numerics["requests"],
        400.0
    );
}

/// Fold-mode scans apply the FULL spec to folded records — a numeric range
/// filters on accumulated totals (narrowing on raw deltas would get this
/// wrong), ordering and limit hold, and plain records sharing the store are
/// unaffected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fold_mode_scan_applies_full_spec_post_fold() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    // Three counters accumulating to 3.0 / 6.0 / 9.0 across segment
    // boundaries (each increment is 1.0 or 2.0 or 3.0 — every RAW delta is
    // below 4.0, so a pre-fold numeric narrowing would drop all of them).
    for round in 0..3 {
        for (i, key) in ["bl/a", "bl/b", "bl/c"].iter().enumerate() {
            engine
                .incr(
                    key,
                    round * 10 + i as i64,
                    deltas(&[("cost", (i + 1) as f64)]),
                )
                .await
                .unwrap();
        }
        engine.flush().await.unwrap();
    }
    // A plain (non-counter) record in the same store.
    engine.put(full_record("row/z", 100, 7.0)).await.unwrap();

    // Numeric range over FOLDED totals: only b (6.0) and z (7.0) qualify.
    let hits = engine
        .scan(&QuerySpec {
            numeric_ranges: vec![("cost".into(), 4.0, 8.0)],
            ..Default::default()
        })
        .await
        .unwrap();
    let mut keys: Vec<&str> = hits.iter().map(|r| r.key.as_str()).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["bl/b", "row/z"], "range applies to folded totals");

    // Ordering + limit through the fold path.
    let hits = engine
        .scan(&QuerySpec {
            key_prefix: Some("bl/".into()),
            order_by: Some(OrderBy::NumericDesc("cost".into())),
            limit: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    let keys: Vec<&str> = hits.iter().map(|r| r.key.as_str()).collect();
    assert_eq!(keys, ["bl/c", "bl/b"], "top-k over folded totals");

    // Time-range: a counter whose BASE is old but latest delta is recent has
    // folded ts = latest activity — it must appear in a recent window.
    let hits = engine
        .scan(&QuerySpec {
            time: Some((20, 30)),
            key_prefix: Some("bl/".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(hits.len(), 3, "folded timestamp is the latest activity");
}
