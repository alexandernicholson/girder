//! Per-prefix retention + tick-driven grooming (plan 0013 §6 TTL hooks):
//! policy-as-data rows resolved longest-prefix-first by ONE oracle shared by
//! compaction and the groomer; grooming runs on ticks, so expiry is never
//! hostage to write volume.
use std::collections::BTreeMap;
use std::time::Duration;

use girder::{FsyncPolicy, Girder, GirderConfig, QuerySpec, Record};

const HOUR: i64 = 3_600_000_000_000;

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

fn record(key: &str, ts: i64) -> Record {
    Record {
        key: key.to_string(),
        timestamp: ts,
        labels: BTreeMap::from([("kind".to_string(), "x".to_string())]),
        numerics: BTreeMap::new(),
        payload: format!("p-{key}").into_bytes(),
        text: None,
    }
}

fn config(dir: &std::path::Path) -> GirderConfig {
    let mut config = GirderConfig::at(dir);
    config.fsync = FsyncPolicy::EveryN(64);
    config.memtable_max_records = 10_000;
    config.compact_min_segments = 100; // isolate grooming from compaction
    config.tick_interval = Duration::from_secs(3600);
    config
}

/// Longest-prefix precedence end-to-end at COMPACTION: the specific tenant
/// row keeps its data while the broad row and the global fallback expire
/// theirs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_prefix_retention_at_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.compact_min_segments = 1; // let compaction fire
    cfg.retention = vec![
        ("s/".to_string(), HOUR),           // broad: 1h
        ("s/keep/".to_string(), 48 * HOUR), // specific tenant: 2 days
    ];
    cfg.retention_nanos = Some(2 * HOUR); // global fallback: 2h
    let engine = Girder::open(cfg).await.unwrap();

    let old = now() - 24 * HOUR; // 1 day ago
    engine.put(record("s/keep/a", old)).await.unwrap(); // 2-day rule → kept
    engine.put(record("s/drop/b", old)).await.unwrap(); // 1-hour rule → dropped
    engine.put(record("t/other", old)).await.unwrap(); // global 2h → dropped
    engine.put(record("s/keep/fresh", now())).await.unwrap();
    engine.flush().await.unwrap();
    engine.maintain().await.unwrap(); // compaction applies per-key retention

    let keys: Vec<String> = engine
        .scan(&QuerySpec::default())
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.key)
        .collect();
    assert!(
        keys.contains(&"s/keep/a".to_string()),
        "specific row governs"
    );
    assert!(keys.contains(&"s/keep/fresh".to_string()));
    assert!(
        !keys.contains(&"s/drop/b".to_string()),
        "broad row expired it"
    );
    assert!(
        !keys.contains(&"t/other".to_string()),
        "global fallback expired it"
    );
}

/// THE grooming acceptance: with ZERO writes after setup, ticks alone age
/// segments out — wholesale zone-provable drops, no compaction involved
/// (compact_min_segments=100 makes ordinary compaction impossible here).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grooming_without_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.retention = vec![("s/".to_string(), HOUR)];
    let engine = Girder::open(cfg).await.unwrap();

    let old = now() - 3 * HOUR;
    for i in 0..20 {
        engine.put(record(&format!("s/{i:03}"), old)).await.unwrap();
    }
    engine.flush().await.unwrap();
    assert!(engine.stats().hot_segments >= 1);

    // No further writes — only maintenance ticks.
    engine.maintain().await.unwrap();
    let stats = engine.stats();
    assert_eq!(stats.compactions, 0, "grooming, not compaction: {stats:?}");
    assert!(stats.groomed_segments >= 1, "{stats:?}");
    assert_eq!(stats.hot_segments + stats.cold_segments, 0, "{stats:?}");
    assert!(engine.scan(&QuerySpec::default()).await.unwrap().is_empty());
    // Files physically gone (only WAL/MANIFEST remain in the hot dir).
    let seg_files = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".gird"))
        .count();
    assert_eq!(seg_files, 0, "expired segment files deleted");
}

/// A partially-expired HOT segment is rewritten in place: expired rows drop,
/// survivors keep serving, newest-wins order intact (same recency slot).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn groom_rewrites_partially_expired_hot_segment() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.retention = vec![("s/".to_string(), HOUR)];
    let engine = Girder::open(cfg).await.unwrap();

    let t = now();
    engine.put(record("s/old", t - 3 * HOUR)).await.unwrap();
    engine.put(record("s/new", t)).await.unwrap();
    engine.flush().await.unwrap(); // one segment, mixed ages
    engine.maintain().await.unwrap();

    let stats = engine.stats();
    assert!(stats.groomed_segments >= 1, "{stats:?}");
    let keys: Vec<String> = engine
        .scan(&QuerySpec::default())
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.key)
        .collect();
    assert_eq!(keys, ["s/new"], "expired row gone, survivor kept");
    // Newest-wins across the rewritten slot: an older version of s/new in an
    // older segment stays shadowed.
    assert!(engine.get("s/new").await.unwrap().is_some());
}

/// A COLD partially-expired segment waits for full expiry (rewrite is
/// hot-only, documented); once everything ages past the TTL it is dropped
/// wholesale from the cold tier.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_segment_waits_for_full_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.hot_ttl_nanos = 0; // tier immediately
    cfg.retention = vec![("s/".to_string(), 2 * HOUR)];
    let engine = Girder::open(cfg).await.unwrap();

    let t = now();
    engine.put(record("s/old", t - 3 * HOUR)).await.unwrap(); // already expired
    engine.put(record("s/mid", t - HOUR)).await.unwrap(); // expires in 1h
    engine.flush().await.unwrap();
    engine.maintain().await.unwrap(); // tiers to cold; partial → NOT rewritten
    let stats = engine.stats();
    assert_eq!(stats.cold_segments, 1, "{stats:?}");
    assert_eq!(stats.groomed_segments, 0, "cold partial waits: {stats:?}");
    // Both rows still readable (expiry is enforced at groom/compact, lazily).
    assert_eq!(engine.scan(&QuerySpec::default()).await.unwrap().len(), 2);

    // Simulate full expiry with a second engine whose TTL is tiny.
    engine.close().await.unwrap();
    let mut cfg = config(dir.path());
    cfg.hot_ttl_nanos = 0;
    cfg.retention = vec![("s/".to_string(), 1)]; // everything now expired
    let engine = Girder::open(cfg).await.unwrap();
    engine.maintain().await.unwrap();
    let stats = engine.stats();
    assert!(stats.groomed_segments >= 1, "{stats:?}");
    assert!(engine.scan(&QuerySpec::default()).await.unwrap().is_empty());
}

/// Keys matching no retention row live forever — a segment whose range is
/// not covered by any row is never groomed, even when ancient.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncovered_keys_live_forever() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.retention = vec![("s/".to_string(), HOUR)]; // no global row
    let engine = Girder::open(cfg).await.unwrap();

    engine.put(record("t/ancient", 1)).await.unwrap(); // epoch-old, uncovered
    engine.flush().await.unwrap();
    engine.maintain().await.unwrap();
    assert!(engine.get("t/ancient").await.unwrap().is_some());
    assert_eq!(engine.stats().groomed_segments, 0);
}

/// Counter interplay: an ACTIVE counter (old base + recent increments) is
/// safe — the folded timestamp is the latest activity, and both compaction
/// and the groomer judge the FOLDED record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_counter_survives_grooming() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.compact_min_segments = 1;
    cfg.retention = vec![("bl/".to_string(), HOUR)];
    let engine = Girder::open(cfg).await.unwrap();

    let t = now();
    // Base created 3h ago (past TTL on its own), flushed.
    engine
        .put(record("bl/counter", t - 3 * HOUR))
        .await
        .unwrap();
    engine.flush().await.unwrap();
    // Recent increment: latest activity is NOW.
    engine
        .incr(
            "bl/counter",
            t,
            BTreeMap::from([("requests".to_string(), 1.0)]),
        )
        .await
        .unwrap();
    engine.flush().await.unwrap();
    engine.maintain().await.unwrap(); // compaction folds THEN retains

    let row = engine.get("bl/counter").await.unwrap();
    assert!(
        row.is_some(),
        "active counter must not be retention-dropped"
    );
    assert_eq!(row.unwrap().numerics["requests"], 1.0);
}

/// The groom-only counter hazard: an old BASE segment must not be
/// wholesale-dropped while a newer delta rides elsewhere (the fold spans
/// segments; the groomer judges one at a time — so it must SKIP
/// delta-affected ranges and leave them to compaction, which folds first).
///
/// Built in two phases for determinism: the data (base + delta + an
/// unrelated expired row) is written under NO retention policy, then the
/// engine reopens WITH the policy — so from the very first tick of the
/// grooming engine, the delta segment already exists and its zone label
/// protects the range, regardless of when the open-time tick fires. (An
/// expired base with no delta yet is legitimately droppable — inactive
/// counters are ordinary expired data.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn groomer_never_drops_a_counter_base() {
    let dir = tempfile::tempdir().unwrap();
    let t = now();
    {
        let cfg = config(dir.path()); // no retention rows: nothing groomable
        let engine = Girder::open(cfg).await.unwrap();
        // Base created 3h ago: past the future TTL, provably-expired by zone.
        engine
            .put(record("bl/counter", t - 3 * HOUR))
            .await
            .unwrap();
        engine.flush().await.unwrap();
        // Recent increment in a NEWER segment.
        engine
            .incr(
                "bl/counter",
                t,
                BTreeMap::from([("requests".to_string(), 1.0)]),
            )
            .await
            .unwrap();
        engine.flush().await.unwrap();
        // An unrelated expired row in a different range (groomable later).
        engine.put(record("s/old", t - 3 * HOUR)).await.unwrap();
        engine.close().await.unwrap();
    }

    // Phase 2: the retention policy arrives. The delta segment predates
    // every tick of this engine, so the bl/ range is protected from the
    // first groom pass onward; the s/ range grooms normally.
    let mut cfg = config(dir.path());
    cfg.retention = vec![("bl/".to_string(), HOUR), ("s/".to_string(), HOUR)];
    let engine = Girder::open(cfg).await.unwrap();
    engine.maintain().await.unwrap();

    let stats = engine.stats();
    assert_eq!(stats.compactions, 0, "{stats:?}");
    let row = engine.get("bl/counter").await.unwrap().unwrap();
    assert_eq!(
        row.numerics["requests"], 1.0,
        "counter value intact — base not dropped"
    );
    assert_eq!(row.payload, b"p-bl/counter", "base identity intact");
    assert!(
        engine.get("s/old").await.unwrap().is_none(),
        "unprotected expired range still grooms: {stats:?}"
    );
    // Repeated passes stay stable (skip is not one-shot).
    engine.maintain().await.unwrap();
    assert_eq!(
        engine.get("bl/counter").await.unwrap().unwrap().numerics["requests"],
        1.0
    );
}
