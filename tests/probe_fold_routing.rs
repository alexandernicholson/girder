//! D-3 probe/fold routing pin: the pull-wise shadow probe is keys-only, so
//! its soundness REQUIRES that no raw counter delta ever reaches the probe
//! paths — a newer delta must fold with its base (`merge_delta`), not
//! LWW-shadow it. That is guaranteed by the fold dispatch: `deltas_possible`
//! is conservative over key prefix alone, so ANY delta sharing a prefix with
//! ordinary records routes the whole scan/count to `scan_fold`. This suite
//! pins the routing behaviorally on a MIXED keyspace: interleaved,
//! range-overlapping segments holding spans, counter deltas, rewrites and a
//! tombstone under one prefix — folded totals exact, LWW exact, no
//! per-version delta rows leaking, count agreeing with scan.
use std::collections::BTreeMap;
use std::time::Duration;

use girder::{FsyncPolicy, Girder, GirderConfig, QuerySpec, Record};

fn config(dir: &std::path::Path) -> GirderConfig {
    let mut config = GirderConfig::at(dir);
    config.fsync = FsyncPolicy::EveryN(64);
    config.memtable_max_records = 10_000;
    config.compact_min_segments = 1000; // never compact: overlap must stay
    config.tick_interval = Duration::from_secs(3600);
    config
}

fn span(key: &str, ts: i64, payload: &str) -> Record {
    Record {
        key: key.to_string(),
        timestamp: ts,
        labels: BTreeMap::from([("kind".to_string(), "span".to_string())]),
        numerics: BTreeMap::new(),
        payload: payload.as_bytes().to_vec(),
        text: None,
    }
}

fn deltas(pairs: &[(&str, f64)]) -> BTreeMap<String, f64> {
    pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

fn prefix_spec(prefix: &str) -> QuerySpec {
    QuerySpec {
        key_prefix: Some(prefix.to_string()),
        ..Default::default()
    }
}

/// Spans, counter deltas, a rewrite and a tombstone interleaved under ONE
/// prefix across range-overlapping segments — exactly the keyspace shape
/// where the probe would run if the fold dispatch didn't preempt it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_prefix_counters_route_to_fold_and_stay_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    // Segment 1: spans spanning the prefix range end to end.
    engine.put(span("p/a", 10, "a-v1")).await.unwrap();
    engine.put(span("p/z", 11, "z-v1")).await.unwrap();
    engine.flush().await.unwrap();
    // Segment 2: a counter delta in the middle of that range.
    engine
        .incr("p/ctr", 20, deltas(&[("cost", 5.0)]))
        .await
        .unwrap();
    engine.flush().await.unwrap();
    // Segment 3: another delta + a rewrite of p/a (must LWW-shadow v1
    // despite living two segments away) + a fresh key.
    engine
        .incr("p/ctr", 30, deltas(&[("cost", 7.0)]))
        .await
        .unwrap();
    engine.put(span("p/a", 31, "a-v2")).await.unwrap();
    engine.put(span("p/m", 32, "m-v1")).await.unwrap();
    engine.flush().await.unwrap();

    let rows = engine.scan(&prefix_spec("p/")).await.unwrap();
    assert_eq!(rows.len(), 4, "one row per key, no per-version delta leak");
    let by_key: BTreeMap<&str, &Record> = rows.iter().map(|r| (r.key.as_str(), r)).collect();
    assert_eq!(
        by_key["p/a"].payload, b"a-v2",
        "rewrite LWW-shadows across overlapping segments"
    );
    assert_eq!(by_key["p/z"].payload, b"z-v1");
    assert_eq!(by_key["p/m"].payload, b"m-v1");
    assert_eq!(
        by_key["p/ctr"].numerics["cost"], 12.0,
        "deltas FOLD (newer-delta = keep-and-fold), never LWW-suppress"
    );
    assert_eq!(
        engine.count(&prefix_spec("p/")).await.unwrap(),
        4,
        "count agrees with scan on the fold path"
    );

    // A tombstone basifies/terminates: delete p/z, then everything above
    // still holds minus the deleted key.
    engine.delete("p/z", 40).await.unwrap();
    engine.flush().await.unwrap();
    let rows = engine.scan(&prefix_spec("p/")).await.unwrap();
    assert_eq!(rows.len(), 3, "tombstone shadows through the fold path");
    assert!(rows.iter().all(|r| r.key != "p/z"));
    assert_eq!(engine.count(&prefix_spec("p/")).await.unwrap(), 3);

    // Restart: the routing decision must survive reopen (segment zone labels
    // carry the delta flag; memtable is empty after replay+flush).
    drop(engine);
    let engine = Girder::open(config(dir.path())).await.unwrap();
    let rows = engine.scan(&prefix_spec("p/")).await.unwrap();
    assert_eq!(rows.len(), 3);
    let ctr = rows.iter().find(|r| r.key == "p/ctr").unwrap();
    assert_eq!(ctr.numerics["cost"], 12.0, "fold exact after reopen");
}

/// The inverse pin: a delta under a DIFFERENT prefix must NOT drag an
/// unrelated prefix onto the fold path's semantics — but whichever path
/// runs, results agree. (Guards the conservative gate from silently
/// widening or narrowing: the two prefixes see independent, exact results.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreign_prefix_deltas_leave_span_results_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    engine.put(span("s/a", 10, "a-v1")).await.unwrap();
    engine.put(span("s/b", 11, "b-v1")).await.unwrap();
    engine.flush().await.unwrap();
    engine
        .incr("bl/ctr", 20, deltas(&[("cost", 1.0)]))
        .await
        .unwrap();
    engine.put(span("s/a", 21, "a-v2")).await.unwrap();
    engine.flush().await.unwrap();

    let rows = engine.scan(&prefix_spec("s/")).await.unwrap();
    assert_eq!(rows.len(), 2);
    let by_key: BTreeMap<&str, &Record> = rows.iter().map(|r| (r.key.as_str(), r)).collect();
    assert_eq!(by_key["s/a"].payload, b"a-v2", "LWW exact under s/");
    assert_eq!(by_key["s/b"].payload, b"b-v1");
    assert_eq!(engine.count(&prefix_spec("s/")).await.unwrap(), 2);

    let ctr_rows = engine.scan(&prefix_spec("bl/")).await.unwrap();
    assert_eq!(ctr_rows.len(), 1);
    assert_eq!(ctr_rows[0].numerics["cost"], 1.0);
}
