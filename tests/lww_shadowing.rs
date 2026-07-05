//! LWW shadowing under zone pruning + the delete API (`docs/GUARANTEES.md`
//! §Deletes / §Shadowing).
//!
//! Regression suite for the un-shadowing bug: a segment holding the NEWER
//! version of a key (a tombstone, or a rewrite whose labels changed) used to
//! be zone-pruned out of the scan walk whenever it couldn't match the spec —
//! its keys never seeded the shadow set, and the older, matching version in
//! an unpruned segment was returned. Deleted data resurrected; overwritten
//! label values kept matching. The fix is the shared walk plan: every
//! key-overlapping segment contributes its keys (zone-pruned ones via
//! keys-only reads), and disjointness is computed over the full
//! prefix-overlapping set. All three read paths (scan_full / scan_topk /
//! count) walk the SAME plan and are held to the naive oracle here.
use std::collections::BTreeMap;
use std::time::Duration;

use girder::{FsyncPolicy, Girder, GirderConfig, OrderBy, QuerySpec, Record};

fn live(key: &str, ts: i64, project: &str, payload: &str) -> Record {
    Record {
        key: key.to_string(),
        timestamp: ts,
        labels: BTreeMap::from([("project".to_string(), project.to_string())]),
        numerics: BTreeMap::new(),
        payload: payload.as_bytes().to_vec(),
        text: None,
    }
}

fn config(dir: &std::path::Path) -> GirderConfig {
    let mut config = GirderConfig::at(dir);
    config.fsync = FsyncPolicy::Always;
    config.memtable_max_records = 1000;
    config.compact_min_segments = 1000; // never compact: pruning must be sound WITHOUT it
    config.tick_interval = Duration::from_secs(3600);
    config
}

/// Newest-write-wins over the full write history, then the spec, then the
/// tombstone rule — the hand-rolled truth all three read paths must match.
/// (No deltas in these scenarios; counter folding has its own oracle in
/// `tests/counters.rs`.)
fn oracle(history: &[Record], spec: &QuerySpec) -> Vec<Record> {
    let mut latest: BTreeMap<String, Record> = BTreeMap::new();
    for r in history {
        latest.insert(r.key.clone(), r.clone());
    }
    let mut out: Vec<Record> = latest
        .into_values()
        .filter(|r| !r.is_tombstone() && spec.matches(r))
        .collect();
    out.sort_by(|a, b| b.timestamp.cmp(&a.timestamp).then(a.key.cmp(&b.key)));
    out
}

fn keys(records: &[Record]) -> Vec<&str> {
    records.iter().map(|r| r.key.as_str()).collect()
}

/// Assert scan_full, scan_topk and count all agree with the oracle for `spec`.
async fn assert_all_paths(engine: &Girder, history: &[Record], spec: &QuerySpec, ctx: &str) {
    let want = oracle(history, spec);

    let full = engine.scan(spec).await.unwrap();
    assert_eq!(keys(&full), keys(&want), "scan_full vs oracle: {ctx}");

    let topk = engine
        .scan(&QuerySpec {
            order_by: Some(OrderBy::TimestampDesc),
            limit: want.len().max(1) + 8, // bounded but roomy: membership is the question
            ..spec.clone()
        })
        .await
        .unwrap();
    assert_eq!(keys(&topk), keys(&want), "scan_topk vs oracle: {ctx}");

    let n = engine.count(spec).await.unwrap();
    assert_eq!(n, want.len(), "count vs oracle: {ctx}");
}

fn label_spec(project: &str) -> QuerySpec {
    QuerySpec {
        labels: vec![("project".to_string(), project.to_string())],
        ..Default::default()
    }
}

// --- the two original repros, promoted -------------------------------------

/// A pure-tombstone segment must keep shadowing under a label-scoped scan
/// even though the tombstone itself can never match the label.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pruned_tombstone_segment_still_shadows_label_scope() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    let history = vec![
        live("s/x", 1_000, "prod", "v1"),
        Record::tombstone("s/x", 2_000),
    ];

    engine.put(history[0].clone()).await.unwrap();
    engine.flush().await.unwrap(); // segment 1: the live labeled record
    engine.put(history[1].clone()).await.unwrap();
    engine.flush().await.unwrap(); // segment 2: ONLY its tombstone (bulk-delete flush)

    assert_all_paths(&engine, &history, &label_spec("prod"), "tombstone/label").await;
    assert_all_paths(
        &engine,
        &history,
        &QuerySpec::default(),
        "tombstone/unscoped",
    )
    .await;
}

/// Same shape under a time window the tombstone sits outside of.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pruned_tombstone_segment_still_shadows_time_window() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    // A deliberately BACK-DATED tombstone (ts=0, the historical rivet shape):
    // shadowing must hold anyway — only retention needs ts ≥ shadowed.
    let history = vec![
        live("s/x", 1_000, "prod", "v1"),
        Record::tombstone("s/x", 0),
    ];

    engine.put(history[0].clone()).await.unwrap();
    engine.flush().await.unwrap();
    engine.put(history[1].clone()).await.unwrap();
    engine.flush().await.unwrap();

    let spec = QuerySpec {
        time: Some((500, 2_000)),
        ..Default::default()
    };
    assert_all_paths(&engine, &history, &spec, "tombstone/time").await;
}

/// No tombstone anywhere: a rewrite whose labels CHANGED must stop matching
/// the old label the moment it lands, flushed into its own segment or not.
/// This is the A1 last-write-wins guarantee under a conditioned spec.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pruned_rewrite_segment_still_shadows_stale_labels() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    let history = vec![
        live("s/x", 1_000, "prod", "v1"),
        live("s/x", 1_000, "staging", "v2"), // same ts: recency = write order, not timestamp
    ];

    engine.put(history[0].clone()).await.unwrap();
    engine.flush().await.unwrap();
    engine.put(history[1].clone()).await.unwrap();
    engine.flush().await.unwrap();

    assert_all_paths(&engine, &history, &label_spec("prod"), "rewrite/old-label").await;
    assert_all_paths(
        &engine,
        &history,
        &label_spec("staging"),
        "rewrite/new-label",
    )
    .await;
}

// --- a mixed store held to the oracle across every path ---------------------

/// Several keys, several segments, tombstones AND rewrites interleaved —
/// membership must match the oracle for unscoped, label-scoped and
/// time-scoped specs on all three read paths.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_store_agrees_with_oracle_on_every_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    // Write order (= recency order). Flush boundaries create overlapping
    // key ranges so the walk cannot lean on the disjoint fast path.
    let batches: Vec<Vec<Record>> = vec![
        vec![
            live("s/a", 100, "prod", "a1"),
            live("s/b", 200, "prod", "b1"),
            live("s/c", 300, "staging", "c1"),
        ],
        vec![
            Record::tombstone("s/a", 400),     // delete a
            live("s/b", 500, "staging", "b2"), // move b out of prod
            live("s/d", 600, "prod", "d1"),    // new key
        ],
        vec![
            live("s/a", 700, "prod", "a2"), // re-create a AFTER its delete: visible again
            Record::tombstone("s/d", 800),  // delete d
        ],
    ];
    let mut history: Vec<Record> = Vec::new();
    for batch in &batches {
        for r in batch {
            engine.put(r.clone()).await.unwrap();
            history.push(r.clone());
        }
        engine.flush().await.unwrap();
    }

    assert_all_paths(&engine, &history, &QuerySpec::default(), "mixed/unscoped").await;
    assert_all_paths(&engine, &history, &label_spec("prod"), "mixed/prod").await;
    assert_all_paths(&engine, &history, &label_spec("staging"), "mixed/staging").await;
    let window = QuerySpec {
        time: Some((150, 750)),
        ..Default::default()
    };
    assert_all_paths(&engine, &history, &window, "mixed/window").await;

    // And the same store re-opened: shadowing must survive recovery.
    drop(engine);
    let engine = Girder::open(config(dir.path())).await.unwrap();
    assert_all_paths(
        &engine,
        &history,
        &label_spec("prod"),
        "mixed/prod/reopened",
    )
    .await;
}

// --- the delete API ----------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_hides_the_key_from_get_scan_and_count() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    engine.put(live("s/x", 1_000, "prod", "v1")).await.unwrap();
    engine.flush().await.unwrap();
    engine.delete("s/x", 2_000).await.unwrap();

    assert!(
        engine.get("s/x").await.unwrap().is_none(),
        "get after delete"
    );
    assert!(engine.scan(&QuerySpec::default()).await.unwrap().is_empty());
    assert_eq!(engine.count(&QuerySpec::default()).await.unwrap(), 0);

    // Flushed into its own segment, the tombstone keeps working…
    engine.flush().await.unwrap();
    assert!(
        engine.get("s/x").await.unwrap().is_none(),
        "get after flush"
    );
    assert!(engine.scan(&label_spec("prod")).await.unwrap().is_empty());

    // …and a later re-put wins over it (LWW: write order, not timestamp).
    engine.put(live("s/x", 1_500, "prod", "v2")).await.unwrap();
    let got = engine.get("s/x").await.unwrap().expect("re-put visible");
    assert_eq!(got.payload, b"v2");
    assert_eq!(engine.scan(&label_spec("prod")).await.unwrap().len(), 1);
}

/// Delete-then-incr resets the counter: increments newer than the tombstone
/// re-create the row from zero (the delta-chain-with-no-base rule).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_then_incr_resets_the_counter() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    engine
        .incr("bl/p1", 1_000, BTreeMap::from([("cost".to_string(), 5.0)]))
        .await
        .unwrap();
    engine.delete("bl/p1", 2_000).await.unwrap();
    assert!(
        engine.get("bl/p1").await.unwrap().is_none(),
        "deleted counter"
    );

    let after = engine
        .incr("bl/p1", 3_000, BTreeMap::from([("cost".to_string(), 3.0)]))
        .await
        .unwrap();
    assert_eq!(
        after.get("cost"),
        Some(&3.0),
        "post-delete incr starts from zero"
    );
    let got = engine.get("bl/p1").await.unwrap().expect("re-created row");
    assert_eq!(got.numerics.get("cost"), Some(&3.0));

    // Same answer once everything is flushed and folded from segments.
    engine.flush().await.unwrap();
    let got = engine.get("bl/p1").await.unwrap().expect("folded row");
    assert_eq!(got.numerics.get("cost"), Some(&3.0));
}

/// The keyset bound composes with shadow reads: a page resumed after a bound
/// never resurrects a deleted key either.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyset_pages_never_resurrect_through_shadow_segments() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    engine.put(live("s/a", 3_000, "prod", "a1")).await.unwrap();
    engine.put(live("s/b", 2_000, "prod", "b1")).await.unwrap();
    engine.put(live("s/c", 1_000, "prod", "c1")).await.unwrap();
    engine.flush().await.unwrap();
    engine.put(Record::tombstone("s/b", 4_000)).await.unwrap();
    engine.flush().await.unwrap();

    // Page 1: newest row only.
    let page1 = engine
        .scan(&QuerySpec {
            labels: vec![("project".to_string(), "prod".to_string())],
            order_by: Some(OrderBy::TimestampDesc),
            limit: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(keys(&page1), vec!["s/a"]);

    // Page 2, resumed after s/a: the deleted s/b must NOT appear.
    let page2 = engine
        .scan(&QuerySpec {
            labels: vec![("project".to_string(), "prod".to_string())],
            order_by: Some(OrderBy::TimestampDesc),
            limit: 2,
            after: Some((3_000, "s/a".to_string())),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(keys(&page2), vec!["s/c"]);
}
