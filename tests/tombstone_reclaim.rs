//! Sealed-tombstone reclaim (plan 0014 §1, ruling T1 — the tombstone
//! disjunct): a FIRST-CLASS `del` row inside a sealed segment is dead once
//! nothing shadows it (no newer delta rides its key) and it shadows nothing
//! (no strictly-older durable segment holds its key). The conservative
//! never-judge-by-below floor stays inviolate for live records
//! (`tombstone_shadowing_older_rows_survives_reclaim` in seal_reclaim.rs is
//! that pin; this file pins the disjunct's own boundary from every side).
use std::collections::BTreeMap;
use std::time::Duration;

use girder::{FsyncPolicy, Girder, GirderConfig, QuerySpec, Record};

fn rec(key: String, ts: i64, payload_len: usize) -> Record {
    Record {
        key,
        timestamp: ts,
        labels: BTreeMap::from([("project".to_string(), "prod".to_string())]),
        numerics: BTreeMap::new(),
        payload: vec![7u8; payload_len],
        text: None,
    }
}

/// Count-seal shape: compaction outputs seal at exactly the record cap, so
/// a merged 100-row tombstone segment (tiny bytes) still seals.
fn config(dir: &std::path::Path) -> GirderConfig {
    let mut cfg = GirderConfig::at(dir);
    cfg.fsync = FsyncPolicy::EveryN(64);
    cfg.memtable_max_records = 20;
    cfg.compact_min_segments = 3;
    cfg.max_segment_records = 100; // seal by COUNT
    cfg.max_segment_bytes = 256 * 1024 * 1024; // byte seal never trips
    cfg.tick_interval = Duration::from_secs(3600); // manual maintain() only
    cfg
}

/// Drive maintenance until a full pass changes nothing.
async fn settle(engine: &Girder) {
    let mut stable = 0;
    for _ in 0..300 {
        let before = engine.stats();
        engine.maintain().await.unwrap();
        let after = engine.stats();
        if after.compactions == before.compactions
            && after.reclaimed_segments == before.reclaimed_segments
        {
            stable += 1;
            if stable >= 3 {
                return;
            }
        } else {
            stable = 0;
        }
    }
    panic!("maintenance never converged: {:?}", engine.stats());
}

fn gird_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| e.path().extension().is_some_and(|x| x == "gird"))
                .count()
        })
        .unwrap_or(0)
}

/// THE disjunct: delete an entire sealed corpus; once the data rows are
/// reclaimed (shadowed-by-tombstone, the existing rule), the sealed
/// tombstone segment itself — nothing below, nothing above — is dropped
/// whole. End state: ZERO segment files, reads exact, count == scan,
/// reopen-stable, further passes no-ops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nothing_below_tombstones_reclaim_to_empty() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    for i in 0..100usize {
        engine
            .put(rec(format!("d/{i:08}"), i as i64, 1024))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    settle(&engine).await;

    for i in 0..100usize {
        engine
            .delete(format!("d/{i:08}"), 10_000 + i as i64)
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    settle(&engine).await;

    let spec = QuerySpec {
        key_prefix: Some("d/".into()),
        ..Default::default()
    };
    assert_eq!(engine.scan(&spec).await.unwrap().len(), 0, "reads exact");
    assert_eq!(engine.count(&spec).await.unwrap(), 0, "count == scan");
    assert!(engine.get("d/00000000").await.unwrap().is_none());
    assert!(
        engine.stats().reclaimed_segments >= 2,
        "data segment AND tombstone segment must both reclaim: {:?}",
        engine.stats()
    );
    assert_eq!(
        gird_files(dir.path()),
        0,
        "the tombstones must be PHYSICALLY gone, not merely suppressed"
    );

    // Reopen: the drop is durable and further maintenance is a no-op.
    drop(engine);
    let engine = Girder::open(config(dir.path())).await.unwrap();
    assert_eq!(engine.scan(&spec).await.unwrap().len(), 0);
    assert_eq!(engine.count(&spec).await.unwrap(), 0);
    settle(&engine).await;
    assert_eq!(gird_files(dir.path()), 0);
}

/// The floor from below: a sealed tombstone whose key still has a LIVE row
/// in an older segment must survive its segment's rewrite (dropping it
/// would resurrect the data) — retention has not reached those rows, and
/// reclaim must never front-run it (ruled retention-order pin, direction b).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tombstone_with_live_row_below_survives() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    // Older sealed segment: the live version of m/k among filler.
    engine.put(rec("m/k".to_string(), 10, 256)).await.unwrap();
    for i in 0..99usize {
        engine
            .put(rec(format!("a/{i:08}"), i as i64, 1024))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    settle(&engine).await;

    // Newer sealed segment: the FIRST-CLASS tombstone for m/k among its own
    // filler (n/* — overlapping the older segment only via zone range).
    engine.delete("m/k".to_string(), 20).await.unwrap();
    for i in 0..99usize {
        engine
            .put(rec(format!("n/{i:08}"), 500 + i as i64, 1024))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    settle(&engine).await;

    // Kill most of the tombstone's segment so its dead ratio trips.
    for i in 0..90usize {
        engine
            .put(rec(format!("n/{i:08}"), 10_000 + i as i64, 1024))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    settle(&engine).await;
    assert!(
        engine.stats().reclaimed_segments > 0,
        "the tombstone's segment must have been rewritten: {:?}",
        engine.stats()
    );

    // No resurrection: m/k stays deleted through the rewrite and a reopen.
    assert!(engine.get("m/k").await.unwrap().is_none(), "resurrected");
    drop(engine);
    let engine = Girder::open(config(dir.path())).await.unwrap();
    assert!(engine.get("m/k").await.unwrap().is_none(), "after reopen");
}

/// Delta safety, BOTH directions (extending probe_fold_routing's mixed
/// keyspace to the reclaim seam):
/// - an older DELTA row below a tombstone refuses the disjunct (dropping
///   the tombstone would un-shadow the delta);
/// - a NEWER delta chain above a tombstone refuses it (the tombstone is
///   the chain's base — dropping it would change the fold).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deltas_refuse_the_disjunct_in_both_directions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    // Older sealed segment: a counter delta for c/base among filler.
    engine
        .incr("c/base", 10, BTreeMap::from([("v".to_string(), 5.0)]))
        .await
        .unwrap();
    for i in 0..99usize {
        engine
            .put(rec(format!("a/{i:08}"), i as i64, 1024))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    settle(&engine).await;

    // Newer sealed segment: tombstones for c/base (delta BELOW) and c/chain
    // (delta ABOVE, written later) among filler.
    engine.delete("c/base".to_string(), 20).await.unwrap();
    engine.delete("c/chain".to_string(), 20).await.unwrap();
    for i in 0..98usize {
        engine
            .put(rec(format!("n/{i:08}"), 500 + i as i64, 1024))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    settle(&engine).await;

    // The NEWER delta chain above c/chain's tombstone.
    engine
        .incr("c/chain", 30, BTreeMap::from([("v".to_string(), 3.0)]))
        .await
        .unwrap();
    // Trip the tombstone segment's ratio via its filler.
    for i in 0..90usize {
        engine
            .put(rec(format!("n/{i:08}"), 10_000 + i as i64, 1024))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    settle(&engine).await;
    assert!(
        engine.stats().reclaimed_segments > 0,
        "{:?}",
        engine.stats()
    );

    // c/base stays deleted: the tombstone survived (delta below) — a drop
    // would have un-shadowed the +5 delta.
    assert!(
        engine.get("c/base").await.unwrap().is_none(),
        "older delta un-shadowed"
    );
    // c/chain folds from the tombstone base: exactly +3.
    let got = engine.get("c/chain").await.unwrap().expect("chain folds");
    assert_eq!(got.numerics.get("v").copied(), Some(3.0), "fold changed");

    drop(engine);
    let engine = Girder::open(config(dir.path())).await.unwrap();
    assert!(engine.get("c/base").await.unwrap().is_none());
    let got = engine.get("c/chain").await.unwrap().expect("after reopen");
    assert_eq!(got.numerics.get("v").copied(), Some(3.0));
}

/// Retention-order, direction (a): retention removes the data rows first
/// (their TTL expired), then the still-fresh sealed tombstones — now
/// shadowing nothing — reclaim through the disjunct, well before their own
/// TTL.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retention_then_reclaim_drops_fresh_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    let day = 86_400 * 1_000_000_000i64;
    let mut cfg = config(dir.path());
    cfg.retention = vec![("d/".to_string(), 7 * day)];
    let engine = Girder::open(cfg).await.unwrap();

    // Data rows already PAST retention (30 days old)...
    for i in 0..100usize {
        engine
            .put(rec(format!("d/{i:08}"), now - 30 * day + i as i64, 1024))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    // ...deleted with FRESH tombstones (ts rule: a tombstone must not
    // expire before what it shadows — `now` is ≥ every data ts).
    for i in 0..100usize {
        engine
            .delete(format!("d/{i:08}"), now + i as i64)
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    settle(&engine).await;

    let spec = QuerySpec {
        key_prefix: Some("d/".into()),
        ..Default::default()
    };
    assert_eq!(engine.scan(&spec).await.unwrap().len(), 0);
    assert_eq!(engine.count(&spec).await.unwrap(), 0);
    assert_eq!(
        gird_files(dir.path()),
        0,
        "expired data + fresh nothing-below tombstones must BOTH be gone"
    );
}

/// Kill-safety, by construction (the blobs-B3 pattern): the reclaim crash
/// window is segment-written-but-manifest-not-swapped — an orphan `.gird`
/// file the manifest never lists. Reads must ignore it, a reopen must stay
/// exact, and maintenance must stay convergent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_segment_file_is_inert() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    for i in 0..40usize {
        engine
            .put(rec(format!("k/{i:08}"), i as i64, 1024))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    settle(&engine).await;

    // Enact the kill residue: a stray file with the segment extension that
    // no manifest entry points at (contents irrelevant — never opened).
    std::fs::write(
        dir.path().join("seg-9999999999999999.gird"),
        b"orphaned by a kill between segment write and manifest swap",
    )
    .unwrap();

    let spec = QuerySpec {
        key_prefix: Some("k/".into()),
        ..Default::default()
    };
    assert_eq!(engine.scan(&spec).await.unwrap().len(), 40);
    settle(&engine).await; // idempotent re-audit, no churn, no panic
    assert_eq!(engine.scan(&spec).await.unwrap().len(), 40);

    drop(engine);
    let engine = Girder::open(config(dir.path())).await.unwrap();
    assert_eq!(engine.scan(&spec).await.unwrap().len(), 40);
    assert_eq!(engine.count(&spec).await.unwrap(), 40);
}
