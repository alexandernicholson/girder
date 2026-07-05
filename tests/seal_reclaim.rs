//! Sealed-segment reclamation (track F slice F3, rulings 8–12): overwritten
//! rows inside byte-sealed segments are reclaimed by dead-ratio solo
//! rewrites — WITHOUT reopening the D1 write-amp hole the seal fix closed
//! (`fat_record_compaction_converges` in tests/engine.rs is the floor: an
//! overwrite-free corpus must never trip a reclaim, pinned again here).
//!
//! Invariants pinned:
//! - overwrite-heavy sealed corpora reclaim AND converge (no perpetual
//!   rewrites);
//! - zero overwrites ⇒ zero reclaims;
//! - a counter BASE is never dropped while newer deltas ride above it (the
//!   fold needs it — the count()/compaction rule);
//! - a tombstone-convention record shadowing OLDER versions survives a
//!   reclaim of its own segment (rows are only ever dropped because of what
//!   sits ABOVE them, never below — the track-d resurrection rule).
use std::collections::BTreeMap;
use std::time::Duration;

use girder::{FsyncPolicy, Girder, GirderConfig, QuerySpec, Record};

fn big_record(key: String, ts: i64, payload_len: usize) -> Record {
    Record {
        key,
        timestamp: ts,
        labels: BTreeMap::from([("project".to_string(), "prod".to_string())]),
        numerics: BTreeMap::from([("latency_ms".to_string(), (ts % 2000) as f64)]),
        payload: vec![7u8; payload_len],
        text: None,
    }
}

/// The fat-record shape from `fat_record_compaction_converges`: byte cap
/// trips long before the record cap, so compaction outputs seal at >= 64KB.
fn config(dir: &std::path::Path) -> GirderConfig {
    let mut cfg = GirderConfig::at(dir);
    cfg.fsync = FsyncPolicy::EveryN(64);
    cfg.memtable_max_records = 20; // 20 x 1KB = ~20KB flushed segments
    cfg.compact_min_segments = 3;
    cfg.max_segment_records = 1_000; // never reached: bytes trip first
    cfg.max_segment_bytes = 128 * 1024; // outputs seal at >= 64KB (cap/2)
    cfg.tick_interval = Duration::from_secs(3600); // manual maintain() only
    cfg
}

/// Drive maintenance until a full pass changes nothing (compaction AND
/// reclaim), like the convergence loop in the fat-record test.
async fn settle(engine: &Girder) {
    let mut stable = 0;
    for _ in 0..200 {
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

/// Overwrite-heavy: most keys of a sealed corpus rewritten later. The dead
/// rows must be reclaimed (fully-dead sealed segments dropped whole), the
/// data must stay exact, and further passes must be no-ops forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaim_overwrite_heavy_and_converge() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    for i in 0..400usize {
        engine
            .put(big_record(format!("s/{i:08}"), i as i64, 1024))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    settle(&engine).await;
    assert_eq!(
        engine.stats().reclaimed_segments,
        0,
        "no overwrites yet — reclaim must not fire during consolidation"
    );

    // Overwrite the first 300 keys (sequential — the sealed segments covering
    // them become FULLY dead: the wholesale-drop path).
    for i in 0..300usize {
        engine
            .put(big_record(format!("s/{i:08}"), 10_000 + i as i64, 1024))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    settle(&engine).await;

    let stats = engine.stats();
    assert!(
        stats.reclaimed_segments > 0,
        "dead sealed rows must be reclaimed: {stats:?}"
    );
    // Dead rows actually left the disk: 400 live rows, and the residual dead
    // tail (below the 1/2 ratio in partially-dead segments) stays bounded.
    assert!(
        stats.total_records_in_segments <= 500,
        "reclaim must shed the 300 dead rows: {stats:?}"
    );

    // Exactness: newest-wins everywhere, nothing lost, count agrees.
    let all = engine.scan(&QuerySpec::default()).await.unwrap();
    assert_eq!(all.len(), 400);
    for r in &all {
        let i: usize = r.key[2..].parse().unwrap();
        let want_ts = if i < 300 { 10_000 + i as i64 } else { i as i64 };
        assert_eq!(
            r.timestamp, want_ts,
            "key {} lost its newest version",
            r.key
        );
    }
    assert_eq!(engine.count(&QuerySpec::default()).await.unwrap(), 400);

    // Converged = further passes are no-ops, forever (the write-amp bound).
    let at = engine.stats();
    for _ in 0..5 {
        engine.maintain().await.unwrap();
    }
    let after = engine.stats();
    assert_eq!(
        (after.compactions, after.reclaimed_segments),
        (at.compactions, at.reclaimed_segments),
        "reclaim must converge, never churn: {after:?}"
    );
}

/// The regression floor restated locally: an overwrite-free sealed corpus
/// never trips a reclaim (dead ratio is zero everywhere), no matter how many
/// passes run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_overwrites_no_reclaim() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    for i in 0..400usize {
        engine
            .put(big_record(format!("s/{i:08}"), i as i64, 1024))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    settle(&engine).await;
    for _ in 0..10 {
        engine.maintain().await.unwrap();
    }
    let stats = engine.stats();
    assert_eq!(stats.reclaimed_segments, 0, "{stats:?}");
    assert_eq!(engine.scan(&QuerySpec::default()).await.unwrap().len(), 400);
}

/// A counter BASE inside a sealed segment must survive a reclaim while any
/// newer delta rides above it, even when every other row of the segment is
/// dead — dropping it would silently regress the fold (ruling 8's
/// any-newer-delta = keep).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn counter_base_survives_reclaim() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    // Sealed segment: 99 fat rows + the counter base (non-delta, n = 5).
    let base_key = "s/00000050c";
    for i in 0..100usize {
        engine
            .put(big_record(format!("s/{i:08}"), i as i64, 1024))
            .await
            .unwrap();
    }
    let mut base = big_record(base_key.to_string(), 50, 1024);
    base.numerics.insert("n".to_string(), 5.0);
    engine.put(base).await.unwrap();
    engine.flush().await.unwrap();
    settle(&engine).await;

    // Newer segments: deltas on the base + overwrites making >= half of the
    // sealed rows dead.
    for k in 0..3 {
        engine
            .incr(base_key, 200 + k, BTreeMap::from([("n".to_string(), 1.0)]))
            .await
            .unwrap();
    }
    for i in 0..80usize {
        engine
            .put(big_record(format!("s/{i:08}"), 10_000 + i as i64, 1024))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    settle(&engine).await;
    assert!(
        engine.stats().reclaimed_segments > 0,
        "the dead ratio must have tripped: {:?}",
        engine.stats()
    );

    // The fold is exact: base survived wherever it now lives.
    let got = engine.get(base_key).await.unwrap().expect("base key lives");
    assert_eq!(
        got.numerics.get("n"),
        Some(&8.0),
        "counter base lost by reclaim: {got:?}"
    );
    // And it stays exact after further passes.
    for _ in 0..5 {
        engine.maintain().await.unwrap();
    }
    let got = engine.get(base_key).await.unwrap().expect("base key lives");
    assert_eq!(got.numerics.get("n"), Some(&8.0));
}

/// The track-d resurrection rule, reclaim-side: a tombstone-convention
/// record (a newer full record whose job is to SHADOW an older live version
/// below) must survive a reclaim of its own sealed segment — it has nothing
/// newer above it, so it is never dead, by construction. Dropping it would
/// resurrect the older version.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tombstone_shadowing_older_rows_survives_reclaim() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    // Older sealed segment(s): the live version of m/k among fat filler.
    let mut live = big_record("m/k".to_string(), 10, 256);
    live.labels.insert("state".to_string(), "live".to_string());
    engine.put(live).await.unwrap();
    for i in 0..100usize {
        engine
            .put(big_record(format!("a/{i:08}"), i as i64, 1024))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    settle(&engine).await;

    // Newer sealed segment: the tombstone for m/k among its own fat filler
    // (keys n/* — overlapping the older segment only on m/k).
    let mut tomb = big_record("m/k".to_string(), 20, 8);
    tomb.labels
        .insert("state".to_string(), "deleted".to_string());
    engine.put(tomb).await.unwrap();
    for i in 0..100usize {
        engine
            .put(big_record(format!("n/{i:08}"), 500 + i as i64, 1024))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    settle(&engine).await;

    // Kill most of the tombstone's segment: overwrite the n/* filler so the
    // dead ratio trips and the segment is solo-rewritten.
    for i in 0..90usize {
        engine
            .put(big_record(format!("n/{i:08}"), 10_000 + i as i64, 1024))
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

    // The tombstone survived the rewrite; point-get and the unconditioned
    // scan resolve to it, never the shadowed older version.
    let got = engine.get("m/k").await.unwrap().expect("m/k must resolve");
    assert_eq!(got.timestamp, 20, "old version resurrected: {got:?}");
    assert_eq!(got.labels.get("state").map(String::as_str), Some("deleted"));
    let by_prefix = engine
        .scan(&QuerySpec {
            key_prefix: Some("m/".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(by_prefix.len(), 1);
    assert_eq!(
        by_prefix[0].timestamp, 20,
        "old version resurrected in scan"
    );

    // NOTE (deliberate omission): a LABEL-conditioned scan (`state == live`)
    // can still resurrect the old version here — the tombstone-holding
    // segment is zone-pruned out of the walk, so its keys never shadow.
    // That is the pre-existing engine bug track-d is fixing (first-class
    // delete + has_tombstones zone flag + force-visit-for-seen); it fires
    // with or without a reclaim and is NOT introduced by this slice.
    // Once that fix lands and this branch rebases onto it, add:
    //   scan(labels: [(state, live)], key_prefix: m/) must be empty.
}
