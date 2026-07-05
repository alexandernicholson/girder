//! End-to-end engine tests: durability, recovery, flush, dedupe, compaction,
//! tiering, retention, pruning, cache.
use std::collections::BTreeMap;
use std::time::Duration;

use girder::{FsyncPolicy, Girder, GirderConfig, OrderBy, QuerySpec, Record};

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
            .put(record(
                &format!("s/{i:03}"),
                i,
                if i % 2 == 0 { "gpt-4o" } else { "claude" },
                i as f64,
            ))
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
        .scan(&QuerySpec {
            key_prefix: Some("s/".into()),
            ..Default::default()
        })
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
            engine
                .put(record(&format!("k{i}"), i, "gpt-4o", 1.0))
                .await
                .unwrap();
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
    let records: Vec<Record> = (0..250)
        .map(|i| record(&format!("k{i:04}"), i, "gpt-4o", 1.0))
        .collect();
    for chunk in records.chunks(50) {
        engine.put_batch(chunk.to_vec()).await.unwrap();
    }
    engine.flush().await.unwrap();
    let stats = engine.stats();
    assert!(stats.hot_segments >= 2, "{stats:?}");
    assert_eq!(
        stats.total_records_in_segments + stats.memtable_records,
        250
    );

    // First scan loads segments (misses), second scan hits the cache.
    let spec = QuerySpec {
        labels: vec![("model".into(), "gpt-4o".into())],
        ..Default::default()
    };
    engine.scan(&spec).await.unwrap();
    let misses_after_first = engine.stats().cache_misses;
    engine.scan(&spec).await.unwrap();
    let stats = engine.stats();
    assert_eq!(
        stats.cache_misses, misses_after_first,
        "second scan fully cached"
    );
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
                .put(record(
                    &format!("k{i:02}"),
                    round * 100 + i,
                    &format!("v{round}"),
                    1.0,
                ))
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
    assert_eq!(
        (stats.hot_segments, stats.cold_segments),
        (0, 1),
        "{stats:?}"
    );
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
        engine
            .put(record(&format!("a{i}"), i, "alpha", 1.0))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    for i in 0..50 {
        engine
            .put(record(&format!("b{i}"), i, "beta", 1.0))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();

    // Query for gamma: zone maps exclude BOTH segments → zero disk loads.
    let before = engine.stats().cache_misses;
    let none = engine
        .scan(&QuerySpec {
            labels: vec![("model".into(), "gamma".into())],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(none.is_empty());
    assert_eq!(engine.stats().cache_misses, before, "no segment was loaded");

    // Query for alpha: only segment A loads.
    engine
        .scan(&QuerySpec {
            labels: vec![("model".into(), "alpha".into())],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        engine.stats().cache_misses,
        before + 1,
        "exactly one segment loaded"
    );
}

/// Newest-wins across *overlapping* (non-disjoint) segments: a key rewritten
/// in a newer segment with a value that no longer matches must not be emitted
/// from the older (still-matching) segment — as long as the newer segment is
/// itself visited (not zone-pruned). Exercises the column-native scan's
/// cross-segment shadow tracking through the block-pruned emit path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn column_scan_newest_wins_shadowing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    // Segment 1 (older): k matches latency 1500.
    engine.put(record("k", 1, "gpt-4o", 1500.0)).await.unwrap();
    engine.flush().await.unwrap();
    // Segment 2 (newer): k now has latency 5 (won't match), plus m (matches).
    // The extra `m` keeps segment 2's latency zone map wide enough that the
    // query can't prune it, so the scan actually visits it and shadows k.
    engine.put(record("k", 2, "gpt-4o", 5.0)).await.unwrap();
    engine.put(record("m", 2, "gpt-4o", 1600.0)).await.unwrap();
    engine.flush().await.unwrap();

    let hits = engine
        .scan(&QuerySpec {
            numeric_ranges: vec![("latency_ms".into(), 1000.0, f64::MAX)],
            ..Default::default()
        })
        .await
        .unwrap();
    let keys: Vec<&str> = hits.iter().map(|r| r.key.as_str()).collect();
    assert_eq!(
        keys,
        vec!["m"],
        "newest (non-matching) k is shadowed; only m matches"
    );

    // The current value of k is the newer one.
    let g = engine.get("k").await.unwrap().unwrap();
    assert_eq!(g.timestamp, 2);
    assert_eq!(g.numerics["latency_ms"], 5.0);
}

/// Differential test: the column-native scan must agree with a naive
/// newest-wins oracle across many specs, mixed schemas, and overlapping
/// segments (both the disjoint fast path and the overlap shadow path).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn column_scan_matches_naive_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.memtable_max_records = 200;
    let engine = Girder::open(cfg).await.unwrap();

    // Deterministic pseudo-random puts, some keys rewritten (overlap), mixed
    // labels/numerics. Track the newest-wins truth in `oracle`.
    let mut oracle: BTreeMap<String, Record> = BTreeMap::new();
    let mut state = 0xC0FFEEu64;
    let mut rng = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut ts = 0i64;
    for _ in 0..2500 {
        ts += 1;
        let key = format!("k/{:04}", rng() % 400); // 400 keys → lots of rewrites
        let model = ["gpt-4o", "claude", "llama"][(rng() % 3) as usize];
        let mut labels = BTreeMap::from([("model".to_string(), model.to_string())]);
        if rng() % 2 == 0 {
            labels.insert("region".to_string(), format!("r{}", rng() % 4));
        }
        let mut numerics = BTreeMap::new();
        if rng() % 5 != 0 {
            numerics.insert("latency_ms".to_string(), (rng() % 2000) as f64);
        }
        let rec = Record {
            key: key.clone(),
            timestamp: ts,
            labels,
            numerics,
            payload: format!("pl-{key}-{ts}").into_bytes(),
        };
        oracle.insert(key, rec.clone());
        engine.put(rec).await.unwrap();
        if rng() % 300 == 0 {
            engine.flush().await.unwrap(); // create overlapping segments
        }
    }
    engine.maintain().await.unwrap(); // exercise compaction (v2 merge) too

    let specs = vec![
        QuerySpec::default(),
        QuerySpec {
            numeric_ranges: vec![("latency_ms".into(), 1500.0, f64::MAX)],
            ..Default::default()
        },
        QuerySpec {
            labels: vec![("model".into(), "gpt-4o".into())],
            numeric_ranges: vec![("latency_ms".into(), 500.0, f64::MAX)],
            ..Default::default()
        },
        QuerySpec {
            labels: vec![("region".into(), "r2".into())],
            ..Default::default()
        },
        QuerySpec {
            time: Some((2000, 2400)),
            ..Default::default()
        },
        QuerySpec {
            key_prefix: Some("k/01".into()),
            ..Default::default()
        },
        QuerySpec {
            labels: vec![("model".into(), "claude".into())],
            limit: 10,
            ..Default::default()
        },
    ];
    for spec in &specs {
        let mut expected: Vec<Record> = oracle
            .values()
            .filter(|r| spec.matches(r))
            .cloned()
            .collect();
        expected.sort_by(|a, b| {
            b.timestamp
                .cmp(&a.timestamp)
                .then_with(|| a.key.cmp(&b.key))
        });
        if spec.limit > 0 {
            expected.truncate(spec.limit);
        }
        let got = engine.scan(spec).await.unwrap();
        assert_eq!(got, expected, "spec {spec:?}");
    }
}

// ---------------------------------------------------------------------------
// WS2: order_by / top-k pushdown
// ---------------------------------------------------------------------------

/// Numeric-column value for ordering: absent or NaN ⇒ `None` (ranks last).
fn ord_num(r: &Record, name: &str) -> Option<f64> {
    r.numerics.get(name).copied().filter(|v| !v.is_nan())
}

/// Reference total order mirroring the engine's `order_by` semantics: the
/// ordered dimension first, key ascending as the tiebreak, missing values last.
fn oracle_cmp(order: &OrderBy, a: &Record, b: &Record) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let primary = match order {
        OrderBy::TimestampDesc => b.timestamp.cmp(&a.timestamp),
        OrderBy::TimestampAsc => a.timestamp.cmp(&b.timestamp),
        OrderBy::NumericDesc(n) => match (ord_num(a, n), ord_num(b, n)) {
            (Some(x), Some(y)) => y.total_cmp(&x), // higher first
            (Some(_), None) => Ordering::Less,     // present before missing
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        },
        OrderBy::NumericAsc(n) => match (ord_num(a, n), ord_num(b, n)) {
            (Some(x), Some(y)) => x.total_cmp(&y), // lower first
            (Some(_), None) => Ordering::Less,     // present before missing
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        },
    };
    primary.then_with(|| a.key.cmp(&b.key))
}

/// Naive newest-wins oracle: filter, sort by the given order, truncate.
fn oracle_page(truth: &BTreeMap<String, Record>, spec: &QuerySpec, order: &OrderBy) -> Vec<Record> {
    let mut v: Vec<Record> = truth
        .values()
        .filter(|r| spec.matches(r))
        .cloned()
        .collect();
    v.sort_by(|a, b| oracle_cmp(order, a, b));
    if spec.limit > 0 {
        v.truncate(spec.limit);
    }
    v
}

/// Top-k with every `OrderBy` and several limits must match a naive full-scan
/// oracle across a corpus with rewrites (overlapping segments), mixed schemas,
/// and missing/NaN order-by values. This is the soundness proof for the
/// bounded-heap + early-termination scan path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topk_matches_naive_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.memtable_max_records = 150; // many segments, some overlapping
    let engine = Girder::open(cfg).await.unwrap();

    let mut truth: BTreeMap<String, Record> = BTreeMap::new();
    let mut state = 0xABCDEF01u64;
    let mut rng = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for ts in 1..=3000i64 {
        // 300 keys → lots of rewrites; some rewrites carry a LOWER timestamp.
        let key = format!("k/{:04}", rng() % 300);
        let this_ts = if rng() % 7 == 0 {
            (rng() % 3000) as i64
        } else {
            ts
        };
        let model = ["gpt-4o", "claude", "llama"][(rng() % 3) as usize];
        let mut labels = BTreeMap::from([("model".to_string(), model.to_string())]);
        if rng() % 2 == 0 {
            labels.insert("region".to_string(), format!("r{}", rng() % 4));
        }
        let mut numerics = BTreeMap::new();
        if rng() % 5 != 0 {
            numerics.insert("latency_ms".to_string(), (rng() % 2000) as f64);
        }
        if rng() % 3 == 0 {
            numerics.insert("tokens".to_string(), (rng() % 500) as f64);
        }
        let rec = Record {
            key: key.clone(),
            timestamp: this_ts,
            labels,
            numerics,
            payload: format!("pl-{key}-{this_ts}").into_bytes(),
        };
        truth.insert(key, rec.clone());
        engine.put(rec).await.unwrap();
        if rng() % 250 == 0 {
            engine.flush().await.unwrap(); // overlapping segments
        }
    }
    engine.maintain().await.unwrap(); // exercise compaction too

    let orders = [
        OrderBy::TimestampDesc,
        OrderBy::TimestampAsc,
        OrderBy::NumericDesc("latency_ms".into()),
        OrderBy::NumericAsc("latency_ms".into()),
        OrderBy::NumericDesc("tokens".into()),
    ];
    let filters = [
        QuerySpec::default(),
        QuerySpec {
            labels: vec![("model".into(), "gpt-4o".into())],
            numeric_ranges: vec![("latency_ms".into(), 1000.0, f64::MAX)],
            ..Default::default()
        },
        QuerySpec {
            labels: vec![("region".into(), "r1".into())],
            ..Default::default()
        },
        QuerySpec {
            time: Some((1000, 2500)),
            ..Default::default()
        },
    ];
    for order in &orders {
        for base in &filters {
            for &limit in &[1usize, 5, 50, 100000] {
                let spec = QuerySpec {
                    order_by: Some(order.clone()),
                    limit,
                    ..base.clone()
                };
                let got = engine.scan(&spec).await.unwrap();
                let want = oracle_page(&truth, &spec, order);
                assert_eq!(got, want, "order {order:?} limit {limit} base {base:?}");
            }
        }
    }
}

/// `order_by: None` and `order_by: Some(TimestampDesc)` must produce the exact
/// same page (same set, same order) — the full-sort path and the bounded-heap
/// path agree.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn order_by_none_equals_timestamp_desc() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.memtable_max_records = 120;
    let engine = Girder::open(cfg).await.unwrap();
    for i in 0..900 {
        let ts = ((i * 7) % 900) as i64; // non-monotonic timestamps
        engine
            .put(record(&format!("k{i:04}"), ts, "gpt-4o", (i % 300) as f64))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();

    let base = QuerySpec {
        numeric_ranges: vec![("latency_ms".into(), 50.0, f64::MAX)],
        limit: 50,
        ..Default::default()
    };
    let none = engine.scan(&base).await.unwrap();
    let desc = engine
        .scan(&QuerySpec {
            order_by: Some(OrderBy::TimestampDesc),
            ..base.clone()
        })
        .await
        .unwrap();
    assert_eq!(none, desc, "None must equal TimestampDesc, page for page");
}

/// Newest-wins vs early termination (the subtle one): a key rewritten in a
/// NEWER segment with a LOWER timestamp must resolve to the new version, and
/// the stale high-timestamp version must never surface in a timestamp-desc
/// page — even though the older segment's zone map has a higher `max_ts`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn early_termination_respects_lower_ts_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.compact_min_segments = 100; // keep segments separate
    let engine = Girder::open(cfg).await.unwrap();

    // Older segment: high timestamps, including K@500.
    engine.put(record("A", 501, "gpt-4o", 1.0)).await.unwrap();
    engine.put(record("B", 502, "gpt-4o", 1.0)).await.unwrap();
    engine.put(record("K", 500, "gpt-4o", 1.0)).await.unwrap();
    engine.flush().await.unwrap();
    // Newer segment: K rewritten with a LOWER timestamp, plus low-ts fillers.
    engine.put(record("C", 2, "gpt-4o", 1.0)).await.unwrap();
    engine.put(record("D", 3, "gpt-4o", 1.0)).await.unwrap();
    engine.put(record("K", 1, "gpt-4o", 1.0)).await.unwrap();
    engine.flush().await.unwrap();

    let spec = QuerySpec {
        order_by: Some(OrderBy::TimestampDesc),
        limit: 3,
        ..Default::default()
    };
    let got = engine.scan(&spec).await.unwrap();
    let keys: Vec<(&str, i64)> = got.iter().map(|r| (r.key.as_str(), r.timestamp)).collect();
    // Top-3 by ts desc over newest-wins truth {A@501,B@502,C@2,D@3,K@1}.
    assert_eq!(keys, vec![("B", 502), ("A", 501), ("D", 3)]);
    // K resolves to its newest (lower-ts) version; the stale K@500 is gone.
    let k = engine.get("K").await.unwrap().unwrap();
    assert_eq!(k.timestamp, 1);
    assert!(!got.iter().any(|r| r.key == "K" && r.timestamp == 500));
}

/// Early termination must not touch old segments: a newest-page query over
/// many time-adjacent segments loads only the trailing (newest) segment(s),
/// so `cache_misses` stays far below the segment count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn early_termination_skips_old_segments() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.memtable_max_records = 100;
    cfg.compact_min_segments = 1000; // never compact
    let engine = Girder::open(cfg).await.unwrap();

    // 10 disjoint, time-adjacent segments of 100 records (ts increasing).
    for seg in 0..10 {
        for i in 0..100 {
            let n = seg * 100 + i;
            engine
                .put(record(&format!("k{n:05}"), n as i64, "gpt-4o", 1.0))
                .await
                .unwrap();
        }
        engine.flush().await.unwrap();
    }
    assert_eq!(engine.stats().hot_segments, 10);

    let before = engine.stats().cache_misses;
    let spec = QuerySpec {
        order_by: Some(OrderBy::TimestampDesc),
        limit: 50,
        ..Default::default()
    };
    let got = engine.scan(&spec).await.unwrap();
    let loaded = engine.stats().cache_misses - before;
    // The newest segment (ts 900..999) already fills the 50-row page; the
    // suffix bound stops the scan before any older segment is loaded.
    assert_eq!(loaded, 1, "only the newest segment was loaded");
    // And the page is correct: the 50 newest timestamps, descending.
    let want: Vec<i64> = (950..1000).rev().collect();
    let got_ts: Vec<i64> = got.iter().map(|r| r.timestamp).collect();
    assert_eq!(got_ts, want);
}

// ---------------------------------------------------------------------------
// WS3: size-capped time-adjacent tiered compaction + zero-clone flush
// ---------------------------------------------------------------------------

/// Deterministic xorshift for reproducible pseudo-random corpora.
fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// Compaction splits the merged stream into cap-sized segments (never one
/// giant), preserves every record (no loss), and — crucially — does NOT
/// re-merge already-capped ("sealed") segments, so write amplification and the
/// live segment count stay bounded instead of churning forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiered_compaction_caps_segments_and_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = GirderConfig::at(dir.path());
    cfg.fsync = FsyncPolicy::Always;
    cfg.memtable_max_records = 100;
    cfg.compact_min_segments = 4;
    cfg.max_segment_records = 250;
    cfg.tick_interval = Duration::from_secs(3600);
    let engine = Girder::open(cfg).await.unwrap();

    // 2000 distinct keys across 20 flushed segments (100 each). No rewrites.
    for seg in 0..20 {
        for i in 0..100 {
            let n = seg * 100 + i;
            engine
                .put(record(
                    &format!("k{n:05}"),
                    n as i64,
                    "gpt-4o",
                    (n % 500) as f64,
                ))
                .await
                .unwrap();
        }
        engine.flush().await.unwrap();
    }
    assert_eq!(engine.stats().hot_segments, 20);

    engine.maintain().await.unwrap(); // one tiered compaction pass
    let stats = engine.stats();
    assert_eq!(stats.total_records_in_segments, 2000, "no records lost");
    // 2000 records / cap 250 = 8 segments, NOT one giant merge-all segment.
    assert_eq!(stats.hot_segments, 8, "{stats:?}");

    // Every record is still queryable exactly once.
    let all = engine
        .scan(&QuerySpec {
            key_prefix: Some("k".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(all.len(), 2000);

    // Idempotent: sealed (at-cap) segments are never re-compacted → no churn.
    let comps = engine.stats().compactions;
    for _ in 0..3 {
        engine.maintain().await.unwrap();
    }
    assert_eq!(
        engine.stats().compactions,
        comps,
        "at-cap segments must not re-compact (bounded write amplification)"
    );
}

/// Compaction invariant: newest-wins dedupe and no lost records hold across
/// MANY tiered compaction passes with splitting and overlapping (rewritten)
/// keys, matched against a naive newest-wins oracle over several query shapes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiered_compaction_preserves_newest_wins_and_no_loss() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = GirderConfig::at(dir.path());
    cfg.fsync = FsyncPolicy::Always;
    cfg.memtable_max_records = 150;
    cfg.compact_min_segments = 3;
    cfg.max_segment_records = 300; // small cap → forces splits during compaction
    cfg.tick_interval = Duration::from_secs(3600);
    let engine = Girder::open(cfg).await.unwrap();

    let mut oracle: BTreeMap<String, Record> = BTreeMap::new();
    let mut state = 0x5EED_1234u64;
    let mut ts = 0i64;
    for _ in 0..4000 {
        ts += 1;
        let key = format!("k/{:04}", xorshift(&mut state) % 500); // rewrites
        let model = ["gpt-4o", "claude", "llama"][(xorshift(&mut state) % 3) as usize];
        let mut labels = BTreeMap::from([("model".to_string(), model.to_string())]);
        if xorshift(&mut state).is_multiple_of(2) {
            labels.insert(
                "region".to_string(),
                format!("r{}", xorshift(&mut state) % 4),
            );
        }
        let mut numerics = BTreeMap::new();
        if !xorshift(&mut state).is_multiple_of(5) {
            numerics.insert(
                "latency_ms".to_string(),
                (xorshift(&mut state) % 2000) as f64,
            );
        }
        let rec = Record {
            key: key.clone(),
            timestamp: ts,
            labels,
            numerics,
            payload: format!("pl-{key}-{ts}").into_bytes(),
        };
        oracle.insert(key, rec.clone());
        engine.put(rec).await.unwrap();
        if xorshift(&mut state).is_multiple_of(200) {
            engine.flush().await.unwrap();
        }
        if xorshift(&mut state).is_multiple_of(500) {
            engine.maintain().await.unwrap(); // force compaction mid-build
        }
    }
    engine.flush().await.unwrap();
    for _ in 0..4 {
        engine.maintain().await.unwrap(); // several more compaction passes
    }
    assert!(engine.stats().compactions >= 2, "compaction actually ran");

    let specs = vec![
        QuerySpec::default(),
        QuerySpec {
            numeric_ranges: vec![("latency_ms".into(), 1500.0, f64::MAX)],
            ..Default::default()
        },
        QuerySpec {
            labels: vec![("model".into(), "gpt-4o".into())],
            numeric_ranges: vec![("latency_ms".into(), 500.0, f64::MAX)],
            ..Default::default()
        },
        QuerySpec {
            labels: vec![("region".into(), "r2".into())],
            ..Default::default()
        },
        QuerySpec {
            key_prefix: Some("k/01".into()),
            ..Default::default()
        },
    ];
    for spec in &specs {
        let mut expected: Vec<Record> = oracle
            .values()
            .filter(|r| spec.matches(r))
            .cloned()
            .collect();
        expected.sort_by(|a, b| {
            b.timestamp
                .cmp(&a.timestamp)
                .then_with(|| a.key.cmp(&b.key))
        });
        let got = engine.scan(spec).await.unwrap();
        assert_eq!(got, expected, "spec {spec:?}");
    }
}

/// `recent` pruning guaranteed BY CONSTRUCTION: with time-adjacent tiered
/// compaction forced mid-build, a newest-page (timestamp desc) query touches
/// only the trailing (newest) segments — not the whole corpus — even though
/// everything has been compacted and re-split.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recent_pruning_guaranteed_after_tiered_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = GirderConfig::at(dir.path());
    cfg.fsync = FsyncPolicy::Always;
    cfg.memtable_max_records = 100;
    cfg.compact_min_segments = 4;
    cfg.max_segment_records = 250;
    cfg.tick_interval = Duration::from_secs(3600);
    let engine = Girder::open(cfg).await.unwrap();

    // Time-correlated keys/timestamps across 3000 records, compacting mid-build.
    for seg in 0..30 {
        for i in 0..100 {
            let n = seg * 100 + i;
            engine
                .put(record(&format!("k{n:06}"), n as i64, "gpt-4o", 1.0))
                .await
                .unwrap();
        }
        engine.flush().await.unwrap();
        if seg % 5 == 4 {
            engine.maintain().await.unwrap(); // force tiered compaction mid-build
        }
    }
    engine.maintain().await.unwrap();
    let stats = engine.stats();
    assert_eq!(stats.total_records_in_segments, 3000, "no loss");
    assert!(stats.hot_segments >= 8, "many segments: {stats:?}");
    assert!(stats.compactions >= 3, "compaction ran repeatedly");

    // Newest page: order_by timestamp desc, limit 50.
    let before = engine.stats().cache_misses;
    let spec = QuerySpec {
        order_by: Some(OrderBy::TimestampDesc),
        limit: 50,
        ..Default::default()
    };
    let got = engine.scan(&spec).await.unwrap();
    let loaded = engine.stats().cache_misses - before;

    // Correct page: the 50 newest timestamps, descending.
    let want: Vec<i64> = (2950..3000).rev().collect();
    let got_ts: Vec<i64> = got.iter().map(|r| r.timestamp).collect();
    assert_eq!(got_ts, want);
    // By construction the suffix-max bound stops the scan after the trailing
    // segment(s) — a small constant, far below the total segment count.
    assert!(
        loaded * 4 <= stats.hot_segments as u64,
        "recent page loaded {loaded} of {} segments — pruning not by construction",
        stats.hot_segments
    );
}

/// Manifest atomicity: after compaction, the manifest is the sole source of
/// truth (old input files are garbage). Re-opening the engine reloads only the
/// manifest's segments and every record survives with newest-wins intact — a
/// torn compaction would surface here as loss or duplication.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_manifest_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let build_cfg = || {
        let mut cfg = GirderConfig::at(dir.path());
        cfg.fsync = FsyncPolicy::Always;
        cfg.memtable_max_records = 100;
        cfg.compact_min_segments = 3;
        cfg.max_segment_records = 250;
        cfg.tick_interval = Duration::from_secs(3600);
        cfg
    };
    let mut oracle: BTreeMap<String, Record> = BTreeMap::new();
    {
        let engine = Girder::open(build_cfg()).await.unwrap();
        let mut state = 0xA11CE99u64;
        let mut ts = 0i64;
        for _ in 0..1500 {
            ts += 1;
            let key = format!("k{:04}", xorshift(&mut state) % 300); // rewrites
            let rec = record(&key, ts, "gpt-4o", (xorshift(&mut state) % 100) as f64);
            oracle.insert(key, rec.clone());
            engine.put(rec).await.unwrap();
            if xorshift(&mut state).is_multiple_of(120) {
                engine.flush().await.unwrap();
            }
        }
        engine.flush().await.unwrap();
        engine.maintain().await.unwrap();
        engine.maintain().await.unwrap();
        assert!(engine.stats().compactions >= 1);
        // Drop WITHOUT close() — the manifest on disk must already be complete.
        drop(engine);
    }

    // Re-open: state comes only from the persisted manifest + segments.
    let engine = Girder::open(build_cfg()).await.unwrap();
    let all = engine.scan(&QuerySpec::default()).await.unwrap();
    let mut expected: Vec<Record> = oracle.values().cloned().collect();
    expected.sort_by(|a, b| {
        b.timestamp
            .cmp(&a.timestamp)
            .then_with(|| a.key.cmp(&b.key))
    });
    assert_eq!(all, expected, "manifest reload: no lost/duplicated records");
    // Spot-check newest-wins for a specific key after reopen.
    let k = &expected[0].key;
    assert_eq!(engine.get(k).await.unwrap().unwrap(), expected[0]);
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
        .scan(&QuerySpec {
            key_prefix: Some("w".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(all.len(), 800);
}

// ---------------------------------------------------------------------------
// WS4: section-granular cache + targeted read_exact_at I/O
// ---------------------------------------------------------------------------

/// A bench-shaped record with a controllable payload size (payloads dominate
/// the on-disk footprint; columns are tiny).
fn big_record(i: usize, payload_len: usize) -> Record {
    Record {
        key: format!("s/{i:08}"),
        timestamp: i as i64,
        labels: BTreeMap::from([
            (
                "model".to_string(),
                ["gpt-4o", "claude", "llama"][i % 3].to_string(),
            ),
            ("project".to_string(), "prod".to_string()),
        ]),
        numerics: BTreeMap::from([("latency_ms".to_string(), (i % 2000) as f64)]),
        payload: vec![7u8; payload_len],
    }
}

/// Sum of all `.gird` segment file sizes across the hot + cold dirs.
fn on_disk_segment_bytes(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    for d in [dir.to_path_buf(), dir.join("cold")] {
        if let Ok(rd) = std::fs::read_dir(&d) {
            for e in rd.flatten() {
                if e.file_name().to_string_lossy().ends_with(".gird") {
                    total += e.metadata().map(|m| m.len()).unwrap_or(0);
                }
            }
        }
    }
    total
}

/// A newest-first descending page comparator matching the engine's default.
fn ts_desc_key(a: &Record, b: &Record) -> std::cmp::Ordering {
    b.timestamp
        .cmp(&a.timestamp)
        .then_with(|| a.key.cmp(&b.key))
}

/// A cold `selective` scan reads only the column sections it needs plus the
/// surviving rows' payloads — never the payload blob. With payloads far larger
/// than the columns, `bytes_read` stays a small fraction of the on-disk size and
/// well under the 64 MB budget (the WS4 acceptance target, scaled to run as a
/// unit test; the 1M number is in `benches/engine.rs`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_selective_reads_only_columns_not_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = GirderConfig::at(dir.path());
    cfg.fsync = FsyncPolicy::EveryN(256);
    cfg.memtable_max_records = 10_000;
    cfg.compact_min_segments = 8;
    cfg.tick_interval = Duration::from_secs(3600);
    let engine = Girder::open(cfg).await.unwrap();

    let n = 100_000usize;
    let payload_len = 1200usize; // ~realistic span JSON size
    for b in 0..n / 500 {
        let chunk: Vec<Record> = (0..500)
            .map(|i| big_record(b * 500 + i, payload_len))
            .collect();
        engine.put_batch(chunk).await.unwrap();
    }
    engine.flush().await.unwrap();
    engine.maintain().await.unwrap();

    let on_disk = on_disk_segment_bytes(dir.path());
    assert!(
        on_disk > 100 * 1024 * 1024,
        "corpus should be >100MB on disk (payloads dominate): {on_disk}"
    );
    // Cold: nothing has been read yet (the build/compaction paths don't touch
    // the query-side counter).
    assert_eq!(
        engine.stats().bytes_read,
        0,
        "no query reads before the first scan"
    );

    let spec = QuerySpec {
        numeric_ranges: vec![("latency_ms".into(), 1995.0, f64::MAX)],
        limit: 50,
        ..Default::default()
    };
    let hits = engine.scan(&spec).await.unwrap();
    assert_eq!(hits.len(), 50);
    assert!(hits.iter().all(|r| r.numerics["latency_ms"] >= 1995.0));

    let read = engine.stats().bytes_read;
    assert!(read > 0, "the scan must have read the columns");
    // The payload blob (the bulk of on_disk) was NOT faulted in: reads scale with
    // columns + survivor payloads, not with the whole file.
    assert!(
        read * 4 < on_disk,
        "cold selective read {read} B of {on_disk} B on disk — payload blob not skipped"
    );
    assert!(
        read < 64 * 1024 * 1024,
        "cold selective read {read} B exceeds the 64MB budget"
    );
}

/// Under a `cache_bytes` far smaller than the working set, scans stay correct
/// across many segments — sections are evicted and re-read, never all pinned,
/// which is exactly what keeps resident memory bounded by `cache_bytes`. Results
/// are checked against a naive newest-wins oracle, and a repeat scan (heavy
/// eviction/reload churn) still agrees.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiny_cache_scans_are_correct_under_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = GirderConfig::at(dir.path());
    cfg.fsync = FsyncPolicy::EveryN(256);
    cfg.memtable_max_records = 1000;
    cfg.compact_min_segments = 100_000; // never compact → many small segments
    cfg.cache_bytes = 128 * 1024; // tiny: one segment's columns don't all fit
    cfg.tick_interval = Duration::from_secs(3600);
    let engine = Girder::open(cfg).await.unwrap();

    let n = 20_000usize;
    let mut oracle: Vec<Record> = Vec::with_capacity(n);
    for i in 0..n {
        let r = big_record(i, 256);
        oracle.push(r.clone());
        engine.put(r).await.unwrap();
    }
    engine.flush().await.unwrap();
    let segs = engine.stats().hot_segments;
    assert!(segs >= 15, "want many segments to force eviction: {segs}");

    let specs = vec![
        QuerySpec {
            labels: vec![("model".into(), "gpt-4o".into())],
            numeric_ranges: vec![("latency_ms".into(), 1000.0, f64::MAX)],
            ..Default::default()
        },
        QuerySpec {
            numeric_ranges: vec![("latency_ms".into(), 1998.0, f64::MAX)],
            ..Default::default()
        },
        QuerySpec {
            key_prefix: Some("s/000001".into()),
            ..Default::default()
        },
    ];
    for spec in &specs {
        let mut want: Vec<Record> = oracle.iter().filter(|r| spec.matches(r)).cloned().collect();
        want.sort_by(ts_desc_key);
        let got = engine.scan(spec).await.unwrap();
        assert_eq!(got, want, "spec {spec:?}");
    }
    // Repeat the busiest scan: eviction + reload must not corrupt results.
    let mut want0: Vec<Record> = oracle
        .iter()
        .filter(|r| specs[0].matches(r))
        .cloned()
        .collect();
    want0.sort_by(ts_desc_key);
    assert_eq!(engine.scan(&specs[0]).await.unwrap(), want0);
    // And the query did real I/O (sections were actually re-read from disk).
    assert!(engine.stats().bytes_read > 0);
}

/// A scan reading across hot and cold tiers stays correct while segments are
/// tiered hot→cold concurrently: the open falls back to the other tier if a
/// rename lands between the manifest snapshot and the open, and a held fd
/// survives a rename on unix (WS4 tiering-while-scanning).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_survives_concurrent_tiering() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = GirderConfig::at(dir.path());
    cfg.fsync = FsyncPolicy::EveryN(64);
    cfg.memtable_max_records = 500;
    cfg.compact_min_segments = 100_000; // never compact — isolate tiering
    cfg.hot_ttl_nanos = 0; // every segment is instantly cold-eligible
    cfg.tick_interval = Duration::from_secs(3600);
    let engine = std::sync::Arc::new(Girder::open(cfg).await.unwrap());

    for i in 0..6000usize {
        engine.put(big_record(i, 300)).await.unwrap();
    }
    engine.flush().await.unwrap();
    assert!(engine.stats().hot_segments >= 8);

    let spec = QuerySpec {
        labels: vec![("model".into(), "gpt-4o".into())],
        ..Default::default()
    };
    let expected = engine.scan(&spec).await.unwrap().len();
    assert!(expected > 0);

    let scanner = {
        let e = engine.clone();
        let spec = spec.clone();
        tokio::spawn(async move {
            for _ in 0..40 {
                let got = e.scan(&spec).await.unwrap();
                assert_eq!(got.len(), expected, "scan result changed under tiering");
            }
        })
    };
    let tierer = {
        let e = engine.clone();
        tokio::spawn(async move {
            for _ in 0..10 {
                e.maintain().await.unwrap();
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    };
    scanner.await.unwrap();
    tierer.await.unwrap();

    let stats = engine.stats();
    assert_eq!(
        stats.hot_segments, 0,
        "everything tiered to cold: {stats:?}"
    );
    assert_eq!(engine.scan(&spec).await.unwrap().len(), expected);
    // A point get from the cold tier still resolves.
    assert!(engine.get("s/00000000").await.unwrap().is_some());
}

/// Reads racing compaction must neither error (input files are deleted after
/// the manifest swap — the reader retries on a fresh snapshot) nor serve
/// stale bytes (compaction REUSES manifest ids for its outputs, so cached
/// sections are keyed by the never-reused file seq, not the id). Regression
/// test for the close/reopen `Io(NotFound)` caught by the upsert pinning
/// suite: the same race fires on every compaction, not just at reopen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_survive_concurrent_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = GirderConfig::at(dir.path());
    cfg.fsync = FsyncPolicy::EveryN(64);
    cfg.memtable_max_records = 10_000; // manual flushes control segment count
    cfg.compact_min_segments = 2; // compact as eagerly as possible
    cfg.tick_interval = Duration::from_secs(3600);
    let engine = std::sync::Arc::new(Girder::open(cfg).await.unwrap());

    // The `record` helper's payload encodes the key, so any stale-section
    // read is detectable by content.
    let rec = |key: &str, ts: i64| record(key, ts, "gpt-4o", 1.0);

    engine.put(rec("anchor", 1)).await.unwrap();
    for i in 0..50 {
        engine.put(rec(&format!("r/{i:04}"), i)).await.unwrap();
    }
    engine.flush().await.unwrap();

    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let compactor = {
        let e = engine.clone();
        let done = done.clone();
        tokio::spawn(async move {
            // Keep creating fresh segments so every maintain() finds a run to
            // merge — the anchor's segment is rewritten over and over.
            for round in 0..30i64 {
                for i in 0..20 {
                    e.put(rec(&format!("w/{round:02}/{i:02}"), round * 100 + i))
                        .await
                        .unwrap();
                }
                e.flush().await.unwrap();
                e.maintain().await.unwrap();
                tokio::task::yield_now().await;
            }
            done.store(true, std::sync::atomic::Ordering::Release);
        })
    };
    let reader = {
        let e = engine.clone();
        let done = done.clone();
        tokio::spawn(async move {
            let spec = QuerySpec {
                key_prefix: Some("r/".into()),
                ..Default::default()
            };
            while !done.load(std::sync::atomic::Ordering::Acquire) {
                let got = e.get("anchor").await.unwrap().expect("anchor must exist");
                assert_eq!(got.payload, b"payload-anchor", "stale bytes served");
                let hits = e.scan(&spec).await.unwrap();
                assert_eq!(hits.len(), 50, "r/ records lost under compaction");
                for r in &hits {
                    assert_eq!(
                        r.payload,
                        format!("payload-{}", r.key).into_bytes(),
                        "stale section served for {}",
                        r.key
                    );
                }
                tokio::task::yield_now().await;
            }
        })
    };
    compactor.await.unwrap();
    reader.await.unwrap();
    let stats = engine.stats();
    assert!(
        stats.compactions >= 10,
        "compaction actually raced: {stats:?}"
    );
}
