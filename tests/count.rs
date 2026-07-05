//! `Girder::count` (plan 0013 §7 D2c2): the payload-free exact count. THE
//! oracle: for every spec, `count(spec) == scan(spec, limit 0).len()` —
//! held across newest-wins shadowing (incl. a newer NON-matching version
//! shadowing an older matching one), tombstone-convention records, FTS,
//! counter-delta fold fallback, and compaction/tiering stages.
use std::collections::BTreeMap;
use std::time::Duration;

use girder::{FsyncPolicy, Girder, GirderConfig, QuerySpec, Record};

fn record(key: &str, ts: i64, model: &str, latency: f64, text: Option<&str>) -> Record {
    Record {
        key: key.to_string(),
        timestamp: ts,
        labels: BTreeMap::from([
            ("model".to_string(), model.to_string()),
            ("project".to_string(), "prod".to_string()),
        ]),
        numerics: BTreeMap::from([("latency_ms".to_string(), latency)]),
        payload: format!("p-{key}").into_bytes(),
        text: text.map(String::from),
    }
}

/// A rivet-convention tombstone: `del` label only — NO project label.
fn tombstone(key: &str) -> Record {
    Record {
        key: key.to_string(),
        timestamp: 0,
        labels: BTreeMap::from([("del".to_string(), "1".to_string())]),
        numerics: BTreeMap::new(),
        payload: Vec::new(),
        text: None,
    }
}

fn config(dir: &std::path::Path) -> GirderConfig {
    let mut config = GirderConfig::at(dir);
    config.fsync = FsyncPolicy::EveryN(64);
    config.memtable_max_records = 10_000;
    config.compact_min_segments = 3;
    config.tick_interval = Duration::from_secs(3600);
    config
}

async fn assert_count_matches_scan(engine: &Girder, spec: &QuerySpec, label: &str) {
    let unbounded = QuerySpec {
        limit: 0,
        order_by: None,
        ..spec.clone()
    };
    let scanned = engine.scan(&unbounded).await.unwrap().len();
    let counted = engine.count(spec).await.unwrap();
    assert_eq!(counted, scanned, "count != scan oracle for {label}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn count_agrees_with_scan_everywhere() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    // Corpus across two segments + memtable, with text.
    for i in 0..200usize {
        let model = ["gpt-4o", "claude"][i % 2];
        let text = if i % 10 == 0 {
            Some("rare zebra note")
        } else {
            Some("common note")
        };
        engine
            .put(record(
                &format!("s/{i:04}"),
                i as i64,
                model,
                (i % 50) as f64,
                text,
            ))
            .await
            .unwrap();
        if i == 80 {
            engine.flush().await.unwrap();
        }
    }
    // Shadowing: overwrite 20 keys so the NEWER version does NOT match a
    // model filter the OLDER version matched (the count must exclude them).
    engine.flush().await.unwrap();
    for i in 0..20usize {
        engine
            .put(record(
                &format!("s/{:04}", i * 2),
                500 + i as i64,
                "claude",
                1.0,
                None,
            ))
            .await
            .unwrap();
    }
    // Tombstone-convention: kill 5 keys (del label, no project label).
    for i in 0..5 {
        engine
            .put(tombstone(&format!("s/{:04}", 100 + i)))
            .await
            .unwrap();
    }
    engine.maintain().await.unwrap(); // some compaction in the mix

    let project = ("project".to_string(), "prod".to_string());
    let specs: Vec<(&str, QuerySpec)> = vec![
        (
            "all-in-project",
            QuerySpec {
                labels: vec![project.clone()],
                ..Default::default()
            },
        ),
        (
            "label eq (shadow-sensitive)",
            QuerySpec {
                labels: vec![project.clone(), ("model".into(), "gpt-4o".into())],
                ..Default::default()
            },
        ),
        (
            "numeric range",
            QuerySpec {
                labels: vec![project.clone()],
                numeric_ranges: vec![("latency_ms".into(), 10.0, 30.0)],
                ..Default::default()
            },
        ),
        (
            "time window",
            QuerySpec {
                labels: vec![project.clone()],
                time: Some((50, 150)),
                ..Default::default()
            },
        ),
        (
            "fts selective",
            QuerySpec {
                labels: vec![project.clone()],
                text_match: Some("zebra rare".into()),
                ..Default::default()
            },
        ),
        (
            "fts + label + numeric",
            QuerySpec {
                labels: vec![project.clone(), ("model".into(), "gpt-4o".into())],
                text_match: Some("note".into()),
                numeric_ranges: vec![("latency_ms".into(), 0.0, 40.0)],
                ..Default::default()
            },
        ),
        (
            "key prefix",
            QuerySpec {
                key_prefix: Some("s/00".into()),
                labels: vec![project.clone()],
                ..Default::default()
            },
        ),
        (
            "unscoped (tombstones count as they match: not at all)",
            QuerySpec::default(),
        ),
    ];
    for (label, spec) in &specs {
        assert_count_matches_scan(&engine, spec, label).await;
    }

    // The shadow case specifically: gpt-4o count must EXCLUDE the 20
    // overwritten-to-claude keys and the tombstoned keys.
    let gpt = engine
        .count(&QuerySpec {
            labels: vec![project.clone(), ("model".into(), "gpt-4o".into())],
            ..Default::default()
        })
        .await
        .unwrap();
    // 100 even-i gpt-4o originals − 20 overwritten (even keys) − tombstoned
    // evens among s/0100..0104 (100,102,104 are even = 3 were gpt-4o).
    assert_eq!(gpt, 100 - 20 - 3, "shadowing + tombstones excluded");

    // `after` is rejected loudly.
    let bad = QuerySpec {
        after: Some((5, "s/0000".into())),
        ..Default::default()
    };
    assert!(engine.count(&bad).await.is_err());
}

/// Counter deltas in range force the fold fallback — and the oracle still
/// holds (partial values never count; a key's N delta rows count once).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn count_folds_deltas_never_counts_versions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    engine.put(record("bl/a", 1, "m", 1.0, None)).await.unwrap();
    engine.flush().await.unwrap();
    for i in 0..5 {
        engine
            .incr(
                "bl/a",
                10 + i,
                BTreeMap::from([("latency_ms".to_string(), 10.0)]),
            )
            .await
            .unwrap();
        engine.flush().await.unwrap(); // spread deltas across segments
    }
    // 1 base + 5 delta rows on disk; the folded row is ONE record with
    // latency 51.0.
    let spec = QuerySpec::default();
    assert_eq!(engine.count(&spec).await.unwrap(), 1, "one folded row");
    assert_count_matches_scan(&engine, &spec, "delta fold").await;
    // Predicate over the FOLDED total (each raw delta is 10 < 45).
    let folded_only = QuerySpec {
        numeric_ranges: vec![("latency_ms".into(), 45.0, 100.0)],
        ..Default::default()
    };
    assert_eq!(engine.count(&folded_only).await.unwrap(), 1);
    assert_count_matches_scan(&engine, &folded_only, "delta folded range").await;
}
