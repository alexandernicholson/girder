//! `QuerySpec.omit_text` (track F-2, plan line 1730): the explicit
//! projection for callers that never read text back. Contract pinned here:
//! identical membership/order/count to the unprojected query, `text: None`
//! on EVERY returned row (memtable-sourced and fold-sourced rows included —
//! a page is uniform), text PREDICATES still exact, and the observable win:
//! segment text bytes are never read (no read, no D8 inflate).
use std::collections::BTreeMap;
use std::time::Duration;

use girder::{FsyncPolicy, Girder, GirderConfig, OrderBy, QuerySpec, Record};

fn record(key: &str, ts: i64, model: &str, text: Option<&str>) -> Record {
    Record {
        key: key.to_string(),
        timestamp: ts,
        labels: BTreeMap::from([("model".to_string(), model.to_string())]),
        numerics: BTreeMap::from([("latency_ms".to_string(), (ts % 100) as f64)]),
        payload: format!("payload-{key}").into_bytes(),
        text: text.map(String::from),
    }
}

fn config(dir: &std::path::Path) -> GirderConfig {
    let mut config = GirderConfig::at(dir);
    config.fsync = FsyncPolicy::EveryN(64);
    config.memtable_max_records = 10_000;
    config.compact_min_segments = 2;
    config.tick_interval = Duration::from_secs(3600);
    config
}

const PHRASES: &[Option<&str>] = &[
    Some("Error: Database timeout after 30s"),
    Some("error: database TIMEOUT after 30s"),
    Some("100% CPU on shard_7"),
    Some("İstanbul region latency spike"),
    Some("ΟΣΑ ΤΕΛΟΣ"),
    Some(""),
    None,
    Some("the quick brown fox"),
];

/// Corpus across memtable + segments (+ one compaction) with a truth map.
async fn build(engine: &Girder) -> BTreeMap<String, Record> {
    let mut truth = BTreeMap::new();
    let mut batch = Vec::new();
    for i in 0..400usize {
        let r = record(
            &format!("r/{i:04}"),
            i as i64,
            if i % 3 == 0 { "gpt-4o" } else { "claude" },
            PHRASES[i % PHRASES.len()],
        );
        truth.insert(r.key.clone(), r.clone());
        batch.push(r);
    }
    for (n, chunk) in batch.chunks(100).enumerate() {
        engine.put_batch(chunk.to_vec()).await.unwrap();
        if n < 3 {
            engine.flush().await.unwrap(); // last chunk stays in the memtable
        }
    }
    engine.maintain().await.unwrap();
    truth
}

/// Oracle keys for a spec (LWW truth map), timestamp-desc / key-asc, limit.
fn oracle(truth: &BTreeMap<String, Record>, spec: &QuerySpec) -> Vec<String> {
    let mut hits: Vec<&Record> = truth.values().filter(|r| spec.matches(r)).collect();
    hits.sort_by(|a, b| b.timestamp.cmp(&a.timestamp).then(a.key.cmp(&b.key)));
    if spec.limit > 0 {
        hits.truncate(spec.limit);
    }
    hits.iter().map(|r| r.key.clone()).collect()
}

fn assert_projected(records: &[Record], what: &str) {
    for r in records {
        assert_eq!(r.text, None, "{what}: row {} leaked text", r.key);
    }
}

/// Membership/order/count are IDENTICAL with and without the projection,
/// across plain, text-predicated (both kinds), top-k, and keyset shapes —
/// and every projected row carries text: None, including memtable rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn omit_text_is_membership_neutral() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    let truth = build(&engine).await;

    let shapes = [
        QuerySpec::default(),
        QuerySpec {
            labels: vec![("model".into(), "gpt-4o".into())],
            ..Default::default()
        },
        QuerySpec {
            text_match: Some("timeout database".into()),
            ..Default::default()
        },
        QuerySpec {
            text_like: Some("Error%".into()),
            ..Default::default()
        },
        QuerySpec {
            text_like: Some("%fox".into()), // fallthrough shape
            ..Default::default()
        },
        QuerySpec {
            text_like: Some("İst%".into()),
            text_match: Some("region".into()),
            ..Default::default()
        },
        QuerySpec {
            limit: 7,
            order_by: Some(OrderBy::TimestampDesc),
            ..Default::default()
        },
        QuerySpec {
            text_like: Some("%o%".into()),
            limit: 9,
            order_by: Some(OrderBy::NumericDesc("latency_ms".into())),
            ..Default::default()
        },
    ];
    for (i, base) in shapes.iter().enumerate() {
        let projected = QuerySpec {
            omit_text: true,
            ..base.clone()
        };
        let plain = engine.scan(base).await.unwrap();
        let omitted = engine.scan(&projected).await.unwrap();
        let keys = |v: &[Record]| v.iter().map(|r| r.key.clone()).collect::<Vec<_>>();
        assert_eq!(keys(&plain), keys(&omitted), "shape[{i}] membership/order");
        assert_projected(&omitted, &format!("shape[{i}]"));
        // Non-text fields are untouched by the projection.
        for (a, b) in plain.iter().zip(&omitted) {
            assert_eq!(a.payload, b.payload, "shape[{i}] payload");
            assert_eq!(a.labels, b.labels, "shape[{i}] labels");
            assert_eq!(a.timestamp, b.timestamp, "shape[{i}] ts");
        }
        // count() is membership-neutral wrt the projection.
        if base.limit == 0 {
            assert_eq!(
                engine.count(base).await.unwrap(),
                engine.count(&projected).await.unwrap(),
                "shape[{i}] count"
            );
            assert_eq!(keys(&omitted), oracle(&truth, base), "shape[{i}] oracle");
        }
    }
}

/// The observable win: a projected scan reads FEWER segment bytes than the
/// same scan with text (the text column is never touched — no read, no D8
/// inflate). Big texts make the delta unmissable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn omit_text_skips_text_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    let big = "attribute structure ".repeat(200); // ~4 KB, D8-compressible
    for i in 0..500usize {
        engine
            .put(record(&format!("k/{i:04}"), i as i64, "m", Some(&big)))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();

    let spec = QuerySpec::default();
    let before = engine.stats().bytes_read;
    let with_text = engine.scan(&spec).await.unwrap();
    let with_text_bytes = engine.stats().bytes_read - before;

    let before = engine.stats().bytes_read;
    let projected = engine
        .scan(&QuerySpec {
            omit_text: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let projected_bytes = engine.stats().bytes_read - before;

    assert_eq!(with_text.len(), 500);
    assert_eq!(projected.len(), 500);
    assert_projected(&projected, "big-text corpus");
    assert!(
        projected_bytes < with_text_bytes,
        "projection must read fewer bytes: {projected_bytes} vs {with_text_bytes}"
    );
}

/// The fold path (counter deltas in range): text predicates still evaluate
/// exactly under the projection (the fold fetches text for its OWN
/// predicate, then the output is stripped) — and the folded numbers are
/// exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn omit_text_fold_path_keeps_predicates_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    let mut base = record("c/hit", 10, "m", Some("counter with timeout text"));
    base.numerics.insert("n".to_string(), 5.0);
    engine.put(base).await.unwrap();
    let mut other = record("c/miss", 11, "m", Some("counter without the word"));
    other.numerics.insert("n".to_string(), 1.0);
    engine.put(other).await.unwrap();
    engine.flush().await.unwrap();
    for k in 0..3 {
        engine
            .incr("c/hit", 20 + k, BTreeMap::from([("n".to_string(), 1.0)]))
            .await
            .unwrap();
    }

    let spec = QuerySpec {
        text_like: Some("%timeout%".into()),
        omit_text: true,
        ..Default::default()
    };
    let got = engine.scan(&spec).await.unwrap();
    assert_eq!(got.len(), 1, "text predicate must still select exactly");
    assert_eq!(got[0].key, "c/hit");
    assert_eq!(got[0].numerics.get("n"), Some(&8.0), "fold exact");
    assert_projected(&got, "fold output");
}
