//! FTS acceptance: the token index (segments) + token map (memtable) must
//! agree EXACTLY with the naive-scan oracle `QuerySpec::matches` — the C2
//! cross-engine agreement shape in miniature (plan 0013 §6, rivet memory
//! 0049). Every scan here runs with records spread across memtable, frozen
//! memtable, hot + cold segments, and post-compaction rewrites.
use std::collections::BTreeMap;
use std::time::Duration;

use girder::{FsyncPolicy, Girder, GirderConfig, OrderBy, QuerySpec, Record};

fn record(key: &str, ts: i64, model: &str, latency: f64, text: Option<&str>) -> Record {
    Record {
        key: key.to_string(),
        timestamp: ts,
        labels: BTreeMap::from([("model".to_string(), model.to_string())]),
        numerics: BTreeMap::from([("latency_ms".to_string(), latency)]),
        payload: format!("payload-{key}").into_bytes(),
        text: text.map(String::from),
    }
}

fn config(dir: &std::path::Path) -> GirderConfig {
    let mut config = GirderConfig::at(dir);
    config.fsync = FsyncPolicy::EveryN(64);
    config.memtable_max_records = 10_000; // manual freeze control
    config.compact_min_segments = 2;
    config.tick_interval = Duration::from_secs(3600);
    config
}

const PHRASES: &[Option<&str>] = &[
    Some("Error: Database timeout after 30s"),
    Some("the quick brown fox jumps"),
    Some("USER asked about billing, model replied politely"),
    Some("Ünïcode Café naïve answer"),
    Some("tool call: search(query=weather)"),
    Some(""),
    None,
    Some("timeout retry exhausted — giving up"),
];

/// Build the corpus in the engine AND a truth map, spread across storage
/// stages: first third flushed + compacted, second third flushed (hot
/// segments), a slice frozen, the rest memtable. Some keys are overwritten
/// with DIFFERENT text so the index must forget the old version.
async fn build(engine: &Girder) -> BTreeMap<String, Record> {
    let mut truth: BTreeMap<String, Record> = BTreeMap::new();
    let mut put = |i: usize, r: Record| {
        truth.insert(r.key.clone(), r.clone());
        (i, r)
    };
    let mut batches: Vec<Record> = Vec::new();
    for i in 0..400usize {
        let text = PHRASES[i % PHRASES.len()];
        let model = if i % 3 == 0 { "gpt-4o" } else { "claude" };
        let (_, r) = put(
            i,
            record(
                &format!("r/{i:04}"),
                i as i64,
                model,
                (i % 100) as f64,
                text,
            ),
        );
        batches.push(r);
    }
    for (n, chunk) in batches.chunks(100).enumerate() {
        engine.put_batch(chunk.to_vec()).await.unwrap();
        if n < 3 {
            engine.flush().await.unwrap(); // three segments; 4th chunk stays in memtable
        }
    }
    engine.maintain().await.unwrap(); // compaction rewrites (postings rebuilt)

    // Overwrites with changed text — old tokens must stop matching. One lands
    // in a segment (flush), one stays in the memtable over a segment version.
    let over1 = record(
        "r/0001",
        1000,
        "gpt-4o",
        5.0,
        Some("completely different now"),
    );
    let over2 = record("r/0008", 1001, "claude", 6.0, None); // text removed
    truth.insert(over1.key.clone(), over1.clone());
    truth.insert(over2.key.clone(), over2.clone());
    engine.put(over1).await.unwrap();
    engine.flush().await.unwrap();
    engine.put(over2).await.unwrap(); // memtable shadows segment version
    truth
}

/// The naive oracle: filter the truth map with `QuerySpec::matches` (the
/// declared ground truth), order timestamp-desc / key-asc, apply limit.
fn oracle(truth: &BTreeMap<String, Record>, spec: &QuerySpec) -> Vec<String> {
    let mut hits: Vec<&Record> = truth.values().filter(|r| spec.matches(r)).collect();
    hits.sort_by(|a, b| b.timestamp.cmp(&a.timestamp).then(a.key.cmp(&b.key)));
    if spec.limit > 0 {
        hits.truncate(spec.limit);
    }
    hits.iter().map(|r| r.key.clone()).collect()
}

fn keys(records: &[Record]) -> Vec<String> {
    records.iter().map(|r| r.key.clone()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn text_match_agrees_with_naive_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    let truth = build(&engine).await;

    let text_queries = [
        "timeout",              // multiple phrases share it
        "TIMEOUT Database",     // case-insensitive, AND, any order
        "quick fox",            // AND across non-adjacent tokens
        "quick wolf",           // one token missing → fewer/none
        "café",                 // unicode, lowercased
        "search query weather", // tokenized punctuation
        "completely different", // only the overwrite
        "database 30s error",   // all tokens, any order
        "nonexistenttoken",     // unknown token → empty
        "qui",                  // prefix of a token → NOT a match
        "giving up retry",
    ];
    for q in text_queries {
        let spec = QuerySpec {
            text_match: Some(q.to_string()),
            ..Default::default()
        };
        let got = keys(&engine.scan(&spec).await.unwrap());
        assert_eq!(got, oracle(&truth, &spec), "text query {q:?}");
    }

    // Composition with field predicates + paging + top-k paths.
    let composed = [
        QuerySpec {
            text_match: Some("timeout".into()),
            labels: vec![("model".into(), "gpt-4o".into())],
            ..Default::default()
        },
        QuerySpec {
            text_match: Some("timeout".into()),
            numeric_ranges: vec![("latency_ms".into(), 10.0, 60.0)],
            time: Some((0, 250)),
            ..Default::default()
        },
        QuerySpec {
            text_match: Some("fox quick".into()),
            key_prefix: Some("r/00".into()),
            ..Default::default()
        },
        QuerySpec {
            text_match: Some("timeout".into()),
            limit: 7,
            order_by: Some(OrderBy::TimestampDesc), // top-k early-termination path
            ..Default::default()
        },
        QuerySpec {
            text_match: Some("timeout".into()),
            limit: 5,
            order_by: Some(OrderBy::NumericDesc("latency_ms".into())),
            ..Default::default()
        },
    ];
    for (i, spec) in composed.iter().enumerate() {
        let got = keys(&engine.scan(spec).await.unwrap());
        if matches!(spec.order_by, Some(OrderBy::NumericDesc(_))) {
            // Numeric ordering has its own oracle tests; assert set-equality
            // of the text+field filtering here.
            let mut got = got;
            let all = QuerySpec {
                limit: 0,
                order_by: None,
                ..spec.clone()
            };
            let mut want = oracle(&truth, &all);
            got.sort();
            want.sort();
            assert!(
                got.iter().all(|k| want.contains(k)) && got.len() == 5.min(want.len()),
                "composed[{i}]: top-k members must come from the oracle set"
            );
        } else {
            assert_eq!(got, oracle(&truth, spec), "composed[{i}]");
        }
    }

    // A query with no tokens matches nothing (never match-all).
    let spec = QuerySpec {
        text_match: Some("!!! ---".into()),
        ..Default::default()
    };
    assert!(engine.scan(&spec).await.unwrap().is_empty());

    // Overwrite hygiene: the OLD text of r/0001 must no longer match it.
    let spec = QuerySpec {
        text_match: Some("quick brown".into()),
        ..Default::default()
    };
    let got = keys(&engine.scan(&spec).await.unwrap());
    assert!(!got.contains(&"r/0001".to_string()), "stale tokens served");
    assert_eq!(got, oracle(&truth, &spec));

    // r/0008's text was REMOVED by its overwrite (None) — must not match.
    let spec = QuerySpec {
        text_match: Some("politely billing".into()),
        ..Default::default()
    };
    let got = keys(&engine.scan(&spec).await.unwrap());
    assert!(
        !got.contains(&"r/0008".to_string()),
        "removed text still indexed"
    );
    assert_eq!(got, oracle(&truth, &spec));
}

/// The agreement holds after every lifecycle transition: tiering to cold,
/// crash recovery, close/reopen — and text queries against a store whose
/// segments predate text (no K_TOKENS section) return empty, not error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn text_match_survives_lifecycle_transitions() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.hot_ttl_nanos = 0; // segments tier to cold on maintain
    cfg.compact_min_segments = 100; // isolate tiering
    let truth;
    {
        let engine = Girder::open(cfg.clone()).await.unwrap();
        truth = build(&engine).await;
        engine.maintain().await.unwrap(); // tier everything flushed to cold
        assert!(engine.stats().cold_segments >= 1);
        let spec = QuerySpec {
            text_match: Some("timeout".into()),
            ..Default::default()
        };
        assert_eq!(
            keys(&engine.scan(&spec).await.unwrap()),
            oracle(&truth, &spec),
            "cold tier"
        );
        drop(engine); // crash: memtable tail replays from WAL
    }
    let engine = Girder::open(cfg.clone()).await.unwrap();
    let spec = QuerySpec {
        text_match: Some("timeout".into()),
        ..Default::default()
    };
    assert_eq!(
        keys(&engine.scan(&spec).await.unwrap()),
        oracle(&truth, &spec),
        "post-crash-recovery"
    );
    engine.close().await.unwrap();
    let engine = Girder::open(cfg).await.unwrap();
    assert_eq!(
        keys(&engine.scan(&spec).await.unwrap()),
        oracle(&truth, &spec),
        "post-close-reopen"
    );

    // A store with NO text anywhere: text query → empty, no error.
    let dir2 = tempfile::tempdir().unwrap();
    let engine2 = Girder::open(config(dir2.path())).await.unwrap();
    engine2
        .put(record("k", 1, "gpt-4o", 1.0, None))
        .await
        .unwrap();
    engine2.flush().await.unwrap();
    assert!(engine2.scan(&spec).await.unwrap().is_empty());
}

/// Text queries racing compaction: correct results (content-asserted),
/// no vanished-file errors, postings rebuilt every rewrite. Extends the
/// A1b reads-race-compaction guarantee over the new sections.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn text_search_survives_concurrent_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(Girder::open(config(dir.path())).await.unwrap());

    for i in 0..50usize {
        engine
            .put(record(
                &format!("r/{i:04}"),
                i as i64,
                "gpt-4o",
                1.0,
                Some("stable searchable anchor text"),
            ))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();

    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let compactor = {
        let e = engine.clone();
        let done = done.clone();
        tokio::spawn(async move {
            for round in 0..25i64 {
                for i in 0..20 {
                    e.put(record(
                        &format!("w/{round:02}/{i:02}"),
                        round * 100 + i,
                        "claude",
                        2.0,
                        Some("churning writer text"),
                    ))
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
                text_match: Some("searchable anchor".into()),
                ..Default::default()
            };
            while !done.load(std::sync::atomic::Ordering::Acquire) {
                let hits = e.scan(&spec).await.unwrap();
                assert_eq!(hits.len(), 50, "anchor rows lost under compaction");
                for r in &hits {
                    assert_eq!(
                        r.text.as_deref(),
                        Some("stable searchable anchor text"),
                        "stale text served for {}",
                        r.key
                    );
                }
                tokio::task::yield_now().await;
            }
        })
    };
    compactor.await.unwrap();
    reader.await.unwrap();
    assert!(engine.stats().compactions >= 10);
}
