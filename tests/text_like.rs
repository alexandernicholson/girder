//! `text_like` acceptance (plan 0013 §6 follow-on, track F slice F1): the
//! SQL-LIKE predicate must agree EXACTLY with the naive-scan oracle
//! `QuerySpec::matches` on a hostile corpus — records spread across memtable,
//! frozen memtable, hot + compacted segments, with overwrites, text-less
//! records, empty-string text (absent ≠ empty), Unicode case traps (Greek
//! final sigma, dotted İ) and literal `%`/`_` chars in the TEXT. F1 is the
//! unaccelerated exact path; F2's index narrowing must keep every one of
//! these green (the verifier is the exactness guarantee, the index only
//! narrows).
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

/// Hostile text pool. Deliberate traps: case variants of the same words,
/// literal `%` and `_` in text, Greek final sigma (str::to_lowercase is not
/// prefix-stable there), dotted İ (multi-char lowercase), empty string
/// (matches `'%'` — unlike None, which never matches), and None.
const PHRASES: &[Option<&str>] = &[
    Some("Error: Database timeout after 30s"),
    Some("error: database TIMEOUT after 30s"),
    Some("Err"),
    Some("Error"),
    Some("100% CPU on shard_7"),
    Some("usage_percent=93 disk_usage high"),
    Some("ΟΣΑ ΤΕΛΟΣ"),
    Some("İstanbul region latency spike"),
    Some("istanbul lowercase variant"),
    Some("café naïve Ünïcode"),
    Some(""),
    None,
    Some("prefix"),
    Some("prefixed words here"),
    Some("the quick brown fox"),
];

/// Deterministic xorshift — hostile corpus without a rand dep.
fn rng(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// Build the corpus in the engine AND a truth map, spread across storage
/// stages: chunks flushed (segments) + compacted, a final chunk left in the
/// memtable, plus overwrites that change / remove / add text so shadowed
/// versions must stop matching.
async fn build(engine: &Girder) -> BTreeMap<String, Record> {
    let mut truth: BTreeMap<String, Record> = BTreeMap::new();
    let mut state = 0x51ce_f00d_u64;
    let mut batches: Vec<Record> = Vec::new();
    for i in 0..600usize {
        let text = PHRASES[(rng(&mut state) % PHRASES.len() as u64) as usize];
        let model = if i % 3 == 0 { "gpt-4o" } else { "claude" };
        let r = record(
            &format!("r/{i:04}"),
            i as i64,
            model,
            (i % 100) as f64,
            text,
        );
        truth.insert(r.key.clone(), r.clone());
        batches.push(r);
    }
    for (n, chunk) in batches.chunks(150).enumerate() {
        engine.put_batch(chunk.to_vec()).await.unwrap();
        if n < 3 {
            engine.flush().await.unwrap(); // three segments; 4th chunk stays in memtable
        }
    }
    engine.maintain().await.unwrap(); // compaction rewrites

    // Overwrites: text changed (old text must stop matching), text removed
    // (record must stop matching ANY pattern, even '%'), text added onto a
    // previously text-less record. One overwrite lands in a segment, the
    // rest stay in the memtable shadowing segment versions.
    let over = [
        record(
            "r/0001",
            1000,
            "gpt-4o",
            5.0,
            Some("Error replaced entirely"),
        ),
        record("r/0002", 1001, "claude", 6.0, None),
        record("r/0011", 1002, "claude", 7.0, Some("100%_literal traps")),
    ];
    for (i, r) in over.into_iter().enumerate() {
        truth.insert(r.key.clone(), r.clone());
        engine.put(r).await.unwrap();
        if i == 0 {
            engine.flush().await.unwrap();
        }
    }
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

/// The hostile pattern battery. Shapes that F2 will accelerate (`prefix%`,
/// interior-token patterns) AND shapes that must fall through honestly
/// (`%infix%`, suffix, `_`-poisoned, all-wildcard) — in F1 every one is
/// exact by the same unaccelerated path; the split only starts mattering
/// for SPEED in F2, never for membership.
const PATTERNS: &[&str] = &[
    // prefix% (the F2 acceleration target)
    "Error%",
    "error%",
    "Err%",
    "İst%",
    "ΟΣ%",
    "prefix%",
    "100%",
    // exact (no wildcards) — anchored both ends
    "Err",
    "Error",
    "prefix",
    "",
    // %infix% / %suffix — F2 fallthrough shapes
    "%timeout%",
    "%TIMEOUT%",
    "%after 30s",
    "%fox",
    "%ΤΕΛΟΣ",
    "%stanbul%",
    "%_literal%",
    "%usage%",
    // `_` shapes
    "_rror%",
    "Err_r%",
    "100_ CPU%",
    "_",
    "___",
    // all-wildcard / degenerate
    "%",
    "%%",
    "%_%",
    // multi-fragment backtracking
    "%database%30s%",
    "%quick%fox%",
    "Error%30s",
    "%disk%usage%",
    // matches nothing anywhere
    "zzz-not-present%",
    "%zzz-not-present%",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn text_like_agrees_with_naive_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    let truth = build(&engine).await;

    for p in PATTERNS {
        let spec = QuerySpec {
            text_like: Some(p.to_string()),
            ..Default::default()
        };
        let got = keys(&engine.scan(&spec).await.unwrap());
        assert_eq!(got, oracle(&truth, &spec), "pattern {p:?}");

        // count() must agree with the page's own membership (the D2c2
        // oracle) for every pattern too.
        let n = engine.count(&spec).await.unwrap();
        assert_eq!(n, got.len(), "count drifted from scan for pattern {p:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn text_like_composes_with_fields_and_orders() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    let truth = build(&engine).await;

    let composed = [
        // AND with text_match: both predicates must hold.
        QuerySpec {
            text_like: Some("%timeout%".into()),
            text_match: Some("database error".into()),
            ..Default::default()
        },
        // Case split: text_match is case-insensitive, LIKE is not — only the
        // lowercase phrase variant survives both.
        QuerySpec {
            text_like: Some("error: database%".into()),
            text_match: Some("TIMEOUT".into()),
            ..Default::default()
        },
        QuerySpec {
            text_like: Some("Error%".into()),
            labels: vec![("model".into(), "gpt-4o".into())],
            ..Default::default()
        },
        QuerySpec {
            text_like: Some("%fox".into()),
            numeric_ranges: vec![("latency_ms".into(), 10.0, 80.0)],
            time: Some((0, 400)),
            ..Default::default()
        },
        QuerySpec {
            text_like: Some("%e%".into()),
            key_prefix: Some("r/00".into()),
            ..Default::default()
        },
        // top-k paths (early-termination + heap) with a LIKE predicate.
        QuerySpec {
            text_like: Some("%a%".into()),
            limit: 9,
            order_by: Some(OrderBy::TimestampDesc),
            ..Default::default()
        },
        QuerySpec {
            text_like: Some("%o%".into()),
            limit: 5,
            order_by: Some(OrderBy::TimestampAsc),
            ..Default::default()
        },
    ];
    for (i, spec) in composed.iter().enumerate() {
        let got = keys(&engine.scan(spec).await.unwrap());
        let want = match spec.order_by {
            Some(OrderBy::TimestampAsc) => {
                let mut hits: Vec<&Record> = truth.values().filter(|r| spec.matches(r)).collect();
                hits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then(a.key.cmp(&b.key)));
                hits.truncate(spec.limit);
                hits.iter().map(|r| r.key.clone()).collect::<Vec<_>>()
            }
            _ => oracle(&truth, spec),
        };
        assert_eq!(got, want, "composed[{i}]");
    }

    // Keyset resume (QuerySpec.after) pages a LIKE-filtered set without
    // gaps or duplicates.
    let full = QuerySpec {
        text_like: Some("%e%".into()),
        ..Default::default()
    };
    let want = oracle(&truth, &full);
    let mut paged: Vec<String> = Vec::new();
    let mut after: Option<(i64, String)> = None;
    loop {
        let page = engine
            .scan(&QuerySpec {
                limit: 7,
                after: after.clone(),
                ..full.clone()
            })
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        after = page.last().map(|r| (r.timestamp, r.key.clone()));
        paged.extend(keys(&page));
    }
    assert_eq!(paged, want, "keyset paging over a LIKE filter");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn text_like_absent_is_not_empty() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    // One record with empty-string text, one with no text, both segment-side.
    engine
        .put(record("empty", 1, "m", 1.0, Some("")))
        .await
        .unwrap();
    engine.put(record("none", 2, "m", 1.0, None)).await.unwrap();
    engine.flush().await.unwrap();

    // '%' matches every record WITH text (including empty) — never text-less.
    let spec = QuerySpec {
        text_like: Some("%".into()),
        ..Default::default()
    };
    assert_eq!(keys(&engine.scan(&spec).await.unwrap()), vec!["empty"]);

    // Empty pattern matches only empty text — same absence rule.
    let spec = QuerySpec {
        text_like: Some(String::new()),
        ..Default::default()
    };
    assert_eq!(keys(&engine.scan(&spec).await.unwrap()), vec!["empty"]);
}

/// A segment written before any record carried text (no K_TEXT section at
/// all) returns nothing for every pattern — and never errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn text_like_on_textless_segment() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    for i in 0..50usize {
        engine
            .put(record(&format!("k/{i:02}"), i as i64, "m", 1.0, None))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    for p in ["%", "", "abc%", "_"] {
        let spec = QuerySpec {
            text_like: Some(p.into()),
            ..Default::default()
        };
        assert!(
            engine.scan(&spec).await.unwrap().is_empty(),
            "pattern {p:?}"
        );
        assert_eq!(engine.count(&spec).await.unwrap(), 0, "count {p:?}");
    }
}
