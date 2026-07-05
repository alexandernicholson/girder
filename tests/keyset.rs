//! Keyset pagination (`QuerySpec.after`, plan 0013 §7 D2c1): pages chained
//! through the strictly-after (timestamp, key) bound must tile the full
//! ordered result exactly — including across memtable/frozen/segments,
//! delta fold-mode and FTS — and must be STABLE under concurrent ingest
//! (no duplicates, no skips past the anchor: the whole point of keyset).
use std::collections::BTreeMap;
use std::time::Duration;

use girder::{FsyncPolicy, Girder, GirderConfig, OrderBy, QuerySpec, Record};

fn record(key: &str, ts: i64) -> Record {
    Record {
        key: key.to_string(),
        timestamp: ts,
        labels: BTreeMap::from([("model".to_string(), "m".to_string())]),
        numerics: BTreeMap::from([("latency_ms".to_string(), (ts % 97) as f64)]),
        payload: format!("p-{key}").into_bytes(),
        text: Some(format!(
            "note {}",
            if ts % 3 == 0 { "zebra" } else { "plain" }
        )),
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

/// Drain everything through keyset pages of `page_size` and return the
/// concatenation.
async fn drain_pages(engine: &Girder, base: &QuerySpec, page_size: usize) -> Vec<Record> {
    let mut out: Vec<Record> = Vec::new();
    let mut after: Option<(i64, String)> = None;
    loop {
        let spec = QuerySpec {
            after: after.clone(),
            limit: page_size,
            ..base.clone()
        };
        let page = engine.scan(&spec).await.unwrap();
        if page.is_empty() {
            break;
        }
        let last = page.last().unwrap();
        let next = Some((last.timestamp, last.key.clone()));
        assert_ne!(
            after, next,
            "cursor must advance (a stall = infinite pagination)"
        );
        after = next;
        out.extend(page);
    }
    out
}

/// Page-chain equality with the unpaged oracle, across storage stages, both
/// timestamp orders, equal-timestamp tiebreaks, filters and FTS.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyset_pages_tile_the_full_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    // 300 records: duplicate timestamps every 3 keys (tiebreak coverage),
    // spread across two flushed segments + memtable.
    for i in 0..300usize {
        engine
            .put(record(&format!("s/{i:04}"), (i / 3) as i64))
            .await
            .unwrap();
        if i == 100 || i == 200 {
            engine.flush().await.unwrap();
        }
    }
    engine.maintain().await.unwrap(); // some compaction too

    for order in [
        None,
        Some(OrderBy::TimestampDesc),
        Some(OrderBy::TimestampAsc),
    ] {
        let base = QuerySpec {
            order_by: order.clone(),
            ..Default::default()
        };
        let full = engine
            .scan(&QuerySpec {
                limit: 0,
                ..base.clone()
            })
            .await
            .unwrap();
        assert_eq!(full.len(), 300);
        for page_size in [7, 50, 299] {
            let paged = drain_pages(&engine, &base, page_size).await;
            assert_eq!(
                paged.iter().map(|r| &r.key).collect::<Vec<_>>(),
                full.iter().map(|r| &r.key).collect::<Vec<_>>(),
                "pages must tile the full scan (order {order:?}, page {page_size})"
            );
        }
    }

    // With a label + numeric + FTS filter (post-LWW, index-served text).
    let base = QuerySpec {
        labels: vec![("model".into(), "m".into())],
        text_match: Some("zebra".into()),
        order_by: Some(OrderBy::TimestampDesc),
        ..Default::default()
    };
    let full = engine
        .scan(&QuerySpec {
            limit: 0,
            ..base.clone()
        })
        .await
        .unwrap();
    assert!(!full.is_empty() && full.len() < 300, "selective filter");
    let paged = drain_pages(&engine, &base, 11).await;
    assert_eq!(paged.len(), full.len());
    assert_eq!(
        paged.iter().map(|r| &r.key).collect::<Vec<_>>(),
        full.iter().map(|r| &r.key).collect::<Vec<_>>()
    );

    // Numeric order + after = a loud error, never a wrong page.
    let bad = QuerySpec {
        order_by: Some(OrderBy::NumericDesc("latency_ms".into())),
        after: Some((5, "s/0000".into())),
        ..Default::default()
    };
    assert!(engine.scan(&bad).await.is_err());
}

/// Keyset never resurrects a shadowed version: overwriting a row with a
/// DIFFERENT timestamp must not let the old version slip into a page whose
/// bound excludes the new one (bound applies post-LWW).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyset_bound_applies_after_newest_wins() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    engine.put(record("k/a", 10)).await.unwrap();
    engine.put(record("k/b", 20)).await.unwrap();
    engine.flush().await.unwrap();
    // Overwrite k/a with a NEWER timestamp (30) — old version (10) is dead.
    engine.put(record("k/a", 30)).await.unwrap();

    // Page strictly after (25, "") descending: contains k/b(20) and must
    // NOT contain the dead k/a@10 (its winner k/a@30 is before the bound).
    let page = engine
        .scan(&QuerySpec {
            order_by: Some(OrderBy::TimestampDesc),
            after: Some((25, String::new())),
            ..Default::default()
        })
        .await
        .unwrap();
    let keys: Vec<&str> = page.iter().map(|r| r.key.as_str()).collect();
    assert_eq!(keys, ["k/b"], "dead version must not resurrect: {keys:?}");
}

/// THE ruling-3 acceptance: pages taken from an anchor stay exact while a
/// writer ingests NEWER rows concurrently — every pre-anchor row appears
/// exactly once across the chained pages (no duplicates, no skips).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyset_pages_stable_under_live_ingest() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(Girder::open(config(dir.path())).await.unwrap());

    // Seed the "old" corpus: timestamps 0..500.
    for i in 0..500i64 {
        engine.put(record(&format!("old/{i:04}"), i)).await.unwrap();
    }
    engine.flush().await.unwrap();

    // Anchor: newest-first page 1, cursor after its last row.
    let base = QuerySpec {
        order_by: Some(OrderBy::TimestampDesc),
        ..Default::default()
    };
    let page1 = engine
        .scan(&QuerySpec {
            limit: 50,
            ..base.clone()
        })
        .await
        .unwrap();
    assert_eq!(page1.len(), 50);
    let anchor = {
        let last = page1.last().unwrap();
        (last.timestamp, last.key.clone())
    };

    // A writer keeps ingesting NEWER rows (timestamps 1000+) + maintenance
    // races (flush + compaction) while the reader drains the rest.
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let e = engine.clone();
        let done = done.clone();
        tokio::spawn(async move {
            for i in 0..600i64 {
                e.put(record(&format!("new/{i:04}"), 1000 + i))
                    .await
                    .unwrap();
                if i % 150 == 149 {
                    e.flush().await.unwrap();
                    e.maintain().await.unwrap();
                }
            }
            done.store(true, std::sync::atomic::Ordering::Release);
        })
    };

    // Drain from the anchor in pages of 37 while the writer runs.
    let mut seen: Vec<String> = page1.iter().map(|r| r.key.clone()).collect();
    let mut after = Some(anchor);
    loop {
        let spec = QuerySpec {
            after: after.clone(),
            limit: 37,
            ..base.clone()
        };
        let page = engine.scan(&spec).await.unwrap();
        if page.is_empty() {
            break;
        }
        let last = page.last().unwrap();
        after = Some((last.timestamp, last.key.clone()));
        seen.extend(page.iter().map(|r| r.key.clone()));
        tokio::task::yield_now().await;
    }
    writer.await.unwrap();

    // Exactly the 500 old rows, each once — the concurrent new/ rows land
    // BEFORE the anchor in the order and never shift a served page.
    let old: Vec<&String> = seen.iter().filter(|k| k.starts_with("old/")).collect();
    assert_eq!(old.len(), 500, "every pre-anchor row exactly once");
    let mut dedup = old.clone();
    dedup.sort();
    dedup.dedup();
    assert_eq!(dedup.len(), 500, "no duplicates across pages");
    assert!(
        !seen.iter().any(|k| k.starts_with("new/")),
        "post-anchor writes never leak into pre-anchor pages"
    );
}
