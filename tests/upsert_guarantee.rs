//! Pinning tests for the public upsert/merge guarantee (`docs/GUARANTEES.md`).
//!
//! Each test pins one normative statement (G1–G4 + the retention caveat). A
//! behavior change here is a public-contract change: update the document and
//! these tests in the same commit.
use std::collections::BTreeMap;
use std::time::Duration;

use girder::{FsyncPolicy, Girder, GirderConfig, QuerySpec, Record};

fn record(key: &str, ts: i64, version: &str) -> Record {
    Record {
        key: key.to_string(),
        timestamp: ts,
        labels: BTreeMap::from([("version".to_string(), version.to_string())]),
        numerics: BTreeMap::new(),
        payload: format!("payload-{version}").into_bytes(),
        text: None,
    }
}

fn config(dir: &std::path::Path) -> GirderConfig {
    let mut config = GirderConfig::at(dir);
    config.fsync = FsyncPolicy::Always;
    config.memtable_max_records = 100;
    config.compact_min_segments = 2;
    config.tick_interval = Duration::from_secs(3600); // manual ticks only
    config
}

/// The single visible version of `key`, from both read paths (they must agree).
async fn winner(engine: &Girder, key: &str) -> String {
    let got = engine.get(key).await.unwrap().expect("key must exist");
    let scanned = engine.scan(&QuerySpec::default()).await.unwrap();
    let hits: Vec<_> = scanned.iter().filter(|r| r.key == key).collect();
    assert_eq!(hits.len(), 1, "one record per key, never two (G1)");
    assert_eq!(
        hits[0].labels["version"], got.labels["version"],
        "get and scan agree on the winner"
    );
    got.labels["version"].clone()
}

// G1: last write wins within the memtable, by write order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g1_lww_within_memtable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    engine.put(record("k", 1, "first")).await.unwrap();
    engine.put(record("k", 2, "second")).await.unwrap();
    assert_eq!(winner(&engine, "k").await, "second");
}

// G1: write order decides, NOT timestamp — an older-timestamp overwrite wins.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g1_write_order_beats_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    engine.put(record("k", 100, "newer-ts")).await.unwrap();
    engine.flush().await.unwrap(); // first version in a segment
    engine.put(record("k", 50, "older-ts")).await.unwrap();
    assert_eq!(
        winner(&engine, "k").await,
        "older-ts",
        "arrival order at the writer decides, not Record.timestamp"
    );
}

// G1: within one put_batch, later elements overwrite earlier duplicates.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g1_batch_duplicate_keys_last_wins() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    engine
        .put_batch(vec![
            record("k", 1, "first"),
            record("k", 2, "second"),
            record("k", 3, "third"),
        ])
        .await
        .unwrap();
    assert_eq!(winner(&engine, "k").await, "third");
}

// G2: the winner is stable across segment-over-segment and compaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g2_lww_across_segments_and_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    engine.put(record("k", 1, "v1")).await.unwrap();
    engine.flush().await.unwrap();
    engine.put(record("k", 2, "v2")).await.unwrap();
    engine.flush().await.unwrap();
    engine.put(record("k", 3, "v3")).await.unwrap();
    engine.flush().await.unwrap(); // three segments, three versions of k
    assert_eq!(winner(&engine, "k").await, "v3", "segment over segment");

    engine.maintain().await.unwrap(); // compaction merges + dedupes
    assert_eq!(winner(&engine, "k").await, "v3", "survives compaction");
    let stats = engine.stats();
    assert!(stats.compactions >= 1, "compaction actually ran: {stats:?}");
}

// G2: the winner is stable across hot→cold tiering (recency = segment id, not tier).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g2_lww_across_tiers() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.hot_ttl_nanos = 0; // segments tier to cold immediately
    config.compact_min_segments = 100; // isolate tiering from compaction
    let engine = Girder::open(config).await.unwrap();
    engine.put(record("k", 1, "old")).await.unwrap();
    engine.flush().await.unwrap();
    engine.maintain().await.unwrap(); // old version now in a COLD segment
    assert!(engine.stats().cold_segments >= 1);
    engine.put(record("k", 2, "new")).await.unwrap(); // memtable overwrite
    assert_eq!(winner(&engine, "k").await, "new", "memtable over cold");

    engine.flush().await.unwrap();
    engine.maintain().await.unwrap(); // new version tiers to cold too
    assert_eq!(
        winner(&engine, "k").await,
        "new",
        "newer segment id wins even with both versions cold"
    );
}

// G2: the winner survives close() + reopen (checkpoint path).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g2_lww_survives_close_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Girder::open(config(dir.path())).await.unwrap();
        engine.put(record("k", 1, "old")).await.unwrap();
        engine.flush().await.unwrap();
        engine.put(record("k", 2, "new")).await.unwrap(); // memtable only
        engine.close().await.unwrap();
    }
    let engine = Girder::open(config(dir.path())).await.unwrap();
    assert_eq!(winner(&engine, "k").await, "new");
}

// G2 + G3: the winner survives a crash — WAL replay reapplies append order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g2_lww_survives_crash_recovery() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Girder::open(config(dir.path())).await.unwrap();
        engine.put(record("k", 1, "old")).await.unwrap();
        engine.flush().await.unwrap(); // old version durable in a segment
        engine.put(record("k", 2, "mid")).await.unwrap();
        engine.put(record("k", 3, "new")).await.unwrap(); // both in WAL only
        drop(engine); // crash: no flush, no close
    }
    let engine = Girder::open(config(dir.path())).await.unwrap();
    assert_eq!(
        winner(&engine, "k").await,
        "new",
        "replay preserves append order across segment + WAL versions"
    );
}

// G4: a put_batch becomes visible atomically — a concurrent reader never
// observes a partially applied batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn g4_batch_visibility_is_atomic_in_process() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.fsync = FsyncPolicy::Os; // keep the writer fast; durability is not under test
    let engine = std::sync::Arc::new(Girder::open(config).await.unwrap());

    let writer = {
        let engine = engine.clone();
        tokio::spawn(async move {
            for v in 0..40i64 {
                let version = format!("v{v}");
                engine
                    .put_batch(vec![
                        record("pair/a", v, &version),
                        record("pair/b", v, &version),
                    ])
                    .await
                    .unwrap();
            }
        })
    };
    let reader = {
        let engine = engine.clone();
        tokio::spawn(async move {
            for _ in 0..200 {
                let hits = engine
                    .scan(&QuerySpec {
                        key_prefix: Some("pair/".into()),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                if hits.len() == 2 {
                    assert_eq!(
                        hits[0].labels["version"], hits[1].labels["version"],
                        "torn batch observed: {hits:?}"
                    );
                }
                tokio::task::yield_now().await;
            }
        })
    };
    writer.await.unwrap();
    reader.await.unwrap();
    assert_eq!(winner(&engine, "pair/a").await, "v39");
    assert_eq!(winner(&engine, "pair/b").await, "v39");
}

// Documented caveat: the surviving record is judged by ITS OWN timestamp for
// retention — an older-timestamp overwrite can be TTL-dropped at compaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caveat_overwrite_is_judged_by_its_own_timestamp_for_retention() {
    let dir = tempfile::tempdir().unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    let mut config = config(dir.path());
    config.retention_nanos = Some(Duration::from_secs(3600).as_nanos() as i64);
    config.compact_min_segments = 1;
    let engine = Girder::open(config).await.unwrap();
    engine.put(record("k", now, "fresh")).await.unwrap();
    engine.flush().await.unwrap();
    // Overwrite with a timestamp far past the TTL cutoff: it WINS (G1)...
    engine.put(record("k", 1, "expired-ts")).await.unwrap();
    assert_eq!(winner(&engine, "k").await, "expired-ts");
    // ...but compaction then drops it by its own timestamp.
    engine.flush().await.unwrap();
    engine.maintain().await.unwrap();
    assert!(
        engine.get("k").await.unwrap().is_none(),
        "the winning record is retention-judged by its own timestamp"
    );
}
