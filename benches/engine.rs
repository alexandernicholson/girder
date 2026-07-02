//! Quick throughput/latency numbers without a criterion dependency:
//! `cargo bench` runs this as a plain binary (harness = false).
//!
//! Measures the query shapes from docs/PERF-PLAN.md §0 at scale so the WS1/WS2
//! acceptance targets are checkable inside this repo:
//!
//!   - selective (uncorrelated): `latency_ms > 1995`  (~0.25%, no pruning)
//!   - broad (label + numeric):  `model=gpt-4o & latency_ms > 1000` (~17%)
//!   - broad sorted page (WS2):  same filter, `order_by` latency desc, limit 50
//!   - newest page (WS2):        `order_by` timestamp desc, limit 50
//!   - recent (time range):      newest ~1% by timestamp
//!
//! Set `GIRDER_BENCH_N` to override the corpus size (default 1_000_000).
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use girder::{FsyncPolicy, Girder, GirderConfig, OrderBy, QuerySpec, Record};

fn record(i: usize) -> Record {
    Record {
        key: format!("s/{:08}", i),
        timestamp: i as i64,
        labels: BTreeMap::from([
            (
                "model".to_string(),
                ["gpt-4o", "claude-sonnet-5", "llama"][i % 3].to_string(),
            ),
            ("project".to_string(), "prod".to_string()),
        ]),
        numerics: BTreeMap::from([("latency_ms".to_string(), (i % 2000) as f64)]),
        payload: vec![7u8; 1200], // ~realistic span JSON size
    }
}

/// Median of `reps` warm runs of `f` (one warmup run first, discarded).
fn warm_p50<F: FnMut() -> usize>(reps: usize, mut f: F) -> (Duration, usize) {
    let hits = f(); // warmup
    let mut samples = Vec::with_capacity(reps);
    for _ in 0..reps {
        let start = Instant::now();
        let _ = f();
        samples.push(start.elapsed());
    }
    samples.sort_unstable();
    (samples[samples.len() / 2], hits)
}

fn total_segment_bytes(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    for d in [dir.to_path_buf(), dir.join("cold")] {
        if let Ok(rd) = std::fs::read_dir(&d) {
            for e in rd.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                if name.ends_with(".gird") {
                    total += e.metadata().map(|m| m.len()).unwrap_or(0);
                }
            }
        }
    }
    total
}

fn main() {
    let n: usize = std::env::var("GIRDER_BENCH_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut config = GirderConfig::at(dir.path());
        config.fsync = FsyncPolicy::EveryN(256);
        config.memtable_max_records = 20_000;
        config.tick_interval = Duration::from_secs(3600);
        let engine = Girder::open(config).await.unwrap();

        // Write path (durable, batches of 500).
        let start = Instant::now();
        for b in 0..n / 500 {
            let chunk: Vec<Record> = (0..500).map(|i| record(b * 500 + i)).collect();
            engine.put_batch(chunk).await.unwrap();
        }
        let write = start.elapsed();
        engine.flush().await.unwrap();
        let stats = engine.stats();
        println!(
            "build:  {n} records in {write:.2?} ({:.0} rec/s, batches of 500, fsync/256)",
            n as f64 / write.as_secs_f64()
        );
        println!(
            "segments: {} hot ({} records), on-disk {:.1} MB",
            stats.hot_segments,
            stats.total_records_in_segments,
            total_segment_bytes(dir.path()) as f64 / (1024.0 * 1024.0),
        );

        // --- selective (uncorrelated): latency_ms > 1995 (~0.25%) ---
        let selective = QuerySpec {
            numeric_ranges: vec![("latency_ms".into(), 1995.0, f64::MAX)],
            limit: 50,
            ..Default::default()
        };
        let (cold_dur, _) = {
            let start = Instant::now();
            let h = engine.scan(&selective).await.unwrap().len();
            (start.elapsed(), h)
        };
        let (p50, hits) = warm_p50(7, || runtime_block(&engine, &selective));
        println!("selective  cold={cold_dur:.2?}  warm_p50={p50:.2?}  ({hits} hits, limit 50)");

        // --- broad (label + numeric): model=gpt-4o & latency_ms > 1000 (~17%) ---
        let broad = QuerySpec {
            labels: vec![("model".into(), "gpt-4o".into())],
            numeric_ranges: vec![("latency_ms".into(), 1000.0, f64::MAX)],
            limit: 50,
            ..Default::default()
        };
        let (p50, hits) = warm_p50(5, || runtime_block(&engine, &broad));
        println!("broad      warm_p50={p50:.2?}  ({hits} hits returned, limit 50)");

        // --- broad SORTED PAGE (WS2): same filter, order_by latency desc,
        //     limit 50. Bounded heap over the numeric column materializes only
        //     50 records instead of ~166k. Target: <= 80 ms p50 warm. ---
        let broad_sorted = QuerySpec {
            labels: vec![("model".into(), "gpt-4o".into())],
            numeric_ranges: vec![("latency_ms".into(), 1000.0, f64::MAX)],
            order_by: Some(OrderBy::NumericDesc("latency_ms".into())),
            limit: 50,
            ..Default::default()
        };
        let (p50, hits) = warm_p50(5, || runtime_block(&engine, &broad_sorted));
        println!("broad(sorted page, order_by latency desc)  warm_p50={p50:.2?}  ({hits} hits, limit 50)");

        // --- newest page (WS2): order_by timestamp desc, limit 50, no filter.
        //     Suffix-max early termination should touch only the newest
        //     segment(s) instead of the whole corpus. ---
        let newest_before = engine.stats().cache_misses;
        let newest = QuerySpec {
            order_by: Some(OrderBy::TimestampDesc),
            limit: 50,
            ..Default::default()
        };
        let (p50, hits) = warm_p50(7, || runtime_block(&engine, &newest));
        let newest_loaded = engine.stats().cache_misses - newest_before;
        println!(
            "newest page(order_by ts desc)  warm_p50={p50:.2?}  ({hits} hits, limit 50; \
             segments loaded across warmup+reps: {newest_loaded})"
        );

        // --- recent (time range): newest ~1% by timestamp ---
        let lo = (n - n / 100) as i64;
        let recent = QuerySpec {
            time: Some((lo, n as i64)),
            limit: 50,
            ..Default::default()
        };
        let (p50, hits) = warm_p50(7, || runtime_block(&engine, &recent));
        println!("recent     warm_p50={p50:.2?}  ({hits} hits, limit 50)");

        // Pruned query (zone maps exclude everything).
        let start = Instant::now();
        engine
            .scan(&QuerySpec {
                labels: vec![("model".into(), "nonexistent".into())],
                ..Default::default()
            })
            .await
            .unwrap();
        println!("pruned (zero loads): {:.2?}", start.elapsed());

        // Point gets.
        let start = Instant::now();
        let step = (n / 1000).max(1);
        let mut gets = 0;
        for i in (0..n).step_by(step) {
            engine.get(&format!("s/{:08}", i)).await.unwrap().unwrap();
            gets += 1;
        }
        println!("point gets (warm): {:.2?} / {gets}", start.elapsed());

        let stats = engine.stats();
        println!(
            "cache: {} hits / {} misses",
            stats.cache_hits, stats.cache_misses
        );
    });
}

/// Block-on a scan from inside an already-async closure boundary. The bench
/// runtime is multi-thread; `warm_p50` wants a sync `FnMut`, so hop through a
/// current-thread handle via `block_in_place`.
fn runtime_block(engine: &Girder, spec: &QuerySpec) -> usize {
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current()
            .block_on(engine.scan(spec))
            .unwrap()
            .len()
    })
}
