//! Quick throughput/latency numbers without a criterion dependency:
//! `cargo bench` runs this as a plain binary (harness = false).
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use girder::{FsyncPolicy, Girder, GirderConfig, QuerySpec, Record};

fn record(i: usize) -> Record {
    Record {
        key: format!("s/{:08}", i),
        timestamp: i as i64,
        labels: BTreeMap::from([
            ("model".to_string(), ["gpt-4o", "claude-sonnet-5", "llama"][i % 3].to_string()),
            ("project".to_string(), "prod".to_string()),
        ]),
        numerics: BTreeMap::from([("latency_ms".to_string(), (i % 2000) as f64)]),
        payload: vec![7u8; 1200], // ~realistic span JSON size
    }
}

fn main() {
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

        const N: usize = 100_000;
        // Write path.
        let start = Instant::now();
        let batch: Vec<Vec<Record>> = (0..N / 500).map(|b| (0..500).map(|i| record(b * 500 + i)).collect()).collect();
        for chunk in batch {
            engine.put_batch(chunk).await.unwrap();
        }
        let write = start.elapsed();
        engine.flush().await.unwrap();
        let stats = engine.stats();
        println!(
            "write: {N} records in {write:.2?} ({:.0} rec/s, batches of 500, fsync every 256)",
            N as f64 / write.as_secs_f64()
        );
        println!(
            "segments: {} hot ({} records)",
            stats.hot_segments, stats.total_records_in_segments
        );

        // Cold query (loads + decodes segments).
        let spec = QuerySpec {
            labels: vec![("model".into(), "gpt-4o".into())],
            numeric_ranges: vec![("latency_ms".into(), 1000.0, 2000.0)],
            limit: 50,
            ..Default::default()
        };
        let start = Instant::now();
        let hits = engine.scan(&spec).await.unwrap();
        let cold = start.elapsed();

        // Warm query (cache).
        let start = Instant::now();
        let hits_warm = engine.scan(&spec).await.unwrap();
        let warm = start.elapsed();
        assert_eq!(hits.len(), hits_warm.len());
        println!("query over {N}: cold={cold:.2?} warm={warm:.2?} (limit 50, {} hits)", hits.len());

        // Pruned query (zone maps exclude everything).
        let start = Instant::now();
        engine
            .scan(&QuerySpec {
                labels: vec![("model".into(), "nonexistent".into())],
                ..Default::default()
            })
            .await
            .unwrap();
        println!("pruned query (zero segment loads): {:.2?}", start.elapsed());

        // Point gets.
        let start = Instant::now();
        for i in (0..N).step_by(1000) {
            engine.get(&format!("s/{:08}", i)).await.unwrap().unwrap();
        }
        println!("point gets (warm): {:.2?} / {}", start.elapsed(), N / 1000);

        let stats = engine.stats();
        println!("cache: {} hits / {} misses", stats.cache_hits, stats.cache_misses);
    });
}
