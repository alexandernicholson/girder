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
        // FTS document (message-content stand-in). `zebracorn` marks ~0.1%
        // of records — the selective FTS leg; the phrase pool gives common
        // tokens for the broad leg.
        text: Some(if i.is_multiple_of(1000) {
            format!("user asked about billing zebracorn case {i}")
        } else {
            [
                "the quick brown fox jumps over the lazy dog",
                "error database timeout retry exhausted",
                "tool call search weather in paris",
                "model replied politely about the invoice",
            ][i % 4]
                .to_string()
        }),
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

/// Resident set size (bytes) via `/proc/self/statm`, or 0 if unavailable.
fn rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1).map(str::to_string))
        .and_then(|pages| pages.parse::<u64>().ok())
        .map(|pages| pages * 4096)
        .unwrap_or(0)
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
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
    // Compaction cadence during the build (batches between maintenance passes).
    // The build measures the durable write path *with compaction running*, the
    // way rivet's search-bench does (its 5 s tick fires ~continuously across the
    // 44 s baseline build) — WS3's whole target is that build time and write
    // amplification. Set GIRDER_BENCH_COMPACT_EVERY=0 to disable.
    let compact_every: usize = std::env::var("GIRDER_BENCH_COMPACT_EVERY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(160);

    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut config = GirderConfig::at(dir.path());
        config.fsync = FsyncPolicy::EveryN(256);
        // rivet search-bench defaults (memtable 10k, compact_min 8): the config
        // the plan's §0 numbers were measured under.
        config.memtable_max_records = 10_000;
        config.compact_min_segments = 8;
        config.tick_interval = Duration::from_secs(3600); // compaction driven manually below
        let engine = Girder::open(config).await.unwrap();

        // Write path (durable, batches of 500), compacting periodically so the
        // build cost includes compaction (the WS3 acceptance scenario).
        let start = Instant::now();
        for b in 0..n / 500 {
            let chunk: Vec<Record> = (0..500).map(|i| record(b * 500 + i)).collect();
            engine.put_batch(chunk).await.unwrap();
            if compact_every > 0 && (b + 1) % compact_every == 0 {
                engine.maintain().await.unwrap();
            }
        }
        let write = start.elapsed();
        engine.flush().await.unwrap();
        engine.maintain().await.unwrap(); // final settle (not counted in build)
        let stats = engine.stats();
        let write_amp = if stats.bytes_flushed > 0 {
            stats.bytes_compacted as f64 / stats.bytes_flushed as f64
        } else {
            0.0
        };
        println!(
            "build:  {n} records in {write:.2?} ({:.0} rec/s, batches of 500, fsync/256, \
             compact every {compact_every} batches)",
            n as f64 / write.as_secs_f64()
        );
        println!(
            "segments: {} hot + {} cold ({} records), on-disk {:.1} MB",
            stats.hot_segments,
            stats.cold_segments,
            stats.total_records_in_segments,
            total_segment_bytes(dir.path()) as f64 / (1024.0 * 1024.0),
        );
        println!(
            "write-amp: {write_amp:.2}x  (flushed {:.1} MB, compacted {:.1} MB, {} compactions)",
            stats.bytes_flushed as f64 / (1024.0 * 1024.0),
            stats.bytes_compacted as f64 / (1024.0 * 1024.0),
            stats.compactions,
        );

        // --- selective (uncorrelated): latency_ms > 1995 (~0.25%) ---
        // WS4: the *cold* first query must read only the columns it needs plus
        // the surviving rows' payloads — not the ~GB payload blob. `bytes_read`
        // makes that observable; target: cold <= 400 ms and <= 64 MB read.
        let selective = QuerySpec {
            numeric_ranges: vec![("latency_ms".into(), 1995.0, f64::MAX)],
            limit: 50,
            ..Default::default()
        };
        let rss_before = rss_bytes();
        let reads_before = engine.stats().bytes_read;
        let (cold_dur, _) = {
            let start = Instant::now();
            let h = engine.scan(&selective).await.unwrap().len();
            (start.elapsed(), h)
        };
        let cold_read = engine.stats().bytes_read - reads_before;
        let rss_after = rss_bytes();
        let (p50, hits) = warm_p50(7, || runtime_block(&engine, &selective));
        println!(
            "selective  cold={cold_dur:.2?} ({:.1} MB read; target <=400ms & <=64MB)  \
             warm_p50={p50:.2?}  ({hits} hits, limit 50)",
            mb(cold_read)
        );
        println!(
            "  RSS {:.0} MB → {:.0} MB across the cold selective query (cache_bytes {:.0} MB)",
            mb(rss_before),
            mb(rss_after),
            mb(256 * 1024 * 1024),
        );

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

        // --- FTS (plan 0013 §6): the token index in the headline numbers. ---
        // Selective: AND of a rare marker + a common token (~0.1% of rows).
        let fts_selective = QuerySpec {
            text_match: Some("zebracorn billing".into()),
            limit: 50,
            ..Default::default()
        };
        // Broad: two common tokens (~25% of rows).
        let fts_broad = QuerySpec {
            text_match: Some("timeout database".into()),
            limit: 50,
            ..Default::default()
        };
        // Composed: FTS + label predicate.
        let fts_composed = QuerySpec {
            text_match: Some("zebracorn".into()),
            labels: vec![("model".into(), "gpt-4o".into())],
            limit: 50,
            ..Default::default()
        };
        // Cold: a fresh engine (empty section cache) — the honest first-query
        // cost of loading token postings sections.
        let engine = {
            drop(engine);
            let mut config = GirderConfig::at(dir.path());
            config.fsync = FsyncPolicy::EveryN(256);
            config.memtable_max_records = 10_000;
            config.compact_min_segments = 8;
            config.tick_interval = Duration::from_secs(3600);
            Girder::open(config).await.unwrap()
        };
        let reads_before = engine.stats().bytes_read;
        let start = Instant::now();
        let cold_hits = engine.scan(&fts_selective).await.unwrap().len();
        let fts_cold = start.elapsed();
        let fts_cold_read = engine.stats().bytes_read - reads_before;
        let (p50, hits) = warm_p50(7, || runtime_block(&engine, &fts_selective));
        println!(
            "fts selective ('zebracorn billing', ~0.1%)  cold={fts_cold:.2?} ({:.1} MB read)               warm_p50={p50:.2?}  ({hits} hits, limit 50; cold hits {cold_hits})",
            mb(fts_cold_read)
        );
        let (p50, hits) = warm_p50(7, || runtime_block(&engine, &fts_broad));
        println!("fts broad ('timeout database', ~25%)       warm_p50={p50:.2?}  ({hits} hits, limit 50)");
        let (p50, hits) = warm_p50(7, || runtime_block(&engine, &fts_composed));
        println!("fts + label (composed)                     warm_p50={p50:.2?}  ({hits} hits, limit 50)");

        // --- LIKE pushdown (track F, F2): prefix analysis over the token
        // index. Accelerated shapes narrow through token/prefix constraints
        // then verify against raw text; the bare %infix% shape derives NO
        // constraint and pays the honest full-verify walk — both measured.
        let like_prefix = QuerySpec {
            text_like: Some("user asked about billing zebracorn%".into()),
            limit: 50,
            ..Default::default()
        };
        let like_delimited = QuerySpec {
            text_like: Some("% zebracorn case %".into()),
            limit: 50,
            ..Default::default()
        };
        // NB: a bare single word between wildcards — `%zebracorn case%`
        // would NOT fall through (the space makes `case` left-complete →
        // a Prefix constraint; the analyzer is sharper than intuition).
        let like_infix = QuerySpec {
            text_like: Some("%zebracorn%".into()),
            limit: 50,
            ..Default::default()
        };
        let (p50, hits) = warm_p50(7, || runtime_block(&engine, &like_prefix));
        println!(
            "like anchored-prefix ('…zebracorn%', ~0.1%, accelerated)  warm_p50={p50:.2?}  ({hits} hits, limit 50)"
        );
        let (p50, hits) = warm_p50(7, || runtime_block(&engine, &like_delimited));
        println!(
            "like delimited-infix ('% zebracorn case %', accelerated)  warm_p50={p50:.2?}  ({hits} hits, limit 50)"
        );
        let (p50, hits) = warm_p50(3, || runtime_block(&engine, &like_infix));
        println!(
            "like bare-infix ('%zebracorn%', fallthrough verify)        warm_p50={p50:.2?}  ({hits} hits, limit 50)"
        );

        // --- put-ack latency under load (the durability ack: WAL append +
        // memtable insert, fsync/256), single puts, compaction racing. ---
        let mut acks: Vec<Duration> = Vec::with_capacity(5_000);
        for i in 0..5_000usize {
            let r = record(n + i);
            let start = Instant::now();
            engine.put(r).await.unwrap();
            acks.push(start.elapsed());
            if (i + 1) % 1_000 == 0 {
                engine.maintain().await.unwrap(); // compaction races the acks
            }
        }
        acks.sort_unstable();
        println!(
            "put-ack (single puts, fsync/256, compaction racing): p50={:.2?}  p99={:.2?}  max={:.2?}",
            acks[acks.len() / 2],
            acks[acks.len() * 99 / 100],
            acks[acks.len() - 1],
        );

        // --- flush lag under memtable pressure: burst 3x the memtable cap,
        // then time how long the frozen queue takes to drain to durable
        // segments (the freeze->flush pipeline, kicked automatically). ---
        for b in 0..60 {
            let chunk: Vec<Record> = (0..500).map(|i| record(n + 10_000 + b * 500 + i)).collect();
            engine.put_batch(chunk).await.unwrap();
        }
        let start = Instant::now();
        loop {
            if engine.stats().frozen_memtables == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_micros(500)).await;
        }
        println!(
            "flush lag (30k-record burst, 10k memtable): frozen queue drained in {:.2?}",
            start.elapsed()
        );

        let stats = engine.stats();
        println!(
            "cache: {} hits / {} misses  ·  total bytes read {:.1} MB  ·  RSS {:.0} MB",
            stats.cache_hits,
            stats.cache_misses,
            mb(stats.bytes_read),
            mb(rss_bytes()),
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
