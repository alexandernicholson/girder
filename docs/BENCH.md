# Benchmark methodology & baselines

Reproducible invocations for every number the README claims. Per plan-0013
ruling 9: **the first honest run establishes the baseline; later runs are
regression-guarded against this table** (no thresholds were pinned before
measurement). One bench home for cross-store comparisons: the 10M soak and
engine-vs-engine numbers live in rivet-bench; this file covers the
girder-repo legs.

## Invocation

```bash
cargo bench                        # 1M corpus (default)
GIRDER_BENCH_N=100000 cargo bench  # smaller corpus
GIRDER_BENCH_COMPACT_EVERY=0 cargo bench  # disable mid-build compaction
```

Release profile (`lto = "thin"`, `codegen-units = 1`). Corpus: N records,
~1.3 KB payload each, 3 model labels, one numeric, plus an FTS text document
per record (~45 chars; a `zebracorn` marker on every 1000th record gives the
selective FTS leg its ~0.1% selectivity). Config: `fsync = EveryN(256)`,
`memtable_max_records = 10_000`, `compact_min_segments = 8` — the same
shape rivet's search-bench measures.

## What each leg measures

- **build** — durable write throughput WITH compaction racing (maintenance
  every 160 batches): the real ingest path, not a WAL-only sprint.
- **selective / broad / sorted page / newest page / recent** — the
  plan-0008/PERF-PLAN §0 query shapes (see that doc for targets). *Note:
  un-ordered `limit` scans (`broad`, `fts broad`) materialize the full match
  set before truncation — the production UI path is the SORTED page (top-k
  heap), which is the number that matters for latency claims.*
- **fts selective / broad / composed** — the token index (plan 0013 §6):
  postings intersection + per-candidate predicate checks. `cold` = a fresh
  engine (empty section cache): first-touch cost of footer + postings
  sections. The composed leg is FTS + a label predicate.
- **put-ack** — the durability ack (WAL append + memtable fold) for single
  `put`s, p50/p99/max over 5,000 samples with compaction racing every
  1,000. This is the latency a caller's `.await` sees.
- **flush lag** — a 30k-record burst against a 10k-record memtable (3
  freezes), then the time for the frozen queue to drain to durable
  segments via the automatically-kicked flush.

## Baseline (2026-07-05, laptop, 1M × ~1.3 KB + text, columnar v2 + FTS)

| leg | result |
|---|---|
| build (durable, compaction racing) | **12.21 s (~82k rec/s)**, write-amp 1.63× |
| on-disk | 1238.9 MB, 12 hot segments |
| selective (~0.25%, limit 50) | cold **19.8 ms** (54.7 MB read) · warm p50 **2.8 ms** |
| broad unsorted (~17%, limit 50) | warm p50 291 ms (full-match materialize — see note) |
| broad **sorted page** (top-k 50) | warm p50 **3.1 ms** |
| newest page (ts desc, limit 50) | warm p50 **796 µs** (0 extra segments loaded) |
| recent (~1% time range) | warm p50 **8.3 ms** |
| **fts selective** (`zebracorn billing`, ~0.1%) | cold **29.5 ms** (58.9 MB) · warm p50 **834 µs** |
| fts broad (`timeout database`, ~25%, unsorted) | warm p50 450 ms (see note) |
| **fts + label composed** | warm p50 **297 µs** |
| **put-ack** (single puts, fsync/256, compaction racing) | p50 **36 µs** · p99 **70 µs** · max 2.1 ms |
| **flush lag** (30k burst / 10k memtable) | **9.75 ms** to drain |
| point get (warm) | ~5.5 µs |
| pruned (zone-map no-match) | ~7.7 µs |

The corpus now carries a text document on every record, so these numbers
INCLUDE the cost of writing K_TEXT + K_TOKENS on every segment — the build
throughput and write-amp above are the honest with-FTS figures.
