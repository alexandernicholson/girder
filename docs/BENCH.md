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

## The rivet-seam baselines (search-bench, 2026-07-05)

Cross-store numbers measured through rivet's `SearchIndex` seam
(`rivet-bench`, one backend per process; spans ≈ 3 KB incl. their FTS text):

```bash
cargo run -p rivet-bench --bin search-bench --release --features girder -- 1000000 --only girder
# the 10M soak (TMPDIR must be real disk; ~40 GB transient):
cargo run -p rivet-bench --bin search-bench --release --features girder -- 10000000 --only girder --iters 20
```

**Seam note:** these are NOT the engine-native numbers above. The rivet
`SearchIndex::query` contract materializes the FULL match set (exact totals
+ contractual sort) before paging, so its latencies scale with match count;
girder-native top-k (2.8 ms selective, 834 µs FTS at 1M) measures the
engine. Both are honest; top-k pushdown through the rivet seam is the
ledgered follow-up (D2-adjacent).

| spans | backend | build | query | p50 |
|---|---|---|---|---|
| 1M | Girder | **105.9 s (~9.4k spans/s durable, prod 5 s ticks)** | selective ~0.25% | 299 ms |
| 1M | Girder | | fts matches ~0.1% | 432 ms |
| 1M | Girder | | search box ~0.1% | 441 ms |
| 1M | MemoryIndex | 2.9 s (RAM) | fts matches ~0.1% | 1.29 s |
| 1M | MemoryIndex | | search box ~0.1% (naive scan) | **7.97 s** |
| **10M (soak)** | Girder | **1043.6 s — LINEAR ×10 vs 1M** (the byte-cap seal fix's proof) | selective ~0.25% (25k matched) | 6.59 s |
| 10M | Girder | | recent ~1% | 662 ms |
| 10M | Girder | | fts matches ~0.1% (10k matched) | 7.35 s |
| 10M | Girder | | search box ~0.1% | 7.64 s |

The headline pair: at 1M spans the search box through the girder token
index answers in **441 ms** where the naive in-RAM scan takes **7.97 s** —
18× — and the engine itself serves the same query in **834 µs** when asked
top-k. The 10M build's exact ×10 linearity is the regression guard for the
byte-cap seal (a record-count-only seal made this same build superlinear —
1,135 segment writes for 1M records; see the `fat_record_compaction_converges`
test). The broad (~17%) row is skipped above 2M spans, loudly, because the
seam materializes full match sets.
