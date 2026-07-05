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

### D7 re-run (2026-07-06, K_TOKENS v2 lazy postings + block-aware text candidates)

| leg | D7 | vs baseline |
|---|---|---|
| fts selective | cold 29.20 ms (58.9 MB) · warm p50 **848 µs** | parity (834 µs) |
| fts broad (~25%, unsorted) | warm p50 **385 ms** | −14% (450 ms) |
| fts + label composed | warm p50 **361 µs** | see noise note |
| build / write-amp | 12.15 s / 1.63× | parity |

Noise note: same-run CONTROL legs that D7 does not touch drifted +15–22%
(non-FTS selective warm 3.43 ms vs 2.8 ms baseline; put-ack p50 42 µs vs
36 µs) — the box was under concurrent load. The composed leg's 361 µs vs
297 µs sits inside that envelope. Honesty note on the design: the first
D7-b cut decoded queried postings PER QUERY and regressed the warm FTS
legs (977 µs / 439 µs) — caught by this re-run and fixed with per-token
decode-once (`OnceLock`); the lazy layout's real target is the 10M shape
(cold decode + memory), re-measured when the 10M soak rides the bench
slice.

### F2 LIKE-pushdown legs (2026-07-06, prefix analysis over K_TOKENS v2)

New legs; same 1M corpus, measured on D7's code (same-run FTS-selective
control 867 µs ≈ D7's 848 µs — comparable run). Accelerated shapes narrow
through token/prefix constraints, then verify every candidate against the
raw text (exactness never depends on the index); the bare `%infix%` shape
derives no constraint and pays the full verify walk — kept in the table
so the cost of the ledgered n-gram deferral stays visible.

| leg | result |
|---|---|
| like anchored-prefix (`…zebracorn%`, ~0.1%, accelerated) | warm p50 **1.14 ms** |
| like delimited-infix (`% zebracorn case %`, accelerated) | warm p50 **1.05 ms** |
| like bare-infix (`%zebracorn%`, fallthrough full verify) | warm p50 **281 ms** |

The accelerated/fallthrough gap (~250×) is the pushdown's value statement.
Honesty note: the first F2 run measured all three legs at ~270 ms — the
walk keyed K_TOKENS loading on `text_match` alone, so LIKE specs never saw
the index (fixed: `need_tokens` consults the prefix analysis). And the
original fallthrough leg pattern `%zebracorn case%` turned out to
ACCELERATE (the interior space makes `case` a left-complete prefix
fragment) — the analyzer is sharper than intuition; the leg now uses a
truly constraint-free pattern.

### D8 re-run (2026-07-06, K_TEXT v2 per-row-compress-if-large)

| leg | D8 | vs baseline |
|---|---|---|
| on-disk (1M, ~45-char texts) | **1239.0 MB** | ratio ≈ 1.00 — HONESTLY none: texts below the 512 B threshold stay raw by design |
| point gets | 4.90 ms / 1000 | parity (5.5–5.7 µs/get) |
| selective warm | 3.14 ms | parity (2.8 ms) |
| fts selective / broad / composed | 1.12 ms / 384 ms / 469 µs | broad exact parity; selective+composed inside the same-run ambient drift seen since D7 |
| **doc corpus** (50k × ~3.6 KiB document-shaped text) | raw text 177.0 MB → stored **11.9 MB** (**14.8×**) | new leg |
| doc corpus scattered point gets (inflate per row) | **7.40 ms / 1000** | ~7.4 µs/get incl. inflate |

The doc leg is where D8 lives: rivet's span-text documents (name + every
string attribute) are KB-scale and structure-heavy. Honesty note on the
ratio: the synthetic document repeats structure heavily, which flatters
deflate — treat 14.8× as the shape's ceiling, not a general claim;
moderately repetitive real corpora should expect low single digits.
Design-history note (why per-row, recorded so nobody reintroduces
chunks): the first D8 cut deflated 64 KiB chunks and the bench caught
scattered reads regressing 12× (fts selective warm 10.8 ms, point gets
20.5 ms/1000, RSS +900 MB — every materialized row inflated a whole
chunk). Per-row storage makes the small-text corpus byte-identical to v1
and prices each inflate by the row's own size.

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

### 10M soak re-run (2026-07-06, on D7 lazy-postings + D8 per-row text compression)

| spans | query | p50 (was 2026-07-05) |
|---|---|---|
| 10M | build | **1320.8 s** (was 1043.6 — +27%: per-row deflate at encode + compaction) |
| 10M | selective ~0.25% (25k matched) | 6.86 s (was 6.59 — ~parity) |
| 10M | recent ~1% (100k matched) | 1.08 s (was 0.662 — +63%) |
| 10M | fts matches ~0.1% (10k matched) | 8.61 s (was 7.35 — +17%) |
| 10M | search box ~0.1% | 8.79 s (was 7.64 — +15%) |

Honest reading: the regressions are D8's read/write costs surfacing on the
SEAM's materialize-everything path — and the diagnosis found real waste:
**the rivet seam decodes spans from `Record.payload` and DISCARDS
`Record.text`**, so every materialized row now pays a per-row inflate for
bytes it throws away (100k inflates on the `recent` leg ≈ the +0.4 s). Two
ledgered follow-ups fix it: the already-ledgered seam top-k pushdown
(materialize 50, not the match set), and a `materialize` variant that
skips the text column when the caller doesn't verify text (`text_like`
verification is the only reader). Neither engine-native leg regressed
(D7/D8 1M tables above); the disk win stands.

### 10M soak, omit_text (2026-07-06, seam sets `QuerySpec.omit_text` — rivet memory 0071)

Same box, girder rev `990bbbb` resolved from the public remote, seam legs:

| leg | D8 (regressed) | omit_text |
|---|---|---|
| recent ~1% (100k matched) | 1.08 s (+63%) | **637.75 ms — collapsed, below the pre-D8 662 ms baseline** |
| selective ~0.25% | 6.86 s | 6.95 s (parity — never text-bound) |
| fts matches ~0.1% | 8.61 s (+17%) | 8.60 s (UNCHANGED — see below) |
| search box ~0.1% | 8.79 s (+15%) | 8.58 s (−2.4%) |
| build (control) | 1320.8 s | 1509.8 s — writes untouched by design; this run's box was slower, so the read wins above are if anything understated |

The `recent` collapse is the D12 prediction quantified: "100k inflates ≈
the +0.4 s" — omit_text removed 0.44 s. And an honest negative: the
fts-leg regressions did NOT move under omit_text, so they are NOT
discarded-text costs — their cause lives elsewhere in D8's read path
(handed to track D as a finding).

The headline pair: at 1M spans the search box through the girder token
index answers in **441 ms** where the naive in-RAM scan takes **7.97 s** —
18× — and the engine itself serves the same query in **834 µs** when asked
top-k. The 10M build's exact ×10 linearity is the regression guard for the
byte-cap seal (a record-count-only seal made this same build superlinear —
1,135 segment writes for 1M records; see the `fat_record_compaction_converges`
test). The seal's documented trade-off — overwritten rows in sealed
segments reclaimable only via retention — is closed by the dead-ratio
reclaimer (`docs/GUARANTEES.md` §Sealed-segment reclamation,
`tests/seal_reclaim.rs`): solo rewrites at ≥½ dead, so the write-amp bound
above survives overwrite-heavy workloads too. The broad (~17%) row is
skipped above 2M spans, loudly, because the seam materializes full match
sets.
