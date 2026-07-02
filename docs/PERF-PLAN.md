# Girder performance plan — columnar segments, top-k pushdown, tiered compaction

Status: **planned** · Owner: implementation agents, one workstream each · Scope: girder only
(one small, explicitly-flagged consumer adoption in rivet-store at the end of WS2).

## 0. Evidence

Measured in rivet's `search-bench` (release, laptop, single-thread runtime; corpus =
1M real extracted spans, ~1.3KB JSON payloads; girder opened with `GirderConfig::at`
defaults: memtable 10k, cache 256MB, compact_min 8). See
`rivet/lore/plans/0008-benchmarks.md` § "Search backends at scale" and
`rivet/crates/rivet-bench/src/bin/search_bench.rs`.

| query @1M (p50, limit-50 sorted page) | Girder today | MemoryIndex | comment |
|---|---|---|---|
| `recent` — time range, ~1% | **70 ms** | 56 ms | zone maps prune; the strength to preserve |
| `selective` — `latency_ms > 1995`, ~0.25% | **3.00 s** | 28 ms | latency cycles every 2000 rows → zero pruning |
| `broad` — label + `latency_ms > 1000`, ~17% | **4.75 s** | 1.12 s | 166k full-record clones, sort, truncate |
| build (batches of 500, durable) | **44 s** | 4.9 s (volatile bulk) | compaction rewrites everything repeatedly |

Do-not-regress set: `recent` ≤ 90 ms p50 @1M, pruned-no-match scan ~µs, warm point
get ~6 µs, WAL durability ack semantics, crash recovery, Rebar actor write path,
newest-wins, and the exact public semantics of `Record`/`QuerySpec`/`put`/`get`/
`scan`/`stats` (rivet's conformance suite pins them).

## 1. Where the time actually goes (current architecture)

The engine (v1) is a small LSM: WAL → memtable → key-sorted segments
(`rmp(Vec<Record>)`, one crc over the whole body) + per-segment zone maps in the
manifest + whole-decoded-segment LRU. Reads walk memtable → frozen → segments
newest-id-first with a `seen` key-set for newest-wins.

Concrete cost centers, by file:

1. **`segment.rs::read_segment` decodes everything.** One `std::fs::read` of the
   whole file, crc over the whole body, then msgpack-decode of *every* record —
   keys, label BTreeMaps, numeric BTreeMaps, and the ~1.3KB payload — even when
   the query touches one numeric. At 1M records that is ~1.3GB on disk against a
   256MB cache, so the `selective` query re-reads and re-decodes nearly the whole
   dataset on every call → 3.0 s. This is problem (a) and (d) in one mechanism.

2. **`engine.rs::scan` materializes before paging.** Every visited record pays
   `seen.insert(record.key.clone())` (a String clone + hash for all ~1M records,
   matching or not), `spec.matches` is evaluated **twice** for non-matching
   records (the `else if !spec.matches(...)` branch), and every match is
   `record.clone()`d — payload included. `broad` clones ~166k × 1.3KB ≈ 200MB,
   sorts all of it, then truncates to 50. Problem (b).

3. **`actors.rs::compact` is all-or-nothing.** Whenever ≥ 8 hot segments exist it
   merges *all* of them into *one* segment via a full `BTreeMap<String, Record>`
   rebuild. Under sustained ingest this rewrites the entire accumulated dataset
   every ~8 flushes → ~6× write amplification at 1M and a large share of the 44 s
   build (problem (c)). It also collapses the corpus into one giant segment whose
   zone map spans all time — `recent` currently survives only because the final
   compaction happens to land before the last 1% of writes. Fragile.

4. **`actors.rs::flush_pending` clones the memtable.** `map.values().cloned()`
   copies every record (payloads again) out of the frozen `Arc`, and
   `write_segment` re-sorts data that is already key-sorted (BTreeMap iteration
   order) and builds a second full-size output buffer.

5. **`cache.rs` granularity = whole decoded segment.** A single compacted segment
   can exceed `cache_bytes` but is retained anyway (`guard.len() > 1` floor),
   pinning ~GB of decoded `Record`s; any query that touches it either hits that
   giant allocation or evicts everything else.

What already works and must be preserved: segment-level zone-map pruning keyed on
flush order (time-correlated), key-sorted segments giving binary-search `get` and
`key_prefix` pruning (rivet's `get_trace`/`spans_of` depend on prefix scans), the
single-writer WAL ack path, and the manifest/tiering machinery.

## 2. Chosen design

**One structural change carries almost all of the win: a block-structured
columnar segment format (v2) where filters run over typed columns and the payload
is sliced out only for rows that survive.** Around it: top-k pushdown so
sorted+limit pages never materialize the full match set, a size-capped
time-adjacent compaction policy that kills the O(n²) rewrite, and section-level
cache/I/O so one query never faults in more than it reads.

### 2.1 Options considered

| option | verdict | why |
|---|---|---|
| Columnar segment layout, payload decoded only for survivors | **adopt (WS1)** | attacks (a), (b), (d) at the root; predicate cost becomes ~8MB/1M-rows column scan instead of 1.3GB decode |
| Label dictionary encoding | **adopt (WS1)** | labels are low-cardinality by design; codes make equality a byte-compare scan and shrink files ~25%+ |
| Block-level zone maps (per ~4096 rows) | **adopt, cheap (WS1)** | rides in the v2 footer; prunes time/key blocks inside big compacted segments. Honesty note: it does *not* help `selective` (latency cycles every 2000 < block size → every block spans the full range); columnar scan is what fixes that |
| Streaming top-k + early termination for sorted+limit | **adopt (WS2)** | `broad` becomes heap-of-50 over a column; timestamp-ordered pages terminate via suffix-max zone bounds |
| Size-capped, time-adjacent compaction | **adopt (WS3)** | removes the quadratic rewrite (build 44 s → target ≤ 20 s) and *guarantees* time-correlation instead of relying on luck |
| Section/block cache granularity + targeted `read_exact_at` I/O | **adopt (WS4)** | caches column sections (MBs) not decoded segments (GBs); payload bytes fetched per-row on demand |
| mmap (memmap2) zero-copy reads | **reject** | `Mmap::map` is `unsafe`; the crate has `#![forbid(unsafe_code)]` and the constraint is no-unsafe. Safe `FileExt::read_exact_at` section reads achieve the same "read only what you need" at these scales |
| Per-value posting/bitmap lists (roaring) | **reject** | a dictionary-code scan over 1M u16s is sub-ms already; postings add a dep + format complexity for no measurable win at rivet's scale |
| Per-segment key bloom filters | **reject (keep on roadmap)** | point `get` is already ~6 µs warm via key-sorted binary search; blooms don't serve prefix scans, which are the hot key-lookup shape |
| Time-ordered (or dual-ordered) segment layout | **reject** | segments are already time-correlated *between* themselves via flush order + WS3 policy; reordering rows inside segments would break binary-search `get`/`key_prefix` for zero gain on the measured queries |
| Parallel segment scan (rayon) | **defer** | after WS1 the per-query work is tens of MB; parallelism is a contingency if targets are missed, not a first lever. Revisit only then |
| Batched/async fsync, WAL group-commit rework | **mostly reject** | the WAL is not the build bottleneck (compaction is); `EveryN` already amortizes fsync to ~1/batch. WS3 keeps a small optional item (frame-per-batch) strictly behind profiling evidence |
| Payload compression (lz4/zstd) | **reject for now** | orthogonal to the latency targets; a good later follow-up for the cold tier |

New dependencies: **none required.** (rayon/memmap2 evaluated and not taken.)

### 2.2 Segment format v2 — sketch

Row order inside a segment stays **key-sorted** (preserves `get` binary search,
prefix pruning, and the merge logic). All integers little-endian; every section
independently crc32'd so WS4 can verify without reading the whole file.

```
[ u32 magic "gird" ][ u32 version = 2 ]
-- sections, back to back, each: [u32 crc32(body)][body] --
  keys       : u32 count · u64 offsets[count+1] · utf8 bytes   (sorted)
  timestamps : i64[count]
  labels     : per label name:
                 dict  : u32 n · (u32 len, utf8)×n          (n ≤ u16::MAX → codes u16;
                 codes : u16[count], 0 = absent               overflow → "plain" strings
                                                              section, same shape as keys)
  numerics   : per numeric name:
                 presence : bitmap (count bits)
                 values   : f64[dense present values]
  payload    : u64 offsets[count+1] · raw bytes              (never decoded at filter time)
  block_index: rmp-encoded; per ~4096-row block:
                 row range · min/max ts · first/last key ·
                 per-numeric (min,max) · per-label dict-code bitset (u64; >64 codes → unprunable)
-- footer --
  rmp section directory { kind, name, offset, len, crc } · u64 footer_off · u32 footer_crc · u32 magic
```

Manifest and `ZoneMap` are **unchanged** (block index lives inside the file), so
the manifest format doesn't migrate. Scan pipeline per segment:

```
segment-level zone map (manifest, as today)
  → load/cached column sections needed by the spec
  → block index prune (time / key / numeric / label-code blocks)
  → vectorized predicate over surviving blocks → selected row indices
  → materialize Records only for selected rows (payload sliced per row)
```

`Record` on the wire is unchanged: `scan` still returns owned `Vec<Record>`.

### 2.3 QuerySpec addition (WS2, additive)

```rust
pub struct QuerySpec {
    ...existing fields...,
    /// None ⇒ exactly today's semantics (timestamp desc, truncate after full sort).
    pub order_by: Option<OrderBy>,   // TimestampDesc | TimestampAsc | NumericDesc(String) | NumericAsc(String)
}
```

With `order_by` + `limit > 0` the engine keeps a bounded heap instead of
materializing all matches. All existing call sites construct
`QuerySpec { ..., ..Default::default() }`, so the field is source-compatible
(girder is pinned by rev; note the semver-minor break in the changelog anyway).

**Early-termination soundness (timestamp order).** Segments are visited strictly
newest-id-first (write recency — required for newest-wins shadowing). The scan
stops when `suffix_max_ts[i] < kth_smallest_in_heap`, where `suffix_max_ts` is the
running max of `zone.max_ts` over all not-yet-visited segments. Stopping (never
skip-then-continue) means no record is ever emitted from a segment older than a
skipped one, so shadowing by unvisited newer versions cannot occur. With WS3's
time-adjacent compaction, id-order ≈ time-order and the suffix bound is tight.

### 2.4 Compaction policy v2 (WS3)

Replace "merge all hot when ≥ 8" with size-tiered over **id-adjacent runs**:

- pick the longest run of ≥ `compact_min_segments` *adjacent-by-id* hot segments
  whose sizes are within one tier (e.g. each < 4× the smallest in the run);
- merge with the existing newest-wins BTreeMap + retention logic;
- **cap output at `max_segment_records` (default 128k) / `max_segment_bytes`**,
  splitting the merged key-sorted stream into consecutive segments that keep
  fresh (adjacent) ids.

Adjacent-by-id runs have adjacent time ranges, so merged zone maps stay tight and
`recent` pruning is guaranteed by construction, not by scheduling luck. Total
write amplification becomes O(log n) tiers instead of O(n/threshold) full
rewrites. Add a `bytes_compacted` counter to `Stats` (additive) so amplification
is observable.

### 2.5 Cache v2 (WS4)

Cache key becomes `(segment_id, section)` over *decoded* section objects (key
column, ts column, one label column, …) with the same byte-bounded LRU; payload
sections are never cached wholesale — payload bytes are `read_exact_at` per
selected row range (with per-row crc-free slicing; the section crc is verified
once on first touch and remembered). `stats.cache_misses` keeps its observable
meaning (first touch of a segment in a query counts one miss) so the existing
zone-map test semantics hold; hold the segment file handle open across a scan so
a concurrent hot→cold rename can't tear reads (fd stays valid on unix).

### 2.6 Migration story

`read` dispatches on the version word: the v1 decoder (~80 lines, current
`read_segment`) is retained behind `version == 1`. v1 segments stay fully
queryable through a compat shim (decode-to-columns in memory) and are rewritten
to v2 opportunistically by the first compaction that touches them; a
`Girder::open` one-shot upgrade pass (`upgrade_segments: bool` config, default
off) force-rewrites for users who want it now. WAL format is unchanged. Nuclear
option remains: delete the data dir and `rivet reextract` from the raw archive.
Compat is dropped (v1 decoder deleted) once rivet's pinned rev has migrated.

## 3. Workstreams

Each is independently landable and gated on: `cargo test` (girder), `cargo bench`
(girder, extend as noted), and rivet's suite against the bumped pin —
`cargo test --workspace --features rivet-store/tantivy,rivet-store/girder,rivet-ingest/rebar-runtime`
plus
`cargo run -p rivet-bench --bin search-bench --release --features girder -- 1000000 --only girder`.
Baseline numbers to beat are §0. Bench extension (first WS to land adds it):
teach `girder/benches/engine.rs` the three search-bench shapes at 1M so targets
are measurable inside this repo too.

### WS1 — Columnar segment format v2 + column-native scan  *(the big rock; land first)*

**Scope.** `segment.rs`: v2 writer/reader (sections, dictionaries, presence
bitmaps, block index, per-section crc), version-dispatching reader with v1 compat;
`engine.rs::scan`/`get`: evaluate `QuerySpec` over columns → row selection →
materialize `Record`s (payload sliced only for selected rows); cache stores the
decoded column set per segment (payloads excluded — WS1 may still read the whole
file into a transient buffer on cold load; targeted I/O is WS4); compaction/
recovery keep working via a `read_all_records` compat that materializes rows.

**Acceptance @1M (search-bench, warm p50).**
- `selective` ≤ **100 ms** (from 3.00 s; expected ~20–50 ms) — stretch ≤ 40 ms.
- `recent` ≤ **90 ms** (no regression from 70 ms).
- `broad` ≤ **2.5 s** (payload decode for non-survivors gone; full fix is WS2).
- Pruned no-match scan and warm `get` within 2× of current µs numbers.
- Segment files ≥ **25% smaller** than v1 for the bench corpus.
- All girder tests + full rivet conformance green; v1 segment dirs open and
  query correctly (add a fixture test with a checked-in v1 segment).

**Risks.** Format bugs → per-section crc + a property test (random records:
v1 write/read vs v2 write/read must agree record-for-record). NaN numerics →
exclude NaN from block min/max, keep `matches` comparison semantics (NaN never
matches a range — same as today). High-cardinality labels (e.g. rivet's
`target` on annotations) → dict-overflow to plain-string column, bitset marked
unprunable. Mixed schemas (absent labels/numerics) → code 0 / presence bitmap,
with tests mirroring the zone-map truth table.

### WS2 — Top-k pushdown + scan-path cost fixes

**Scope.** `record.rs`: `order_by` field per §2.3; `engine.rs`: bounded-heap
top-k over selected rows (numeric orders read the f64 column, timestamp orders
read the ts column), suffix-max early termination for timestamp order per §2.3,
dedupe without per-record `String` clones (borrow keys from the loaded column
sections held alive for the scan; only consult `seen` when older sources remain),
kill the double `spec.matches` evaluation; `benches/engine.rs`: add the broad
page shape (label + numeric range, `order_by` latency desc, limit 50) at 1M.
**Consumer adoption (separate, one commit in rivet-store, flagged for the
operator):** when `GirderBackend::narrow` compiles the *entire* filter (no
residual) and the sort key maps to an indexed column, push `order_by`+
`limit`+`offset` into the spec and skip the oracle re-sort. Conformance suite is
the gate; `total` for pushed-down pages follows the same contract Tantivy's
hybrid uses.

**Acceptance @1M.**
- girder-native broad-shape bench (order_by pushdown) ≤ **80 ms** p50 warm.
- search-bench `broad` after the rivet adoption commit ≤ **200 ms** p50
  (from 4.75 s; beats MemoryIndex's 1.12 s) — stretch ≤ 100 ms.
- `order_by: None` path byte-identical results to today (conformance + a
  differential test: same corpus, spec with/without pushdown, same page).
- `recent` still ≤ 90 ms; early termination proven by a test asserting old
  segments are not touched (`cache_misses` stays flat) for a newest-page query.

**Risks.** Newest-wins vs early termination (the subtle one) → the stop-don't-skip
rule in §2.3; encode it in a test where a key is rewritten with a *lower*
timestamp in a newer segment. Heap ties → stable tiebreak on key asc to match
today's sort. Consumer `total` semantics under pushdown → keep exact-count by
counting matches during selection (cheap: counting is column-side), so
`QueryPage.total` is unchanged.

### WS3 — Ingest: tiered compaction + zero-clone flush

**Scope.** `actors.rs::compact`: policy per §2.4 (adjacent-id runs, size tiers,
output cap + split, fresh ids), `max_segment_records`/`max_segment_bytes` config
knobs with defaults; `flush_pending`: consume the frozen `Arc`
(`Arc::try_unwrap` — maintenance is the only holder by then; fall back to clone)
and write straight from the already-key-sorted iterator, skipping the redundant
sort and intermediate Vec; `Stats`: add `bytes_compacted` (additive). Optional,
only if profiling still shows WAL encode hot after the above: frame-per-batch
WAL records (new frame tag, replay accepts both).

**Acceptance @1M (search-bench build column, same batching).**
- Build ≤ **20 s** (from 44 s) — stretch ≤ 15 s.
- Write amplification (`bytes_compacted / bytes_flushed`) ≤ **3×** over the 1M
  build (from ~6×).
- Post-build segment count bounded (≈ n / 128k + tail) and `recent` ≤ 90 ms
  **by construction** (test: force compactions mid-build, assert newest-page
  query touches only trailing segments).
- Crash-recovery, newest-wins-across-tiers, retention, tiering tests green;
  compaction of mixed v1+v2 inputs produces v2.

**Risks.** Too many small segments if tiers never trigger → tick-driven
escalation (merge the two smallest adjacent tiers when hot count > 4×
`compact_min_segments`). Split boundaries breaking key binary search → each
output segment is a contiguous key range; `get` already consults every
non-pruned segment, and per-segment key zone maps disjointly partition the run.
Retention semantics unchanged — enforced during merge exactly as today.

### WS4 — Section-granular cache + targeted I/O

**Scope.** `cache.rs`: LRU over `(segment_id, section)` decoded sections, same
byte budget, segment-level hit/miss accounting preserved; `segment.rs` reader:
open-once handle + footer directory + `FileExt::read_exact_at` per section (and
per payload row-range at materialize time), per-section crc verified on first
load; `engine.rs::load_segment` becomes `load_sections(meta, needed)`. Add a
`bytes_read` stat (additive) so per-query I/O is observable.

**Acceptance @1M.**
- Cold-cache `selective` first query ≤ **400 ms** (today effectively ~3 s every
  time, since nothing fits); steady-state warm targets from WS1/2 unchanged.
- A `selective` query reads ≤ **64 MB** from disk (vs ~1.3 GB) — assert via
  `bytes_read`.
- Peak RSS during the 1M query phase stays under `cache_bytes` + O(page) —
  no gigabyte decoded-segment residency.
- Cache/zone-map tests keep passing with unchanged miss-count semantics;
  tiering-while-scanning test (rename mid-scan) passes.

**Risks.** Accounting drift (sections counted twice) → single source of truth
sizing on insert, test with tiny budgets. Windows portability of `read_at` →
unix `FileExt` now, `seek+read` fallback behind cfg if ever needed (personal
project, Linux). Cold-read syscall count → sections are few and large; payload
row reads are batched per contiguous run of selected rows.

## 4. Target summary

| metric @1M | today | after WS1 | after WS2 | after WS3 | after WS4 |
|---|---|---|---|---|---|
| selective p50 (warm) | 3.00 s | **≤ 100 ms** | ≤ 100 ms | ≤ 100 ms | ≤ 100 ms |
| broad p50 (warm) | 4.75 s | ≤ 2.5 s | **≤ 200 ms**¹ | ≤ 200 ms | ≤ 200 ms |
| recent p50 (never regress) | 70 ms | ≤ 90 ms | ≤ 90 ms | ≤ 90 ms² | ≤ 90 ms |
| build (durable, batch 500) | 44 s | ~44 s | ~44 s | **≤ 20 s** | ≤ 20 s |
| selective cold / bytes read | ~3 s / ~1.3 GB | improved | improved | improved | **≤ 400 ms / ≤ 64 MB** |

¹ with the one-commit rivet-store pushdown adoption; girder-native bench ≤ 80 ms
regardless. ² becomes guaranteed-by-construction instead of scheduling luck.

Recommended landing order: WS1 → WS2 and WS3 in parallel → WS4. Each workstream
ends with: rerun girder benches + search-bench @100k and @1M, paste the table
into the PR, and bump the rev pin in rivet-store (adoption commits only where
flagged).
