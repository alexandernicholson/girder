# girder

**An embedded, actor-supervised storage engine for trace-shaped data, built on
the [Rebar](https://github.com/alexandernicholson/rebar) actor runtime.**

Girders carry rivets: this is the storage layer built for
[Rivet](https://github.com/alexandernicholson/rivet) (LLM traffic
observability), but the engine is generic — any workload of append-mostly
records with a timestamp, low-cardinality labels, numeric metrics, and an
opaque payload.

## Architecture

```
 put/put_batch ──► WriterActor (GenServer: single-writer WAL, crc32, fsync policy)
       │                   durability ack
       ▼
   MemTable (sorted, newest-wins) ──freeze+rotate──► MaintenanceActor ──► segment
       │                                                  │        (rmp + crc32 + zone map)
       ▼                                                  ▼
     scan ◄── zone-map pruning ◄──────────── Manifest (atomic rename)
       │                                                  ▲
       ▼                                                  │
 section LRU cache                       compaction (merge+dedupe+TTL)
                                         tiering (hot dir ──age──► cold dir)
```

Two Rebar `GenServer`s own all mutation — the engine is race-free by
construction, not by lock choreography:

- **WriterActor** — every `put` is a serialized `call`: WAL append (the
  durability ack) → memtable insert → freeze + WAL rotate when full.
- **MaintenanceActor** — sole custodian of the manifest: flushes frozen
  memtables to segments, compacts (newest-wins dedupe + retention), tiers
  segments hot→cold by age.

## Production features

- **Durability**: length-prefixed crc32 WAL frames; fsync policy
  (`Always` / `EveryN(n)` / `Os`); `put` acks only after the WAL append.
- **Crash recovery**: WAL tails replay on open and are checkpointed straight
  into a segment; torn/corrupt tails end replay cleanly.
- **Segments**: immutable, key-sorted, crc32-verified msgpack; written
  tmp→fsync→rename.
- **Zone maps**: per-segment time range, key range, label value sets
  (cardinality-capped), numeric min/max — queries skip whole segments without
  touching disk.
- **Caching**: byte-bounded LRU of decoded column *sections* keyed by
  `(segment, section)` — a query reads only the sections it touches (targeted
  `read_exact_at`, never the payload blob), and `stats.bytes_read` makes the
  per-query I/O observable. RSS stays bounded by `cache_bytes`.
- **Compaction**: merges hot segments, newest write wins, enforces retention
  TTL, invalidates cache, deletes dead files.
- **Disk tiering**: segments older than `hot_ttl` move to a cold directory
  (rename, or copy+remove across filesystems) and stay fully queryable.
- **Atomic manifest**: the single source of truth for live segments
  (tmp→fsync→rename); anything not in it is garbage.
- **Stats**: puts, flushes, compactions, tiering moves, cache hit/miss,
  per-tier segment counts.
- **Full-text search**: an optional `Record.text` document (caller-supplied;
  the payload stays opaque) is tokenized at write time into a per-segment
  token postings index + an in-memtable token map; `QuerySpec.text_match`
  intersects postings for exact AND-of-tokens matches (case-insensitive) —
  no post-scan, no payload decode. `QuerySpec::matches` is the naive oracle
  the index provably agrees with (`tests/text_search.rs`).
- **Retention & grooming**: per-key-prefix TTLs as policy-as-data
  (`retention: Vec<(prefix, ttl_nanos)>`, longest-prefix wins; the global
  `retention_nanos` knob is the match-all row). Enforced exactly at
  compaction and proactively by a tick-driven groomer — segments age out
  with zero incoming writes. See `docs/GUARANTEES.md` §Retention.
- **Counters**: `incr(key, ts, deltas)` — atomic numeric increments through
  the single writer (concurrent increments never lose an update), folded by
  one merge oracle across memtable/read/compaction/WAL-replay; ordinary
  `put` still replaces (LWW). See `docs/GUARANTEES.md` §Counters.
- **Versioned formats + background migration**: segments, manifest and WAL
  all carry version words (absent = v0, readable forever; unknown future
  versions fail closed); legacy segments are rewritten to the current
  format one-per-tick, restart-safe by construction. `docs/COMPAT.md` has
  the full matrix.
- **Blobs**: content-addressed `put_blob`/`get_blob`/`delete_blob` —
  sha256-keyed file-per-hash outside the WAL (the hash is the integrity
  check, verified on every read, fail-closed), manifest-tracked existence,
  dedup by construction, explicit-delete-only. `docs/GUARANTEES.md` §Blobs.
- **Graceful shutdown**: `close()` checkpoints everything to segments.

## Upsert / merge semantics (public guarantee)

Every `put` is an **upsert**: `Record.key` is the identity, and the last
*acked* write wins — by write order (WAL-ack order), not by timestamp. The
winner is stable across memtable, flush, compaction, tiering, close/reopen,
and crash recovery; a `put_batch` becomes visible atomically in-process.
Actor-owned single-writer mutation makes this hold by construction, not by
lock choreography. Normative wording (including the explicit
**non-guarantees**: no cross-key transactionality, batches are prefix-durable
under crash) lives in [`docs/GUARANTEES.md`](docs/GUARANTEES.md), pinned by
`tests/upsert_guarantee.rs`.

## Numbers (release, laptop, **1M × ~1.3 KB records + FTS text**, v2 + token index)

| operation | result |
|---|---|
| durable build, 1M records (compaction racing, incl. K_TEXT/K_TOKENS) | **12.2 s (~82k rec/s)**, write-amp 1.63× |
| **put-ack** (single put: WAL append + memtable fold, fsync/256) | **p50 36 µs · p99 70 µs** with compaction racing |
| **flush lag** (30k-record burst / 10k memtable) | **9.75 ms** to fully durable |
| **full-text search** (`zebracorn billing`, ~0.1%, limit 50) — warm | **834 µs** (postings intersection; cold first-touch 29.5 ms) |
| **FTS + label predicate** (composed) — warm | **297 µs** |
| selective numeric scan (~0.25%, limit 50) — warm / cold | **2.8 ms** / 19.8 ms (54.7 MB read) |
| broad filtered + sorted page (~17%, top-k 50) — warm | **3.1 ms** |
| newest-first page (`order_by` ts desc, limit 50) | **796 µs** (early termination) |
| recent time-range scan (~1%) — warm | **8.3 ms** |
| point get (warm) | ~5.5 µs |
| zone-map-pruned query (no match) | ~7.7 µs |

Methodology, every leg's definition, and the un-ordered-scan caveat live in
[`docs/BENCH.md`](docs/BENCH.md) — the first honest run there is the
baseline later runs are regression-guarded against. Pre-v2 history: the v2
engine (`docs/PERF-PLAN.md`) was ~1,000–2,000× faster than v1 on the weak
paths; this table now also carries the FTS/counter-era numbers.

Run them: `cargo bench` (set `GIRDER_BENCH_N` to change corpus size; default 1M).

## Use

```rust
use girder::{Girder, GirderConfig, QuerySpec, Record};

let engine = Girder::open(GirderConfig::at("/var/lib/myapp/girder")).await?;

engine.put(Record {
    key: "s/trace-1/span-1".into(),
    timestamp: 1_700_000_000_000_000_000,
    labels: [("model".into(), "gpt-4o".into())].into(),
    numerics: [("latency_ms".into(), 812.0)].into(),
    payload: serde_json::to_vec(&span)?,
}).await?;

let hits = engine.scan(&QuerySpec {
    time: Some((t0, t1)),
    labels: vec![("model".into(), "gpt-4o".into())],
    numeric_ranges: vec![("latency_ms".into(), 1000.0, f64::MAX)],
    limit: 50,
    ..Default::default()
}).await?;
```

`GirderConfig` knobs: `fsync`, `memtable_max_records`, `cache_bytes`,
`compact_min_segments`, `hot_ttl_nanos`, `retention_nanos`, `retention`
(per-prefix TTL rows), `tick_interval`, `hot_dir` / `cold_dir`.

## Tests

`cargo test` covers: WAL roundtrip + torn/corrupt tails, segment crc guards,
zone-map pruning truth table, crash recovery, newest-wins across tiers,
freeze/flush/cache behavior, compaction dedupe, tiering to cold, retention,
LRU eviction, and 8-way concurrent writers. `tests/upsert_guarantee.rs` pins
the documented upsert guarantee (`docs/GUARANTEES.md`) statement by statement.

## Roadmap

- Multi-node: the actor topology maps onto `rebar-cluster` (distributed
  runtime + SWIM) the same way [barkeeper](https://github.com/alexandernicholson/barkeeper)
  distributes its Raft actors.
- Secondary key index (per-segment key bloom filters) for point-get-heavy loads.
- Streaming scan (iterator) to avoid materializing large result sets.

## License

MIT OR Apache-2.0
