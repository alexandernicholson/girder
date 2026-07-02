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
- **Graceful shutdown**: `close()` checkpoints everything to segments.

## Numbers (release, laptop, **1M × ~1.3KB records**, columnar v2 engine)

| operation | result |
|---|---|
| durable build, 1M records (batch 500, fsync/256) | **7.15s (~140k rec/s)** |
| selective scan, uncorrelated numeric (~0.25% match, limit 50) — warm | **~2.3ms** |
| selective scan — cold | **13.1ms, reading 46.9MB** (columns + survivor payloads only; was 361ms / 1.19GB whole-file) |
| broad filtered + sorted page (~17% match, top-k limit 50) — warm | **~2.5ms** |
| newest-first page (`order_by` ts desc, limit 50) | **~1.2ms** (early termination: 1 segment loaded) |
| recent time-range scan (~1% match) — warm | **~4.7ms** |
| point get (warm) | ~3.5µs |
| zone-map-pruned query (no match) | ~29µs |

Pre-v2 baselines for the same shapes @1M: selective ~3.0s, broad ~4.75s,
recent ~70ms — the v2 engine (columnar segments + block pruning + top-k
pushdown + tiered compaction + section cache, `docs/PERF-PLAN.md`) is
**~1,000–2,000× faster on the weak paths** without regressing the strong one.
`stats.bytes_read` makes per-query I/O observable.

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
`compact_min_segments`, `hot_ttl_nanos`, `retention_nanos`, `tick_interval`,
`hot_dir` / `cold_dir`.

## Tests

`cargo test` covers: WAL roundtrip + torn/corrupt tails, segment crc guards,
zone-map pruning truth table, crash recovery, newest-wins across tiers,
freeze/flush/cache behavior, compaction dedupe, tiering to cold, retention,
LRU eviction, and 8-way concurrent writers.

## Roadmap

- Multi-node: the actor topology maps onto `rebar-cluster` (distributed
  runtime + SWIM) the same way [barkeeper](https://github.com/alexandernicholson/barkeeper)
  distributes its Raft actors.
- Secondary key index (per-segment key bloom filters) for point-get-heavy loads.
- Streaming scan (iterator) to avoid materializing large result sets.

## License

MIT OR Apache-2.0
