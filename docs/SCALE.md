# Girder scale-out: cluster reads + the remote (object-storage) tier

**Status: design, ratified pre-implementation** (rivet plan 0014, track-f
design round, rulings 1–7 of 2026-07-06, including the operator's
storage-mandate addition folding the S3 cold tier and the hot_ttl knob into
this design space). Nothing in this document is built yet; each section
names the slice that will build it and the invariant that gates it. When a
slice lands, its section becomes normative and gains test pins, following
the GUARANTEES.md discipline.

Scope split, stated once: **girder owns bytes and tiers; rivet owns the
cluster and the network.** Girder never learns about nodes, SWIM, sigv4, or
HTTP — it gains exactly one new seam (`ObjectStore`) and one new tier.
Everything cross-node lives rivet-side, behind the existing
`TraceStore`/`SearchIndex` trait objects.

## 1. The problem (why this design space is one space)

Rivet's cluster distributes **ingest compute only** (plan 0003; rivet
memory 0060): each node writes its own local girder store, and a per-node
UI shows that node's share. Two consequences:

1. **Reads are per-node.** A query answers from whichever node you asked.
2. **Disk is per-node.** Every node retains its full share of history on
   local disk; retention is the only reclaim.

Both are the same question — *where do aged bytes live, and who serves
them* — which is why one design round covers both (operator ruling). The
answer: aged segments live in **shared object storage** (the remote tier),
so any node can serve any cold segment, per-node disks only need the hot
window, and the cluster read layer (scatter-gather) makes the per-node
split invisible to callers.

## 2. Ratified decisions (the record)

| # | Decision | Ruling |
|---|---|---|
| 1 | Scale-out axis = **scatter-gather reads over existing placement**. Shard routing by `hash(trace_id)` DEFERRED; replication RF≥2 DEFERRED (demand-evidence). | ratified |
| 2 | Girder defines a narrow **`ObjectStore` trait in girder** (no AWS SDK, no new deps); rivet injects the sigv4 impl. Out-of-engine file mover REJECTED (breaks query-path transparency). | ratified |
| 3 | Remote read v1 = **whole-segment pull-through** into a bounded local cache. Range-GET section reads ledgered as follow-up. | ratified |
| 4 | Tier progression = **hot → cold → remote**, independent `remote_ttl`, remote tier OPTIONAL — absent ⇒ byte-identical to today. | ratified |
| 5 | `--hot-ttl-hours` passthrough landed separately (rivet F-3a), no design dependency. | done |
| 6 | This document lives in girder `docs/`; rivet gets a lore/memory pointer when the slices land. | ratified |
| 7 | Track-f designs the wire interface (§5) and lands the `cluster.rs` touch in its own slice, with **track-e as named reviewer** of that file before handoff. | ratified |

## 3. The remote tier (girder side)

### 3.1 The `ObjectStore` seam

Girder defines the trait; girder never implements a network client:

```rust
/// Remote segment bytes. Implementations live OUTSIDE girder (the rivet
/// consumer injects one over its existing sigv4/HTTP stack). Keys are
/// girder-chosen, flat strings; the store treats them as opaque.
pub trait ObjectStore: Send + Sync {
    fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()>;
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    fn delete(&self, key: &str) -> Result<()>;
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
}
```

Opened via `Girder::open_with_object_store(config, Arc<dyn ObjectStore>)`.
No store injected ⇒ no remote tier exists, no code path changes — the
optionality is structural, not a flag check sprinkled through the engine.
(Sync signatures deliberately match girder's internal blocking-IO idiom —
segment reads already do `std::fs` under the actor runtime; the rivet impl
bridges to its async client the same way `ObjectPut` consumers do.)

Doctrine held: **girder gains zero dependencies.** sigv4, endpoints,
credentials, retries, and TLS are the injector's problem (rivet extends its
X4 `ObjectPut` seam — PUT-only today — to this shape, rivet-side).

### 3.2 Manifest: `Tier::Remote`

`SegmentMeta.tier` gains a third variant. A remote segment keeps its full
metadata (zone maps, row counts, key ranges) in the manifest exactly as hot
and cold segments do — **planning never touches the object store.** Zone
pruning, shadow probing, and `walk_plan` work unchanged on metadata; only a
segment that survives pruning and actually needs bytes triggers a fetch.
Object key = the segment filename (`seg_<seq>`, sequence numbers are never
reused — uploads are idempotent by construction).

### 3.3 The move protocol (crash posture)

`TieringActor::tier()` today moves hot→cold past `hot_ttl_nanos` by rename,
with a copy+remove cross-fs fallback; `open_segment_file` tolerates the
concurrent rename by trying the other tier (engine.rs:1522). The remote
move extends the same posture, cold→remote past `remote_ttl_nanos`:

1. **PUT** the segment bytes to the object store (idempotent — same key,
   same immutable bytes; a re-run overwrites with identical content).
2. **Flip** the manifest entry to `Tier::Remote` and persist the manifest.
3. **Delete** the local file.

A kill between (1) and (2) leaves the manifest saying `Cold` with the file
still local — reads unaffected, the next tick re-PUTs (idempotent) and
proceeds. A kill between (2) and (3) leaves the file both places — the
open path prefers local (free), the next tick re-deletes. At no step is
the segment readable in fewer than one place, which is the same invariant
the hot↔cold rename tolerance already pins. One move per tick, same
bounded-background-work discipline as reclaim.

**Tombstone/reclaim interaction:** seal-reclaim (F3) audits **hot v2 only**
— unchanged. Compaction and retention never operate on remote segments in
v1; a remote segment leaves the remote tier only by retention (manifest
drop + object `delete`, in that order: dropping the manifest entry first
means a crash strands a harmless orphan object, reconciled by `list` at
open — never a manifest entry pointing at deleted bytes).

### 3.4 The read path: pull-through

`open_segment_file` grows a third arm: `Tier::Remote` ⇒ fetch the whole
object into a bounded local **pull cache** directory, then open the local
file and proceed through the existing, unchanged read code (sections,
payload slices, section cache — all identical). Eviction is LRU by bytes
(`pull_cache_bytes` config, default sized like `cache_bytes`); a segment
being read holds its fd, so unix semantics make eviction safe mid-read
(same fd-across-rename argument as tiering).

Why whole-segment, not range-GET (ruling 3): pull-through touches ONE
function; range-GET section reads touch every section reader and re-open
the D7 cache-sizing question for remote latencies. Range-GET stays
ledgered with its demand evidence: pull latency complaints on stores whose
individual segments are large (fat-record shapes) — measured, not guessed.

**Oversized entries — budget-plus-one, never refuse (lead addition).** A
single segment may be LARGER than `pull_cache_bytes` — production carries a
422 MB segment TODAY. The pull cache MUST still fetch it: evict everything
else and hold the one oversized entry, rather than ever refusing a read (a
refused fetch would fail a query for data that exists — unacceptable). So
`pull_cache_bytes` bounds the *sum of resident entries when it can*, but a
single entry is always admitted; the budget is a target, not a hard cap
that can starve a legal read. (This mirrors the memory-side lesson that a
hard cap on an unbounded single item is a footgun — the 422 MB segment
OOMd a different path once already; the pull cache must not repeat it, and
it fetches whole-segment so the resident set is explicit.)

### 3.5 Knobs

- `hot_ttl_nanos` — exists (24h default); rivet passthrough landed (F-3a,
  `--hot-ttl-hours`).
- `remote_ttl_nanos` — new; segments older than this age cold→remote.
  Only meaningful with an object store injected; `i64::MAX/2` idiom = never.
  Rivet passthrough: `--remote-ttl-hours`, same `open_tuned` shape.
- `pull_cache_bytes` — new; bounds the pull-through cache directory
  (budget-plus-one, per §3.4). Default = `cache_bytes`.

**Age is the SEGMENT's age, and misconfiguration is safe (lead addition).**
Both TTLs measure `created_unix_nanos` — the segment's own age, NOT how
long it has sat in its current tier. So the thresholds are absolute points
on one timeline. The mover only ever promotes a segment ONE tier per pass
(hot→cold, then cold→remote), so `remote_ttl_nanos < hot_ttl_nanos` does
NOT skip the cold hop: a segment already past both thresholds moves
hot→cold on one tick and cold→remote on the next (the cold hop is never
skipped — the mover reads the current tier and advances it by one). A
segment younger than `remote_ttl_nanos` but past `hot_ttl_nanos` simply
stops at cold until it ages further. There is no configuration that strands
a segment or moves it backward.

### 3.6 Security posture (lead addition)

Remote objects carry FULL trace payloads. Encryption-at-rest and bucket
access control are the DEPLOYMENT boundary: girder ships bytes to the store
the operator names and trusts it to be private — self-host doctrine, the
same boundary the rivet P2 compliance page draws for the local store.
Girder adds no application-layer encryption of its own (the `ObjectStore`
seam is opaque bytes in, opaque bytes out); an operator who needs
encryption configures it on the bucket / SSE, exactly as they secure the
local hot/cold disks today.

## 4. Cluster reads (rivet side): scatter-gather

A new rivet-store type, `ClusterQuery`, implements `SearchIndex` +
`TraceStore` by fanning out to cluster members and merging — an **additive
trait impl**; the single instantiation site (rivet-cli main.rs) picks it
when `--cluster-*` flags are present, and every caller (API, monitors,
proxy quality routing) is untouched behind `Arc<dyn _>`.

- **`query`**: fan out to all live members (SWIM view), k-way merge pages
  by the existing total order `(start_time, trace_id, span_id)` — the sort
  contract makes the merge well-defined and keyset resumption sound (each
  node's `after` bound is the global bound; a node with no rows past the
  bound returns empty, honestly).
- **`aggregate`**: fan out, fold partial results through the existing
  `aggregate_spans` oracle shapes (count/sum fold trivially; avg folds as
  (sum, count) pairs — the wire carries the pair, never a pre-divided
  average).
- **`get_trace` / `trace_id_for_span`**: broadcast, first non-null wins
  (traces are node-local under existing placement — exactly one node
  answers). A gossiped trace→node hint map is a ledgered optimization,
  not v1.
- **Partial failure = partial answer, said honestly**: a member that times
  out is reported in the page metadata (`nodes_answered`/`nodes_total`),
  never silently dropped — error-is-no-information applies across the
  wire too.

## 5. The wire interface (track-e review surface)

The only touch in `rivet-ingest/src/cluster.rs` (track-e's file; ruling 7:
designed here, landed in a track-f slice, track-e named reviewer):

- **One new message kind**, `QueryFrame`, alongside the existing
  announcement/mirror frames: `{ request_id: u64, body: QueryBody }` where
  `QueryBody = Query | Aggregation | GetTrace | TraceIdForSpan` (serde,
  same encoding as the existing frames), and the response frame
  `{ request_id, body: Result<Page/Result/Option<Trace>/Option<String>> }`.
- **One serve-loop arm**: on `QueryFrame`, dispatch to the node's local
  `Arc<dyn TraceStore>/<dyn SearchIndex>` (handed to `ClusterNode` at
  construction, the same way `ClusterMirror` gets its sink) and reply on
  the router. Bounded: queries execute on the existing worker pool, replies
  use the same bounded `try_send` discipline as mirroring — an overloaded
  node sheds queries visibly (counted, reported as a non-answering member)
  rather than queueing unboundedly.
- **No new transport, no new membership**: rides `DistributedRouter` + the
  SWIM liveness view as-is. Cross-node rate limiting (track-e's own 0014
  row) shares the bus but not the frames — no coupling.

## 6. Deferred, with their triggers (the ledger)

- **Shard routing by `hash(trace_id)`** — placement change; trigger:
  broadcast `get_trace` measurably hurting at fleet sizes where N-node
  fan-out dominates (measure first — ruling 1b).
- **Replication RF≥2** — durability posture change, write-path fan-out,
  consistency decisions; trigger: operator demand for node-loss
  survivability beyond object-storage-backed history (ruling 1c). Note the
  remote tier already gives aged data node-loss survivability for free.
- **Range-GET section reads** — §3.4; trigger: pull-latency complaints on
  fat-segment stores.
- **Trace→node hint gossip** — §4; trigger: same as shard routing.

## 7. Slice plan

1. **SCALE-1 (girder):** `ObjectStore` trait + `Tier::Remote` + move
   protocol + pull-through read + `remote_ttl_nanos`/`pull_cache_bytes`.
   Tests: kill-point matrix on the move protocol (file present/absent ×
   manifest tier, all four states readable), pull-cache eviction under
   read, retention-drop orphan reconciliation, and the no-store-injected
   byte-identical control.
2. **SCALE-2 (rivet):** rivet-side `ObjectStore` impl over the extended X4
   seam (sigv4 GET/DELETE/LIST beside the existing PUT, AWS-vector-gated)
   + `open_tuned` growth (`--remote-ttl-hours`) + wiring.
3. **SCALE-3 (rivet):** `QueryFrame` + serve-loop arm in cluster.rs
   (track-e reviews) + `ClusterQuery` scatter-gather impl + merge tests
   (sort-contract merge, keyset resume across nodes, partial-failure
   honesty, (sum,count) aggregate folding).

Each slice gates independently; SCALE-1 is useful alone (single node,
bottomless history on cheap storage), SCALE-3 is useful without SCALE-1
(cluster reads over local-only tiers) — they compose but don't depend.
