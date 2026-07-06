# Girder public guarantees — upsert / merge semantics

This document is normative. Every statement here is pinned by
`tests/upsert_guarantee.rs`; a change in behavior must change this document
and those tests in the same commit.

## The guarantee: per-key last-write-wins (LWW) upsert

`Record.key` is the unique identity of a record. Writing a record whose key
already exists **replaces** it — a `put` is always an upsert, and the engine
never returns two records for one key.

**G1 — Last write wins, by write order.** For any key, the record returned by
`get`/`scan` is the one from the most recently *acked* `put`/`put_batch`
containing that key. "Most recent" means **arrival order at the single
writer** (WAL-ack order), **not** `Record.timestamp` order: overwriting a key
with an older timestamp still wins. Within one `put_batch`, later elements
overwrite earlier ones for duplicate keys.

**G2 — The winner is stable across every lifecycle stage.** LWW holds
regardless of where the versions physically live, and survives every
background transition:

- memtable over frozen memtable over segment (scan shadowing, newest-id-first);
- segment over older segment (flush order = write order);
- compaction (merge dedupes, newest write wins, old versions are dropped);
- hot→cold tiering (recency order is by segment id, not tier);
- `close()` + reopen (checkpoint);
- crash + WAL replay (recovery reapplies the log in append order).

**G3 — Durability ack.** `put`/`put_batch` returns only after the WAL append
(under the configured `FsyncPolicy`). An acked write survives a crash — with
`FsyncPolicy::Always` unconditionally; with `EveryN`/`Os`, up to the fsync
window the embedder chose.

**G4 — Batch visibility is atomic in-process.** A `put_batch` becomes visible
to concurrent `get`/`scan` all at once (the batch is inserted under one
memtable write lock): a reader never observes a partially applied batch from
a live engine.

**G5 — LWW holds under every query condition (shadowing).** G1–G2 are
promises about the *whole* store, not just unconditioned reads: a
label-scoped, time-windowed or numeric-ranged `scan`/`count` never returns a
key's older version when a newer version exists — even when that newer
version itself matches nothing (a tombstone, or a rewrite whose label values
changed). Mechanically: the three read paths share one **walk plan** —
zone-matching segments are full visits, zone-pruned overlapping ones stay as
probe targets; each candidate row binary-searches the sorted key column of
every NEWER plan step whose key range covers it (keys only, payloads
untouched — and a step no candidate lands in is never read at all);
range-disjointness (computed over the full prefix-overlapping set) skips all
shadow bookkeeping in the compacted common case. Pinned by `tests/lww_shadowing.rs`, which holds all three paths
to a naive newest-write-wins oracle.

## Deletes (`delete`)

`delete(key, timestamp)` writes the canonical **tombstone**: an empty-payload
record labelled `del=1` (`TOMBSTONE_LABEL`). Tombstones are engine
vocabulary:

- **A deleted key reads as absent.** `get` returns `None`; `scan`/`count`
  never return or count a tombstone. The tombstone still *shadows* every
  older version of its key (G5) until compaction/retention physically drops
  them.
- **LWW applies.** A later `put` of the key simply wins (write order, G1) —
  delete is not a lock, and there is no un-delete other than rewriting.
- **Delete-then-`incr` resets the counter.** A tombstone base terminates the
  delta fold contributing nothing and *basifies* the chain (the
  delta-chain-with-no-base rule, in the single `merge_delta` oracle):
  increments newer than the tombstone re-create the row from zero, and
  nothing older can fold in beneath the delete.
- **Timestamp rule.** The tombstone's `timestamp` must be ≥ that of every
  version it shadows — pass the delete time. Retention judges a key by its
  winning version's own timestamp (see §Retention), so a back-dated
  tombstone can expire and be groomed *before* the data it shadows,
  resurrecting it at the next label- or time-scoped read. Pre-existing
  embedder tombstones written with `timestamp: 0` shadow correctly (G5) but
  carry that retention hazard until rewritten.
- The label `del` is reserved alongside `girder.delta`: a record carrying
  `del=1` **is** a tombstone to the engine, whoever wrote it. (Deliberately
  un-namespaced: it formalizes the embedder convention already on disk, so
  every historical tombstone gained these semantics retroactively with no
  migration.)

## Counters (`incr`)

`Girder::incr(key, ts, deltas)` adds numeric deltas onto a key atomically:
increments are serialized through the single writer and folded by ONE merge
oracle (`merge_delta`) shared by the memtable, every read path, compaction
and WAL replay — so **concurrent increments never lose an update**
(`tests/counters.rs`, the concurrent-accrual test). Semantics:

- numerics ADD; identity fields (labels/payload/text) come from the base —
  a delta only adds numbers (its own fields seed a row it creates);
- folded `timestamp` = latest activity (max), so an active counter is never
  aged into retention by an old base;
- an ordinary `put` still REPLACES the accumulated value — G1 last-write-wins
  holds unchanged; `incr` is opt-in per write;
- the returned post-increment snapshot is read after the ack and may include
  later concurrent increments (monotone counters: never less than this
  call's own contribution);
- the `girder.` label prefix is reserved for the engine (the delta flag
  rides `girder.delta`, stripped from every record the engine returns);
- scans that can touch counters run in fold mode: predicates, ordering and
  limits apply to FOLDED totals only — **partial values must never rank**
  (a raw delta never matches a filter, orders a page, or leaks out).

## Blobs (`put_blob` / `get_blob` / `delete_blob`)

Content-addressed immutable byte objects, keyed by sha256, stored one file
per hash under `blobs/` — OUTSIDE the WAL, memtable and segments (the hash
IS the integrity check; content never churns the record machinery):

- `put_blob` is idempotent: same content, same id, one file — dedup by
  construction. Write is tmp→fsync→rename + a manifest listing, both under
  the manifest lock.
- **The manifest is the existence oracle**: `get_blob` of an unlisted hash
  is `None` even if a file exists (kill residue = garbage, swept by the
  maintenance tick under the same lock so a mid-put blob can never be
  swept). A LISTED blob whose file is missing or whose content no longer
  matches its hash is loud corruption — never `None`, never served bytes.
- **Deletion is explicit only** (`delete_blob`, idempotent; manifest first,
  then the file is garbage). No TTL applies to blobs: content addressing
  means dedup, dedup means shared referents, and only the embedder knows
  references — a TTL-from-last-put would delete under live references the
  engine cannot see.

## Retention & grooming

Retention is policy-as-data: `GirderConfig.retention` is a list of
`(key_prefix, ttl_nanos)` rows (the legacy global `retention_nanos` knob
folds in as the `""` match-all row; an explicit `""` row overrides it).
Resolution is **longest-prefix wins**; duplicate prefixes fold
last-entry-wins; a key matching no row is **kept forever**. One oracle
(`RetentionPolicy`) resolves TTLs for both enforcement points:

- **Compaction** retains per key, exactly, after delta folding — an active
  counter (folded timestamp = latest activity) is never expired by an old
  base.
- **The tick-driven groomer** ages segments out with ZERO incoming writes:
  a segment provably all-expired from its zone map alone is dropped
  wholesale (any tier, manifest swap first — files are garbage); a
  provably partially-expired HOT segment is rewritten in place (per-key
  exact retain, same recency slot, guaranteed progress — never churn).
  Cold partially-expired segments wait for full expiry. The groomer
  **never touches a key range where counter deltas exist** (its own zone,
  another segment's, or the live memtables') — a fold spans sources, and
  judging one segment in isolation could drop a counter's base; counter
  ranges are groomed by compaction, which folds first. An INACTIVE
  counter — no live deltas, folded timestamp past its TTL — is ordinary
  expired data and may be dropped; a later increment starts a fresh row.

Expiry is lazy: an expired record may remain readable until the next
compaction or groom pass touches it (there is no read-time filter).
Point deletes remain an embedder convention (tombstones) — retention is
the only engine-level removal.

## Sealed-segment reclamation

Compaction seals a segment at either output cap (records, or half the byte
cap) and never re-merges it — that bound is what keeps write amplification
finite (see `docs/BENCH.md`, the D1 finding). The cost is that rows
overwritten *after* their segment sealed are dead bytes. Those are
reclaimed by a bounded background audit (`reclaim_sealed`, pinned by
`tests/seal_reclaim.rs`):

- One audit per tick, rotating over sealed hot segments; an audit reads
  key columns only. A row is **dead** iff a strictly newer durable segment
  holds a non-delta record for its key and no newer durable segment holds
  a delta for it (a delta needs its base — the same fold rule compaction
  and `count()` honor). A row is never judged by what sits *below* it, so
  a tombstone-convention record shadowing older versions always survives
  until it is itself shadowed.
- A segment is rewritten (solo, same recency slot — never merged) only
  when at least **half** its rows are dead, so each rewrite at least
  halves it: bounded rewrites per segment lifetime, and an overwrite-free
  corpus never triggers reclaim at all. Fully-dead segments are dropped
  wholesale.
- Hot v2 segments only: cold segments wait (mirroring the groomer's cold
  deferral), v1-format segments wait for migration.

## Explicit non-guarantees

These are deliberate; do not build on their absence being accidental.

- **No cross-key transactionality.** There are no multi-key transactions and
  no read-modify-write primitive. G4 is an *in-process visibility* property
  only: after a **crash**, a `put_batch` may be **prefix-durable** — WAL
  replay stops at the first torn frame, so a torn tail can persist a prefix
  of the batch's records. Embedders needing cross-record invariants must
  encode them in one record (one key) or reconcile on read.
- **No timestamp-ordered conflict resolution.** `Record.timestamp` is a query
  dimension (time pruning, retention, ordering), not a version. If you
  overwrite a key with an older timestamp, the overwrite wins (G1) — and the
  surviving record is then judged by *its own* timestamp for retention TTL,
  so it may be dropped at the next compaction if that timestamp is expired.
- **Deletes are logical until compaction.** `delete` (see §Deletes) makes a
  key read as absent immediately, but the bytes of shadowed versions leave
  the store only when compaction/retention physically drops them — there is
  no synchronous physical erase.

## Why this holds by construction

All mutation is serialized through two Rebar `GenServer`s: the **WriterActor**
(every `put` is one serialized call: WAL append → memtable insert → freeze
when full), and the **MaintenanceActor** (sole custodian of the manifest:
flush, compaction, tiering, retention). There is no lock choreography to get
wrong — write order *is* the actor's mailbox order, and every read path
(memtable → frozen, newest first → segments, newest-id-first) shadows older
sources with a seen-key set.

Background maintenance is **invisible to readers**: a `get`/`scan` never
spuriously fails because a segment was compacted (files deleted) or tiered
(file renamed) mid-read. Compaction deletes input files only after the
replacement manifest is durably stored, so a read that raced the deletion
retries against a fresh manifest snapshot; a tiering rename is tolerated by
falling back to the other tier's path. `close()` quiesces all background
work before returning, so manifest + files are always consistent for an
immediate reopen.
