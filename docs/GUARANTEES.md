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
- **No delete API.** Records leave only via retention TTL at compaction.
  Embedders needing point deletes layer a tombstone convention on top (write
  a marker record for the key and filter it at read — last write wins does
  the rest).

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
