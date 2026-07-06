//! # Girder
//!
//! An embedded storage engine for trace-shaped data (append-mostly records
//! with a timestamp, low-cardinality labels, numeric metrics, and an opaque
//! payload), built on the [Rebar](https://github.com/alexandernicholson/rebar)
//! actor runtime. Girders carry rivets:
//! [Rivet](https://github.com/alexandernicholson/rivet) is the observability
//! platform this engine was built for — but the engine is generic.
//!
//! ## Architecture
//!
//! ```text
//!  put/put_batch ──► WalActor (GenServer: single-writer WAL, crc32, fsync policy)
//!        │                 durability ack
//!        ▼
//!    MemTable (sorted, newest-wins) ──freeze──► FlushActor ──► segment file
//!        │                                          │      (rmp + crc32 + zone map)
//!        ▼                                          ▼
//!      scan ◄── zone-map pruning ◄── Manifest (atomic rename)
//!        │                               ▲
//!        ▼                               │
//!   block LRU cache            CompactorActor (merge + dedupe + TTL)
//!                              TieringActor  (hot dir ──age──► cold dir)
//! ```
//!
//! - **Durability**: `put` acks only after the WAL append (fsync per policy);
//!   crash recovery replays the WAL into the memtable on open.
//! - **Reads**: memtable → frozen memtables → segments newest-first, pruned
//!   by per-segment zone maps (time range, label values, numeric min/max),
//!   deduped by key (newest wins), through an LRU segment cache.
//! - **Background actors** (Rebar `GenServer`s): flush, compaction, tiering —
//!   isolated, supervised by the engine (respawn-on-dead-mailbox), and
//!   crash-safe because every visible state change goes through WAL+manifest.
#![forbid(unsafe_code)]

mod actors;
mod blob;
mod cache;
mod engine;
mod error;
mod manifest;
mod memtable;
mod object_store;
mod record;
mod retention;
mod segment;
pub mod text;
mod wal;

pub use engine::{Girder, GirderConfig, Stats};
pub use error::{GirderError, Result};
pub use object_store::{ObjectStore, ObjectStoreRef};
pub use record::{OrderBy, QuerySpec, Record, TOMBSTONE_LABEL};
pub use text::fts_tokens;
pub use wal::FsyncPolicy;
