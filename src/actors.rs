//! The Rebar actor layer.
//!
//! Two supervised actors own all mutation, which is what makes the engine
//! race-free without lock choreography:
//!
//! - [`WriterActor`] — the single writer. Every `put` is a `call` through its
//!   mailbox: WAL append (durability ack) → memtable insert → freeze+rotate
//!   when full. Serial by construction.
//! - [`MaintenanceActor`] — the single custodian of the manifest: flushes
//!   frozen memtables to segments, compacts, tiers hot→cold, enforces
//!   retention. Driven by flush casts from the writer and a periodic tick.
//!
//! Crash-safety: every visible state change is WAL- or manifest-backed, so an
//! actor death (or whole-process crash) at any point recovers to a consistent
//! state on open. Dead maintenance mailboxes are respawned by the engine.
use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use rebar_core::gen_server::{GenServer, GenServerContext};
use rebar_core::process::ProcessId;

use crate::engine::EngineInner;
use crate::error::Result;
use crate::manifest::{segment_path, SegmentMeta, Tier};
use crate::record::Record;
use crate::segment;
use crate::wal::Wal;

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

pub enum WriterMsg {
    Append(Vec<Record>),
    /// Force a freeze+rotate even below the threshold (shutdown/flush()).
    Freeze,
    Sync,
}

pub struct WriterActor {
    pub inner: Arc<EngineInner>,
}

pub struct WriterState {
    pub wal: Wal,
    pub wal_seq: u64,
}

impl WriterActor {
    fn freeze(&self, state: &mut WriterState) -> std::result::Result<Option<u64>, String> {
        let mut memtable = self.inner.memtable.write().unwrap();
        if memtable.is_empty() {
            return Ok(None);
        }
        let frozen_map = std::mem::take(&mut *memtable);
        drop(memtable);
        let frozen_seq = state.wal_seq;
        state.wal_seq += 1;
        state.wal = Wal::open(&self.inner.wal_path(state.wal_seq), self.inner.config.fsync)
            .map_err(|e| e.to_string())?;
        self.inner
            .frozen
            .write()
            .unwrap()
            .push((frozen_seq, Arc::new(frozen_map)));
        Ok(Some(frozen_seq))
    }
}

#[async_trait]
impl GenServer for WriterActor {
    type State = WriterState;
    type Call = WriterMsg;
    type Cast = ();
    type Reply = std::result::Result<Option<u64>, String>;

    async fn init(&self, _ctx: &GenServerContext) -> std::result::Result<WriterState, String> {
        let seq = self.inner.initial_wal_seq;
        let wal = Wal::open(&self.inner.wal_path(seq), self.inner.config.fsync)
            .map_err(|e| e.to_string())?;
        Ok(WriterState { wal, wal_seq: seq })
    }

    async fn handle_call(
        &self,
        msg: WriterMsg,
        _from: ProcessId,
        state: &mut WriterState,
        _ctx: &GenServerContext,
    ) -> Self::Reply {
        match msg {
            WriterMsg::Append(records) => {
                // 1. Durability.
                state
                    .wal
                    .append_batch(&records)
                    .map_err(|e| e.to_string())?;
                // 2. Visibility.
                {
                    let mut memtable = self.inner.memtable.write().unwrap();
                    for record in records {
                        memtable.insert(record.key.clone(), record);
                    }
                }
                self.inner.note_put();
                // 3. Freeze when full (serial with appends — no races).
                let over = self.inner.memtable.read().unwrap().len()
                    >= self.inner.config.memtable_max_records;
                if over {
                    return self.freeze(state);
                }
                Ok(None)
            }
            WriterMsg::Freeze => self.freeze(state),
            WriterMsg::Sync => {
                state.wal.sync().map_err(|e| e.to_string())?;
                Ok(None)
            }
        }
    }

    async fn handle_cast(&self, _msg: (), _state: &mut WriterState, _ctx: &GenServerContext) {}
}

// ---------------------------------------------------------------------------
// Maintenance
// ---------------------------------------------------------------------------

pub enum MaintCall {
    /// Flush every frozen memtable to segments (returns segments written).
    FlushPending,
    /// One compaction + tiering + retention pass.
    Tick,
}

pub struct MaintenanceActor {
    pub inner: Arc<EngineInner>,
}

#[async_trait]
impl GenServer for MaintenanceActor {
    type State = ();
    type Call = MaintCall;
    type Cast = MaintCall;
    type Reply = std::result::Result<u64, String>;

    async fn init(&self, _ctx: &GenServerContext) -> std::result::Result<(), String> {
        Ok(())
    }

    async fn handle_call(
        &self,
        msg: MaintCall,
        _from: ProcessId,
        _state: &mut (),
        _ctx: &GenServerContext,
    ) -> Self::Reply {
        self.run(msg).map_err(|e| e.to_string())
    }

    async fn handle_cast(&self, msg: MaintCall, _state: &mut (), _ctx: &GenServerContext) {
        if let Err(err) = self.run(msg) {
            tracing::warn!(%err, "girder maintenance failed (will retry on next tick)");
        }
    }
}

impl MaintenanceActor {
    fn run(&self, msg: MaintCall) -> Result<u64> {
        match msg {
            MaintCall::FlushPending => self.flush_pending(),
            MaintCall::Tick => {
                let flushed = self.flush_pending()?;
                self.compact()?;
                self.tier()?;
                Ok(flushed)
            }
        }
    }

    /// Frozen memtable → segment file → manifest → delete covered WAL.
    fn flush_pending(&self) -> Result<u64> {
        let mut flushed = 0;
        loop {
            let Some((wal_seq, map)) = self.inner.frozen.read().unwrap().first().cloned() else {
                break;
            };
            let mut records: Vec<Record> = map.values().cloned().collect();
            let id = {
                let mut manifest = self.inner.manifest.write().unwrap();
                let id = manifest.next_segment_id;
                manifest.next_segment_id += 1;
                id
            };
            let file = format!("seg-{id:016}.gird");
            let path = self.inner.config.hot_dir.join(&file);
            let zone = segment::write_segment(&path, &mut records)?;
            let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            {
                let mut manifest = self.inner.manifest.write().unwrap();
                manifest.segments.push(SegmentMeta {
                    id,
                    file,
                    tier: Tier::Hot,
                    zone,
                    bytes,
                    created_unix_nanos: now_nanos(),
                });
                self.inner.store_manifest(&manifest)?;
            }
            // Segment durable → the WAL that covered it can go.
            std::fs::remove_file(self.inner.wal_path(wal_seq)).ok();
            self.inner.frozen.write().unwrap().remove(0);
            self.inner
                .stats_flushes
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            flushed += 1;
        }
        Ok(flushed)
    }

    /// Merge hot segments (newest-wins dedupe) + retention enforcement.
    fn compact(&self) -> Result<u64> {
        let candidates: Vec<SegmentMeta> = {
            let manifest = self.inner.manifest.read().unwrap();
            let hot: Vec<_> = manifest
                .segments
                .iter()
                .filter(|s| s.tier == Tier::Hot)
                .cloned()
                .collect();
            if hot.len() < self.inner.config.compact_min_segments {
                return Ok(0);
            }
            hot
        };
        // Merge newest-wins: read ascending by id; later inserts overwrite.
        let mut merged: BTreeMap<String, Record> = BTreeMap::new();
        let mut sorted = candidates.clone();
        sorted.sort_by_key(|s| s.id);
        for meta in &sorted {
            let path = segment_path(
                &self.inner.config.hot_dir,
                &self.inner.config.cold_dir,
                meta,
            );
            for record in segment::read_all_records(&path)? {
                merged.insert(record.key.clone(), record);
            }
        }
        // Retention: drop expired records during the rewrite.
        if let Some(ttl) = self.inner.config.retention_nanos {
            let cutoff = now_nanos() - ttl;
            merged.retain(|_, r| r.timestamp >= cutoff);
        }
        let mut records: Vec<Record> = merged.into_values().collect();
        let id = {
            let mut manifest = self.inner.manifest.write().unwrap();
            let id = manifest.next_segment_id;
            manifest.next_segment_id += 1;
            id
        };
        let file = format!("seg-{id:016}.gird");
        let path = self.inner.config.hot_dir.join(&file);
        let zone = segment::write_segment(&path, &mut records)?;
        let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        {
            let mut manifest = self.inner.manifest.write().unwrap();
            let old_ids: Vec<u64> = candidates.iter().map(|c| c.id).collect();
            manifest.segments.retain(|s| !old_ids.contains(&s.id));
            manifest.segments.push(SegmentMeta {
                id,
                file,
                tier: Tier::Hot,
                zone,
                bytes,
                created_unix_nanos: candidates
                    .iter()
                    .map(|c| c.created_unix_nanos)
                    .min()
                    .unwrap_or_else(now_nanos),
            });
            self.inner.store_manifest(&manifest)?;
        }
        for meta in &candidates {
            let path = segment_path(
                &self.inner.config.hot_dir,
                &self.inner.config.cold_dir,
                meta,
            );
            std::fs::remove_file(path).ok();
            self.inner.cache.invalidate(meta.id);
        }
        self.inner
            .stats_compactions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(1)
    }

    /// Move hot segments past `hot_ttl` to the cold tier.
    fn tier(&self) -> Result<u64> {
        let cutoff = now_nanos() - self.inner.config.hot_ttl_nanos;
        let to_move: Vec<SegmentMeta> = {
            let manifest = self.inner.manifest.read().unwrap();
            manifest
                .segments
                .iter()
                .filter(|s| s.tier == Tier::Hot && s.created_unix_nanos < cutoff)
                .cloned()
                .collect()
        };
        let mut moved = 0;
        for meta in to_move {
            let from = self.inner.config.hot_dir.join(&meta.file);
            let to = self.inner.config.cold_dir.join(&meta.file);
            // rename first (same fs); fall back to copy+remove (cross-fs).
            if std::fs::rename(&from, &to).is_err() {
                std::fs::copy(&from, &to)?;
                std::fs::remove_file(&from)?;
            }
            let mut manifest = self.inner.manifest.write().unwrap();
            if let Some(entry) = manifest.segments.iter_mut().find(|s| s.id == meta.id) {
                entry.tier = Tier::Cold;
            }
            self.inner.store_manifest(&manifest)?;
            moved += 1;
        }
        if moved > 0 {
            self.inner
                .stats_tiered
                .fetch_add(moved, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(moved)
    }
}

pub fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}
