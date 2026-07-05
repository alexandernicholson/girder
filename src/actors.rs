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
use std::collections::{BTreeMap, HashSet};
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
                        memtable.insert(record);
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
    ///
    /// **Zero-clone flush.** The frozen memtable is a key-sorted
    /// `BTreeMap<String, Record>`; the segment is encoded straight from its
    /// `values()` borrows (`&Record`), so no payload is cloned into an
    /// intermediate `Vec` and no re-sort happens. The frozen entry stays in the
    /// `frozen` list (visible to concurrent reads) until the segment is durable
    /// and in the manifest, preserving read-your-writes — hence encoding from
    /// borrows rather than `Arc::try_unwrap` (which would need an early removal
    /// and open a visibility gap).
    fn flush_pending(&self) -> Result<u64> {
        let mut flushed = 0;
        loop {
            let Some((wal_seq, map)) = self.inner.frozen.read().unwrap().first().cloned() else {
                break;
            };
            let seq = self.alloc_seq();
            let file = seg_file(seq);
            let path = self.inner.config.hot_dir.join(&file);
            // BTreeMap::values() is already key-ascending — encode from borrows.
            let refs: Vec<&Record> = map.values().collect();
            let zone = segment::write_segment_refs(&path, &refs)?;
            drop(refs);
            let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            {
                let mut manifest = self.inner.manifest.write().unwrap();
                manifest.segments.push(SegmentMeta {
                    id: seq,
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
            self.inner
                .stats_bytes_flushed
                .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
            flushed += 1;
        }
        Ok(flushed)
    }

    /// Allocate a fresh, never-reused sequence number (used for unique segment
    /// filenames, and as the recency id for flushed segments).
    fn alloc_seq(&self) -> u64 {
        let mut manifest = self.inner.manifest.write().unwrap();
        let seq = manifest.next_segment_id;
        manifest.next_segment_id += 1;
        seq
    }

    /// Size-capped, time-adjacent tiered compaction (WS3).
    ///
    /// Instead of merging *all* hot segments into one giant segment (the old
    /// O(n²) rewrite), pick the longest run of adjacent-by-id, size-compatible
    /// hot segments, merge them newest-wins with retention, and split the
    /// merged key-sorted stream into consecutive segments capped at
    /// `max_segment_records` / `max_segment_bytes`.
    ///
    /// **Id positioning / newest-wins.** Runs are chosen to be contiguous in
    /// the *global* id order (a cold segment breaks a run), so the run occupies
    /// an id interval with no non-run segment inside it. Output segments reuse
    /// the run's own ids (the top `k`), which keeps them newer than every
    /// segment below the run and older than every segment above it — exactly
    /// the run's recency slot — so key-overlap shadowing across segments stays
    /// correct. Filenames come from a fresh, never-reused sequence
    /// ([`alloc_seq`]) so no on-disk file is clobbered.
    ///
    /// Because adjacent ids ⇒ adjacent time ranges, merged zone maps stay tight
    /// and `recent` pruning holds by construction, not scheduling luck.
    fn compact(&self) -> Result<u64> {
        let hot_dir = &self.inner.config.hot_dir;
        let cold_dir = &self.inner.config.cold_dir;

        // 1. Pick a run under the read lock, then release it for the merge I/O.
        let run: Vec<SegmentMeta> = {
            let manifest = self.inner.manifest.read().unwrap();
            match self.choose_run(&manifest.segments) {
                Some(run) => run,
                None => return Ok(0),
            }
        };
        let run_ids: Vec<u64> = run.iter().map(|m| m.id).collect();

        // 2. Merge newest-wins: read ascending by id; later inserts overwrite.
        let mut ascending = run.clone();
        ascending.sort_by_key(|s| s.id);
        let mut merged: BTreeMap<String, Record> = BTreeMap::new();
        for meta in &ascending {
            let path = segment_path(hot_dir, cold_dir, meta);
            for record in segment::read_all_records(&path)? {
                merged.insert(record.key.clone(), record);
            }
        }
        // 3. Retention: drop expired records during the rewrite.
        if let Some(ttl) = self.inner.config.retention_nanos {
            let cutoff = now_nanos() - ttl;
            merged.retain(|_, r| r.timestamp >= cutoff);
        }
        let records: Vec<Record> = merged.into_values().collect();

        // 4. Split the merged (key-sorted) stream into size-capped chunks; each
        //    chunk is a contiguous, disjoint key range.
        let chunks = split_chunks(
            &records,
            self.inner.config.max_segment_records,
            self.inner.config.max_segment_bytes,
        );

        // 5. Assign recency ids: reuse the run's own ids (the top `k`), handed
        //    out in ascending key order so higher key ranges — which track
        //    newer timestamps in append workloads — get higher ids. This keeps
        //    id order ≈ time order, so timestamp-desc early termination and
        //    `recent` pruning touch only the trailing segments. Any id in the
        //    run's interval is correct for cross-segment newest-wins (outputs
        //    are disjoint by key, so their intra-run order only affects
        //    pruning efficiency, never correctness).
        //
        //    `k <= run.len()` whenever each input <= the cap (true by config:
        //    memtable_max_records <= max_segment_records); the surplus branch
        //    is a documented misconfig safety valve only.
        let mut reuse_ids = run_ids.clone();
        reuse_ids.sort_unstable(); // ascending
        let k = chunks.len();
        // The top-k reused ids, ascending, aligned to ascending-key chunks.
        let id_base = reuse_ids.len().saturating_sub(k);
        let created = run
            .iter()
            .map(|m| m.created_unix_nanos)
            .min()
            .unwrap_or_else(now_nanos);
        let mut outputs: Vec<SegmentMeta> = Vec::with_capacity(chunks.len());
        let mut bytes_written = 0u64;
        for (ci, chunk) in chunks.iter().enumerate() {
            let id = if id_base + ci < reuse_ids.len() {
                reuse_ids[id_base + ci]
            } else {
                self.alloc_seq()
            };
            let seq = self.alloc_seq();
            let file = seg_file(seq);
            let path = hot_dir.join(&file);
            let zone = segment::write_segment_presorted(&path, chunk)?;
            let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            bytes_written += bytes;
            outputs.push(SegmentMeta {
                id,
                file,
                tier: Tier::Hot,
                zone,
                bytes,
                created_unix_nanos: created,
            });
        }

        // 6. Atomic manifest swap: drop the run's ids, add the outputs.
        {
            let run_id_set: HashSet<u64> = run_ids.iter().copied().collect();
            let mut manifest = self.inner.manifest.write().unwrap();
            manifest.segments.retain(|s| !run_id_set.contains(&s.id));
            manifest.segments.extend(outputs.iter().cloned());
            self.inner.store_manifest(&manifest)?;
        }

        // 7. Manifest is the source of truth → the old input files are garbage.
        //    (Output filenames are fresh, so this never deletes an output.)
        for meta in &run {
            std::fs::remove_file(segment_path(hot_dir, cold_dir, meta)).ok();
            // Hygiene only: sections are keyed by the never-reused file seq
            // (`SegmentMeta::cache_key`), so a dead segment's entries can never
            // be served for a live one — this just frees their bytes early.
            self.inner.cache.invalidate(meta.cache_key());
        }
        self.inner
            .stats_compactions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner
            .stats_bytes_compacted
            .fetch_add(bytes_written, std::sync::atomic::Ordering::Relaxed);
        Ok(1)
    }

    /// Choose a compaction run: the longest run of adjacent-by-global-id,
    /// size-compatible hot segments. Returns `None` when nothing is worth
    /// compacting. See [`compact`] for the id-positioning rationale.
    fn choose_run(&self, segments: &[SegmentMeta]) -> Option<Vec<SegmentMeta>> {
        let min_seg = self.inner.config.compact_min_segments.max(1);
        // With retention off, a run of one segment is pure churn (no dedupe, no
        // eviction) → require >= 2 so single segments are never rewritten. With
        // retention on, allow a single segment so expired rows can be dropped.
        let effective_min = if self.inner.config.retention_nanos.is_some() {
            min_seg
        } else {
            min_seg.max(2)
        };

        // A segment already at the record cap is "sealed": compacting it with
        // peers would just rewrite disjoint key ranges (no dedupe, no shrink),
        // so re-merging same-size cap segments forever is pure write
        // amplification. Excluding sealed segments bounds write-amp to
        // small→cap (~2-3×) and the live segment count to ≈ n / cap + tail.
        let cap = self.inner.config.max_segment_records.max(1);
        let mergeable = |m: &&SegmentMeta| m.tier == Tier::Hot && m.zone.count < cap;

        // Global recency order.
        let mut all: Vec<&SegmentMeta> = segments.iter().collect();
        all.sort_by_key(|m| m.id);
        // Maximal windows of consecutive mergeable segments. A cold OR sealed
        // segment breaks a window, so a run never straddles a non-run segment's
        // id — reused output ids stay correctly positioned for newest-wins.
        let mut windows: Vec<Vec<&SegmentMeta>> = Vec::new();
        let mut cur: Vec<&SegmentMeta> = Vec::new();
        for m in all {
            if mergeable(&m) {
                cur.push(m);
            } else if !cur.is_empty() {
                windows.push(std::mem::take(&mut cur));
            }
        }
        if !cur.is_empty() {
            windows.push(cur);
        }
        let hot_count: usize = windows.iter().map(|w| w.len()).sum();

        // Primary: longest size-tier run of >= effective_min within a window.
        let mut best: Option<Vec<&SegmentMeta>> = None;
        for w in &windows {
            if let Some(run) = longest_tier_run(w, effective_min) {
                if best.as_ref().is_none_or(|b| run.len() > b.len()) {
                    best = Some(run);
                }
            }
        }

        // Escalation: too many small segments and no size-tier run formed →
        // merge the longest adjacent (globally contiguous) window to make
        // progress, ignoring the size-tier constraint.
        if best.is_none() && hot_count > 4 * min_seg {
            if let Some(w) = windows
                .iter()
                .filter(|w| w.len() >= 2)
                .max_by_key(|w| w.len())
            {
                best = Some(w.clone());
            }
        }

        best.map(|run| run.into_iter().cloned().collect())
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

/// Segment filename for a (never-reused) sequence number.
fn seg_file(seq: u64) -> String {
    format!("seg-{seq:016}.gird")
}

/// Approximate on-disk footprint of one record, for byte-cap splitting. The
/// payload dominates; keys and the fixed per-row column overhead are added so a
/// stream of tiny-payload rows still splits by record count, not runaway bytes.
fn record_bytes(r: &Record) -> u64 {
    (r.payload.len() + r.key.len() + 32) as u64
}

/// Split a key-sorted record slice into consecutive chunks, each capped at
/// `max_records` records and `max_bytes` estimated bytes (whichever trips
/// first). Every chunk is a contiguous, disjoint key range, so the output
/// segments' key zone maps partition the run and `get`'s per-segment binary
/// search stays valid. A single over-cap record still occupies its own chunk
/// (records are never split).
fn split_chunks(records: &[Record], max_records: usize, max_bytes: u64) -> Vec<&[Record]> {
    let max_records = max_records.max(1);
    let mut chunks: Vec<&[Record]> = Vec::new();
    if records.is_empty() {
        return chunks;
    }
    let mut start = 0usize;
    let mut bytes = 0u64;
    for i in 0..records.len() {
        let rb = record_bytes(&records[i]);
        if i > start && ((i - start) >= max_records || bytes + rb > max_bytes) {
            chunks.push(&records[start..i]);
            start = i;
            bytes = 0;
        }
        bytes += rb;
    }
    chunks.push(&records[start..]);
    chunks
}

/// The longest contiguous sub-run of `window` (already global-id-sorted) whose
/// segments are within one size tier — the largest record count is < 4× the
/// smallest in the run — with length >= `min_len`. Returns `None` if no such
/// run exists.
///
/// `max/min` over a contiguous window is monotonic non-decreasing as the window
/// grows (max ↑, min ↓), so the longest valid run from each start is a prefix;
/// restarting at the break point therefore never skips a longer run.
fn longest_tier_run<'a>(
    window: &[&'a SegmentMeta],
    min_len: usize,
) -> Option<Vec<&'a SegmentMeta>> {
    let n = window.len();
    let size = |m: &SegmentMeta| m.zone.count.max(1) as u64;
    let mut best: Option<(usize, usize)> = None; // (start, len)
    let mut i = 0;
    while i < n {
        let mut j = i;
        let mut mn = u64::MAX;
        let mut mx = 0u64;
        while j < n {
            let s = size(window[j]);
            let nmn = mn.min(s);
            let nmx = mx.max(s);
            if j > i && nmx >= 4 * nmn {
                break; // adding window[j] would straddle two tiers
            }
            mn = nmn;
            mx = nmx;
            j += 1;
        }
        let len = j - i;
        if len >= min_len && best.is_none_or(|(_, bl)| len > bl) {
            best = Some((i, len));
        }
        i = j.max(i + 1);
    }
    best.map(|(s, l)| window[s..s + l].to_vec())
}
