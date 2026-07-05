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
    /// Counter increment: WAL-append the delta record, fold it into the
    /// memtable (`MemTable::insert_delta`) — one serialized step, so
    /// concurrent increments never lose an update.
    Incr(Record),
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
            WriterMsg::Incr(record) => {
                state
                    .wal
                    .append_batch(std::slice::from_ref(&record))
                    .map_err(|e| e.to_string())?;
                {
                    let mut memtable = self.inner.memtable.write().unwrap();
                    memtable.insert_delta(record);
                }
                self.inner.note_put();
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
                self.groom()?;
                self.migrate()?;
                self.sweep_blob_orphans();
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
                if record.is_delta() {
                    // Compaction collapse: fold the increment onto whatever
                    // this run has for the key (the same merge_delta oracle
                    // reads use). No base in the run → the fold STAYS
                    // delta-flagged; its base may live below the run and
                    // reads keep folding across segments.
                    let folded = crate::record::merge_delta(merged.get(&record.key), &record);
                    merged.insert(record.key.clone(), folded);
                } else {
                    merged.insert(record.key.clone(), record);
                }
            }
        }
        // 3. Retention: drop expired records during the rewrite — per-key,
        //    longest-prefix wins, via the single RetentionPolicy oracle the
        //    groomer shares.
        let policy = self.retention_policy();
        if !policy.is_empty() {
            let now = now_nanos();
            merged.retain(|key, r| match policy.cutoff_for_key(key, now) {
                Some(cutoff) => r.timestamp >= cutoff,
                None => true, // no matching row: keep forever
            });
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
        let effective_min = if self.inner.config.retention_nanos.is_some()
            || !self.inner.config.retention.is_empty()
        {
            min_seg
        } else {
            min_seg.max(2)
        };

        // A segment already at EITHER output cap is "sealed": compacting it
        // with peers would just rewrite disjoint key ranges (no dedupe, no
        // shrink), so re-merging cap-sized segments forever is pure write
        // amplification. Excluding sealed segments bounds write-amp to
        // small→cap (~2-3×) and the live segment count to ≈ n / cap + tail.
        //
        // BOTH caps matter: `split_chunks` splits outputs at whichever of
        // max_segment_records / max_segment_bytes trips FIRST, so with fat
        // records (rivet spans + their FTS text ≈ 3 KB) the byte cap seals
        // long before the record cap is reachable — a record-count-only
        // predicate then never seals anything and compaction re-merges the
        // whole hot set on every pass (unbounded write-amp; found by the D1
        // 10M soak: 1,135 segment writes for a 1M-record build). The byte
        // seal uses half the cap: any output at ≥ max_segment_bytes/2 can
        // only ever merge into ≤2-input rewrites — churn, not consolidation.
        let cap = self.inner.config.max_segment_records.max(1);
        let byte_seal = (self.inner.config.max_segment_bytes / 2).max(1);
        let mergeable =
            |m: &&SegmentMeta| m.tier == Tier::Hot && m.zone.count < cap && m.bytes < byte_seal;

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

    /// The compiled retention policy (config rows + the legacy global knob).
    fn retention_policy(&self) -> crate::retention::RetentionPolicy {
        crate::retention::RetentionPolicy::compile(
            &self.inner.config.retention,
            self.inner.config.retention_nanos,
        )
    }

    /// Tick-driven retention grooming — expiry is not hostage to write
    /// volume: with zero incoming writes, segments still age out.
    ///
    /// Two moves, both resolved through the SAME RetentionPolicy oracle
    /// compaction uses:
    ///
    /// - **Wholesale drop** (any tier): a segment is provably all-expired
    ///   from its zone map alone when (a) one policy row covers its whole
    ///   key range (a lexicographic interval whose endpoints share a prefix
    ///   consists entirely of keys sharing it — so every key has SOME row)
    ///   and (b) `max_ts` is older than the LARGEST TTL of any row
    ///   intersecting the range (so whichever longest-prefix row governs a
    ///   key, that key is expired). Removed by manifest swap; the file is
    ///   garbage.
    /// - **Rewrite** (hot tier): same coverage condition, but only `min_ts`
    ///   has passed the largest TTL — at least one record is provably
    ///   expired, so the exact per-key rewrite always makes progress (no
    ///   rewrite churn). Cold partially-expired segments wait for full
    ///   expiry (documented; rewriting cold in place is deferred).
    ///
    /// Segments whose key range no single row covers are groomed only by
    /// ordinary compaction (uncovered keys may be keep-forever).
    fn groom(&self) -> Result<u64> {
        let policy = self.retention_policy();
        if policy.is_empty() {
            return Ok(0);
        }
        let now = now_nanos();
        // Counter safety: a fold spans sources, but the groomer judges one
        // segment at a time — dropping an old BASE segment while a newer
        // delta rides elsewhere would silently regress the counter (and
        // dropping an expired delta while its base survives, likewise). So
        // any segment whose key range overlaps delta presence ANYWHERE
        // (its own zone label, another segment's, or the live memtables'
        // conservative delta range) is skipped: counter ranges are groomed
        // by compaction, which folds before it retains.
        let delta_ranges: Vec<(String, String)> = {
            let manifest = self.inner.manifest.read().unwrap();
            let mut ranges: Vec<(String, String)> = manifest
                .segments
                .iter()
                .filter(|m| match m.zone.labels.get(crate::record::DELTA_LABEL) {
                    Some(Some(values)) => values.contains("1"),
                    Some(None) => true,
                    None => false,
                })
                .map(|m| (m.zone.min_key.clone(), m.zone.max_key.clone()))
                .collect();
            if let Some(r) = self.inner.memtable.read().unwrap().delta_range() {
                ranges.push(r);
            }
            for (_, map) in self.inner.frozen.read().unwrap().iter() {
                if let Some(r) = map.delta_range() {
                    ranges.push(r);
                }
            }
            ranges
        };
        let overlaps_deltas = |min_key: &str, max_key: &str| {
            delta_ranges
                .iter()
                .any(|(lo, hi)| lo.as_str() <= max_key && min_key <= hi.as_str())
        };
        let candidates: Vec<SegmentMeta> = {
            let manifest = self.inner.manifest.read().unwrap();
            manifest
                .segments
                .iter()
                .filter(|m| !overlaps_deltas(&m.zone.min_key, &m.zone.max_key))
                .filter(|m| policy.covers_range(&m.zone.min_key, &m.zone.max_key))
                .filter(|m| {
                    let Some(max_ttl) = policy.max_ttl_in_range(&m.zone.min_key, &m.zone.max_key)
                    else {
                        return false;
                    };
                    let cutoff = now.saturating_sub(max_ttl);
                    // Fully expired, or (hot only) provably partially expired.
                    m.zone.max_ts < cutoff || (m.tier == Tier::Hot && m.zone.min_ts < cutoff)
                })
                .cloned()
                .collect()
        };
        let mut groomed = 0;
        for meta in candidates {
            let max_ttl = policy
                .max_ttl_in_range(&meta.zone.min_key, &meta.zone.max_key)
                .expect("candidate has intersecting rows");
            let all_expired = meta.zone.max_ts < now.saturating_sub(max_ttl);
            if all_expired {
                // Manifest swap first (source of truth), then the file is garbage.
                {
                    let mut manifest = self.inner.manifest.write().unwrap();
                    manifest.segments.retain(|s| s.id != meta.id);
                    self.inner.store_manifest(&manifest)?;
                }
                std::fs::remove_file(segment_path(
                    &self.inner.config.hot_dir,
                    &self.inner.config.cold_dir,
                    &meta,
                ))
                .ok();
                self.inner.cache.invalidate(meta.cache_key());
                groomed += 1;
                continue;
            }
            // Exact rewrite: fold deltas, per-key retain, same recency slot.
            let path = segment_path(
                &self.inner.config.hot_dir,
                &self.inner.config.cold_dir,
                &meta,
            );
            let mut merged: BTreeMap<String, Record> = BTreeMap::new();
            for record in segment::read_all_records(&path)? {
                if record.is_delta() {
                    let folded = crate::record::merge_delta(merged.get(&record.key), &record);
                    merged.insert(record.key.clone(), folded);
                } else {
                    merged.insert(record.key.clone(), record);
                }
            }
            merged.retain(|key, r| match policy.cutoff_for_key(key, now) {
                Some(cutoff) => r.timestamp >= cutoff,
                None => true,
            });
            if merged.len() == meta.zone.count {
                continue; // nothing actually expired (zone bound was loose)
            }
            if merged.is_empty() {
                let mut manifest = self.inner.manifest.write().unwrap();
                manifest.segments.retain(|s| s.id != meta.id);
                self.inner.store_manifest(&manifest)?;
                drop(manifest);
                std::fs::remove_file(&path).ok();
                self.inner.cache.invalidate(meta.cache_key());
                groomed += 1;
                continue;
            }
            let records: Vec<Record> = merged.into_values().collect();
            let seq = self.alloc_seq();
            let file = seg_file(seq);
            let out_path = self.inner.config.hot_dir.join(&file);
            let zone = segment::write_segment_presorted(&out_path, &records)?;
            let bytes = std::fs::metadata(&out_path).map(|m| m.len()).unwrap_or(0);
            {
                let mut manifest = self.inner.manifest.write().unwrap();
                manifest.segments.retain(|s| s.id != meta.id);
                manifest.segments.push(SegmentMeta {
                    id: meta.id, // same recency slot: newest-wins order preserved
                    file,
                    tier: Tier::Hot,
                    zone,
                    bytes,
                    created_unix_nanos: meta.created_unix_nanos,
                });
                self.inner.store_manifest(&manifest)?;
            }
            std::fs::remove_file(&path).ok();
            self.inner.cache.invalidate(meta.cache_key());
            groomed += 1;
        }
        if groomed > 0 {
            self.inner
                .stats_groomed
                .fetch_add(groomed, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(groomed)
    }

    /// Delete blob files not listed in the manifest (kill residue from a
    /// crash between file-rename and manifest-store). Holding the manifest
    /// lock for the whole sweep excludes `put_blob` (which holds the WRITE
    /// lock across its file-write + listing), so a mid-put blob can never
    /// be swept; the sweep itself only reads, so a read lock suffices.
    fn sweep_blob_orphans(&self) {
        let manifest = self.inner.manifest.read().unwrap();
        for hash in crate::blob::on_disk_hashes(&self.inner.config.hot_dir) {
            if !manifest.blobs.contains(&hash) {
                std::fs::remove_file(crate::blob::blob_path(&self.inner.config.hot_dir, &hash))
                    .ok();
            }
        }
    }

    /// Background format migration (docs/COMPAT.md): rewrite at most ONE
    /// under-versioned segment per tick to the current format — bounded work,
    /// restart-safe by construction (the rewrite is tmp→fsync→rename + an
    /// atomic manifest swap; a kill at any point leaves either the old
    /// segment manifest-listed — retried next tick — or the new one, done).
    /// This formalizes the previously-opportunistic v1→v2 path: with ticks
    /// running, every legacy segment converges to current without waiting
    /// for a compaction to happen to touch it. Reads never depend on
    /// migration (v1 stays readable forever); this is hygiene, not repair.
    fn migrate(&self) -> Result<u64> {
        let candidates: Vec<SegmentMeta> = {
            let manifest = self.inner.manifest.read().unwrap();
            let mut metas: Vec<SegmentMeta> = manifest.segments.to_vec();
            metas.sort_by_key(|m| m.id); // oldest first, deterministic
            metas
        };
        for meta in candidates {
            let path = segment_path(
                &self.inner.config.hot_dir,
                &self.inner.config.cold_dir,
                &meta,
            );
            let version = match segment::file_version(&path) {
                Ok(v) => v,
                // A racing compaction/groom may have deleted it — skip;
                // real corruption resurfaces on the read path.
                Err(_) => continue,
            };
            if version >= segment::CURRENT_SEGMENT_VERSION {
                continue;
            }
            // Rewrite in place: same id (recency slot), same tier, same
            // created stamp; fresh filename seq (never clobbers).
            let records = segment::read_all_records(&path)?;
            let seq = self.alloc_seq();
            let file = seg_file(seq);
            let dir = match meta.tier {
                Tier::Hot => &self.inner.config.hot_dir,
                Tier::Cold => &self.inner.config.cold_dir,
            };
            let out_path = dir.join(&file);
            let zone = segment::write_segment_presorted(&out_path, &records)?;
            let bytes = std::fs::metadata(&out_path).map(|m| m.len()).unwrap_or(0);
            {
                let mut manifest = self.inner.manifest.write().unwrap();
                manifest.segments.retain(|s| s.id != meta.id);
                manifest.segments.push(SegmentMeta {
                    id: meta.id,
                    file,
                    tier: meta.tier,
                    zone,
                    bytes,
                    created_unix_nanos: meta.created_unix_nanos,
                });
                self.inner.store_manifest(&manifest)?;
            }
            std::fs::remove_file(&path).ok();
            self.inner.cache.invalidate(meta.cache_key());
            self.inner
                .stats_migrated
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(1); // one per tick: bounded background work
        }
        Ok(0)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::Manifest;
    use crate::record::Record;
    use crate::segment::ZoneMap;
    use crate::{FsyncPolicy, Girder, GirderConfig, QuerySpec};
    use std::time::Duration;

    fn rec(key: &str, ts: i64) -> Record {
        Record {
            key: key.to_string(),
            timestamp: ts,
            labels: BTreeMap::from([("model".to_string(), "m".to_string())]),
            numerics: BTreeMap::from([("latency_ms".to_string(), ts as f64)]),
            payload: format!("p-{key}").into_bytes(),
            text: None,
        }
    }

    /// A legacy v1 segment: `[magic][ver=1][crc][rmp(Vec<Record>)]`. The
    /// magic literal pins the on-disk constant (segment.rs `MAGIC`).
    fn write_v1_segment(path: &std::path::Path, records: &[Record]) {
        let body = rmp_serde::to_vec(&records.to_vec()).unwrap();
        let mut out = Vec::new();
        out.extend_from_slice(&0x6769_7264u32.to_le_bytes()); // "gird"
        out.extend_from_slice(&1u32.to_le_bytes()); // VERSION_V1
        out.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
        out.extend_from_slice(&body);
        std::fs::write(path, out).unwrap();
    }

    /// Fabricate a COMPLETE pre-B3 store: three v1 segments listed by a v0
    /// (magic-less, bare-rmp) manifest — then prove the background migration
    /// converges to the current format across restarts, tolerates the exact
    /// on-disk state a kill-mid-rewrite leaves (a written output file NOT yet
    /// manifest-listed: an orphan, ignored — the manifest is truth), and
    /// never changes what reads return.
    ///
    /// The kill state is enacted BY CONSTRUCTION, not by racing a dropped
    /// engine's background actors against a reopened one (two live engines
    /// on one dir have two in-memory manifests — that is not a kill, it is
    /// corruption by test bug). Per the B2 lesson, the open-time tick can
    /// fire at any point: all assertions are monotone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn migration_converges_and_survives_kill() {
        let dir = tempfile::tempdir().unwrap();
        let hot = dir.path();

        let mut all: Vec<Record> = Vec::new();
        let mut manifest = Manifest {
            next_segment_id: 10,
            segments: Vec::new(),
            blobs: Default::default(),
        };
        for seg in 0..3u64 {
            let records: Vec<Record> = (0..20)
                .map(|i| rec(&format!("s/{seg}/{i:03}"), (seg * 100 + i) as i64))
                .collect();
            let file = format!("seg-{seg:016}.gird");
            write_v1_segment(&hot.join(&file), &records);
            manifest.segments.push(SegmentMeta {
                id: seg,
                file,
                tier: Tier::Hot,
                zone: ZoneMap::build(&records),
                bytes: 0,
                created_unix_nanos: 1,
            });
            all.extend(records);
        }
        // v0 manifest: bare rmp, no magic word.
        std::fs::write(hot.join("MANIFEST"), rmp_serde::to_vec(&manifest).unwrap()).unwrap();
        // The kill-mid-rewrite residue: a stray output file a crash between
        // write and manifest-swap would leave. Not manifest-listed = garbage.
        write_v1_segment(&hot.join("seg-0000000000000099.gird"), &all[0..2]);

        let mut cfg = GirderConfig::at(hot);
        cfg.fsync = FsyncPolicy::EveryN(64);
        cfg.compact_min_segments = 100; // migration only, no compaction
        cfg.hot_ttl_nanos = i64::MAX / 2; // no tiering
        cfg.tick_interval = Duration::from_secs(3600);

        let expected_keys = {
            let mut k: Vec<String> = all.iter().map(|r| r.key.clone()).collect();
            k.sort();
            k
        };
        let scan_keys = |records: Vec<Record>| {
            let mut k: Vec<String> = records.into_iter().map(|r| r.key).collect();
            k.sort();
            k
        };
        // Versions of the MANIFEST-LISTED segments (orphans are garbage and
        // deliberately not counted).
        let listed_versions = |hot: &std::path::Path| -> Vec<u32> {
            let manifest = Manifest::load(&hot.join("MANIFEST")).unwrap();
            let mut v: Vec<u32> = manifest
                .segments
                .iter()
                .map(|m| crate::segment::file_version(&hot.join(&m.file)).unwrap())
                .collect();
            v.sort_unstable();
            v
        };

        // Phase 1: open the legacy store (v0 manifest + orphan present),
        // verify reads, migrate at least one segment, quiesce, stop.
        {
            let engine = Girder::open(cfg.clone()).await.unwrap();
            assert_eq!(
                scan_keys(engine.scan(&QuerySpec::default()).await.unwrap()),
                expected_keys,
                "v1 store readable before any migration; orphan ignored"
            );
            engine.maintain().await.unwrap(); // >= 1 migrated (open-tick may add)
            assert_eq!(
                scan_keys(engine.scan(&QuerySpec::default()).await.unwrap()),
                expected_keys,
                "reads unchanged mid-migration"
            );
            engine.close().await.unwrap();
        }
        let mid = listed_versions(hot);
        assert!(mid.contains(&2), "progress before restart: {mid:?}");
        assert_eq!(mid.len(), 3, "no segment lost: {mid:?}");

        // Phase 2: restart mid-migration (mixed v1+v2 manifest, now in the
        // current manifest format); ticks converge the rest.
        {
            let engine = Girder::open(cfg.clone()).await.unwrap();
            assert_eq!(
                scan_keys(engine.scan(&QuerySpec::default()).await.unwrap()),
                expected_keys,
                "mixed-version store readable after restart"
            );
            for _ in 0..4 {
                engine.maintain().await.unwrap();
            }
            assert_eq!(engine.stats().compactions, 0, "migration, not compaction");
            assert_eq!(
                scan_keys(engine.scan(&QuerySpec::default()).await.unwrap()),
                expected_keys,
                "reads unchanged after full migration"
            );
            let got = engine.get("s/1/005").await.unwrap().unwrap();
            assert_eq!(got.payload, b"p-s/1/005");
            engine.close().await.unwrap();
        }
        assert_eq!(
            listed_versions(hot),
            vec![2, 2, 2],
            "every listed segment converged to the current format"
        );

        // Phase 3: a fresh open finds nothing left to migrate (idempotent).
        let engine = Girder::open(cfg).await.unwrap();
        engine.maintain().await.unwrap();
        assert_eq!(engine.stats().migrated_segments, 0, "nothing left to do");
        assert_eq!(
            scan_keys(engine.scan(&QuerySpec::default()).await.unwrap()),
            expected_keys
        );
        engine.close().await.unwrap();
    }
}
