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
    /// Targeted compaction (D-3 heal): rewrite every segment whose zone
    /// contains `key`, physically dropping that key from the outputs. See
    /// [`crate::Girder::purge_key`] for the soundness constraints.
    PurgeKey(String),
}

pub struct MaintenanceActor {
    pub inner: Arc<EngineInner>,
    /// Ticks seen so far — the blob-orphan sweep runs on every Nth
    /// (`GirderConfig::blob_sweep_every_n_ticks`), starting at tick 0 so a
    /// boot always sweeps kill residue promptly (D9, ruling D-7).
    pub ticks: std::sync::atomic::AtomicU64,
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
            MaintCall::PurgeKey(key) => self.purge_key(&key),
            MaintCall::Tick => {
                let flushed = self.flush_pending()?;
                self.compact()?;
                self.reclaim_sealed()?;
                self.tier()?;
                self.groom()?;
                self.migrate()?;
                // D9 (ruling D-7): the orphan sweep lists the whole blob dir
                // under the manifest read lock — every Nth tick is plenty
                // (orphans are rare kill residue). Tick 0 sweeps at boot.
                let tick = self
                    .ticks
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if tick.is_multiple_of(self.inner.config.blob_sweep_every_n_ticks.max(1) as u64) {
                    self.sweep_blob_orphans();
                }
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
        // Pick a run under the read lock, then release it for the merge I/O.
        let run: Vec<SegmentMeta> = {
            let manifest = self.inner.manifest.read().unwrap();
            match self.choose_run(&manifest.segments) {
                Some(run) => run,
                None => return Ok(0),
            }
        };
        self.compact_run(run, None)
    }

    /// The D-3 heal (`Girder::purge_key` documents the caller contract):
    /// rewrite exactly the segments whose zone key range CONTAINS `key`,
    /// dropping the key — record and tombstone alike — from the outputs, so
    /// its poison leaves the zone maps. Sound because every containing
    /// segment is in the run and the caller flushed the memtable first (no
    /// older version left anywhere to un-shadow). Refuses delta-flagged
    /// keys: a partial fold must never be materialized as a base. Kill-safe
    /// like any compaction: one atomic manifest swap, idempotent re-run.
    fn purge_key(&self, key: &str) -> Result<u64> {
        let run: Vec<SegmentMeta> = {
            let manifest = self.inner.manifest.read().unwrap();
            manifest
                .segments
                .iter()
                .filter(|m| m.zone.min_key.as_str() <= key && key <= m.zone.max_key.as_str())
                .cloned()
                .collect()
        };
        if run.is_empty() {
            return Ok(0); // nothing anywhere could hold the key
        }
        let hot_dir = &self.inner.config.hot_dir;
        let cold_dir = &self.inner.config.cold_dir;
        for meta in &run {
            let path = segment_path(hot_dir, cold_dir, meta);
            for record in segment::read_all_records(&path)? {
                if record.key == key && record.is_delta() {
                    return Err(crate::GirderError::Config(
                        "purge_key refuses counter (delta-flagged) keys".into(),
                    ));
                }
            }
        }
        self.compact_run(run, Some(key))
    }

    /// Merge `run` newest-wins into fresh size-capped segments and swap the
    /// manifest atomically — the shared body of `compact` (run chosen by
    /// policy, no purge) and `purge_key` (run = zone-containing segments,
    /// `purge` dropped from the output).
    fn compact_run(&self, run: Vec<SegmentMeta>, purge: Option<&str>) -> Result<u64> {
        let hot_dir = &self.inner.config.hot_dir;
        let cold_dir = &self.inner.config.cold_dir;
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
        if let Some(key) = purge {
            // The purge: physically drop the key — record AND tombstone —
            // so the rewritten zones no longer carry its range/time poison.
            merged.remove(key);
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
            self.inner.drop_segment_file(meta).ok();
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

    /// Sealed-segment reclamation (track F slice F3, rulings 8–12): the
    /// documented cost of the byte-cap seal is that overwritten rows inside
    /// sealed segments were reclaimed only by retention. This closes that
    /// hole with a dead-ratio-triggered SOLO rewrite — sealed segments never
    /// re-MERGE (that was the D1 unbounded-write-amp hole), they only ever
    /// shrink in place.
    ///
    /// Bounded by construction:
    ///
    /// - **One audit per tick**, chosen by a rotating in-memory cursor over
    ///   sealed hot segments (round-robin, not "most overlap", so an
    ///   ineligible segment can never pin the audit forever). An audit reads
    ///   KEY COLUMNS only — its own and newer overlapping segments' — never
    ///   payloads or text. Candidates skippable from manifest METADATA alone
    ///   (no newer overlapping segment exists) advance the rotation for free
    ///   — the tick's single audit budget is spent on the first candidate
    ///   that actually needs its key columns read, so reclaim latency scales
    ///   with auditable segments, not with the total sealed population.
    ///   Nothing is persisted (ruling 11): the audit recomputes honestly; a
    ///   restart just restarts the rotation.
    /// - A row is **dead** iff a strictly NEWER durable segment holds a
    ///   NON-delta record for its key AND no newer durable segment holds a
    ///   delta for it (a delta needs its base to fold — the same rule
    ///   count()/compaction honor; ruling 8). Memtable/frozen shadowing is
    ///   ignored (durable-only, ruling 10): it lands as a segment soon and
    ///   is seen then. A LIVE row is never dropped because of what sits
    ///   BELOW it — so a tombstone-convention record shadowing older
    ///   versions always survives until it is itself shadowed by a newer
    ///   version.
    /// - **The tombstone disjunct** (plan 0014 §1, ruling T1 — the one
    ///   deliberate relaxation of the floor above, for FIRST-CLASS
    ///   tombstones only): a `del` row is ALSO dead iff no newer durable
    ///   segment holds a delta for its key (a tombstone is the base that
    ///   terminates a delta chain — dropping it under a live chain would
    ///   change folds) AND no strictly-OLDER durable segment's key column
    ///   contains its key (delta rows are key-column rows, so an older
    ///   delta that would un-shadow also refuses). Evidence completeness
    ///   from durable segments alone holds by write order: memtable/frozen
    ///   content for a durably-tombstoned key is always NEWER. Older v1 (or
    ///   unreadable) evidence is conservative the other way: any zone-range
    ///   cover marks the key as possibly-below and the tombstone survives.
    ///   Once nothing shadows it and it shadows nothing, dropping it
    ///   changes no membership answer on any read path.
    /// - **Rewrite only when `dead * 2 >= rows`** (f = 1/2, ruling 9 — the
    ///   same half-the-cap style as `byte_seal`): each rewrite at least
    ///   halves the segment, so one segment sees at most log2(rows) solo
    ///   rewrites in its lifetime, and an overwrite-free corpus never trips
    ///   the ratio at all (`fat_record_compaction_converges` is untouched by
    ///   construction). A shrunken output below the seal simply rejoins
    ///   ordinary compaction.
    /// - The output takes the **same id slot** (groom's exact move):
    ///   newest-wins positioning preserved; filenames come from the fresh
    ///   never-reused seq. All-dead segments are dropped whole by manifest
    ///   swap. Rewrite bytes count into `stats_bytes_compacted` — write-amp
    ///   accounting stays honest.
    /// - **Hot v2 only** (ruling 12): cold partially-dead segments wait
    ///   (mirrors groom's documented cold deferral); v1-format segments wait
    ///   for `migrate()`. Newer v1 segments contribute no shadow evidence
    ///   (conservative: undercounting dead only delays reclaim).
    fn reclaim_sealed(&self) -> Result<u64> {
        use std::sync::atomic::Ordering;
        let cap = self.inner.config.max_segment_records.max(1);
        let byte_seal = (self.inner.config.max_segment_bytes / 2).max(1);

        // Pick this tick's audit target + the newer overlapping evidence set
        // under the read lock. "Sealed" is exactly choose_run's exclusion:
        // hot and at either output cap. Rotation-order candidates that have
        // NO newer overlapping segment are skipped from metadata alone (free
        // — no file touched); the tick's one audit goes to the first
        // candidate with actual evidence to read.
        // Does a zone say rows with `label` == "1" may exist in the segment?
        // (`Some(None)` = the value set overflowed: assume yes.)
        let zone_may_have = |m: &SegmentMeta, label: &str| -> bool {
            match m.zone.labels.get(label) {
                Some(Some(values)) => values.contains("1"),
                Some(None) => true,
                None => false,
            }
        };
        let Some((cand, newer, older)) = ({
            let manifest = self.inner.manifest.read().unwrap();
            let mut sealed: Vec<&SegmentMeta> = manifest
                .segments
                .iter()
                .filter(|m| m.tier == Tier::Hot && (m.zone.count >= cap || m.bytes >= byte_seal))
                .collect();
            sealed.sort_by_key(|m| m.id);
            let cursor = self.inner.reclaim_cursor.load(Ordering::Relaxed);
            // Rotation order: ids above the cursor first, then wrap.
            let (tail, head): (Vec<&&SegmentMeta>, Vec<&&SegmentMeta>) =
                sealed.iter().partition(|m| m.id > cursor);
            tail.into_iter().chain(head).find_map(|pick| {
                let newer: Vec<SegmentMeta> = manifest
                    .segments
                    .iter()
                    .filter(|m| m.id > pick.id)
                    .filter(|m| {
                        m.zone.min_key <= pick.zone.max_key && pick.zone.min_key <= m.zone.max_key
                    })
                    .cloned()
                    .collect();
                // Metadata-only skip: with no newer overlap, no LIVE row
                // here can be dead — but a first-class tombstone can (the
                // tombstone disjunct judges by what's below), so a zone
                // that may hold tombstones is still auditable.
                if newer.is_empty() && !zone_may_have(pick, crate::record::TOMBSTONE_LABEL) {
                    return None;
                }
                // Older overlapping evidence — the tombstone disjunct's
                // below-set. Empty when the zone rules tombstones out
                // (never read then).
                let older: Vec<SegmentMeta> = if zone_may_have(pick, crate::record::TOMBSTONE_LABEL)
                {
                    manifest
                        .segments
                        .iter()
                        .filter(|m| m.id < pick.id)
                        .filter(|m| {
                            m.zone.min_key <= pick.zone.max_key
                                && pick.zone.min_key <= m.zone.max_key
                        })
                        .cloned()
                        .collect()
                } else {
                    Vec::new()
                };
                Some(((*pick).clone(), newer, older))
            })
        }) else {
            return Ok(0);
        };
        // Rotate FIRST: audited either way, the next tick moves on.
        self.inner.reclaim_cursor.store(cand.id, Ordering::Relaxed);

        let hot_dir = &self.inner.config.hot_dir;
        let cold_dir = &self.inner.config.cold_dir;
        // Audit I/O is maintenance, not the query path — scratch counter.
        let scratch = std::sync::atomic::AtomicU64::new(0);

        // The candidate's key column (v2 only; v1 waits for migrate()).
        let cand_path = segment_path(hot_dir, cold_dir, &cand);
        let cand_file = std::fs::File::open(&cand_path)?;
        if segment::read_header(&cand_file, &scratch)? != 2 {
            return Ok(0);
        }
        let cand_dir = segment::read_footer(&cand_file, &scratch)?;
        let keys = segment::load_keys(&cand_file, &cand_dir, &scratch)?;
        let total = keys.count();
        if total == 0 {
            return Ok(0);
        }

        // Shadow evidence, one newer segment at a time (keys + the delta
        // label column only where the zone says deltas are possible).
        let mut shadowed = vec![false; total];
        let mut delta_over = vec![false; total];
        for meta in &newer {
            let path = segment_path(hot_dir, cold_dir, meta);
            let file = std::fs::File::open(&path)?;
            if segment::read_header(&file, &scratch)? != 2 {
                continue; // v1 evidence skipped (conservative)
            }
            let dir = segment::read_footer(&file, &scratch)?;
            let nkeys = segment::load_keys(&file, &dir, &scratch)?;
            let delta_col = if zone_may_have(meta, crate::record::DELTA_LABEL) {
                Some(segment::load_label(
                    &file,
                    &dir,
                    crate::record::DELTA_LABEL,
                    &scratch,
                )?)
            } else {
                None
            };
            let row_is_delta =
                |j: usize| -> bool { delta_col.as_ref().is_some_and(|col| label_is_one(col, j)) };
            for i in 0..total {
                if shadowed[i] && delta_over[i] {
                    continue; // both facts known; nothing can change
                }
                if let Some(j) = nkeys.find(keys.key_at(i)) {
                    if row_is_delta(j) {
                        delta_over[i] = true;
                    } else {
                        shadowed[i] = true;
                    }
                }
            }
        }

        // The tombstone disjunct (see the dead-rule doc above): a candidate
        // `del` row with no newer delta and NOTHING below is dead too. The
        // below-evidence reads older overlapping key columns lazily — only
        // segments some still-in-doubt tombstone's key zone-range lands in,
        // and only when the candidate zone says tombstones may exist at all.
        let mut tomb_dead = vec![false; total];
        if zone_may_have(&cand, crate::record::TOMBSTONE_LABEL) {
            let tomb_col = segment::load_label(
                &cand_file,
                &cand_dir,
                crate::record::TOMBSTONE_LABEL,
                &scratch,
            )?;
            let tomb: Vec<bool> = (0..total).map(|i| label_is_one(&tomb_col, i)).collect();
            if tomb.iter().any(|t| *t) {
                // below[i]: some strictly-older durable segment holds the
                // key. Records, deltas and older tombstones alike — all are
                // key-column rows, so one membership test refuses both the
                // un-shadow hazard and the delta-base hazard from below.
                let mut below = vec![false; total];
                for meta in &older {
                    let needs: Vec<usize> = (0..total)
                        .filter(|&i| tomb[i] && !below[i])
                        .filter(|&i| {
                            let k = keys.key_at(i);
                            meta.zone.min_key.as_str() <= k && k <= meta.zone.max_key.as_str()
                        })
                        .collect();
                    if needs.is_empty() {
                        continue; // zone-range pruned: no key column read
                    }
                    let path = segment_path(hot_dir, cold_dir, meta);
                    let file = std::fs::File::open(&path)?;
                    if segment::read_header(&file, &scratch)? != 2 {
                        // v1 evidence unreadable — conservative the SAFE
                        // way: a zone-range cover counts as possibly-below,
                        // the tombstone survives (undercounting dead only
                        // delays reclaim; the mirror of the newer-side v1
                        // rule).
                        for i in needs {
                            below[i] = true;
                        }
                        continue;
                    }
                    let odir = segment::read_footer(&file, &scratch)?;
                    let okeys = segment::load_keys(&file, &odir, &scratch)?;
                    for i in needs {
                        if okeys.find(keys.key_at(i)).is_some() {
                            below[i] = true;
                        }
                    }
                }
                for i in 0..total {
                    tomb_dead[i] = tomb[i] && !delta_over[i] && !below[i];
                }
            }
        }

        let dead: Vec<bool> = (0..total)
            .map(|i| (shadowed[i] && !delta_over[i]) || tomb_dead[i])
            .collect();
        let dead_count = dead.iter().filter(|d| **d).count();
        if dead_count * 2 < total {
            return Ok(0); // below f = 1/2 — not worth a rewrite yet
        }
        drop(cand_file);

        // Rewrite (or drop whole). The dead set is keyed, not row-indexed:
        // `read_all_records` row order is the key order, but keying makes the
        // retain independent of that invariant.
        let dead_keys: HashSet<&str> = (0..total)
            .filter(|&i| dead[i])
            .map(|i| keys.key_at(i))
            .collect();
        let survivors: Vec<Record> = segment::read_all_records(&cand_path)?
            .into_iter()
            .filter(|r| !dead_keys.contains(r.key.as_str()))
            .collect();
        if survivors.is_empty() {
            let mut manifest = self.inner.manifest.write().unwrap();
            manifest.segments.retain(|s| s.id != cand.id);
            self.inner.store_manifest(&manifest)?;
            drop(manifest);
            self.inner.drop_segment_file(&cand).ok();
            self.inner.cache.invalidate(cand.cache_key());
        } else {
            let seq = self.alloc_seq();
            let file = seg_file(seq);
            let out_path = hot_dir.join(&file);
            let zone = segment::write_segment_presorted(&out_path, &survivors)?;
            let bytes = std::fs::metadata(&out_path).map(|m| m.len()).unwrap_or(0);
            {
                let mut manifest = self.inner.manifest.write().unwrap();
                manifest.segments.retain(|s| s.id != cand.id);
                manifest.segments.push(SegmentMeta {
                    id: cand.id, // same recency slot: newest-wins order preserved
                    file,
                    tier: Tier::Hot,
                    zone,
                    bytes,
                    created_unix_nanos: cand.created_unix_nanos,
                });
                self.inner.store_manifest(&manifest)?;
            }
            std::fs::remove_file(&cand_path).ok();
            self.inner.cache.invalidate(cand.cache_key());
            self.inner
                .stats_bytes_compacted
                .fetch_add(bytes, Ordering::Relaxed);
        }
        self.inner.stats_reclaimed.fetch_add(1, Ordering::Relaxed);
        Ok(1)
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
                // Remote segments are modern (v2+) by construction and have no
                // local dir to rewrite into — never a legacy-migration candidate.
                Tier::Remote => continue,
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

    /// Age segments down the tiers: hot→cold past `hot_ttl`, then (if a remote
    /// store is injected) cold→remote past `remote_ttl` (SCALE-1). Both use the
    /// segment's own age; the mover only ever advances a segment ONE tier per
    /// pass, so `remote_ttl < hot_ttl` never skips the cold hop.
    fn tier(&self) -> Result<u64> {
        let now = now_nanos();
        let hot_cutoff = now - self.inner.config.hot_ttl_nanos;
        // Segments already COLD at the start of this tick are the only remote
        // candidates — a segment moved hot→cold below waits for the NEXT tick
        // to go remote, so the cold hop is never skipped (docs/SCALE.md §3.5).
        let already_cold: std::collections::HashSet<u64> = {
            let manifest = self.inner.manifest.read().unwrap();
            manifest
                .segments
                .iter()
                .filter(|s| s.tier == Tier::Cold)
                .map(|s| s.id)
                .collect()
        };
        let to_cold: Vec<SegmentMeta> = {
            let manifest = self.inner.manifest.read().unwrap();
            manifest
                .segments
                .iter()
                .filter(|s| s.tier == Tier::Hot && s.created_unix_nanos < hot_cutoff)
                .cloned()
                .collect()
        };
        let mut moved = 0;
        for meta in to_cold {
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
        moved += self.tier_to_remote(now, &already_cold)?;
        if moved > 0 {
            self.inner
                .stats_tiered
                .fetch_add(moved, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(moved)
    }

    /// Cold→remote pass (SCALE-1). No-op without an injected store. The move
    /// protocol is PUT → flip-manifest → delete-local, each step leaving the
    /// segment readable in AT LEAST one place:
    /// - crash after PUT, before flip: manifest still `Cold`, local file intact
    ///   — reads unaffected; next tick re-PUTs (idempotent) and proceeds.
    /// - crash after flip, before delete: manifest `Remote`, reads fetch from
    ///   the store; the leftover cold file is orphan residue reaped at open.
    ///
    /// At no step is the segment readable in fewer than one place — the same
    /// invariant the hot↔cold rename tolerance pins.
    fn tier_to_remote(
        &self,
        now: i64,
        already_cold: &std::collections::HashSet<u64>,
    ) -> Result<u64> {
        let Some(store) = self.inner.object_store.clone() else {
            return Ok(0);
        };
        let cutoff = now - self.inner.config.remote_ttl_nanos;
        let to_remote: Vec<SegmentMeta> = {
            let manifest = self.inner.manifest.read().unwrap();
            manifest
                .segments
                .iter()
                .filter(|s| {
                    s.tier == Tier::Cold
                        && already_cold.contains(&s.id)
                        && s.created_unix_nanos < cutoff
                })
                .cloned()
                .collect()
        };
        let mut moved = 0;
        for meta in to_remote {
            let local = self.inner.config.cold_dir.join(&meta.file);
            let bytes = std::fs::read(&local)?;
            // 1. PUT (idempotent — the key is the never-reused filename).
            store.put(&meta.file, bytes)?;
            // 2. Flip the manifest and persist BEFORE deleting the local file.
            {
                let mut manifest = self.inner.manifest.write().unwrap();
                if let Some(entry) = manifest.segments.iter_mut().find(|s| s.id == meta.id) {
                    entry.tier = Tier::Remote;
                }
                self.inner.store_manifest(&manifest)?;
            }
            // 3. Delete the local copy (best-effort; a leftover is reaped at open).
            std::fs::remove_file(&local).ok();
            moved += 1;
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

/// Is a label column's value at row `j` the flag "1"? (The delta and
/// tombstone labels share this encoding — reclaim's evidence reads.)
fn label_is_one(col: &segment::LabelColumn, j: usize) -> bool {
    match col {
        segment::LabelColumn::Dict { dict, codes, .. } => {
            let c = codes[j];
            c != 0 && dict[(c - 1) as usize] == "1"
        }
        segment::LabelColumn::Plain { values } => values[j].as_deref() == Some("1"),
    }
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
