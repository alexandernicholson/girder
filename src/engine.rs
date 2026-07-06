//! The engine facade: open (with recovery), put, scan, get, flush, stats.
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use rebar_core::gen_server::{spawn_gen_server, GenServerRef};
use rebar_core::runtime::Runtime;

use crate::actors::{MaintCall, MaintenanceActor, WriterActor, WriterMsg};
use crate::cache::SegmentCache;
use crate::error::{GirderError, Result};
use crate::manifest::{segment_path, Manifest, SegmentMeta, Tier};
use crate::memtable::MemTable;
use crate::record::{OrderBy, QuerySpec, Record};
use crate::segment::{
    self, BlockMeta, KeysSection, LabelColumn, NumericColumn, PayloadIndex, Section, SectionId,
    SegDir, SegmentColumns, TextIndex,
};
use crate::wal::{FsyncPolicy, Wal};

#[derive(Debug, Clone)]
pub struct GirderConfig {
    /// Hot tier directory (fast disk). Also holds WAL + manifest.
    pub hot_dir: PathBuf,
    /// Cold tier directory (cheap disk).
    pub cold_dir: PathBuf,
    pub fsync: FsyncPolicy,
    /// Freeze the memtable at this many records.
    pub memtable_max_records: usize,
    /// Decoded-segment LRU capacity.
    pub cache_bytes: u64,
    /// Compact when at least this many hot segments exist.
    pub compact_min_segments: usize,
    /// Cap on records per output segment produced by compaction; the merged
    /// key-sorted stream is split into consecutive segments at this boundary so
    /// no single segment grows unbounded (WS3 size-capped tiered compaction).
    pub max_segment_records: usize,
    /// Cap on the estimated payload+key bytes per compaction output segment
    /// (splits the merged stream whichever cap trips first).
    pub max_segment_bytes: u64,
    /// Age (nanos) after which segments move to the cold tier.
    pub hot_ttl_nanos: i64,
    /// Drop records older than this at compaction (None = keep forever).
    /// Legacy global knob — folds into `retention` as the `""` (match-all)
    /// row; an explicit `""` row in `retention` overrides it.
    pub retention_nanos: Option<i64>,
    /// Per-key-prefix retention: `(prefix, ttl_nanos)` rows, policy-as-data.
    /// Longest matching prefix governs a key; keys matching no row are kept
    /// forever. Enforced exactly at compaction and proactively by the
    /// tick-driven groomer (`docs/GUARANTEES.md` §Retention).
    pub retention: Vec<(String, i64)>,
    /// Background maintenance cadence.
    pub tick_interval: Duration,
    /// Run the blob-orphan sweep only on every Nth maintenance tick (D9):
    /// the sweep lists the whole blob directory under the manifest read
    /// lock, and orphans are rare kill residue — every tick was busywork.
    /// Tick 0 (boot) always sweeps. 0 is clamped to 1 (sweep every tick).
    pub blob_sweep_every_n_ticks: usize,
}

impl GirderConfig {
    pub fn at(dir: impl Into<PathBuf>) -> Self {
        let hot: PathBuf = dir.into();
        GirderConfig {
            cold_dir: hot.join("cold"),
            hot_dir: hot,
            fsync: FsyncPolicy::EveryN(64),
            memtable_max_records: 10_000,
            cache_bytes: 256 * 1024 * 1024,
            compact_min_segments: 8,
            max_segment_records: 128 * 1024,
            max_segment_bytes: 256 * 1024 * 1024,
            hot_ttl_nanos: 24 * 3600 * 1_000_000_000,
            retention_nanos: None,
            retention: Vec::new(),
            tick_interval: Duration::from_secs(5),
            blob_sweep_every_n_ticks: 12,
        }
    }
}

/// A frozen (immutable, flush-pending) memtable and the WAL seq covering it.
pub type FrozenMemtable = (u64, Arc<MemTable>);

/// Shared engine internals (actors + facade all hold this).
pub struct EngineInner {
    pub config: GirderConfig,
    pub memtable: RwLock<MemTable>,
    /// Frozen memtables awaiting flush.
    pub frozen: RwLock<Vec<FrozenMemtable>>,
    pub manifest: RwLock<Manifest>,
    pub cache: SegmentCache,
    pub initial_wal_seq: u64,
    manifest_path: PathBuf,
    pub stats_puts: AtomicU64,
    pub stats_flushes: AtomicU64,
    pub stats_compactions: AtomicU64,
    pub stats_groomed: AtomicU64,
    /// Sealed segments rewritten (or dropped whole) by the dead-ratio
    /// reclaimer (track F slice F3).
    pub stats_reclaimed: AtomicU64,
    /// Rotating cursor of the seal-reclaim audit (last audited segment id).
    /// In-memory only — a restart just restarts the rotation (ruling 11:
    /// nothing persisted; the audit recomputes honestly).
    pub reclaim_cursor: AtomicU64,
    pub stats_migrated: AtomicU64,
    pub stats_tiered: AtomicU64,
    /// Total bytes written by flushes (denominator for write amplification).
    pub stats_bytes_flushed: AtomicU64,
    /// Total bytes written by compaction outputs (numerator for write amp).
    pub stats_bytes_compacted: AtomicU64,
    /// Segment-level cache hits/misses (one per segment per query — the
    /// historical zone-map-test semantics). Sourced here, not in the cache,
    /// because a segment is now assembled from several cached sections.
    pub stats_cache_hits: AtomicU64,
    pub stats_cache_misses: AtomicU64,
    /// Total bytes read from segment files (footer + column sections + per-row
    /// payload slices). Observable per-query I/O (WS4).
    pub stats_bytes_read: AtomicU64,
}

impl EngineInner {
    pub fn wal_path(&self, seq: u64) -> PathBuf {
        self.config.hot_dir.join(format!("wal-{seq:016}.log"))
    }
    pub fn store_manifest(&self, manifest: &Manifest) -> Result<()> {
        manifest.store(&self.manifest_path)
    }
    pub fn note_put(&self) {
        self.stats_puts.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub puts: u64,
    pub memtable_records: usize,
    pub frozen_memtables: usize,
    pub hot_segments: usize,
    pub cold_segments: usize,
    pub total_records_in_segments: usize,
    pub flushes: u64,
    pub compactions: u64,
    /// Segments removed or rewritten by the retention groomer.
    pub groomed_segments: u64,
    /// Sealed segments rewritten (or dropped whole) by the dead-ratio
    /// reclaimer — overwritten rows evicted without re-merging sealed
    /// segments (write-amp stays bounded; see `actors::reclaim_sealed`).
    pub reclaimed_segments: u64,
    /// Legacy-format segments rewritten to the current format.
    pub migrated_segments: u64,
    /// Content-addressed blobs currently stored.
    pub blobs: usize,
    pub tiered: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    /// Cumulative bytes written by flushes.
    pub bytes_flushed: u64,
    /// Cumulative bytes written by compaction outputs. Write amplification is
    /// `bytes_compacted / bytes_flushed`.
    pub bytes_compacted: u64,
    /// Cumulative bytes read from segment files on the query path — footer,
    /// column sections, and per-row payload slices (WS4). A cold `selective`
    /// query stays bounded (tens of MB) instead of faulting in every payload.
    pub bytes_read: u64,
}

pub struct Girder {
    inner: Arc<EngineInner>,
    writer: GenServerRef<WriterActor>,
    maintenance: Mutex<GenServerRef<MaintenanceActor>>,
    _runtime: Arc<Runtime>,
    _ticker: tokio::task::JoinHandle<()>,
}

const CALL_TIMEOUT: Duration = Duration::from_secs(30);

impl Girder {
    /// Open (or create) an engine at `config.hot_dir`, recovering any WAL
    /// tail from a previous crash into a fresh segment.
    pub async fn open(config: GirderConfig) -> Result<Girder> {
        std::fs::create_dir_all(&config.hot_dir)?;
        std::fs::create_dir_all(&config.cold_dir)?;
        let manifest_path = config.hot_dir.join("MANIFEST");
        let manifest = Manifest::load(&manifest_path)?;

        // Recover: replay every leftover WAL (ascending seq) into the
        // memtable-to-be; they cover records that never reached a segment.
        let mut wal_seqs: Vec<u64> = std::fs::read_dir(&config.hot_dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                name.strip_prefix("wal-")
                    .and_then(|s| s.strip_suffix(".log"))
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .collect();
        wal_seqs.sort_unstable();
        let mut recovered: BTreeMap<String, Record> = BTreeMap::new();
        for seq in &wal_seqs {
            for record in Wal::replay(&config.hot_dir.join(format!("wal-{seq:016}.log")))? {
                if record.is_delta() {
                    // Same fold oracle as the live write path (merge_delta), so
                    // replay reconstructs exactly what the memtable held. The
                    // result may STAY delta-flagged (base in a segment) — the
                    // checkpoint segment then carries it and reads keep folding.
                    let folded = crate::record::merge_delta(recovered.get(&record.key), &record);
                    recovered.insert(record.key.clone(), folded);
                } else {
                    recovered.insert(record.key.clone(), record);
                }
            }
        }
        let next_wal_seq = wal_seqs.last().map(|s| s + 1).unwrap_or(0);

        let inner = Arc::new(EngineInner {
            memtable: RwLock::new(MemTable::default()),
            frozen: RwLock::new(Vec::new()),
            manifest: RwLock::new(manifest),
            cache: SegmentCache::new(config.cache_bytes),
            initial_wal_seq: next_wal_seq,
            manifest_path,
            stats_puts: AtomicU64::new(0),
            stats_flushes: AtomicU64::new(0),
            stats_compactions: AtomicU64::new(0),
            stats_groomed: AtomicU64::new(0),
            stats_reclaimed: AtomicU64::new(0),
            reclaim_cursor: AtomicU64::new(0),
            stats_migrated: AtomicU64::new(0),
            stats_tiered: AtomicU64::new(0),
            stats_bytes_flushed: AtomicU64::new(0),
            stats_bytes_compacted: AtomicU64::new(0),
            stats_cache_hits: AtomicU64::new(0),
            stats_cache_misses: AtomicU64::new(0),
            stats_bytes_read: AtomicU64::new(0),
            config,
        });

        let runtime = Arc::new(Runtime::new(1));
        let writer = spawn_gen_server(
            Arc::clone(&runtime),
            WriterActor {
                inner: Arc::clone(&inner),
            },
        )
        .await;
        let maintenance = spawn_gen_server(
            Arc::clone(&runtime),
            MaintenanceActor {
                inner: Arc::clone(&inner),
                ticks: Default::default(),
            },
        )
        .await;

        // Periodic maintenance tick (cast — lossy is fine, next tick retries).
        // The timer starts one full interval AFTER open (`interval_at`), not
        // immediately: the D9 tick counter's semantics ("tick 0 sweeps") must
        // be deterministic for explicit `maintain()` callers — an immediate
        // timer tick would race them for tick 0. Boot's first sweep therefore
        // lands at +tick_interval (or the first explicit maintain).
        let tick_ref = maintenance.clone();
        let interval = inner.config.tick_interval;
        let ticker = tokio::spawn(async move {
            let mut timer =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                timer.tick().await;
                if tick_ref.cast(MaintCall::Tick).is_err() {
                    break; // engine shut down (or actor dead → engine respawns)
                }
            }
        });

        let engine = Girder {
            inner,
            writer,
            maintenance: Mutex::new(maintenance),
            _runtime: runtime,
            _ticker: ticker,
        };

        // Finish recovery: write the replayed records straight to a segment
        // so old WALs can be deleted (clean state, no double-accounting).
        if !recovered.is_empty() {
            tracing::info!(records = recovered.len(), "girder: recovering WAL tail");
            engine.put_batch(recovered.into_values().collect()).await?;
            engine.flush().await?;
            for seq in wal_seqs {
                std::fs::remove_file(engine.inner.wal_path(seq)).ok();
            }
        }
        Ok(engine)
    }

    /// Durable write: acks after the WAL append. Triggers freeze+flush when
    /// the memtable is full.
    ///
    /// Every write is a per-key last-write-wins **upsert** (by write order,
    /// not timestamp), and the batch becomes visible atomically in-process —
    /// the public guarantee documented in `docs/GUARANTEES.md` and pinned by
    /// `tests/upsert_guarantee.rs`.
    pub async fn put_batch(&self, records: Vec<Record>) -> Result<()> {
        let frozen = self
            .writer
            .call(WriterMsg::Append(records), CALL_TIMEOUT)
            .await
            .map_err(|_| GirderError::ShutDown)?
            .map_err(GirderError::Encode)?;
        if frozen.is_some() {
            self.kick_maintenance(MaintCall::FlushPending);
        }
        Ok(())
    }

    pub async fn put(&self, record: Record) -> Result<()> {
        self.put_batch(vec![record]).await
    }

    /// Delete `key`: write the canonical tombstone (`docs/GUARANTEES.md`
    /// §Deletes). The key vanishes from `get`/`scan`/`count` while the
    /// tombstone keeps shadowing every older version through the same
    /// newest-wins walk as any write. Durable like a put; reclaimed at
    /// compaction/retention like any record.
    ///
    /// `timestamp` must be ≥ the timestamp of every version it shadows —
    /// use the delete time. Retention judges a key by its winning version's
    /// own timestamp, so a back-dated tombstone can expire (and be groomed)
    /// before the data it shadows, resurrecting it.
    ///
    /// Deleting a key with pending counter deltas resets the counter:
    /// increments newer than the tombstone re-create the row from zero
    /// (the delta-chain-with-no-base rule, one fold oracle).
    pub async fn delete(&self, key: impl Into<String>, timestamp: i64) -> Result<()> {
        self.put(Record::tombstone(key, timestamp)).await
    }

    /// Atomic counter increment: add `deltas` onto `key`'s numerics (creating
    /// the record if absent), serialized through the single writer — two
    /// concurrent `incr`s can never lose an update (unlike a read-modify-write
    /// `get`+`put`). Durable like any write: the delta is WAL-appended before
    /// the ack, and folded via the single `merge_delta` oracle shared by the
    /// memtable, reads, compaction and WAL replay.
    ///
    /// Returns the post-increment numerics. The returned snapshot is read
    /// after the ack and may already include later concurrent increments —
    /// benign for monotone counters (never LESS than this call's own
    /// contribution).
    ///
    /// Ordinary `put`s keep last-write-wins: a full record REPLACES any
    /// accumulated value (`docs/GUARANTEES.md` holds unchanged).
    pub async fn incr(
        &self,
        key: &str,
        timestamp: i64,
        deltas: BTreeMap<String, f64>,
    ) -> Result<BTreeMap<String, f64>> {
        let mut record = Record {
            key: key.to_string(),
            timestamp,
            labels: BTreeMap::new(),
            numerics: deltas,
            payload: Vec::new(),
            text: None,
        };
        record.set_delta();
        let frozen = self
            .writer
            .call(WriterMsg::Incr(record), CALL_TIMEOUT)
            .await
            .map_err(|_| GirderError::ShutDown)?
            .map_err(GirderError::Encode)?;
        if frozen.is_some() {
            self.kick_maintenance(MaintCall::FlushPending);
        }
        Ok(self.get(key).await?.map(|r| r.numerics).unwrap_or_default())
    }

    /// Store a content-addressed blob; returns its sha256-hex id. Content
    /// lives OUTSIDE the WAL as one file per hash (the hash is the
    /// integrity check); existence is manifest-tracked. Idempotent — same
    /// content, same id, one file (dedup by construction). Deletion is
    /// EXPLICIT only ([`Girder::delete_blob`]): dedup means shared
    /// referents, and only the embedder knows references.
    pub async fn put_blob(&self, bytes: &[u8]) -> Result<String> {
        let hash = crate::blob::sha256_hex(bytes);
        // File write + manifest add under the manifest write lock: the
        // groomer's orphan sweep runs under the same lock, so a half-added
        // blob can never be swept mid-put.
        let mut manifest = self.inner.manifest.write().unwrap();
        crate::blob::write_blob_file(&self.inner.config.hot_dir, &hash, bytes)?;
        if manifest.blobs.insert(hash.clone()) {
            self.inner.store_manifest(&manifest)?;
        }
        Ok(hash)
    }

    /// Fetch a blob by id. `None` = not stored (per the manifest — an
    /// unlisted on-disk file is garbage, not data). A listed blob whose file
    /// is missing or whose content no longer matches its hash is LOUD
    /// corruption, never `None` and never garbage bytes.
    pub async fn get_blob(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        if !crate::blob::valid_hash(hash) {
            return Ok(None);
        }
        {
            let manifest = self.inner.manifest.read().unwrap();
            if !manifest.blobs.contains(hash) {
                return Ok(None);
            }
        }
        match crate::blob::read_blob_file(&self.inner.config.hot_dir, hash) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(GirderError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(GirderError::Corrupt {
                    what: "blob",
                    detail: format!("manifest lists {hash} but the file is missing"),
                })
            }
            Err(e) => Err(e),
        }
    }

    /// Remove a blob (idempotent; returns whether it existed). Manifest
    /// first — the file is garbage the moment the listing is durable, same
    /// order as segment deletion.
    pub async fn delete_blob(&self, hash: &str) -> Result<bool> {
        let existed = {
            let mut manifest = self.inner.manifest.write().unwrap();
            let existed = manifest.blobs.remove(hash);
            if existed {
                self.inner.store_manifest(&manifest)?;
            }
            existed
        };
        if existed {
            std::fs::remove_file(crate::blob::blob_path(&self.inner.config.hot_dir, hash)).ok();
        }
        Ok(existed)
    }

    /// Freeze + flush everything to segments (durable checkpoint).
    pub async fn flush(&self) -> Result<()> {
        self.writer
            .call(WriterMsg::Freeze, CALL_TIMEOUT)
            .await
            .map_err(|_| GirderError::ShutDown)?
            .map_err(GirderError::Encode)?;
        let maintenance = self.maintenance.lock().unwrap().clone();
        maintenance
            .call(MaintCall::FlushPending, CALL_TIMEOUT)
            .await
            .map_err(|_| GirderError::ShutDown)?
            .map_err(GirderError::Encode)?;
        Ok(())
    }

    /// Physically remove `key` from the store — record, tombstone AND its
    /// zone-map footprint (D-3 heal). Flushes first so no version hides in
    /// the memtable, then rewrites exactly the segments whose zones contain
    /// the key (one atomic manifest swap; idempotent; kill-safe). Refuses
    /// counter (delta-flagged) keys — a partial fold must never be
    /// materialized as a base. Built for META keys that leaked into a
    /// zone-mapped data keyspace and poisoned its pruning (a key range or
    /// wall-clock timestamp spanning everything); see docs/COMPAT.md —
    /// "never mix meta keys into a zone-mapped data keyspace".
    pub async fn purge_key(&self, key: impl Into<String>) -> Result<()> {
        self.flush().await?;
        let maintenance = self.maintenance.lock().unwrap().clone();
        maintenance
            .call(MaintCall::PurgeKey(key.into()), CALL_TIMEOUT)
            .await
            .map_err(|_| GirderError::ShutDown)?
            .map_err(GirderError::Encode)?;
        Ok(())
    }

    /// Run one compaction/tiering pass now (tests, ops).
    pub async fn maintain(&self) -> Result<()> {
        let maintenance = self.maintenance.lock().unwrap().clone();
        maintenance
            .call(MaintCall::Tick, CALL_TIMEOUT)
            .await
            .map_err(|_| GirderError::ShutDown)?
            .map_err(GirderError::Encode)?;
        Ok(())
    }

    fn kick_maintenance(&self, msg: MaintCall) {
        let guard = self.maintenance.lock().unwrap();
        if guard.cast(msg).is_err() {
            tracing::warn!("girder maintenance actor dead; work deferred to next open");
        }
    }

    /// Point lookup (newest wins across memtable → frozen → segments).
    pub async fn get(&self, key: &str) -> Result<Option<Record>> {
        self.retry_vanished(|| self.get_once(key))
    }

    /// Bounded retry for reads that race a compaction's file deletion.
    ///
    /// Compaction removes its input files only AFTER the replacement manifest
    /// is durably stored, so `Io(NotFound)` from a read means the manifest
    /// snapshot the read was holding went stale mid-flight — a fresh snapshot
    /// (the next attempt re-reads the manifest) cannot reference the deleted
    /// file. NotFound that persists across retries means a file is missing
    /// while still manifest-listed: real corruption, surfaced honestly.
    fn retry_vanished<T>(&self, op: impl Fn() -> Result<T>) -> Result<T> {
        const VANISHED_SEGMENT_RETRIES: usize = 4;
        let mut attempt = 0;
        loop {
            match op() {
                Err(GirderError::Io(e))
                    if e.kind() == std::io::ErrorKind::NotFound
                        && attempt < VANISHED_SEGMENT_RETRIES =>
                {
                    attempt += 1;
                }
                other => return other,
            }
        }
    }

    fn get_once(&self, key: &str) -> Result<Option<Record>> {
        // Newest→oldest walk. `pending` accumulates delta-flagged versions
        // until a full record (the base) is found; a full hit with no pending
        // deltas returns immediately (the historical fast path, unchanged
        // when no counters are in play).
        let mut pending: Option<Record> = None;
        let fold = |pending: &mut Option<Record>, version: &Record| -> Option<Record> {
            match pending.take() {
                None if !version.is_delta() => Some(version.clone()),
                None => {
                    *pending = Some(version.clone());
                    None
                }
                Some(p) => {
                    // `p` is the fold of all NEWER deltas; `version` is older.
                    // (A tombstone `version` basifies the chain — the merge
                    // oracle's reset-and-terminate rule.)
                    let merged = crate::record::merge_delta(Some(version), &p);
                    if merged.is_delta() {
                        *pending = Some(merged);
                        None
                    } else {
                        Some(merged)
                    }
                }
            }
        };
        if let Some(record) = self.inner.memtable.read().unwrap().get(key) {
            if let Some(done) = fold(&mut pending, record) {
                return Ok(surface(done));
            }
        }
        {
            let frozen = self.inner.frozen.read().unwrap();
            for (_, map) in frozen.iter().rev() {
                if let Some(record) = map.get(key) {
                    if let Some(done) = fold(&mut pending, record) {
                        return Ok(surface(done));
                    }
                }
            }
        }
        let spec = QuerySpec {
            key_prefix: Some(key.to_string()),
            ..Default::default()
        };
        // Newest-id first.
        for meta in self.pruned_segments(&spec) {
            let (cols, file) = self.load_segment(&meta, false)?;
            if let Some(idx) = cols.find_key(key) {
                let record =
                    cols.materialize(idx, file.as_ref(), &self.inner.stats_bytes_read, true)?;
                if let Some(done) = fold(&mut pending, &record) {
                    return Ok(surface(done));
                }
            }
        }
        // A delta chain with no base = the row as created by increments.
        Ok(pending.and_then(surface))
    }

    /// Scan matching records.
    ///
    /// The segment path is column-native: filters run over typed columns (with
    /// block-index pruning) and payloads are sliced out only for surviving
    /// rows. Newest-wins dedupe is preserved — a key present in a newer source
    /// shadows any older version.
    ///
    /// Ordering follows `spec.order_by` (see [`OrderBy`]); `None` is timestamp
    /// descending, byte-identical to the historical behavior. With an explicit
    /// `order_by` and `limit > 0` the engine keeps a bounded top-k heap instead
    /// of materializing every match, and — for `TimestampDesc` — stops early
    /// once no unvisited (older) segment can beat the weakest kept row.
    pub async fn scan(&self, spec: &QuerySpec) -> Result<Vec<Record>> {
        if spec.after.is_some()
            && matches!(
                spec.order_by,
                Some(OrderBy::NumericAsc(_)) | Some(OrderBy::NumericDesc(_))
            )
        {
            // The keyset bound is defined over the timestamp order only —
            // silently ignoring it would serve wrong pages.
            return Err(GirderError::Config(
                "QuerySpec.after requires a timestamp order".into(),
            ));
        }
        self.retry_vanished(|| {
            // Counters in range → the fold path. Predicate narrowing is
            // UNSOUND over raw delta rows (a delta's zone-mapped numeric is
            // the increment, not the total; a base outside the time window
            // still feeds a fold inside it), so fold-mode narrows by key
            // prefix only, folds, then applies the full spec. When no deltas
            // exist anywhere in range — every pure-trace workload — this is
            // one boolean check and the historical paths run unchanged.
            if self.deltas_possible(spec) {
                return self.scan_fold(spec);
            }
            if spec.limit > 0 {
                if let Some(order) = &spec.order_by {
                    return self.scan_topk(spec, order);
                }
            }
            self.scan_full(spec)
        })
    }

    /// Exact match COUNT for `spec`, without reading a single payload byte
    /// (plan 0013 §7 D2c2): mirrors `scan_full`'s newest-wins walk — the
    /// same shadow probing, the same per-source predicate checks over
    /// columns only — and simply counts survivors instead of materializing
    /// them. `count(spec) == scan(spec with limit 0).len()` is the pinned
    /// oracle (`tests/count.rs`).
    ///
    /// - `spec.after` is rejected (a count is over the whole match set; a
    ///   bounded-suffix count is undefined scope) and `limit`/`order_by`
    ///   are ignored (they don't change membership).
    /// - Counter deltas in range force the fold fallback (decode-heavy but
    ///   exact) — raw delta rows must never be counted per-version.
    /// - Tombstones are engine vocabulary: a deleted key is not counted, and
    ///   its tombstone shadows older versions through the same walk plan
    ///   `scan` uses (`docs/GUARANTEES.md` §Deletes).
    pub async fn count(&self, spec: &QuerySpec) -> Result<usize> {
        if spec.after.is_some() {
            return Err(GirderError::Config(
                "count is over the whole match set; `after` is not meaningful".into(),
            ));
        }
        let unbounded = QuerySpec {
            limit: 0, // engine semantics: 0 = unlimited
            order_by: None,
            ..spec.clone()
        };
        self.retry_vanished(|| {
            if self.deltas_possible(&unbounded) {
                // Fold-first correctness over speed: partial values must
                // never count, same rule as ranking.
                return Ok(self.scan_fold(&unbounded)?.len());
            }
            self.count_full(&unbounded)
        })
    }

    /// `scan_full` minus materialize: identical `seen` newest-wins walk,
    /// counting instead of decoding. Any drift from `scan_full`'s shadowing
    /// makes the count disagree with the page's own membership — the oracle
    /// test holds them together.
    fn count_full(&self, spec: &QuerySpec) -> Result<usize> {
        let mut seen: HashSet<String> = HashSet::new();
        let mut n = 0usize;
        let want = text_query_tokens(spec);
        {
            let memtable = self.inner.memtable.read().unwrap();
            let cand = want.as_ref().map(|w| memtable.text_candidates(w));
            for record in memtable.values() {
                if seen.insert(record.key.clone())
                    && !record.is_tombstone()
                    && mem_matches(spec, record, cand.as_ref())
                {
                    n += 1;
                }
            }
        }
        {
            let frozen = self.inner.frozen.read().unwrap();
            for (_, map) in frozen.iter().rev() {
                let cand = want.as_ref().map(|w| map.text_candidates(w));
                for record in map.values() {
                    if seen.insert(record.key.clone())
                        && !record.is_tombstone()
                        && mem_matches(spec, record, cand.as_ref())
                    {
                        n += 1;
                    }
                }
            }
        }
        // The same shared walk plan as `scan_full` — the count's membership
        // and the page's membership cannot disagree (the pinned oracle).
        let plan = self.walk_plan(spec);
        let mut probe = ShadowProbe::new(plan.steps.len());
        for (idx, step) in plan.steps.iter().enumerate() {
            if !step.visit {
                continue; // probe target only
            }
            let (cols, file) = self.load_segment(&step.meta, need_tokens(spec))?;
            for &row in &cols.matching_rows(spec, file.as_ref(), &self.inner.stats_bytes_read)? {
                let r = row as usize;
                let key = cols.key_at(r);
                if seen.contains(key) || cols.is_tombstone_at(r) {
                    continue;
                }
                if plan.track_shadows && probe.shadowed(self, &plan.steps, idx, key)? {
                    continue;
                }
                n += 1;
            }
        }
        Ok(n)
    }

    /// Any delta-flagged records among the sources this spec's key range can
    /// touch? Conservative superset: memtable/frozen delta presence, or a
    /// prefix-overlapping segment whose zone-map label set carries the
    /// reserved delta label (`Some(None)` = cardinality-overflow = can't rule
    /// out).
    fn deltas_possible(&self, spec: &QuerySpec) -> bool {
        if self.inner.memtable.read().unwrap().has_deltas() {
            return true;
        }
        if self
            .inner
            .frozen
            .read()
            .unwrap()
            .iter()
            .any(|(_, m)| m.has_deltas())
        {
            return true;
        }
        let prefix_only = QuerySpec {
            key_prefix: spec.key_prefix.clone(),
            ..Default::default()
        };
        let manifest = self.inner.manifest.read().unwrap();
        manifest.segments.iter().any(|m| {
            m.zone.may_match(&prefix_only)
                && match m.zone.labels.get(crate::record::DELTA_LABEL) {
                    Some(Some(values)) => values.contains("1"),
                    Some(None) => true,
                    None => false,
                }
        })
    }

    /// Fold-mode scan: prefix-only narrowing, newest→oldest per-key delta
    /// folding via the single `merge_delta` oracle, then the FULL spec
    /// (labels/numerics/time/text/order/limit) applied to the folded records.
    fn scan_fold(&self, spec: &QuerySpec) -> Result<Vec<Record>> {
        use std::collections::hash_map::Entry;
        use std::collections::HashMap;
        enum Fold {
            Done(Record),
            Pending(Record),
        }
        let mut states: HashMap<String, Fold> = HashMap::new();
        let mut feed = |record: &Record| {
            match states.entry(record.key.clone()) {
                Entry::Vacant(v) => {
                    if record.is_delta() {
                        v.insert(Fold::Pending(record.clone()));
                    } else {
                        // A tombstone lands here as Done: it shadows every
                        // older version and is dropped at the output filter.
                        v.insert(Fold::Done(record.clone()));
                    }
                }
                Entry::Occupied(mut o) => {
                    if let Fold::Pending(p) = o.get() {
                        // `p` folds all newer deltas; `record` is older. (A
                        // tombstone `record` basifies the chain — the merge
                        // oracle's reset-and-terminate rule.)
                        let merged = crate::record::merge_delta(Some(record), p);
                        o.insert(if merged.is_delta() {
                            Fold::Pending(merged)
                        } else {
                            Fold::Done(merged)
                        });
                    } // Done: older versions are shadowed (LWW).
                }
            }
        };
        let prefix = spec.key_prefix.as_deref();
        let stripped = QuerySpec {
            key_prefix: spec.key_prefix.clone(),
            ..Default::default()
        };
        // The post-fold filter is `spec.matches`, which reads `record.text`
        // for text predicates — so the fold must fetch text whenever a text
        // predicate exists, even under `omit_text` (the projection then
        // strips the OUTPUT, never the evaluation).
        let fold_wants_text =
            !spec.omit_text || spec.text_match.is_some() || spec.text_like.is_some();
        {
            let memtable = self.inner.memtable.read().unwrap();
            for record in memtable.values() {
                if prefix.is_none_or(|p| record.key.starts_with(p)) {
                    feed(record);
                }
            }
        }
        {
            let frozen = self.inner.frozen.read().unwrap();
            for (_, map) in frozen.iter().rev() {
                for record in map.values() {
                    if prefix.is_none_or(|p| record.key.starts_with(p)) {
                        feed(record);
                    }
                }
            }
        }
        for meta in self.pruned_segments(&stripped) {
            let (cols, file) = self.load_segment(&meta, false)?;
            for &row in
                &cols.matching_rows(&stripped, file.as_ref(), &self.inner.stats_bytes_read)?
            {
                let record = cols.materialize(
                    row as usize,
                    file.as_ref(),
                    &self.inner.stats_bytes_read,
                    fold_wants_text,
                )?;
                feed(&record);
            }
        }

        let mut out: Vec<Record> = states
            .into_values()
            .map(|f| match f {
                Fold::Done(r) | Fold::Pending(r) => finish_fold(r),
            })
            .filter(|r| !r.is_tombstone() && spec.matches(r) && spec.after_bound_ok(r))
            .collect();
        let order = spec.order_by.as_ref();
        out.sort_by(|a, b| record_cmp(order, a, b));
        if spec.limit > 0 {
            out.truncate(spec.limit);
        }
        strip_text_if_omitted(spec, &mut out);
        Ok(out)
    }

    /// Materialize every match, dedupe newest-wins, sort by the effective
    /// order, then truncate. This is the unbounded / `order_by: None` path and
    /// is byte-identical to the historical scan for `None`.
    fn scan_full(&self, spec: &QuerySpec) -> Result<Vec<Record>> {
        let mut seen: HashSet<String> = HashSet::new();
        let mut out: Vec<Record> = Vec::new();
        let want = text_query_tokens(spec);

        // Recency order: memtable → frozen (newest first) seed `seen` with all
        // their keys (matching or not) so they shadow older segment versions.
        // Tombstones seed `seen` like any record but are never emitted.
        {
            let memtable = self.inner.memtable.read().unwrap();
            let cand = want.as_ref().map(|w| memtable.text_candidates(w));
            for record in memtable.values() {
                if seen.insert(record.key.clone())
                    && !record.is_tombstone()
                    && mem_matches(spec, record, cand.as_ref())
                    && spec.after_bound_ok(record)
                {
                    out.push(record.clone());
                }
            }
        }
        {
            let frozen = self.inner.frozen.read().unwrap();
            for (_, map) in frozen.iter().rev() {
                let cand = want.as_ref().map(|w| map.text_candidates(w));
                for record in map.values() {
                    if seen.insert(record.key.clone())
                        && !record.is_tombstone()
                        && mem_matches(spec, record, cand.as_ref())
                        && spec.after_bound_ok(record)
                    {
                        out.push(record.clone());
                    }
                }
            }
        }

        // Segments in the shared newest-wins walk plan (see `walk_plan`).
        // When key ranges are pairwise disjoint (the compacted / append-only
        // common case) a key can live in at most one segment, so no
        // cross-segment shadow tracking is needed — only membership checks
        // against the memtable/frozen seed. Otherwise each CANDIDATE probes
        // the newer plan steps (visit and shadow alike) whose key range
        // covers it — a zone- or block-pruned newer version still shadows an
        // older match, and steps no candidate lands in are never read.
        let plan = self.walk_plan(spec);
        let mut probe = ShadowProbe::new(plan.steps.len());
        for (idx, step) in plan.steps.iter().enumerate() {
            if !step.visit {
                continue; // probe target only — read iff a candidate lands in range
            }
            // One open file handle is held across this segment's whole
            // materialize loop, so a concurrent hot→cold rename can't tear the
            // per-row payload reads (the fd stays valid on unix).
            let (cols, file) = self.load_segment(&step.meta, need_tokens(spec))?;
            let rows = cols.matching_rows(spec, file.as_ref(), &self.inner.stats_bytes_read)?;
            for &row in &rows {
                let r = row as usize;
                let key = cols.key_at(r);
                if seen.contains(key) || cols.is_tombstone_at(r) {
                    continue;
                }
                if plan.track_shadows && probe.shadowed(self, &plan.steps, idx, key)? {
                    continue; // a newer version elsewhere shadows this one (LWW)
                }
                if !after_ok_cols(spec, cols.timestamp_at(r), key) {
                    continue; // bound applies to the winning version
                }
                out.push(cols.materialize(
                    r,
                    file.as_ref(),
                    &self.inner.stats_bytes_read,
                    !spec.omit_text,
                )?);
            }
        }

        let order = spec.order_by.as_ref();
        out.sort_by(|a, b| record_cmp(order, a, b));
        if spec.limit > 0 {
            out.truncate(spec.limit);
        }
        strip_text_if_omitted(spec, &mut out);
        Ok(out)
    }

    /// Bounded top-k over the ordered dimension. Keeps a heap of the `limit`
    /// best rows and materializes payloads only for the survivors — the broad
    /// sorted-page shape never allocates the ~166k-record intermediate.
    ///
    /// Dedupe borrows keys from the loaded columns for membership tests (no
    /// per-matching-row `String` clone) and only pays a clone when a candidate
    /// actually enters the heap (bounded by `limit` + improvements). When key
    /// ranges overlap (rare after append/compaction) each candidate probes the
    /// newer plan steps so a block-pruned newer version still shadows.
    ///
    /// **Early-termination soundness (timestamp desc).** Segments are visited
    /// strictly newest-id first (write recency — required for newest-wins).
    /// The loop *stops* (never skip-then-continue) as soon as `suffix_max_ts`
    /// over all unvisited segments is below the weakest kept row: no unvisited
    /// row can enter the page, and because every remaining segment is older
    /// than every visited one, no emitted row can later be shadowed by an
    /// unvisited newer version. A rewrite with a *lower* timestamp in a newer
    /// segment is therefore handled correctly (the newer, low-ts version was
    /// already seen; the older, high-ts version is never emitted).
    fn scan_topk(&self, spec: &QuerySpec, order: &OrderBy) -> Result<Vec<Record>> {
        let limit = spec.limit;
        let numeric_name = match order {
            OrderBy::NumericAsc(n) | OrderBy::NumericDesc(n) => Some(n.as_str()),
            _ => None,
        };
        let early_term = matches!(order, OrderBy::TimestampDesc);

        let mut heap: BinaryHeap<HeapItem> = BinaryHeap::with_capacity(limit + 1);
        let mut seen: HashSet<String> = HashSet::new();
        let want = text_query_tokens(spec);

        // Phase 1: newest sources (memtable → frozen). Guards are held only for
        // this in-memory pass, never across segment I/O.
        {
            let memtable = self.inner.memtable.read().unwrap();
            let cand = want.as_ref().map(|w| memtable.text_candidates(w));
            for rec in memtable.values() {
                let fresh = seen.insert(rec.key.clone());
                if fresh
                    && !rec.is_tombstone()
                    && mem_matches(spec, rec, cand.as_ref())
                    && spec.after_bound_ok(rec)
                {
                    let num = numeric_name.and_then(|n| rec.numerics.get(n).copied());
                    let prim = make_prim(order, rec.timestamp, num);
                    offer(&mut heap, limit, prim, &rec.key, || {
                        CandSrc::Mem(rec.clone())
                    });
                }
            }
        }
        {
            let frozen = self.inner.frozen.read().unwrap();
            for (_, map) in frozen.iter().rev() {
                let cand = want.as_ref().map(|w| map.text_candidates(w));
                for rec in map.values() {
                    let fresh = seen.insert(rec.key.clone());
                    if fresh
                        && !rec.is_tombstone()
                        && mem_matches(spec, rec, cand.as_ref())
                        && spec.after_bound_ok(rec)
                    {
                        let num = numeric_name.and_then(|n| rec.numerics.get(n).copied());
                        let prim = make_prim(order, rec.timestamp, num);
                        offer(&mut heap, limit, prim, &rec.key, || {
                            CandSrc::Mem(rec.clone())
                        });
                    }
                }
            }
        }

        // Phase 2: the shared newest-wins walk plan (see `walk_plan`), with
        // suffix-max early termination over its visit steps. Breaking early
        // skips the remaining (older) steps entirely — soundly: probes only
        // ever consult steps NEWER than the candidate, all of which precede
        // the break point in the walk.
        let plan = self.walk_plan(spec);
        let suffix_max = suffix_max_ts(&plan.steps);
        let mut probe = ShadowProbe::new(plan.steps.len());
        for (i, step) in plan.steps.iter().enumerate() {
            if early_term && heap.len() >= limit {
                let worst_ts = heap.peek().unwrap().timestamp();
                if suffix_max[i] < worst_ts {
                    break; // sound stop — see the doc comment above.
                }
            }
            if !step.visit {
                continue; // probe target only
            }
            // Heap phase needs only the columns (keys/ts/order-numeric); the
            // file handle is reopened lazily for the surviving rows at drain.
            let (cols, file) = self.load_segment(&step.meta, need_tokens(spec))?;
            for &row in &cols.matching_rows(spec, file.as_ref(), &self.inner.stats_bytes_read)? {
                let r = row as usize;
                let key = cols.key_at(r);
                if seen.contains(key) {
                    continue; // shadowed by memtable/frozen
                }
                if cols.is_tombstone_at(r) {
                    continue; // deleted: never a candidate
                }
                if plan.track_shadows && probe.shadowed(self, &plan.steps, i, key)? {
                    continue; // a newer version elsewhere shadows this one (LWW)
                }
                if !after_ok_cols(spec, cols.timestamp_at(r), key) {
                    continue; // keyset bound: post-LWW (shadow checks above)
                }
                let num = numeric_name.and_then(|n| cols.numeric_at(n, r));
                let prim = make_prim(order, cols.timestamp_at(r), num);
                offer(&mut heap, limit, prim, key, || CandSrc::Seg {
                    cols: Arc::clone(&cols),
                    meta_idx: i,
                    row: r,
                });
            }
        }

        // Drain best-first; materialize (payload slice) only the survivors.
        let items = heap.into_sorted_vec();
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            match item.src {
                CandSrc::Mem(rec) => out.push(rec),
                CandSrc::Seg {
                    cols,
                    meta_idx,
                    row,
                } => {
                    let file = self.open_payload_file(&plan.steps[meta_idx].meta, &cols)?;
                    out.push(cols.materialize(
                        row,
                        file.as_ref(),
                        &self.inner.stats_bytes_read,
                        !spec.omit_text,
                    )?);
                }
            }
        }
        strip_text_if_omitted(spec, &mut out);
        Ok(out)
    }

    /// Zone-map pruned segment metas, newest first (highest id first).
    fn pruned_segments(&self, spec: &QuerySpec) -> Vec<crate::manifest::SegmentMeta> {
        let manifest = self.inner.manifest.read().unwrap();
        let mut metas: Vec<_> = manifest
            .segments
            .iter()
            .filter(|meta| meta.zone.may_match(spec))
            .cloned()
            .collect();
        metas.sort_by_key(|m| std::cmp::Reverse(m.id));
        metas
    }

    /// The newest-wins segment walk for `spec` (`docs/GUARANTEES.md`
    /// §Shadowing). Shared by `scan_full`, `scan_topk` and `count_full` so
    /// the three read paths cannot disagree about shadowing.
    ///
    /// A `visit` step zone-matches the spec and is read fully. A shadow step
    /// (`visit == false`) CANNOT match, but key-overlaps an *older* visit
    /// step — a newer version of a key living there still shadows an older,
    /// matching version (LWW), so the walk keeps it as a PROBE TARGET: a
    /// candidate emitted from an older step binary-searches its keys (never
    /// its payloads). Zone pruning alone would drop it and
    /// resurrect the older version: that was the un-shadowing bug
    /// (`tests/lww_shadowing.rs` — tombstone AND label-changed-rewrite
    /// shapes both reproduce it).
    ///
    /// `track_shadows == false` means the WHOLE plan's key ranges (visit ∪
    /// shadow — computing disjointness over the zone-filtered list only was
    /// part of the bug) are pairwise disjoint: a key lives in at most one
    /// plan segment, shadow steps are dropped, and no shadow probing is
    /// needed — the compacted-store fast path, byte-identical to the
    /// historical walk.
    fn walk_plan(&self, spec: &QuerySpec) -> WalkPlan {
        let mut steps: Vec<WalkStep> = {
            let manifest = self.inner.manifest.read().unwrap();
            manifest
                .segments
                .iter()
                .filter(|meta| meta.zone.may_overlap_prefix(spec))
                .map(|meta| WalkStep {
                    visit: meta.zone.may_match(spec),
                    meta: meta.clone(),
                })
                .collect()
        };
        steps.sort_by_key(|s| std::cmp::Reverse(s.meta.id));
        // A shadow step earns its key read only if some OLDER visit step
        // could share a key with it; anything else contributes nothing.
        // (Trim before the disjointness check: an irrelevant overlap between
        // two never-visited segments must not disable the fast path. Sound:
        // if a trimmed segment held a newer version of an emittable key, the
        // key's older, matching version sits in an older visit step whose
        // range then overlaps it — so it would have been kept.)
        let keep: Vec<bool> = steps
            .iter()
            .map(|s| {
                s.visit
                    || steps.iter().any(|v| {
                        v.visit
                            && v.meta.id < s.meta.id
                            && v.meta.zone.key_range_overlaps(&s.meta.zone)
                    })
            })
            .collect();
        let mut keep = keep.iter();
        steps.retain(|_| *keep.next().unwrap());
        let disjoint = key_ranges_disjoint(steps.iter().map(|s| &s.meta));
        if disjoint {
            steps.retain(|s| s.visit);
        }
        WalkPlan {
            steps,
            track_shadows: !disjoint,
        }
    }

    /// The key column of a segment, for shadow reads: v2 loads (and caches)
    /// the footer directory + keys section only; v1 legacy segments are one
    /// whole-file bundle either way. Payloads are never touched.
    fn load_segment_keys(&self, meta: &crate::manifest::SegmentMeta) -> Result<SegKeys> {
        let id = meta.cache_key();
        if let Some(Section::V1Whole(cols)) = self.inner.cache.get(id, SectionId::V1Whole) {
            self.note_cache_hit();
            return Ok(SegKeys::Whole(cols));
        }
        let file = self.open_segment_file(meta)?;
        let mut disk = false;
        let dir = match self.segment_dir(id, &file, &mut disk)? {
            DirOrWhole::Whole(cols) => {
                self.note_cache_miss();
                return Ok(SegKeys::Whole(cols));
            }
            DirOrWhole::Dir(dir) => dir,
        };
        let keys = self.section_keys(id, &dir, &file, &mut disk)?;
        if disk {
            self.note_cache_miss();
        } else {
            self.note_cache_hit();
        }
        Ok(SegKeys::Slim(keys))
    }

    /// The segment's section directory: cached, or parsed from the footer
    /// (v2) / detected-and-fully-loaded as a legacy v1 bundle. Shared by
    /// `load_segment` and `load_segment_keys` so version detection and
    /// caching cannot diverge.
    fn segment_dir(&self, id: u64, file: &std::fs::File, disk: &mut bool) -> Result<DirOrWhole> {
        if let Some(Section::Dir(d)) = self.inner.cache.get(id, SectionId::Dir) {
            return Ok(DirOrWhole::Dir(d));
        }
        *disk = true;
        let br = &self.inner.stats_bytes_read;
        match segment::read_header(file, br)? {
            1 => {
                let cols = Arc::new(segment::load_v1_whole(file, br)?);
                self.cache_put(id, SectionId::V1Whole, Section::V1Whole(Arc::clone(&cols)));
                Ok(DirOrWhole::Whole(cols))
            }
            2 => {
                let d = Arc::new(segment::read_footer(file, br)?);
                self.cache_put(id, SectionId::Dir, Section::Dir(Arc::clone(&d)));
                Ok(DirOrWhole::Dir(d))
            }
            other => Err(GirderError::Corrupt {
                what: "segment",
                detail: format!("unsupported version {other}"),
            }),
        }
    }

    /// Assemble a segment's decoded columns from the section cache, reading only
    /// the sections not already resident (WS4). Returns the assembled view and
    /// (for v2) the open file handle for per-row payload slicing.
    ///
    /// **I/O.** The footer directory and each column section are fetched via
    /// `read_exact_at`; the ~GB payload blob is never read here — only its
    /// offset table — so a cold scan reads tens of MB of columns, not the whole
    /// file. Every read is tallied into `stats_bytes_read`.
    ///
    /// **Accounting.** One `cache_misses` bump if *any* section (or the
    /// directory) had to be read from disk for this segment, else one
    /// `cache_hits` bump — preserving the historical "first touch of a segment
    /// in a query counts one miss" semantics that the zone-map tests pin. A
    /// warm re-scan re-reads payload bytes (never cached) but that is not a
    /// miss: only column-section loads count.
    fn load_segment(
        &self,
        meta: &crate::manifest::SegmentMeta,
        need_tokens: bool,
    ) -> Result<(Arc<SegmentColumns>, Option<std::fs::File>)> {
        // Cache identity is the never-reused file seq, NOT `meta.id` —
        // compaction reuses ids for its outputs (see SegmentMeta::cache_key).
        let id = meta.cache_key();
        let cache = &self.inner.cache;

        // v1 legacy segments are not section-structured → cached as one bundle.
        if let Some(Section::V1Whole(cols)) = cache.get(id, SectionId::V1Whole) {
            self.note_cache_hit();
            return Ok((cols, None));
        }

        let file = self.open_segment_file(meta)?;
        let mut disk = false;

        let dir = match self.segment_dir(id, &file, &mut disk)? {
            DirOrWhole::Whole(cols) => {
                self.note_cache_miss();
                return Ok((cols, None));
            }
            DirOrWhole::Dir(dir) => dir,
        };

        let count = dir.count();
        let keys = self.section_keys(id, &dir, &file, &mut disk)?;
        let timestamps = self.section_timestamps(id, &dir, &file, &mut disk)?;
        let blocks = self.section_blocks(id, &dir, &file, &mut disk)?;
        let payload_index = self.section_payload_index(id, &dir, &file, &mut disk)?;
        let text_index = self.section_text_index(id, &dir, &file, &mut disk)?;
        let tokens = if need_tokens {
            self.section_tokens(id, &dir, &file, &mut disk)?
        } else {
            None
        };
        let mut labels = BTreeMap::new();
        for name in dir.label_names() {
            let col = self.section_label(id, &dir, &file, &name, &mut disk)?;
            labels.insert(name, col);
        }
        let mut numerics = BTreeMap::new();
        for name in dir.numeric_names() {
            let col = self.section_numeric(id, &dir, &file, &name, &mut disk)?;
            numerics.insert(name, col);
        }

        if disk {
            self.note_cache_miss();
        } else {
            self.note_cache_hit();
        }
        let cols = Arc::new(SegmentColumns::assemble(
            count,
            keys,
            timestamps,
            labels,
            numerics,
            blocks,
            payload_index,
            text_index,
            tokens,
        ));
        Ok((cols, Some(file)))
    }

    fn cache_put(&self, id: u64, section_id: SectionId, section: Section) {
        let bytes = section.bytes();
        self.inner.cache.put(id, section_id, section, bytes);
    }
    fn note_cache_hit(&self) {
        self.inner.stats_cache_hits.fetch_add(1, Ordering::Relaxed);
    }
    fn note_cache_miss(&self) {
        self.inner
            .stats_cache_misses
            .fetch_add(1, Ordering::Relaxed);
    }

    fn section_keys(
        &self,
        id: u64,
        dir: &SegDir,
        file: &std::fs::File,
        disk: &mut bool,
    ) -> Result<Arc<KeysSection>> {
        if let Some(Section::Keys(k)) = self.inner.cache.get(id, SectionId::Keys) {
            return Ok(k);
        }
        *disk = true;
        let k = Arc::new(segment::load_keys(file, dir, &self.inner.stats_bytes_read)?);
        self.cache_put(id, SectionId::Keys, Section::Keys(Arc::clone(&k)));
        Ok(k)
    }

    fn section_timestamps(
        &self,
        id: u64,
        dir: &SegDir,
        file: &std::fs::File,
        disk: &mut bool,
    ) -> Result<Arc<Vec<i64>>> {
        if let Some(Section::Timestamps(t)) = self.inner.cache.get(id, SectionId::Timestamps) {
            return Ok(t);
        }
        *disk = true;
        let t = Arc::new(segment::load_timestamps(
            file,
            dir,
            &self.inner.stats_bytes_read,
        )?);
        self.cache_put(
            id,
            SectionId::Timestamps,
            Section::Timestamps(Arc::clone(&t)),
        );
        Ok(t)
    }

    fn section_blocks(
        &self,
        id: u64,
        dir: &SegDir,
        file: &std::fs::File,
        disk: &mut bool,
    ) -> Result<Arc<Vec<BlockMeta>>> {
        if let Some(Section::Blocks(b)) = self.inner.cache.get(id, SectionId::Blocks) {
            return Ok(b);
        }
        *disk = true;
        let b = Arc::new(segment::load_blocks(
            file,
            dir,
            &self.inner.stats_bytes_read,
        )?);
        self.cache_put(id, SectionId::Blocks, Section::Blocks(Arc::clone(&b)));
        Ok(b)
    }

    fn section_payload_index(
        &self,
        id: u64,
        dir: &SegDir,
        file: &std::fs::File,
        disk: &mut bool,
    ) -> Result<Arc<PayloadIndex>> {
        if let Some(Section::PayloadIndex(p)) = self.inner.cache.get(id, SectionId::PayloadIndex) {
            return Ok(p);
        }
        *disk = true;
        let p = Arc::new(segment::load_payload_index(
            file,
            dir,
            &self.inner.stats_bytes_read,
        )?);
        self.cache_put(
            id,
            SectionId::PayloadIndex,
            Section::PayloadIndex(Arc::clone(&p)),
        );
        Ok(p)
    }

    /// Text offset table, or `None` when the segment has no text section.
    /// The absence itself is worth caching — a cache miss on every scan of a
    /// text-less segment would defeat the warm path — but a `None` payload
    /// can't live in the section cache, so absence is re-derived from the
    /// (cached) directory each time: `load_text_index` returns without I/O
    /// when the dir has no entry.
    fn section_text_index(
        &self,
        id: u64,
        dir: &SegDir,
        file: &std::fs::File,
        disk: &mut bool,
    ) -> Result<Option<Arc<TextIndex>>> {
        if let Some(Section::TextIndex(t)) = self.inner.cache.get(id, SectionId::TextIndex) {
            return Ok(Some(t));
        }
        let Some(t) = segment::load_text_index(file, dir, &self.inner.stats_bytes_read)? else {
            return Ok(None);
        };
        *disk = true;
        let t = Arc::new(t);
        self.cache_put(id, SectionId::TextIndex, Section::TextIndex(Arc::clone(&t)));
        Ok(Some(t))
    }

    /// Decoded token postings index; `None` when the segment has no K_TOKENS
    /// section (absence re-derived from the cached directory — no I/O).
    fn section_tokens(
        &self,
        id: u64,
        dir: &SegDir,
        file: &std::fs::File,
        disk: &mut bool,
    ) -> Result<Option<Arc<segment::TokenIndex>>> {
        if let Some(Section::Tokens(t)) = self.inner.cache.get(id, SectionId::Tokens) {
            return Ok(Some(t));
        }
        let Some(t) = segment::load_tokens(file, dir, &self.inner.stats_bytes_read)? else {
            return Ok(None);
        };
        *disk = true;
        let t = Arc::new(t);
        self.cache_put(id, SectionId::Tokens, Section::Tokens(Arc::clone(&t)));
        Ok(Some(t))
    }

    fn section_label(
        &self,
        id: u64,
        dir: &SegDir,
        file: &std::fs::File,
        name: &str,
        disk: &mut bool,
    ) -> Result<Arc<LabelColumn>> {
        if let Some(Section::Label(l)) =
            self.inner.cache.get(id, SectionId::Label(name.to_string()))
        {
            return Ok(l);
        }
        *disk = true;
        let l = Arc::new(segment::load_label(
            file,
            dir,
            name,
            &self.inner.stats_bytes_read,
        )?);
        self.cache_put(
            id,
            SectionId::Label(name.to_string()),
            Section::Label(Arc::clone(&l)),
        );
        Ok(l)
    }

    fn section_numeric(
        &self,
        id: u64,
        dir: &SegDir,
        file: &std::fs::File,
        name: &str,
        disk: &mut bool,
    ) -> Result<Arc<NumericColumn>> {
        if let Some(Section::Numeric(n)) = self
            .inner
            .cache
            .get(id, SectionId::Numeric(name.to_string()))
        {
            return Ok(n);
        }
        *disk = true;
        let n = Arc::new(segment::load_numeric(
            file,
            dir,
            name,
            &self.inner.stats_bytes_read,
        )?);
        self.cache_put(
            id,
            SectionId::Numeric(name.to_string()),
            Section::Numeric(Arc::clone(&n)),
        );
        Ok(n)
    }

    /// Open the segment file for per-row payload slicing, if the column set
    /// needs it (v2). v1-compat columns carry payloads in memory → no file.
    /// (Used by the top-k drain, which reopens per surviving segment.)
    fn open_payload_file(
        &self,
        meta: &crate::manifest::SegmentMeta,
        cols: &segment::SegmentColumns,
    ) -> Result<Option<std::fs::File>> {
        if !cols.payload_needs_file() {
            return Ok(None);
        }
        Ok(Some(self.open_segment_file(meta)?))
    }

    /// Open a segment file, tolerating a concurrent hot↔cold tiering rename: the
    /// manifest snapshot the scan holds may name the tier the file *was* in, so
    /// if the primary path is gone we try the other tier before failing. Once
    /// open, the handle is held for the segment's whole read (sections + payload
    /// slices), so an fd stays valid across a rename on unix.
    fn open_segment_file(&self, meta: &crate::manifest::SegmentMeta) -> Result<std::fs::File> {
        let hot = &self.inner.config.hot_dir;
        let cold = &self.inner.config.cold_dir;
        let primary = segment_path(hot, cold, meta);
        match std::fs::File::open(&primary) {
            Ok(f) => Ok(f),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let other = match meta.tier {
                    Tier::Hot => cold.join(&meta.file),
                    Tier::Cold => hot.join(&meta.file),
                };
                Ok(std::fs::File::open(other)?)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn stats(&self) -> Stats {
        let manifest = self.inner.manifest.read().unwrap();
        Stats {
            puts: self.inner.stats_puts.load(Ordering::Relaxed),
            memtable_records: self.inner.memtable.read().unwrap().len(),
            frozen_memtables: self.inner.frozen.read().unwrap().len(),
            hot_segments: manifest
                .segments
                .iter()
                .filter(|s| s.tier == Tier::Hot)
                .count(),
            cold_segments: manifest
                .segments
                .iter()
                .filter(|s| s.tier == Tier::Cold)
                .count(),
            total_records_in_segments: manifest.segments.iter().map(|s| s.zone.count).sum(),
            flushes: self.inner.stats_flushes.load(Ordering::Relaxed),
            compactions: self.inner.stats_compactions.load(Ordering::Relaxed),
            groomed_segments: self.inner.stats_groomed.load(Ordering::Relaxed),
            reclaimed_segments: self.inner.stats_reclaimed.load(Ordering::Relaxed),
            migrated_segments: self.inner.stats_migrated.load(Ordering::Relaxed),
            blobs: manifest.blobs.len(),
            tiered: self.inner.stats_tiered.load(Ordering::Relaxed),
            cache_hits: self.inner.stats_cache_hits.load(Ordering::Relaxed),
            cache_misses: self.inner.stats_cache_misses.load(Ordering::Relaxed),
            bytes_flushed: self.inner.stats_bytes_flushed.load(Ordering::Relaxed),
            bytes_compacted: self.inner.stats_bytes_compacted.load(Ordering::Relaxed),
            bytes_read: self.inner.stats_bytes_read.load(Ordering::Relaxed),
        }
    }

    /// Graceful shutdown: checkpoint everything to segments.
    ///
    /// Quiesces before returning: the tick source is stopped FIRST (no new
    /// maintenance can be cast), then the final flush's maintenance `call`
    /// drains the FIFO mailbox behind any already-queued `Tick` — so when
    /// `close` returns, no background flush/compaction/tiering is in flight
    /// and manifest + files are consistent for an immediate reopen.
    pub async fn close(self) -> Result<()> {
        self._ticker.abort();
        self.flush().await?;
        self.writer
            .call(WriterMsg::Sync, CALL_TIMEOUT)
            .await
            .map_err(|_| GirderError::ShutDown)?
            .ok();
        Ok(())
    }
}

/// A folded record leaving the engine: the reserved delta label is internal
/// bookkeeping, never user data — strip it (a chain with no base IS the
/// row's full current value).
fn finish_fold(mut record: Record) -> Record {
    record.labels.remove(crate::record::DELTA_LABEL);
    record
}

/// A folded record leaving `get`: strip internal bookkeeping, then apply the
/// tombstone vocabulary — a deleted key reads as absent, never as its marker.
fn surface(record: Record) -> Option<Record> {
    let record = finish_fold(record);
    if record.is_tombstone() {
        None
    } else {
        Some(record)
    }
}

/// The query's text tokens, when a text predicate is present.
fn text_query_tokens(spec: &QuerySpec) -> Option<Vec<String>> {
    spec.text_match.as_deref().map(crate::text::fts_tokens)
}

/// Should the walk load K_TOKENS for this spec? `text_match` always; a
/// `text_like` only when its prefix analysis yields usable constraints —
/// an unanalyzable pattern never touches the index, so it must not pay
/// for loading it. (The F2 bench caught the miss: keying this on
/// `text_match` alone left every LIKE leg on the fallthrough walk.)
fn need_tokens(spec: &QuerySpec) -> bool {
    spec.text_match.is_some()
        || spec
            .text_like
            .as_deref()
            .is_some_and(|p| !crate::text::like_constraints(p).is_empty())
}

/// The `omit_text` projection's single output gate: rows sourced from the
/// memtable/frozen clones (and fold outputs that needed text for their own
/// predicate) still carry text — one pass strips them so a page is uniform
/// (segment rows never read theirs in the first place; that skip is the
/// win this projection exists for).
fn strip_text_if_omitted(spec: &QuerySpec, out: &mut [Record]) {
    if spec.omit_text {
        for r in out.iter_mut() {
            r.text = None;
        }
    }
}

/// Memtable-phase matcher: field predicates via the oracle, `text_match` via
/// the pre-intersected token-map candidate set (equal to the naive text check
/// by construction — same tokenizer at insert; pinned by the agreement
/// tests), `text_like` always against the raw text (records are in memory —
/// no token shortcut exists or is needed).
fn mem_matches(
    spec: &QuerySpec,
    record: &Record,
    cand: Option<&std::collections::HashSet<&str>>,
) -> bool {
    match cand {
        None => spec.matches(record),
        Some(c) => {
            c.contains(record.key.as_str())
                && spec.matches_fields(record)
                && spec.text_like_ok(record)
        }
    }
}

/// Pull-wise shadow probe over a walk plan (D-3): answers "does any plan
/// step NEWER than `idx` contain `key`?" by binary-searching the
/// sorted-unique key column of just the steps whose zone key range covers
/// the key. Keys sections load lazily — at most once per query per step,
/// through the same cached `load_segment_keys` path push-wise seeding used —
/// and a step no candidate's key lands in is never read at all.
///
/// Soundness rests on the fold dispatch: `scan`/`count` route to `scan_fold`
/// whenever `deltas_possible(spec)`, and that gate is conservative over key
/// prefix alone — so on every path where a probe runs, every newer version
/// of a candidate key is a non-delta (record or tombstone) and key
/// membership alone decides suppression. Push-wise seeding relied on the
/// same invariant; `tests/probe_fold_routing.rs` pins it.
struct ShadowProbe {
    keys: Vec<Option<SegKeys>>,
}

impl ShadowProbe {
    fn new(steps: usize) -> Self {
        ShadowProbe {
            keys: (0..steps).map(|_| None).collect(),
        }
    }

    /// Is `key` present in any plan step newer than `idx` (visit or shadow)?
    fn shadowed(
        &mut self,
        engine: &Girder,
        steps: &[WalkStep],
        idx: usize,
        key: &str,
    ) -> Result<bool> {
        for (j, step) in steps.iter().enumerate().take(idx) {
            let zone = &step.meta.zone;
            if zone.min_key.as_str() <= key && key <= zone.max_key.as_str() {
                if self.keys[j].is_none() {
                    self.keys[j] = Some(engine.load_segment_keys(&step.meta)?);
                }
                if self.keys[j].as_ref().unwrap().contains(key) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

/// Column-side keyset-bound check (same semantics as
/// `QuerySpec::after_bound_ok`, without materializing the row).
fn after_ok_cols(spec: &QuerySpec, ts: i64, key: &str) -> bool {
    let Some((bound_ts, bound_key)) = &spec.after else {
        return true;
    };
    if matches!(spec.order_by, Some(OrderBy::TimestampAsc)) {
        ts > *bound_ts || (ts == *bound_ts && key > bound_key.as_str())
    } else {
        ts < *bound_ts || (ts == *bound_ts && key > bound_key.as_str())
    }
}

/// Are the segments' key ranges pairwise disjoint? If so, a key can appear in
/// at most one segment and cross-segment shadow tracking is unnecessary.
fn key_ranges_disjoint<'a>(metas: impl Iterator<Item = &'a SegmentMeta>) -> bool {
    let mut ranges: Vec<(&str, &str)> = metas
        .map(|m| (m.zone.min_key.as_str(), m.zone.max_key.as_str()))
        .collect();
    ranges.sort_by(|a, b| a.0.cmp(b.0));
    ranges.windows(2).all(|w| w[0].1 < w[1].0)
}

/// One segment in a [`WalkPlan`]: read fully (`visit`) or keys-only (shadow).
struct WalkStep {
    meta: SegmentMeta,
    visit: bool,
}

/// The newest-first segment walk `Girder::walk_plan` builds for a spec: the
/// steps in recency order plus whether cross-segment shadow tracking (`seen`
/// seeding + shadow-step key reads) is needed at all.
struct WalkPlan {
    steps: Vec<WalkStep>,
    track_shadows: bool,
}

/// A segment's section directory, or the whole decoded bundle for legacy v1.
enum DirOrWhole {
    Dir(Arc<SegDir>),
    Whole(Arc<SegmentColumns>),
}

/// A segment's key column for shadow reads (v2: the keys section alone).
enum SegKeys {
    Slim(Arc<KeysSection>),
    Whole(Arc<SegmentColumns>),
}

impl SegKeys {
    /// Binary search the sorted-unique key column (either shape).
    fn contains(&self, key: &str) -> bool {
        match self {
            SegKeys::Slim(k) => k.find(key).is_some(),
            SegKeys::Whole(cols) => cols.find_key(key).is_some(),
        }
    }
}

// ---------------------------------------------------------------------------
// Ordering (order_by + top-k)
// ---------------------------------------------------------------------------

/// Direction-adjusted primary sort key: **smaller `Prim` ⇒ ranks earlier** in
/// the output, for every `OrderBy`. `class == 1` marks a missing/NaN ordered
/// value, which always sorts after present values regardless of direction.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct Prim {
    class: u8,
    ord: i128,
}

/// Map an f64 to an integer with the same total order as `f64::total_cmp`
/// (NaN is excluded upstream). Standard IEEE-754 "radix" transform.
fn f64_ordered(v: f64) -> u64 {
    let bits = v.to_bits();
    if bits & (1 << 63) != 0 {
        !bits // negative: flip everything
    } else {
        bits | (1 << 63) // non-negative: flip sign bit
    }
}

/// Build the primary sort key for one record's `(timestamp, ordered-numeric)`.
/// `num` is only consulted for the numeric orders.
fn make_prim(order: &OrderBy, ts: i64, num: Option<f64>) -> Prim {
    match order {
        OrderBy::TimestampAsc => Prim {
            class: 0,
            ord: ts as i128,
        },
        OrderBy::TimestampDesc => Prim {
            class: 0,
            ord: -(ts as i128),
        },
        OrderBy::NumericAsc(_) => match num {
            Some(v) if !v.is_nan() => Prim {
                class: 0,
                ord: f64_ordered(v) as i128,
            },
            _ => Prim { class: 1, ord: 0 },
        },
        OrderBy::NumericDesc(_) => match num {
            Some(v) if !v.is_nan() => Prim {
                class: 0,
                ord: -(f64_ordered(v) as i128),
            },
            _ => Prim { class: 1, ord: 0 },
        },
    }
}

fn record_prim(order: &OrderBy, r: &Record) -> Prim {
    let num = match order {
        OrderBy::NumericAsc(n) | OrderBy::NumericDesc(n) => r.numerics.get(n).copied(),
        _ => None,
    };
    make_prim(order, r.timestamp, num)
}

/// Total order for the full/`None` path. `None` ⇒ timestamp descending, key
/// ascending — the historical sort, bit-for-bit.
fn record_cmp(order: Option<&OrderBy>, a: &Record, b: &Record) -> CmpOrdering {
    let order = order.unwrap_or(&OrderBy::TimestampDesc);
    record_prim(order, a)
        .cmp(&record_prim(order, b))
        .then_with(|| a.key.cmp(&b.key))
}

/// Where a heap candidate's data lives, so payload materialization can be
/// deferred to the surviving rows.
enum CandSrc {
    /// A memtable/frozen record (owned clone — taken only when it enters the
    /// heap, so non-survivors are never cloned).
    Mem(Record),
    /// A segment row; the `Arc` keeps the column set alive for materialization.
    Seg {
        cols: Arc<SegmentColumns>,
        meta_idx: usize,
        row: usize,
    },
}

/// One kept candidate. `Ord` compares only `(prim, key)`; the heap is a
/// max-heap whose top is therefore the *weakest* kept row (largest `Prim`,
/// then largest key), i.e. the next to be evicted.
struct HeapItem {
    prim: Prim,
    key: Box<str>,
    src: CandSrc,
}

impl HeapItem {
    fn timestamp(&self) -> i64 {
        match &self.src {
            CandSrc::Mem(r) => r.timestamp,
            CandSrc::Seg { cols, row, .. } => cols.timestamp_at(*row),
        }
    }
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.prim == other.prim && self.key == other.key
    }
}
impl Eq for HeapItem {}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.prim
            .cmp(&other.prim)
            .then_with(|| self.key.cmp(&other.key))
    }
}

/// Offer a candidate to the bounded top-k heap. Compares against the current
/// weakest kept row *before* allocating the key / building the source, so the
/// steady state allocates nothing for rejected candidates.
fn offer<F: FnOnce() -> CandSrc>(
    heap: &mut BinaryHeap<HeapItem>,
    limit: usize,
    prim: Prim,
    key: &str,
    make_src: F,
) {
    if heap.len() < limit {
        heap.push(HeapItem {
            prim,
            key: key.into(),
            src: make_src(),
        });
        return;
    }
    let worst = heap.peek().unwrap();
    let better = prim
        .cmp(&worst.prim)
        .then_with(|| key.cmp(worst.key.as_ref()))
        == CmpOrdering::Less;
    if better {
        heap.pop();
        heap.push(HeapItem {
            prim,
            key: key.into(),
            src: make_src(),
        });
    }
}

/// `out[i] = max(zone.max_ts)` over segments `i..` of an already newest-first
/// meta list — the suffix bound driving timestamp-desc early termination.
fn suffix_max_ts(steps: &[WalkStep]) -> Vec<i64> {
    let mut out = vec![i64::MIN; steps.len()];
    let mut acc = i64::MIN;
    for i in (0..steps.len()).rev() {
        // Shadow steps never emit candidates, so they contribute nothing to
        // "can any unvisited row still enter the page?" — only visit steps'
        // max timestamps count toward the early-termination bound.
        if steps[i].visit {
            acc = acc.max(steps[i].meta.zone.max_ts);
        }
        out[i] = acc;
    }
    out
}
