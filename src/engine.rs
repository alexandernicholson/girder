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
use crate::record::{OrderBy, QuerySpec, Record};
use crate::segment::{self, SegmentColumns};
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
    pub retention_nanos: Option<i64>,
    /// Background maintenance cadence.
    pub tick_interval: Duration,
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
            tick_interval: Duration::from_secs(5),
        }
    }
}

/// A frozen (immutable, flush-pending) memtable and the WAL seq covering it.
pub type FrozenMemtable = (u64, Arc<BTreeMap<String, Record>>);

/// Shared engine internals (actors + facade all hold this).
pub struct EngineInner {
    pub config: GirderConfig,
    pub memtable: RwLock<BTreeMap<String, Record>>,
    /// Frozen memtables awaiting flush.
    pub frozen: RwLock<Vec<FrozenMemtable>>,
    pub manifest: RwLock<Manifest>,
    pub cache: SegmentCache,
    pub initial_wal_seq: u64,
    manifest_path: PathBuf,
    pub stats_puts: AtomicU64,
    pub stats_flushes: AtomicU64,
    pub stats_compactions: AtomicU64,
    pub stats_tiered: AtomicU64,
    /// Total bytes written by flushes (denominator for write amplification).
    pub stats_bytes_flushed: AtomicU64,
    /// Total bytes written by compaction outputs (numerator for write amp).
    pub stats_bytes_compacted: AtomicU64,
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
    pub tiered: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    /// Cumulative bytes written by flushes.
    pub bytes_flushed: u64,
    /// Cumulative bytes written by compaction outputs. Write amplification is
    /// `bytes_compacted / bytes_flushed`.
    pub bytes_compacted: u64,
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
                recovered.insert(record.key.clone(), record);
            }
        }
        let next_wal_seq = wal_seqs.last().map(|s| s + 1).unwrap_or(0);

        let inner = Arc::new(EngineInner {
            memtable: RwLock::new(BTreeMap::new()),
            frozen: RwLock::new(Vec::new()),
            manifest: RwLock::new(manifest),
            cache: SegmentCache::new(config.cache_bytes),
            initial_wal_seq: next_wal_seq,
            manifest_path,
            stats_puts: AtomicU64::new(0),
            stats_flushes: AtomicU64::new(0),
            stats_compactions: AtomicU64::new(0),
            stats_tiered: AtomicU64::new(0),
            stats_bytes_flushed: AtomicU64::new(0),
            stats_bytes_compacted: AtomicU64::new(0),
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
            },
        )
        .await;

        // Periodic maintenance tick (cast — lossy is fine, next tick retries).
        let tick_ref = maintenance.clone();
        let interval = inner.config.tick_interval;
        let ticker = tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
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
        if let Some(record) = self.inner.memtable.read().unwrap().get(key) {
            return Ok(Some(record.clone()));
        }
        {
            let frozen = self.inner.frozen.read().unwrap();
            for (_, map) in frozen.iter().rev() {
                if let Some(record) = map.get(key) {
                    return Ok(Some(record.clone()));
                }
            }
        }
        let spec = QuerySpec {
            key_prefix: Some(key.to_string()),
            ..Default::default()
        };
        // Newest-id first: the first segment that holds the key wins.
        for meta in self.pruned_segments(&spec) {
            let cols = self.load_columns(&meta)?;
            if let Some(idx) = cols.find_key(key) {
                let file = self.open_payload_file(&meta, &cols)?;
                return Ok(Some(cols.materialize(idx, file.as_ref())?));
            }
        }
        Ok(None)
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
        if spec.limit > 0 {
            if let Some(order) = &spec.order_by {
                return self.scan_topk(spec, order);
            }
        }
        self.scan_full(spec)
    }

    /// Materialize every match, dedupe newest-wins, sort by the effective
    /// order, then truncate. This is the unbounded / `order_by: None` path and
    /// is byte-identical to the historical scan for `None`.
    fn scan_full(&self, spec: &QuerySpec) -> Result<Vec<Record>> {
        let mut seen: HashSet<String> = HashSet::new();
        let mut out: Vec<Record> = Vec::new();

        // Recency order: memtable → frozen (newest first) seed `seen` with all
        // their keys (matching or not) so they shadow older segment versions.
        {
            let memtable = self.inner.memtable.read().unwrap();
            for record in memtable.values() {
                if seen.insert(record.key.clone()) && spec.matches(record) {
                    out.push(record.clone());
                }
            }
        }
        {
            let frozen = self.inner.frozen.read().unwrap();
            for (_, map) in frozen.iter().rev() {
                for record in map.values() {
                    if seen.insert(record.key.clone()) && spec.matches(record) {
                        out.push(record.clone());
                    }
                }
            }
        }

        // Segments, newest-id first. If their key ranges are pairwise disjoint
        // (the compacted / append-only common case) a key can live in at most
        // one segment, so no cross-segment shadow tracking is needed — only
        // membership checks against the memtable/frozen seed. Otherwise fall
        // back to inserting every visited key of each non-final source so a
        // block-pruned newer version still shadows an older match.
        let metas = self.pruned_segments(spec);
        let disjoint = key_ranges_disjoint(&metas);
        let last = metas.len().saturating_sub(1);
        for (idx, meta) in metas.iter().enumerate() {
            let cols = self.load_columns(meta)?;
            let rows = cols.matching_rows(spec);
            if !rows.is_empty() {
                let file = self.open_payload_file(meta, &cols)?;
                for &row in &rows {
                    let r = row as usize;
                    if !seen.contains(cols.key_at(r)) {
                        out.push(cols.materialize(r, file.as_ref())?);
                    }
                }
            }
            if !disjoint && idx != last {
                for i in 0..cols.count() {
                    seen.insert(cols.key_at(i).to_string());
                }
            }
        }

        let order = spec.order_by.as_ref();
        out.sort_by(|a, b| record_cmp(order, a, b));
        if spec.limit > 0 {
            out.truncate(spec.limit);
        }
        Ok(out)
    }

    /// Bounded top-k over the ordered dimension. Keeps a heap of the `limit`
    /// best rows and materializes payloads only for the survivors — the broad
    /// sorted-page shape never allocates the ~166k-record intermediate.
    ///
    /// Dedupe borrows keys from the loaded columns for membership tests (no
    /// per-matching-row `String` clone) and only pays a clone when a candidate
    /// actually enters the heap (bounded by `limit` + improvements). Segment
    /// keys are recorded in `seen` only when key ranges overlap (rare after
    /// append/compaction) so a block-pruned newer version still shadows.
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

        // Phase 1: newest sources (memtable → frozen). Guards are held only for
        // this in-memory pass, never across segment I/O.
        {
            let memtable = self.inner.memtable.read().unwrap();
            for rec in memtable.values() {
                let fresh = seen.insert(rec.key.clone());
                if fresh && spec.matches(rec) {
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
                for rec in map.values() {
                    let fresh = seen.insert(rec.key.clone());
                    if fresh && spec.matches(rec) {
                        let num = numeric_name.and_then(|n| rec.numerics.get(n).copied());
                        let prim = make_prim(order, rec.timestamp, num);
                        offer(&mut heap, limit, prim, &rec.key, || {
                            CandSrc::Mem(rec.clone())
                        });
                    }
                }
            }
        }

        // Phase 2: segments newest-id first, with suffix-max early termination.
        let metas = self.pruned_segments(spec);
        let disjoint = key_ranges_disjoint(&metas);
        let suffix_max = suffix_max_ts(&metas);
        for (i, meta) in metas.iter().enumerate() {
            if early_term && heap.len() >= limit {
                let worst_ts = heap.peek().unwrap().timestamp();
                if suffix_max[i] < worst_ts {
                    break; // sound stop — see the doc comment above.
                }
            }
            let cols = self.load_columns(meta)?;
            for &row in &cols.matching_rows(spec) {
                let r = row as usize;
                let key = cols.key_at(r);
                if seen.contains(key) {
                    continue; // shadowed by a newer source
                }
                let num = numeric_name.and_then(|n| cols.numeric_at(n, r));
                let prim = make_prim(order, cols.timestamp_at(r), num);
                offer(&mut heap, limit, prim, key, || CandSrc::Seg {
                    cols: Arc::clone(&cols),
                    meta_idx: i,
                    row: r,
                });
            }
            if !disjoint {
                for r in 0..cols.count() {
                    seen.insert(cols.key_at(r).to_string());
                }
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
                    let file = self.open_payload_file(&metas[meta_idx], &cols)?;
                    out.push(cols.materialize(row, file.as_ref())?);
                }
            }
        }
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

    /// Load a segment's decoded column set (cache hit = no I/O, no decode).
    fn load_columns(
        &self,
        meta: &crate::manifest::SegmentMeta,
    ) -> Result<Arc<segment::SegmentColumns>> {
        if let Some(cols) = self.inner.cache.get(meta.id) {
            return Ok(cols);
        }
        let path = segment_path(
            &self.inner.config.hot_dir,
            &self.inner.config.cold_dir,
            meta,
        );
        let cols = Arc::new(segment::read_columns(&path)?);
        self.inner
            .cache
            .put(meta.id, Arc::clone(&cols), cols.heap_bytes());
        Ok(cols)
    }

    /// Open the segment file for per-row payload slicing, if the column set
    /// needs it (v2). v1-compat columns carry payloads in memory → no file.
    fn open_payload_file(
        &self,
        meta: &crate::manifest::SegmentMeta,
        cols: &segment::SegmentColumns,
    ) -> Result<Option<std::fs::File>> {
        if !cols.payload_needs_file() {
            return Ok(None);
        }
        let path = segment_path(
            &self.inner.config.hot_dir,
            &self.inner.config.cold_dir,
            meta,
        );
        Ok(Some(std::fs::File::open(path)?))
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
            tiered: self.inner.stats_tiered.load(Ordering::Relaxed),
            cache_hits: self.inner.cache.hits.load(Ordering::Relaxed),
            cache_misses: self.inner.cache.misses.load(Ordering::Relaxed),
            bytes_flushed: self.inner.stats_bytes_flushed.load(Ordering::Relaxed),
            bytes_compacted: self.inner.stats_bytes_compacted.load(Ordering::Relaxed),
        }
    }

    /// Graceful shutdown: checkpoint everything to segments.
    pub async fn close(self) -> Result<()> {
        self.flush().await?;
        self.writer
            .call(WriterMsg::Sync, CALL_TIMEOUT)
            .await
            .map_err(|_| GirderError::ShutDown)?
            .ok();
        self._ticker.abort();
        Ok(())
    }
}

/// Are the segments' key ranges pairwise disjoint? If so, a key can appear in
/// at most one segment and cross-segment shadow tracking is unnecessary.
fn key_ranges_disjoint(metas: &[SegmentMeta]) -> bool {
    let mut ranges: Vec<(&str, &str)> = metas
        .iter()
        .map(|m| (m.zone.min_key.as_str(), m.zone.max_key.as_str()))
        .collect();
    ranges.sort_by(|a, b| a.0.cmp(b.0));
    ranges.windows(2).all(|w| w[0].1 < w[1].0)
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
fn suffix_max_ts(metas: &[SegmentMeta]) -> Vec<i64> {
    let mut out = vec![i64::MIN; metas.len()];
    let mut acc = i64::MIN;
    for i in (0..metas.len()).rev() {
        acc = acc.max(metas[i].zone.max_ts);
        out[i] = acc;
    }
    out
}
