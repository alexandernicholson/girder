//! The engine facade: open (with recovery), put, scan, get, flush, stats.
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use rebar_core::gen_server::{spawn_gen_server, GenServerRef};
use rebar_core::runtime::Runtime;

use crate::actors::{MaintCall, MaintenanceActor, WriterActor, WriterMsg};
use crate::cache::SegmentCache;
use crate::error::{GirderError, Result};
use crate::manifest::{segment_path, Manifest, Tier};
use crate::record::{QuerySpec, Record};
use crate::segment;
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
            engine
                .put_batch(recovered.into_values().collect())
                .await?;
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
        let metas = self.pruned_segments(&spec);
        for meta in metas {
            let records = self.load_segment(&meta)?;
            // Sorted by key — binary search.
            if let Ok(idx) = records.binary_search_by(|r| r.key.as_str().cmp(key)) {
                return Ok(Some(records[idx].clone()));
            }
        }
        Ok(None)
    }

    /// Scan matching records, newest-first by timestamp. `spec.limit`
    /// truncates after sorting (0 = unlimited).
    pub async fn scan(&self, spec: &QuerySpec) -> Result<Vec<Record>> {
        let mut seen: HashSet<String> = HashSet::new();
        let mut out: Vec<Record> = Vec::new();

        // Recency order: memtable → frozen (newest first) → segments (id desc).
        {
            let memtable = self.inner.memtable.read().unwrap();
            for record in memtable.values() {
                if spec.matches(record) && seen.insert(record.key.clone()) {
                    out.push(record.clone());
                } else if !spec.matches(record) {
                    // Still shadows older versions of the same key.
                    seen.insert(record.key.clone());
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
        for meta in self.pruned_segments(spec) {
            let records = self.load_segment(&meta)?;
            for record in records.iter() {
                if spec.matches(record) && !seen.contains(&record.key) {
                    seen.insert(record.key.clone());
                    out.push(record.clone());
                } else if !spec.matches(record) {
                    seen.insert(record.key.clone());
                }
            }
        }

        out.sort_by(|a, b| {
            b.timestamp
                .cmp(&a.timestamp)
                .then_with(|| a.key.cmp(&b.key))
        });
        if spec.limit > 0 {
            out.truncate(spec.limit);
        }
        Ok(out)
    }

    /// Zone-map pruned segment metas, newest first.
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

    fn load_segment(&self, meta: &crate::manifest::SegmentMeta) -> Result<Arc<Vec<Record>>> {
        if let Some(records) = self.inner.cache.get(meta.id) {
            return Ok(records);
        }
        let path = segment_path(&self.inner.config.hot_dir, &self.inner.config.cold_dir, meta);
        let records = Arc::new(segment::read_segment(&path)?);
        self.inner.cache.put(meta.id, Arc::clone(&records), meta.bytes);
        Ok(records)
    }

    pub fn stats(&self) -> Stats {
        let manifest = self.inner.manifest.read().unwrap();
        Stats {
            puts: self.inner.stats_puts.load(Ordering::Relaxed),
            memtable_records: self.inner.memtable.read().unwrap().len(),
            frozen_memtables: self.inner.frozen.read().unwrap().len(),
            hot_segments: manifest.segments.iter().filter(|s| s.tier == Tier::Hot).count(),
            cold_segments: manifest.segments.iter().filter(|s| s.tier == Tier::Cold).count(),
            total_records_in_segments: manifest.segments.iter().map(|s| s.zone.count).sum(),
            flushes: self.inner.stats_flushes.load(Ordering::Relaxed),
            compactions: self.inner.stats_compactions.load(Ordering::Relaxed),
            tiered: self.inner.stats_tiered.load(Ordering::Relaxed),
            cache_hits: self.inner.cache.hits.load(Ordering::Relaxed),
            cache_misses: self.inner.cache.misses.load(Ordering::Relaxed),
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
