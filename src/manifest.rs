//! The manifest: the authoritative list of live segments, updated atomically
//! (write temp + fsync + rename). Everything not in the manifest is garbage.
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{GirderError, Result};
use crate::segment::ZoneMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    Hot,
    Cold,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub id: u64,
    /// Path relative to the tier directory.
    pub file: String,
    pub tier: Tier,
    pub zone: ZoneMap,
    pub bytes: u64,
    pub created_unix_nanos: i64,
}

impl SegmentMeta {
    /// Identity for cached sections: the never-reused filename sequence
    /// number (`seg-{seq}.gird`, from `alloc_seq`).
    ///
    /// `meta.id` must NOT key the cache: compaction reuses the run's ids for
    /// its outputs (recency positioning), so one id can name two different
    /// on-disk section layouts over time — a stale cached section under a
    /// reused id would serve wrong bytes. The filename seq is allocated fresh
    /// for every physical file, so it is collision-free forever.
    pub fn cache_key(&self) -> u64 {
        self.file
            .strip_prefix("seg-")
            .and_then(|s| s.strip_suffix(".gird"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(self.id)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    pub next_segment_id: u64,
    pub segments: Vec<SegmentMeta>,
}

impl Manifest {
    pub fn load(path: &Path) -> Result<Manifest> {
        match std::fs::read(path) {
            Ok(bytes) => rmp_serde::from_slice(&bytes).map_err(|e| GirderError::Corrupt {
                what: "manifest",
                detail: e.to_string(),
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Manifest::default()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn store(&self, path: &Path) -> Result<()> {
        let bytes = rmp_serde::to_vec(self).map_err(|e| GirderError::Encode(e.to_string()))?;
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &bytes)?;
        let file = std::fs::File::open(&tmp)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, path)?;
        Ok(())
    }
}

/// Resolve a segment's absolute path from its meta.
pub fn segment_path(hot_dir: &Path, cold_dir: &Path, meta: &SegmentMeta) -> PathBuf {
    match meta.tier {
        Tier::Hot => hot_dir.join(&meta.file),
        Tier::Cold => cold_dir.join(&meta.file),
    }
}
