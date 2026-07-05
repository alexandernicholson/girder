//! The manifest: the authoritative list of live segments, updated atomically
//! (write temp + fsync + rename). Everything not in the manifest is garbage.
//!
//! **Versioning (docs/COMPAT.md).** Files written since B3 begin with
//! `[u32 "GMAN"][u32 version]` followed by the rmp body; a file with no
//! magic is v0 (bare rmp) and stays readable forever. Every store() writes
//! the current version. An unknown future version fails CLOSED.
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

const MANIFEST_MAGIC: u32 = 0x4e41_4d47; // "GMAN"
const MANIFEST_VERSION: u32 = 1;

impl Manifest {
    pub fn load(path: &Path) -> Result<Manifest> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Manifest::default()),
            Err(e) => return Err(e.into()),
        };
        let body = if bytes.len() >= 8
            && u32::from_le_bytes(bytes[0..4].try_into().unwrap()) == MANIFEST_MAGIC
        {
            let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
            if version > MANIFEST_VERSION {
                // Fail CLOSED — reading a future manifest as anything else
                // could orphan live segments.
                return Err(GirderError::Corrupt {
                    what: "manifest",
                    detail: format!("unsupported manifest version {version}"),
                });
            }
            &bytes[8..]
        } else {
            &bytes[..] // v0: bare rmp, readable forever
        };
        rmp_serde::from_slice(body).map_err(|e| GirderError::Corrupt {
            what: "manifest",
            detail: e.to_string(),
        })
    }

    pub fn store(&self, path: &Path) -> Result<()> {
        let body = rmp_serde::to_vec(self).map_err(|e| GirderError::Encode(e.to_string()))?;
        let mut bytes = Vec::with_capacity(8 + body.len());
        bytes.extend_from_slice(&MANIFEST_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        bytes.extend_from_slice(&body);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// v0 (bare-rmp, magic-less) manifests read forever; store() writes the
    /// current versioned format; a future version fails CLOSED.
    #[test]
    fn manifest_version_compat() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("MANIFEST");
        let manifest = Manifest {
            next_segment_id: 7,
            segments: Vec::new(),
        };

        // v0 on disk → loads.
        std::fs::write(&path, rmp_serde::to_vec(&manifest).unwrap()).unwrap();
        assert_eq!(Manifest::load(&path).unwrap().next_segment_id, 7);

        // store() → versioned; round-trips; header present.
        manifest.store(&path).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(&bytes[0..4], &MANIFEST_MAGIC.to_le_bytes());
        assert_eq!(Manifest::load(&path).unwrap().next_segment_id, 7);

        // Future version → hard error (never misread as v0).
        let mut future = Vec::new();
        future.extend_from_slice(&MANIFEST_MAGIC.to_le_bytes());
        future.extend_from_slice(&9u32.to_le_bytes());
        future.extend_from_slice(&rmp_serde::to_vec(&manifest).unwrap());
        std::fs::write(&path, future).unwrap();
        assert!(Manifest::load(&path).is_err());

        // Absent file → empty default (unchanged behavior).
        assert_eq!(
            Manifest::load(&dir.path().join("nope"))
                .unwrap()
                .segments
                .len(),
            0
        );
    }
}
