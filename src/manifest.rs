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
    /// Aged out to the injected object store (SCALE-1, docs/SCALE.md). The
    /// segment's bytes live remotely under `file` as the object key; locally it
    /// exists only transiently in the pull-through cache when a read fetches it.
    Remote,
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
    /// Content-addressed blobs that EXIST (sha256 ids) — the source of truth
    /// for `get_blob`; an on-disk blob file not listed here is garbage.
    /// Trailing + defaulted: v0/v1 manifests decode with an empty set.
    #[serde(default)]
    pub blobs: std::collections::BTreeSet<String>,
}

const MANIFEST_MAGIC: u32 = 0x4e41_4d47; // "GMAN"
/// v1 = versioned header; v2 = + the `blobs` existence set (B4); v3 = the
/// `Tier::Remote` variant may appear (SCALE-1). Each bump makes an OLDER
/// binary FAIL CLOSED on a manifest it can't fully understand rather than
/// silently rewrite it — a pre-v3 binary reading a remote-bearing manifest
/// would not know the segment's bytes live in the object store (it would treat
/// `Tier::Remote` as an unknown enum and fail to decode), so refusing is the
/// safe outcome (never a manifest that drops a remote segment's existence).
const MANIFEST_VERSION: u32 = 3;

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

/// Subdirectory of the hot dir holding the pull-through cache of remote
/// segments (SCALE-1) — fast disk, transient, bounded by `pull_cache_bytes`.
pub(crate) const PULL_SUBDIR: &str = "pull";

/// Object-key prefix for tiered segments in the remote store (their filename,
/// `seg-<seq>.gird`). Used to list-and-reconcile orphans at open.
pub(crate) const SEGMENT_KEY_PREFIX: &str = "seg-";

/// Resolve a segment's LOCAL path from its meta. For a `Tier::Remote` segment
/// this is its pull-through cache location under the hot dir — the segment's
/// bytes are fetched there on read; callers that resolve a remote path must
/// have ensured the fetch first (see `open_segment_file`'s pull-through).
pub fn segment_path(hot_dir: &Path, cold_dir: &Path, meta: &SegmentMeta) -> PathBuf {
    match meta.tier {
        Tier::Hot => hot_dir.join(&meta.file),
        Tier::Cold => cold_dir.join(&meta.file),
        Tier::Remote => hot_dir.join(PULL_SUBDIR).join(&meta.file),
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
            blobs: Default::default(),
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
