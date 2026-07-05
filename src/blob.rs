//! Content-addressed blob namespace (plan 0013 §6, B4): large immutable
//! byte objects keyed by their sha256, stored file-per-hash OUTSIDE the
//! WAL/memtable/segment machinery — the hash IS the integrity check
//! (verified on every read, failing CLOSED on mismatch), so content needs
//! no WAL frame and never churns the record cache.
//!
//! Layout: `<hot_dir>/blobs/<hh>/<sha256hex>` (two-hex-char shard dirs).
//! Existence is tracked by the manifest (`Manifest.blobs`) — a file not
//! listed there is garbage (kill residue), swept by the groomer under the
//! manifest lock. Deletion contract: **explicit `delete_blob` only** —
//! content addressing means dedup, dedup means shared referents, and only
//! the embedder knows references; a TTL-from-last-put would delete under
//! live references girder cannot see.
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

use crate::error::{GirderError, Result};

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// A well-formed blob id: exactly 64 lowercase hex chars.
pub(crate) fn valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// The blobs root under the hot tier.
pub(crate) fn blobs_root(hot_dir: &Path) -> PathBuf {
    hot_dir.join("blobs")
}

/// File path for a hash: `blobs/<first-two-chars>/<hash>`.
pub(crate) fn blob_path(hot_dir: &Path, hash: &str) -> PathBuf {
    blobs_root(hot_dir).join(&hash[0..2]).join(hash)
}

/// Write blob content atomically (tmp → fsync → rename). Idempotent: same
/// hash = same content, an existing file is left alone.
pub(crate) fn write_blob_file(hot_dir: &Path, hash: &str, bytes: &[u8]) -> Result<()> {
    let path = blob_path(hot_dir, hash);
    if path.exists() {
        return Ok(()); // dedup: content is immutable per hash
    }
    let dir = path.parent().expect("blob path has a shard dir");
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join(format!(".{hash}.tmp"));
    std::fs::write(&tmp, bytes)?;
    let file = std::fs::File::open(&tmp)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

/// Read + VERIFY a blob. `Ok(bytes)` only when the content re-hashes to its
/// id; a mismatch is loud corruption, never silently-served garbage and
/// never absent.
pub(crate) fn read_blob_file(hot_dir: &Path, hash: &str) -> Result<Vec<u8>> {
    let bytes = std::fs::read(blob_path(hot_dir, hash))?;
    if sha256_hex(&bytes) != hash {
        return Err(GirderError::Corrupt {
            what: "blob",
            detail: format!("content does not match its hash {hash}"),
        });
    }
    Ok(bytes)
}

/// Every blob file currently on disk (hash ids), for the orphan sweep.
pub(crate) fn on_disk_hashes(hot_dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let Ok(shards) = std::fs::read_dir(blobs_root(hot_dir)) else {
        return out;
    };
    for shard in shards.filter_map(|e| e.ok()) {
        let Ok(files) = std::fs::read_dir(shard.path()) else {
            continue;
        };
        for f in files.filter_map(|e| e.ok()) {
            let name = f.file_name().to_string_lossy().into_owned();
            if valid_hash(&name) {
                out.push(name);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the id format against a known sha256 vector.
    #[test]
    fn sha256_hex_golden() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert!(valid_hash(&sha256_hex(b"x")));
        assert!(!valid_hash("short"));
        assert!(!valid_hash(&"Z".repeat(64)));
    }
}
