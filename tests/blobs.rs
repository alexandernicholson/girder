//! Content-addressed blob namespace acceptance (plan 0013 §6, B4):
//! sha256-keyed file-per-hash outside the WAL, verify-on-read fail-closed,
//! manifest-tracked existence, explicit-delete-only contract, orphan sweep.
use std::time::Duration;

use girder::{FsyncPolicy, Girder, GirderConfig};

fn config(dir: &std::path::Path) -> GirderConfig {
    let mut config = GirderConfig::at(dir);
    config.fsync = FsyncPolicy::EveryN(64);
    config.tick_interval = Duration::from_secs(3600);
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blob_roundtrip_dedup_and_absence() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();

    let content = b"a large base64 image, say".to_vec();
    let hash = engine.put_blob(&content).await.unwrap();
    assert_eq!(hash.len(), 64, "sha256 hex id");
    assert_eq!(
        engine.get_blob(&hash).await.unwrap().as_deref(),
        Some(content.as_slice())
    );
    assert_eq!(engine.stats().blobs, 1);

    // Idempotent / dedup: same content, same id, still one blob.
    let again = engine.put_blob(&content).await.unwrap();
    assert_eq!(again, hash);
    assert_eq!(engine.stats().blobs, 1);

    // Different content, different id.
    let other = engine.put_blob(b"other bytes").await.unwrap();
    assert_ne!(other, hash);
    assert_eq!(engine.stats().blobs, 2);

    // Absent (never stored) → None; malformed id → None (not an error).
    let missing = girder::Girder::open(config(tempfile::tempdir().unwrap().path()))
        .await
        .unwrap();
    assert!(missing.get_blob(&hash).await.unwrap().is_none());
    assert!(engine.get_blob("not-a-hash").await.unwrap().is_none());

    // Empty content is a valid blob (absent ≠ empty).
    let empty = engine.put_blob(b"").await.unwrap();
    assert_eq!(engine.get_blob(&empty).await.unwrap(), Some(Vec::new()));
}

/// Integrity is the hash: flipped bytes on disk are LOUD corruption, and a
/// listed-but-missing file likewise — never `None`, never garbage.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blob_verification_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    let hash = engine.put_blob(b"pristine content").await.unwrap();

    // Corrupt the file in place.
    let path = dir.path().join("blobs").join(&hash[0..2]).join(&hash);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();
    assert!(
        engine.get_blob(&hash).await.is_err(),
        "corrupt content must not be served"
    );

    // Listed but missing entirely: corruption, not absence.
    std::fs::remove_file(&path).unwrap();
    assert!(
        engine.get_blob(&hash).await.is_err(),
        "manifest lists it; a missing file is loud"
    );
}

/// Explicit-delete-only contract + restart durability of both the content
/// and the existence set (manifest v2 round-trip).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blob_delete_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (keep, gone) = {
        let engine = Girder::open(config(dir.path())).await.unwrap();
        let keep = engine.put_blob(b"keep me").await.unwrap();
        let gone = engine.put_blob(b"delete me").await.unwrap();
        assert!(engine.delete_blob(&gone).await.unwrap());
        assert!(!engine.delete_blob(&gone).await.unwrap(), "idempotent");
        assert!(engine.get_blob(&gone).await.unwrap().is_none());
        engine.close().await.unwrap();
        (keep, gone)
    };
    let engine = Girder::open(config(dir.path())).await.unwrap();
    assert_eq!(
        engine.get_blob(&keep).await.unwrap().as_deref(),
        Some(b"keep me".as_slice()),
        "content + existence survive restart"
    );
    assert!(engine.get_blob(&gone).await.unwrap().is_none());
    assert_eq!(engine.stats().blobs, 1);
    // The deleted file is physically gone.
    assert!(!dir
        .path()
        .join("blobs")
        .join(&gone[0..2])
        .join(&gone)
        .exists());
}

/// Kill residue (a blob file never manifest-listed — the crash window
/// between file-rename and manifest-store) reads as absent and is swept by
/// the groomer; listed blobs are untouched by sweeps.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_blob_files_are_swept() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Girder::open(config(dir.path())).await.unwrap();
    let listed = engine.put_blob(b"real blob").await.unwrap();

    // Enact the kill residue BY CONSTRUCTION (B3 lesson): a valid blob file
    // on disk that no manifest lists.
    let orphan_content = b"orphaned bytes";
    let orphan_hash = {
        use std::io::Write as _;
        // sha256 via the engine's own id: store-then-delete leaves no file,
        // so compute by writing through a scratch engine instead.
        let scratch_dir = tempfile::tempdir().unwrap();
        let scratch = Girder::open(config(scratch_dir.path())).await.unwrap();
        let h = scratch.put_blob(orphan_content).await.unwrap();
        drop(scratch);
        let shard = dir.path().join("blobs").join(&h[0..2]);
        std::fs::create_dir_all(&shard).unwrap();
        let mut f = std::fs::File::create(shard.join(&h)).unwrap();
        f.write_all(orphan_content).unwrap();
        h
    };

    // Unlisted = absent, even though the file exists (manifest is truth).
    assert!(engine.get_blob(&orphan_hash).await.unwrap().is_none());

    engine.maintain().await.unwrap(); // groom pass sweeps orphans
    assert!(
        !dir.path()
            .join("blobs")
            .join(&orphan_hash[0..2])
            .join(&orphan_hash)
            .exists(),
        "orphan swept"
    );
    assert_eq!(
        engine.get_blob(&listed).await.unwrap().as_deref(),
        Some(b"real blob".as_slice()),
        "listed blob untouched by the sweep"
    );
}

/// Concurrent same-content puts race safely (single manifest lock): both
/// succeed, one blob, content correct.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_puts_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(Girder::open(config(dir.path())).await.unwrap());
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let e = engine.clone();
        tasks.push(tokio::spawn(async move {
            e.put_blob(b"contended content").await.unwrap()
        }));
    }
    let mut hashes = Vec::new();
    for t in tasks {
        hashes.push(t.await.unwrap());
    }
    hashes.dedup();
    assert_eq!(hashes.len(), 1);
    assert_eq!(engine.stats().blobs, 1);
    assert_eq!(
        engine.get_blob(&hashes[0]).await.unwrap().as_deref(),
        Some(b"contended content".as_slice())
    );
}

/// D9 (ruling D-7): the orphan sweep runs on every Nth maintenance tick —
/// tick 0 (the first tick after boot) sweeps, ticks 1..N-1 skip, tick N
/// sweeps again. Deterministic because the boot timer starts one interval
/// late: explicit `maintain()` calls own the tick numbering here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blob_sweep_honors_the_tick_divider() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.blob_sweep_every_n_ticks = 3;
    let engine = Girder::open(cfg).await.unwrap();

    let orphan = |content: &[u8]| {
        use sha2::{Digest as _, Sha256};
        let hash = format!("{:x}", Sha256::digest(content));
        let shard = dir.path().join("blobs").join(&hash[0..2]);
        std::fs::create_dir_all(&shard).unwrap();
        std::fs::write(shard.join(&hash), content).unwrap();
        dir.path().join("blobs").join(&hash[0..2]).join(hash)
    };

    // Tick 0: the boot sweep — removes the pre-existing orphan.
    let first = orphan(b"residue one");
    engine.maintain().await.unwrap();
    assert!(!first.exists(), "tick 0 sweeps at boot");

    // Ticks 1 and 2: within the divider window — the orphan survives.
    let second = orphan(b"residue two");
    engine.maintain().await.unwrap();
    assert!(second.exists(), "tick 1 must not sweep (divider 3)");
    engine.maintain().await.unwrap();
    assert!(second.exists(), "tick 2 must not sweep (divider 3)");

    // Tick 3: the divider fires again.
    engine.maintain().await.unwrap();
    assert!(!second.exists(), "tick 3 sweeps (divider 3)");
}
