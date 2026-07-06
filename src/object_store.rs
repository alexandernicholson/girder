//! The remote-tier seam (SCALE-1, docs/SCALE.md). Girder defines this trait
//! and NEVER implements a network client: the consumer injects an
//! implementation (rivet bridges its sigv4/HTTP stack to it), so girder gains
//! ZERO dependencies — no AWS SDK, no HTTP, no credentials. Keys are
//! girder-chosen flat strings (the segment filename, `seg-<seq>.gird`, whose
//! sequence numbers are never reused — so an upload is idempotent by
//! construction). The store treats them as opaque.
//!
//! Signatures are blocking to match girder's internal `std::fs` segment I/O
//! (reads already block under the actor runtime); the injected implementation
//! bridges to its async client the same way other consumers of a blocking seam
//! do. No store injected ⇒ no remote tier exists and no code path changes
//! (`Girder::open_with_object_store` is the only entry that wires one).
//!
//! Security posture (stated, not implied): remote objects carry FULL trace
//! payloads. Encryption-at-rest and bucket access control are the DEPLOYMENT
//! boundary — girder ships the bytes to the store the operator names and
//! trusts it to be private (self-host doctrine; cross-ref the rivet P2
//! compliance page). Girder adds no application-layer encryption of its own.

use crate::error::Result;
use std::sync::Arc;

/// A remote object store for the aged-segment tier. Implementations live
/// OUTSIDE girder and are injected via [`crate::Girder::open_with_object_store`].
pub trait ObjectStore: Send + Sync {
    /// Upload `bytes` under `key`, overwriting. Idempotent for girder's use:
    /// segment keys are content-stable (never-reused sequence numbers).
    fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()>;
    /// Fetch the object, or `None` if the key is absent.
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    /// Delete the object. A delete of an absent key is not an error (a retried
    /// retention drop must converge).
    fn delete(&self, key: &str) -> Result<()>;
    /// List keys under `prefix` — used at open to reconcile orphan objects a
    /// crash mid-retention-drop may have stranded.
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
}

/// Convenience alias for the injected handle.
pub type ObjectStoreRef = Arc<dyn ObjectStore>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// A minimal in-memory `ObjectStore` — proves the seam is implementable and
    /// is the fake SCALE-1b's move/pull tests will drive the tier through.
    #[derive(Default)]
    struct MemObjectStore(Mutex<HashMap<String, Vec<u8>>>);

    impl ObjectStore for MemObjectStore {
        fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
            self.0.lock().unwrap().insert(key.to_string(), bytes);
            Ok(())
        }
        fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            Ok(self.0.lock().unwrap().get(key).cloned())
        }
        fn delete(&self, key: &str) -> Result<()> {
            self.0.lock().unwrap().remove(key);
            Ok(())
        }
        fn list(&self, prefix: &str) -> Result<Vec<String>> {
            let mut ks: Vec<String> = self
                .0
                .lock()
                .unwrap()
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();
            ks.sort();
            Ok(ks)
        }
    }

    #[test]
    fn seam_round_trips_and_delete_is_idempotent() {
        let store: ObjectStoreRef = Arc::new(MemObjectStore::default());
        assert_eq!(store.get("seg-1.gird").unwrap(), None);
        store.put("seg-1.gird", vec![1, 2, 3]).unwrap();
        store.put("seg-2.gird", vec![4]).unwrap();
        assert_eq!(store.get("seg-1.gird").unwrap(), Some(vec![1, 2, 3]));
        // Idempotent put (never-reused keys ⇒ content-stable overwrite).
        store.put("seg-1.gird", vec![1, 2, 3]).unwrap();
        assert_eq!(
            store.list("seg-").unwrap(),
            vec!["seg-1.gird", "seg-2.gird"]
        );
        store.delete("seg-1.gird").unwrap();
        store.delete("seg-1.gird").unwrap(); // absent delete is not an error
        assert_eq!(store.get("seg-1.gird").unwrap(), None);
        assert_eq!(store.list("seg-").unwrap(), vec!["seg-2.gird"]);
    }

    #[test]
    fn config_defaults_leave_the_remote_tier_off() {
        let cfg = crate::GirderConfig::at("/tmp/does-not-matter");
        // Never-tier-to-remote until injected + a real ttl set.
        assert_eq!(cfg.remote_ttl_nanos, i64::MAX / 2);
        // Pull cache defaults to the section-cache budget.
        assert_eq!(cfg.pull_cache_bytes, cfg.cache_bytes);
    }
}
