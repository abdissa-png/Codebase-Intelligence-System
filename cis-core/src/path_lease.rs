//! Per-path leases (**US-04**, `PathLeaseManager`) + speculative path set for **EI-5** preflight.

use std::collections::HashMap;
use std::sync::Mutex;

use thiserror::Error;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SessionId(pub u64);

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LeaseError {
    #[error("409 conflict — path leased by session {holder:?}")]
    Conflict { holder: SessionId },
}

#[derive(Debug, Default)]
pub struct PathLeaseManager {
    leases: Mutex<HashMap<String, SessionId>>,
}

impl PathLeaseManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn acquire(&self, path: &str, session: SessionId) -> Result<(), LeaseError> {
        let mut g = self.leases.lock().unwrap();
        if let Some(&h) = g.get(path) {
            if h != session {
                return Err(LeaseError::Conflict { holder: h });
            }
        }
        g.insert(path.to_string(), session);
        Ok(())
    }

    pub fn release(&self, path: &str, session: SessionId) {
        let mut g = self.leases.lock().unwrap();
        if g.get(path) == Some(&session) {
            g.remove(path);
        }
    }

    pub fn holder(&self, path: &str) -> Option<SessionId> {
        self.leases.lock().unwrap().get(path).copied()
    }

    /// Active path leases (hardening / invariant checks).
    #[doc(hidden)]
    pub fn active_paths(&self) -> Vec<(String, SessionId)> {
        self.leases
            .lock()
            .unwrap()
            .iter()
            .map(|(p, s)| (p.clone(), *s))
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.leases.lock().unwrap().is_empty()
    }
}

/// Paths touched by unconfirmed speculative patches (agent write-back).
/// Reference-counted: multiple open patches on the same path increment the count.
#[derive(Debug, Default)]
pub struct SpeculativePathTracker {
    paths: Mutex<HashMap<String, u32>>,
}

impl SpeculativePathTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, path: impl Into<String>) {
        let mut g = self.paths.lock().unwrap();
        let key = path.into();
        *g.entry(key).or_insert(0) += 1;
    }

    pub fn unregister(&self, path: &str) {
        let mut g = self.paths.lock().unwrap();
        if let Some(count) = g.get_mut(path) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                g.remove(path);
            }
        }
    }

    pub fn intersects(&self, affected: &[String]) -> bool {
        let g = self.paths.lock().unwrap();
        affected.iter().any(|p| g.contains_key(p))
    }

    pub fn conflicting_paths(&self, affected: &[String]) -> Vec<String> {
        let g = self.paths.lock().unwrap();
        affected
            .iter()
            .filter(|p| g.contains_key(*p))
            .cloned()
            .collect()
    }

    /// Per-path reference counts (hardening / invariant checks).
    #[doc(hidden)]
    pub fn path_counts(&self) -> Vec<(String, u32)> {
        self.paths
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_serializes_same_path() {
        let m = PathLeaseManager::new();
        m.acquire("src/a.py", SessionId(1)).unwrap();
        assert!(matches!(
            m.acquire("src/a.py", SessionId(2)),
            Err(LeaseError::Conflict { .. })
        ));
        m.release("src/a.py", SessionId(1));
        m.acquire("src/a.py", SessionId(2)).unwrap();
    }

    #[test]
    fn spec_tracker_for_merge() {
        let t = SpeculativePathTracker::new();
        t.register("b.rs");
        assert!(t.intersects(&["b.rs".into()]));
        assert!(!t.intersects(&["c.rs".into()]));
    }

    #[test]
    fn spec_tracker_refcount_survives_partial_unregister() {
        let t = SpeculativePathTracker::new();
        t.register("src/a.py");
        t.register("src/a.py");
        t.unregister("src/a.py");
        assert!(
            t.intersects(&["src/a.py".into()]),
            "path should remain speculative while a second patch references it"
        );
        t.unregister("src/a.py");
        assert!(
            !t.intersects(&["src/a.py".into()]),
            "path should be cleared after all patches unregister"
        );
    }
}
