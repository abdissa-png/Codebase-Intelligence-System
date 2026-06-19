//! In-process graph lock: `Mutex` by default; `RwLock` when `CIS_GRAPH_RWLOCK=1` (Phase 3 perf).

use std::ops::{Deref, DerefMut};
use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::graph::InMemoryGraph;

/// When `CIS_GRAPH_RWLOCK=1`, use a read-write lock so concurrent queries can share the graph.
pub fn graph_rwlock_enabled() -> bool {
    std::env::var_os("CIS_GRAPH_RWLOCK").is_some_and(|v| v == "1")
}

pub struct SharedInMemoryGraph {
    inner: Inner,
}

impl std::fmt::Debug for SharedInMemoryGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mode = if graph_rwlock_enabled() {
            "rwlock"
        } else {
            "mutex"
        };
        f.debug_struct("SharedInMemoryGraph").field("mode", &mode).finish()
    }
}

enum Inner {
    Mutex(Mutex<InMemoryGraph>),
    RwLock(RwLock<InMemoryGraph>),
}

pub enum GraphReadGuard<'a> {
    Mutex(MutexGuard<'a, InMemoryGraph>),
    RwLock(RwLockReadGuard<'a, InMemoryGraph>),
}

pub enum GraphWriteGuard<'a> {
    Mutex(MutexGuard<'a, InMemoryGraph>),
    RwLock(RwLockWriteGuard<'a, InMemoryGraph>),
}

impl Deref for GraphReadGuard<'_> {
    type Target = InMemoryGraph;

    fn deref(&self) -> &Self::Target {
        match self {
            GraphReadGuard::Mutex(g) => g,
            GraphReadGuard::RwLock(g) => g,
        }
    }
}

impl Deref for GraphWriteGuard<'_> {
    type Target = InMemoryGraph;

    fn deref(&self) -> &Self::Target {
        match self {
            GraphWriteGuard::Mutex(g) => g,
            GraphWriteGuard::RwLock(g) => g,
        }
    }
}

impl DerefMut for GraphWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            GraphWriteGuard::Mutex(g) => g,
            GraphWriteGuard::RwLock(g) => g,
        }
    }
}

impl SharedInMemoryGraph {
    pub fn new(graph: InMemoryGraph) -> Self {
        let inner = if graph_rwlock_enabled() {
            Inner::RwLock(RwLock::new(graph))
        } else {
            Inner::Mutex(Mutex::new(graph))
        };
        Self { inner }
    }

    pub fn read(&self) -> GraphReadGuard<'_> {
        match &self.inner {
            Inner::Mutex(m) => GraphReadGuard::Mutex(m.lock().unwrap()),
            Inner::RwLock(r) => GraphReadGuard::RwLock(r.read().unwrap()),
        }
    }

    pub fn write(&self) -> GraphWriteGuard<'_> {
        match &self.inner {
            Inner::Mutex(m) => GraphWriteGuard::Mutex(m.lock().unwrap()),
            Inner::RwLock(r) => GraphWriteGuard::RwLock(r.write().unwrap()),
        }
    }

    /// Legacy path: exclusive lock (same as [`write`] when using `Mutex`; blocks writers under `RwLock`).
    pub fn lock(&self) -> GraphWriteGuard<'_> {
        self.write()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rwlock_env_gate() {
        std::env::remove_var("CIS_GRAPH_RWLOCK");
        assert!(!graph_rwlock_enabled());
        std::env::set_var("CIS_GRAPH_RWLOCK", "1");
        assert!(graph_rwlock_enabled());
        std::env::remove_var("CIS_GRAPH_RWLOCK");
    }
}
