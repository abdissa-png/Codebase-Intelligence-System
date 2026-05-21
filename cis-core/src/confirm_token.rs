//! **ConfirmTokenBackend** — xattr vs sidecar (**RC-7**, **M-2**, §01.5).
//!
//! The confirm token is a nonce written alongside an agent-written file so the FS-sync
//! watcher can recognize CIS-originated writes and promote them from `Speculative` to `Active`.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmBackendMode {
    XattrPreferred,
    SidecarFallback,
}

// ── Public trait ────────────────────────────────────────────────────────────

/// Abstraction over the storage mechanism for per-file confirm tokens.
pub trait ConfirmTokenBackend: Send + Sync + std::fmt::Debug {
    /// Write (or overwrite) the confirm token for a file identified by `nonce`.
    fn write_token(&self, nonce: &str, payload: &[u8]) -> io::Result<()>;
    /// Read back the token for `nonce`. Returns `None` if not found.
    fn read_token(&self, nonce: &str) -> io::Result<Option<Vec<u8>>>;
    /// Remove the token for `nonce` (cleanup after promote or revert).
    fn clear_token(&self, nonce: &str) -> io::Result<()>;
    /// Display name for logging.
    fn mode_label(&self) -> &'static str;
    /// Optional fault injector (**Phase 4**); default none.
    fn fault_injector(&self) -> Option<&dyn crate::fault_injection::FaultInjector> {
        None
    }
}

/// Write a confirm token with bounded retry (3 attempts, 50/100/200 ms backoff).
pub fn write_token_with_retry(
    backend: &dyn ConfirmTokenBackend,
    nonce: &str,
    payload: &[u8],
) -> io::Result<()> {
    const BACKOFF_MS: [u64; 3] = [50, 100, 200];
    let mut last_err = None;
    for (attempt, &delay) in BACKOFF_MS.iter().enumerate() {
        if let Some(inj) = backend.fault_injector() {
            crate::fault_injection::apply_fault_io(inj.before_confirm_token_write())?;
        }
        match backend.write_token(nonce, payload) {
            Ok(()) => return Ok(()),
            Err(e) => {
                last_err = Some(e);
                if attempt + 1 < BACKOFF_MS.len() {
                    thread::sleep(Duration::from_millis(delay));
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| {
        io::Error::new(io::ErrorKind::Other, "confirm token write failed")
    }))
}

/// Clear a confirm token with bounded retry (3 attempts, 50/100/200 ms backoff).
pub fn clear_token_with_retry(backend: &dyn ConfirmTokenBackend, nonce: &str) -> io::Result<()> {
    const BACKOFF_MS: [u64; 3] = [50, 100, 200];
    let mut last_err = None;
    for (attempt, &delay) in BACKOFF_MS.iter().enumerate() {
        match backend.clear_token(nonce) {
            Ok(()) => return Ok(()),
            Err(e) => {
                last_err = Some(e);
                if attempt + 1 < BACKOFF_MS.len() {
                    thread::sleep(Duration::from_millis(delay));
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| {
        io::Error::new(io::ErrorKind::Other, "confirm token clear failed")
    }))
}

// ── Sidecar backend ─────────────────────────────────────────────────────────

/// Sidecar file backend: uses `.cis_confirm_<nonce>` files next to the repo root.
/// Uses an atomic write (`.tmp` → rename) for durability (**RC-7**).
#[derive(Debug, Clone)]
pub struct SidecarConfirmBackend {
    repo_root: PathBuf,
}

impl SidecarConfirmBackend {
    pub fn new(repo_root: PathBuf) -> Self {
        Self { repo_root }
    }

    pub fn sidecar_path(&self, nonce: &str) -> PathBuf {
        self.repo_root.join(format!(".cis_confirm_{}", nonce))
    }
}

impl ConfirmTokenBackend for SidecarConfirmBackend {
    fn write_token(&self, nonce: &str, payload: &[u8]) -> io::Result<()> {
        let final_path = self.sidecar_path(nonce);
        let tmp_path = self.repo_root.join(format!(".cis_confirm_{}.tmp", nonce));
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            f.write_all(payload)?;
            f.flush()?;
            f.sync_all()?;
        }
        fs::rename(&tmp_path, &final_path)?;
        Ok(())
    }

    fn read_token(&self, nonce: &str) -> io::Result<Option<Vec<u8>>> {
        let p = self.sidecar_path(nonce);
        match fs::read(&p) {
            Ok(v) => Ok(Some(v)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn clear_token(&self, nonce: &str) -> io::Result<()> {
        let p = self.sidecar_path(nonce);
        match fs::remove_file(&p) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn mode_label(&self) -> &'static str {
        "sidecar"
    }
}

// ── Fault-injecting wrapper (**Phase 4**) ────────────────────────────────────

/// Wraps a confirm backend with optional fault injection on token writes.
pub struct FaultInjectingConfirmBackend {
    inner: std::sync::Arc<Box<dyn ConfirmTokenBackend>>,
    injector: std::sync::Arc<dyn crate::fault_injection::FaultInjector>,
}

impl std::fmt::Debug for FaultInjectingConfirmBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultInjectingConfirmBackend")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl FaultInjectingConfirmBackend {
    pub fn new(
        inner: std::sync::Arc<Box<dyn ConfirmTokenBackend>>,
        injector: std::sync::Arc<dyn crate::fault_injection::FaultInjector>,
    ) -> Self {
        Self { inner, injector }
    }
}

impl ConfirmTokenBackend for FaultInjectingConfirmBackend {
    fn write_token(&self, nonce: &str, payload: &[u8]) -> io::Result<()> {
        self.inner.write_token(nonce, payload)
    }

    fn read_token(&self, nonce: &str) -> io::Result<Option<Vec<u8>>> {
        self.inner.read_token(nonce)
    }

    fn clear_token(&self, nonce: &str) -> io::Result<()> {
        self.inner.clear_token(nonce)
    }

    fn mode_label(&self) -> &'static str {
        self.inner.mode_label()
    }

    fn fault_injector(&self) -> Option<&dyn crate::fault_injection::FaultInjector> {
        Some(self.injector.as_ref())
    }
}

// ── Probe and factory ────────────────────────────────────────────────────────

/// Choose the best available backend for `repo_root`.
/// Currently always returns `SidecarConfirmBackend` (xattr support is behind a feature flag).
pub fn probe_confirm_backend(repo_root: &Path) -> Box<dyn ConfirmTokenBackend> {
    let backend = SidecarConfirmBackend::new(repo_root.to_path_buf());
    eprintln!("cis: confirm_token backend = {}", backend.mode_label());
    Box::new(backend)
}

// ── Legacy free-function compat ───────────────────────────────────────────────

/// Sidecar atomic write: `.tmp` → rename (**RC-7**). Kept for callsite compat.
pub fn write_sidecar_confirm_token(repo_root: &Path, nonce: &str, payload: &[u8]) -> io::Result<()> {
    SidecarConfirmBackend::new(repo_root.to_path_buf()).write_token(nonce, payload)
}

/// Resolve published path for a sidecar (**M-2**).
#[allow(dead_code)]
pub fn sidecar_path(repo_root: &Path, nonce: &str) -> PathBuf {
    SidecarConfirmBackend::new(repo_root.to_path_buf()).sidecar_path(nonce)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sidecar_atomic_roundtrip() {
        let dir = std::env::temp_dir().join(format!("cis-confirm-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        write_sidecar_confirm_token(&dir, "n1", b"tok").unwrap();
        let p = sidecar_path(&dir, "n1");
        assert!(p.ends_with(".cis_confirm_n1"));
        assert_eq!(std::fs::read(&p).unwrap(), b"tok");
    }

    #[test]
    fn backend_trait_roundtrip() {
        let dir = std::env::temp_dir().join(format!("cis-confirm-trait-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let backend = SidecarConfirmBackend::new(dir.clone());

        backend.write_token("abc", b"hello").unwrap();
        assert_eq!(backend.read_token("abc").unwrap(), Some(b"hello".to_vec()));
        backend.clear_token("abc").unwrap();
        assert_eq!(backend.read_token("abc").unwrap(), None);
    }

    #[test]
    fn probe_returns_sidecar() {
        let dir = std::env::temp_dir().join(format!("cis-probe-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let b = probe_confirm_backend(&dir);
        assert_eq!(b.mode_label(), "sidecar");
    }
}
