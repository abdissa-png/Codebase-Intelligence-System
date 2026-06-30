//! Fault injection harness for hardening tests (**Phase 4.1**).

use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use cis_wal::MergeId;

/// Action taken at an instrumented boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultAction {
    Continue,
    Panic,
    DelayMs(u64),
    ReturnErr,
}

/// Injectable fault points across WAL, graph, vector, KV, and confirm-token boundaries.
pub trait FaultInjector: Send + Sync {
    fn before_wal_append(&self) -> FaultAction {
        FaultAction::Continue
    }
    fn before_graph_commit(&self) -> FaultAction {
        FaultAction::Continue
    }
    fn before_vector_commit(&self) -> FaultAction {
        FaultAction::Continue
    }
    fn before_kv_write(&self, _key: &str) -> FaultAction {
        FaultAction::Continue
    }
    fn before_confirm_token_write(&self) -> FaultAction {
        FaultAction::Continue
    }
    fn before_saga_persist(&self, _merge_id: MergeId) -> FaultAction {
        FaultAction::Continue
    }
    /// `phase` is `"allocate"` or `"ready"` at the two CAS steps in [`crate::identity_cas::IdentityProvisionalCas`].
    fn before_identity_cas(&self, _phase: &str) -> FaultAction {
        FaultAction::Continue
    }
}

/// Default no-op injector — zero production behavior change.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoOpFaultInjector;

impl FaultInjector for NoOpFaultInjector {}

/// Panic after N total hook invocations (any hook counts).
#[derive(Debug)]
pub struct CrashAfterNCalls {
    remaining: AtomicUsize,
}

impl CrashAfterNCalls {
    pub fn new(n: usize) -> Self {
        Self {
            remaining: AtomicUsize::new(n),
        }
    }
}

impl FaultInjector for CrashAfterNCalls {
    fn before_wal_append(&self) -> FaultAction {
        self.tick()
    }
    fn before_graph_commit(&self) -> FaultAction {
        self.tick()
    }
    fn before_vector_commit(&self) -> FaultAction {
        self.tick()
    }
    fn before_kv_write(&self, _key: &str) -> FaultAction {
        self.tick()
    }
    fn before_confirm_token_write(&self) -> FaultAction {
        self.tick()
    }
    fn before_saga_persist(&self, _merge_id: MergeId) -> FaultAction {
        self.tick()
    }
    fn before_identity_cas(&self, _phase: &str) -> FaultAction {
        self.tick()
    }
}

impl CrashAfterNCalls {
    fn tick(&self) -> FaultAction {
        let prev = self.remaining.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            FaultAction::Panic
        } else {
            FaultAction::Continue
        }
    }
}

/// Random delay on every hook invocation (interleaving stress, not crash tests).
#[derive(Debug, Clone)]
pub struct RandomDelay {
    pub min_ms: u64,
    pub max_ms: u64,
    /// Fixed seed for reproducibility in tests.
    pub seed: u64,
}

impl RandomDelay {
    pub fn new(min_ms: u64, max_ms: u64, seed: u64) -> Self {
        Self {
            min_ms,
            max_ms: max_ms.max(min_ms),
            seed,
        }
    }

    fn delay(&self) -> FaultAction {
        let span = self.max_ms.saturating_sub(self.min_ms) + 1;
        let offset = (self.seed.wrapping_mul(6364136223846793005).wrapping_add(1)) % span;
        FaultAction::DelayMs(self.min_ms + offset)
    }
}

impl FaultInjector for RandomDelay {
    fn before_wal_append(&self) -> FaultAction {
        self.delay()
    }
    fn before_graph_commit(&self) -> FaultAction {
        self.delay()
    }
    fn before_vector_commit(&self) -> FaultAction {
        self.delay()
    }
    fn before_kv_write(&self, _key: &str) -> FaultAction {
        self.delay()
    }
    fn before_confirm_token_write(&self) -> FaultAction {
        self.delay()
    }
    fn before_saga_persist(&self, _merge_id: MergeId) -> FaultAction {
        self.delay()
    }
    fn before_identity_cas(&self, _phase: &str) -> FaultAction {
        self.delay()
    }
}

/// Random delay **only** at identity provisional CAS boundaries (widens ALLOCATING→READY window).
#[derive(Debug, Clone)]
pub struct IdentityCasDelay {
    pub min_ms: u64,
    pub max_ms: u64,
    pub seed: Arc<AtomicU64>,
}

impl IdentityCasDelay {
    pub fn new(min_ms: u64, max_ms: u64, seed: u64) -> Self {
        Self {
            min_ms,
            max_ms: max_ms.max(min_ms),
            seed: Arc::new(AtomicU64::new(seed)),
        }
    }

    fn delay(&self) -> FaultAction {
        let seed = self.seed.fetch_add(1, Ordering::Relaxed);
        let span = self.max_ms.saturating_sub(self.min_ms) + 1;
        let offset = (seed.wrapping_mul(6364136223846793005).wrapping_add(1)) % span;
        FaultAction::DelayMs(self.min_ms + offset)
    }
}

impl FaultInjector for IdentityCasDelay {
    fn before_identity_cas(&self, _phase: &str) -> FaultAction {
        self.delay()
    }
}

/// Which hooks should return `FaultAction::ReturnErr`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FailHooks(pub u8);

impl FailHooks {
    pub const WAL_APPEND: u8 = 1;
    pub const GRAPH_COMMIT: u8 = 2;
    pub const VECTOR_COMMIT: u8 = 4;
    pub const KV_WRITE: u8 = 8;
    pub const CONFIRM_TOKEN: u8 = 16;
    pub const SAGA_PERSIST: u8 = 32;
    pub const IDENTITY_CAS: u8 = 64;

    pub fn all() -> Self {
        Self(
            Self::WAL_APPEND
                | Self::GRAPH_COMMIT
                | Self::VECTOR_COMMIT
                | Self::KV_WRITE
                | Self::CONFIRM_TOKEN
                | Self::SAGA_PERSIST
                | Self::IDENTITY_CAS,
        )
    }

    pub fn contains(self, flag: u8) -> bool {
        self.0 & flag != 0
    }
}

/// Always fail selected hooks (error-path testing without real disk failures).
#[derive(Debug, Clone, Copy)]
pub struct AlwaysFail {
    pub hooks: FailHooks,
}

impl AlwaysFail {
    pub fn all() -> Self {
        Self {
            hooks: FailHooks::all(),
        }
    }
}

impl FaultInjector for AlwaysFail {
    fn before_wal_append(&self) -> FaultAction {
        if self.hooks.contains(FailHooks::WAL_APPEND) {
            FaultAction::ReturnErr
        } else {
            FaultAction::Continue
        }
    }
    fn before_graph_commit(&self) -> FaultAction {
        if self.hooks.contains(FailHooks::GRAPH_COMMIT) {
            FaultAction::ReturnErr
        } else {
            FaultAction::Continue
        }
    }
    fn before_vector_commit(&self) -> FaultAction {
        if self.hooks.contains(FailHooks::VECTOR_COMMIT) {
            FaultAction::ReturnErr
        } else {
            FaultAction::Continue
        }
    }
    fn before_kv_write(&self, _key: &str) -> FaultAction {
        if self.hooks.contains(FailHooks::KV_WRITE) {
            FaultAction::ReturnErr
        } else {
            FaultAction::Continue
        }
    }
    fn before_confirm_token_write(&self) -> FaultAction {
        if self.hooks.contains(FailHooks::CONFIRM_TOKEN) {
            FaultAction::ReturnErr
        } else {
            FaultAction::Continue
        }
    }
    fn before_saga_persist(&self, _merge_id: MergeId) -> FaultAction {
        if self.hooks.contains(FailHooks::SAGA_PERSIST) {
            FaultAction::ReturnErr
        } else {
            FaultAction::Continue
        }
    }
    fn before_identity_cas(&self, _phase: &str) -> FaultAction {
        if self.hooks.contains(FailHooks::IDENTITY_CAS) {
            FaultAction::ReturnErr
        } else {
            FaultAction::Continue
        }
    }
}

/// Apply a fault action; returns `Err` for `ReturnErr`, panics for `Panic`.
pub fn apply_fault(action: FaultAction) -> Result<(), FaultInjected> {
    match action {
        FaultAction::Continue => Ok(()),
        FaultAction::DelayMs(ms) => {
            thread::sleep(Duration::from_millis(ms));
            Ok(())
        }
        FaultAction::ReturnErr => Err(FaultInjected),
        FaultAction::Panic => panic!("fault injection: deliberate panic"),
    }
}

/// Apply fault and map to `io::Error` (confirm-token path).
pub fn apply_fault_io(action: FaultAction) -> io::Result<()> {
    match action {
        FaultAction::Continue => Ok(()),
        FaultAction::DelayMs(ms) => {
            thread::sleep(Duration::from_millis(ms));
            Ok(())
        }
        FaultAction::ReturnErr => Err(io::Error::new(
            io::ErrorKind::Other,
            "fault injection: deliberate I/O error",
        )),
        FaultAction::Panic => panic!("fault injection: deliberate panic"),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaultInjected;

impl std::fmt::Display for FaultInjected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("fault injection: deliberate error")
    }
}

impl std::error::Error for FaultInjected {}

/// Parse `CIS_FAULT_INJECTOR` env var for subprocess crash tests.
/// Formats: `crash_after:N`, `delay:min-max:seed`, `fail:wal|graph|vector|kv|token|all`
pub fn injector_from_env() -> Option<Arc<dyn FaultInjector>> {
    let spec = std::env::var_os("CIS_FAULT_INJECTOR")?;
    let spec = spec.to_string_lossy();
    if let Some(n) = spec.strip_prefix("crash_after:") {
        let n: usize = n.parse().ok()?;
        return Some(Arc::new(CrashAfterNCalls::new(n)));
    }
    if let Some(rest) = spec.strip_prefix("delay:") {
        let parts: Vec<&str> = rest.split(':').collect();
        if parts.len() >= 2 {
            let min_ms: u64 = parts[0].parse().ok()?;
            let max_ms: u64 = parts[1].parse().ok()?;
            let seed: u64 = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(42);
            return Some(Arc::new(RandomDelay::new(min_ms, max_ms, seed)));
        }
    }
    if let Some(rest) = spec.strip_prefix("fail:") {
        let hooks = match rest {
            "wal" => FailHooks(FailHooks::WAL_APPEND),
            "graph" => FailHooks(FailHooks::GRAPH_COMMIT),
            "vector" => FailHooks(FailHooks::VECTOR_COMMIT),
            "kv" => FailHooks(FailHooks::KV_WRITE),
            "token" => FailHooks(FailHooks::CONFIRM_TOKEN),
            "saga" => FailHooks(FailHooks::SAGA_PERSIST),
            "identity" | "identity_cas" => FailHooks(FailHooks::IDENTITY_CAS),
            "all" => FailHooks::all(),
            _ => return None,
        };
        return Some(Arc::new(AlwaysFail { hooks }));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_never_fails() {
        let inj = NoOpFaultInjector;
        assert_eq!(apply_fault(inj.before_wal_append()), Ok(()));
    }

    #[test]
    fn always_fail_wal() {
        let inj = AlwaysFail {
            hooks: FailHooks(FailHooks::WAL_APPEND),
        };
        assert_eq!(apply_fault(inj.before_wal_append()), Err(FaultInjected));
        assert_eq!(apply_fault(inj.before_graph_commit()), Ok(()));
    }

    #[test]
    #[should_panic(expected = "fault injection")]
    fn crash_after_n_panics() {
        let inj = CrashAfterNCalls::new(2);
        assert_eq!(apply_fault(inj.before_wal_append()), Ok(()));
        let _ = apply_fault(inj.before_graph_commit());
    }

    #[test]
    fn random_delay_does_not_panic() {
        let inj = RandomDelay::new(0, 1, 99);
        assert!(apply_fault(inj.before_kv_write("ri:aa:bb")).is_ok());
    }
}
