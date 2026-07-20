//! **US-07 / US-08 / FR-4.4** — auth, audit, quotas (**FR-4.7**).

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::MemoryKv;

#[derive(Debug, Clone)]
pub struct Session {
    pub id: u64,
    pub admin: bool,
    pub repo_roots: Vec<String>,
}

#[derive(Debug, Default)]
pub struct AuthProvider {
    sessions: Mutex<HashMap<u64, Session>>,
}

impl AuthProvider {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, s: Session) {
        self.sessions.lock().unwrap().insert(s.id, s);
    }

    pub fn require_session(&self, id: u64) -> Result<Session, AuthError> {
        self.sessions
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or(AuthError::Unauthorized)
    }

    /// Require a registered session with `admin: true` for privileged tools.
    pub fn require_admin(&self, id: u64) -> Result<Session, AuthError> {
        let s = self.require_session(id)?;
        if !s.admin {
            return Err(AuthError::Forbidden);
        }
        Ok(s)
    }

    pub fn validate_path(&self, session_id: u64, path: &str) -> Result<(), AuthError> {
        let s = self.require_session(session_id)?;
        if path.contains("..") {
            return Err(AuthError::Forbidden);
        }
        for root in &s.repo_roots {
            if path.starts_with(root) {
                return Ok(());
            }
        }
        Err(AuthError::Forbidden)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    Unauthorized,
    Forbidden,
    /// Bad MCP arguments (e.g. invalid **`commit_hash`** / missing snapshot).
    InvalidInput,
    /// Lease / CAS / merge conflict (HTTP 409 class).
    Conflict,
    /// Patch/merge/coordinator state does not allow the operation.
    State,
    /// Persistence / WAL failure.
    Persist,
    /// **FR-4.7** — concurrent query quota exceeded (HTTP 429).
    QuotaExceeded,
    /// **FR-1.10** — disk pressure; refuse new speculative writes.
    DiskPressure,
    /// Coordinator not ready (e.g. WAL replay failed at startup).
    NotReady,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AuditRecord {
    pub ts_ms: u64,
    pub session_id: u64,
    pub action: String,
}

#[derive(Debug)]
pub struct AuditLog {
    local_queue: Mutex<Vec<AuditRecord>>,
}

impl AuditLog {
    pub fn new() -> Self {
        Self {
            local_queue: Mutex::new(Vec::new()),
        }
    }

    /// **SPF-3** — synchronous durable queue hook (memory stub; **FD queue** replaces in production).
    pub fn record_sync(&self, session_id: u64, action: impl Into<String>) {
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.local_queue.lock().unwrap().push(AuditRecord {
            ts_ms,
            session_id,
            action: action.into(),
        });
    }

    pub fn drained_local(&self) -> Vec<AuditRecord> {
        self.local_queue.lock().unwrap().drain(..).collect()
    }
}

/// **SPF-3** — append-only audit lines (**FD queue** contract).
#[derive(Debug)]
pub struct DurableAuditQueue {
    path: PathBuf,
}

impl DurableAuditQueue {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn append_record(&self, rec: &AuditRecord) -> std::io::Result<()> {
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let line = serde_json::to_string(rec).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
        })?;
        writeln!(f, "{line}")?;
        f.sync_all()?;
        Ok(())
    }
}

/// **`audit:`** hash-chain epoch (**US-08**); returns hex digest stored in KV.
pub fn append_audit_epoch(
    kv: &MemoryKv,
    epoch_id: u64,
    prev_chain_hex: &str,
    summary: &str,
) -> String {
    let digest = compute_audit_epoch_digest(epoch_id, prev_chain_hex, summary);
    let v = serde_json::json!({
        "prev": prev_chain_hex,
        "digest": digest,
        "summary": summary,
    })
    .to_string();
    kv.set(
        &format!("audit:epoch:{epoch_id:020}"),
        v.into_bytes(),
    );
    digest
}

fn compute_audit_epoch_digest(epoch_id: u64, prev_chain_hex: &str, summary: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    epoch_id.hash(&mut h);
    prev_chain_hex.hash(&mut h);
    summary.hash(&mut h);
    format!("{:016x}", h.finish())
}

/// Default epoch seal interval (**Phase 3.1**).
pub const DEFAULT_AUDIT_EPOCH_INTERVAL: Duration = Duration::from_secs(300);
/// Minimum records before time-based seal is skipped.
pub const DEFAULT_AUDIT_EPOCH_MIN_RECORDS: u64 = 1;

/// **Phase 3.1** — durable audit sink with hash-chain epoch sealing.
#[derive(Debug)]
pub struct ProductionAuditSink {
    queue: DurableAuditQueue,
    epoch_interval: Duration,
    epoch_min_records: u64,
    last_epoch_hash: Mutex<String>,
    next_epoch_id: AtomicU64,
    record_count_since_epoch: AtomicU64,
    last_seal_instant: Mutex<Instant>,
}

impl ProductionAuditSink {
    pub fn new(path: impl AsRef<Path>) -> Arc<Self> {
        Self::with_options(path, DEFAULT_AUDIT_EPOCH_INTERVAL, DEFAULT_AUDIT_EPOCH_MIN_RECORDS)
    }

    pub fn with_options(
        path: impl AsRef<Path>,
        epoch_interval: Duration,
        epoch_min_records: u64,
    ) -> Arc<Self> {
        if let Some(parent) = path.as_ref().parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        Arc::new(Self {
            queue: DurableAuditQueue::new(path),
            epoch_interval,
            epoch_min_records,
            last_epoch_hash: Mutex::new(String::new()),
            next_epoch_id: AtomicU64::new(1),
            record_count_since_epoch: AtomicU64::new(0),
            last_seal_instant: Mutex::new(Instant::now()),
        })
    }

    /// **SPF-3** — synchronous durable append (same signature as [`AuditLog::record_sync`]).
    pub fn record_sync(&self, session_id: u64, action: impl Into<String>) {
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let rec = AuditRecord {
            ts_ms,
            session_id,
            action: action.into(),
        };
        if let Err(e) = self.queue.append_record(&rec) {
            eprintln!("cis: audit append failed: {e}");
        } else {
            self.record_count_since_epoch.fetch_add(1, Ordering::SeqCst);
        }
    }

    pub fn record_count_since_epoch(&self) -> u64 {
        self.record_count_since_epoch.load(Ordering::SeqCst)
    }

    /// Seal an epoch when enough records or time have elapsed since the last seal.
    pub fn maybe_seal_epoch(&self, kv: &MemoryKv) -> Option<String> {
        let count = self.record_count_since_epoch.load(Ordering::SeqCst);
        let elapsed = self.last_seal_instant.lock().unwrap().elapsed();
        if count < self.epoch_min_records && elapsed < self.epoch_interval {
            return None;
        }
        let epoch_id = self.next_epoch_id.fetch_add(1, Ordering::SeqCst);
        let prev = self.last_epoch_hash.lock().unwrap().clone();
        let summary = format!("sealed {count} audit records");
        let digest = append_audit_epoch(kv, epoch_id, &prev, &summary);
        *self.last_epoch_hash.lock().unwrap() = digest.clone();
        self.record_count_since_epoch.store(0, Ordering::SeqCst);
        *self.last_seal_instant.lock().unwrap() = Instant::now();
        Some(digest)
    }

    pub fn force_seal_epoch(&self, kv: &MemoryKv, summary: &str) -> String {
        let epoch_id = self.next_epoch_id.fetch_add(1, Ordering::SeqCst);
        let prev = self.last_epoch_hash.lock().unwrap().clone();
        let digest = append_audit_epoch(kv, epoch_id, &prev, summary);
        *self.last_epoch_hash.lock().unwrap() = digest.clone();
        self.record_count_since_epoch.store(0, Ordering::SeqCst);
        *self.last_seal_instant.lock().unwrap() = Instant::now();
        digest
    }

    /// Restore epoch chain cursor from persisted `audit:epoch:*` KV rows after restart.
    pub fn resume_from_kv(&self, kv: &MemoryKv) {
        let mut epochs: Vec<(u64, String)> = kv
            .scan_prefix("audit:epoch:")
            .into_iter()
            .filter_map(|(k, v)| {
                let id = k.strip_prefix("audit:epoch:")?.parse().ok()?;
                let j: serde_json::Value = serde_json::from_slice(&v).ok()?;
                let digest = j.get("digest")?.as_str()?.to_string();
                Some((id, digest))
            })
            .collect();
        epochs.sort_by_key(|(id, _)| *id);
        if let Some((max_id, digest)) = epochs.last() {
            self.next_epoch_id
                .store(max_id.saturating_add(1), Ordering::SeqCst);
            *self.last_epoch_hash.lock().unwrap() = digest.clone();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditChainVerification {
    pub ok: bool,
    pub chain_length: usize,
    pub broken_at_epoch: Option<u64>,
    pub message: String,
}

/// Walk `audit:epoch:{id}` keys and verify hash-chain linkage.
pub fn verify_audit_chain(kv: &MemoryKv) -> AuditChainVerification {
    let mut epochs: Vec<(u64, serde_json::Value)> = kv
        .scan_prefix("audit:epoch:")
        .into_iter()
        .filter_map(|(k, v)| {
            let id = k.strip_prefix("audit:epoch:")?.parse().ok()?;
            let j: serde_json::Value = serde_json::from_slice(&v).ok()?;
            Some((id, j))
        })
        .collect();
    epochs.sort_by_key(|(id, _)| *id);
    if epochs.is_empty() {
        return AuditChainVerification {
            ok: true,
            chain_length: 0,
            broken_at_epoch: None,
            message: "no epochs sealed yet".into(),
        };
    }
    let mut expected_prev = String::new();
    for (epoch_id, j) in &epochs {
        let prev = j.get("prev").and_then(|v| v.as_str()).unwrap_or("");
        let digest = j.get("digest").and_then(|v| v.as_str()).unwrap_or("");
        if prev != expected_prev {
            return AuditChainVerification {
                ok: false,
                chain_length: epochs.len(),
                broken_at_epoch: Some(*epoch_id),
                message: format!("epoch {epoch_id}: prev mismatch (expected {expected_prev:?}, got {prev:?})"),
            };
        }
        let summary = j.get("summary").and_then(|v| v.as_str()).unwrap_or("");
        let recomputed = compute_audit_epoch_digest(*epoch_id, prev, summary);
        if recomputed != digest {
            return AuditChainVerification {
                ok: false,
                chain_length: epochs.len(),
                broken_at_epoch: Some(*epoch_id),
                message: format!("epoch {epoch_id}: digest mismatch"),
            };
        }
        expected_prev = digest.to_string();
    }
    AuditChainVerification {
        ok: true,
        chain_length: epochs.len(),
        broken_at_epoch: None,
        message: format!("verified {} epoch(s)", epochs.len()),
    }
}

/// RAII guard releasing a query slot on drop (**Phase 3.5**).
pub struct QuotaGuard<'a> {
    tracker: &'a QuotaTracker,
    session_id: u64,
    active: bool,
}

impl<'a> QuotaGuard<'a> {
    pub fn acquire(tracker: &'a QuotaTracker, session_id: u64) -> Result<Self, QuotaError> {
        tracker.acquire_query(session_id)?;
        Ok(Self {
            tracker,
            session_id,
            active: true,
        })
    }
}

impl Drop for QuotaGuard<'_> {
    fn drop(&mut self) {
        if self.active {
            self.tracker.release_query(self.session_id);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaError {
    /// **FR-4.7** — maps to HTTP 429 for MCP layer.
    TooManyConcurrentQueries,
}

#[derive(Debug, Default)]
pub struct QuotaTracker {
    max_concurrent_queries: u32,
    active: Mutex<HashMap<u64, u32>>,
}

impl QuotaTracker {
    pub fn new(max_concurrent_queries: u32) -> Self {
        Self {
            max_concurrent_queries,
            active: Mutex::new(HashMap::new()),
        }
    }

    pub fn acquire_query(&self, session_id: u64) -> Result<(), QuotaError> {
        let mut g = self.active.lock().unwrap();
        let n = g.entry(session_id).or_insert(0);
        if *n >= self.max_concurrent_queries {
            return Err(QuotaError::TooManyConcurrentQueries);
        }
        *n += 1;
        Ok(())
    }

    pub fn release_query(&self, session_id: u64) {
        let mut g = self.active.lock().unwrap();
        if let Some(x) = g.get_mut(&session_id) {
            *x = x.saturating_sub(1);
        }
    }

    pub fn active_count(&self) -> u32 {
        self.active
            .lock()
            .unwrap()
            .values()
            .copied()
            .sum()
    }

    pub fn max_concurrent(&self) -> u32 {
        self.max_concurrent_queries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_path_rejects_dotdot() {
        let a = AuthProvider::new();
        a.register(Session {
            id: 1,
            admin: false,
            repo_roots: vec!["/repo".into()],
        });
        assert!(a.validate_path(1, "/repo/../etc").is_err());
    }

    #[test]
    fn audit_fifo() {
        let al = AuditLog::new();
        al.record_sync(9, "write_file");
        let r = al.drained_local();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].session_id, 9);
    }

    #[test]
    fn durable_audit_append() {
        let dir = std::env::temp_dir().join(format!("cis-audit-{}", std::process::id()));
        let _ = std::fs::remove_file(&dir);
        let q = DurableAuditQueue::new(&dir);
        q.append_record(&AuditRecord {
            ts_ms: 1,
            session_id: 2,
            action: "x".into(),
        })
        .unwrap();
        let s = std::fs::read_to_string(&dir).unwrap();
        assert!(s.contains("\"x\"") || s.contains("x"));
    }

    #[test]
    fn audit_chain_kv() {
        let kv = MemoryKv::new();
        let d = append_audit_epoch(&kv, 1, "genesis", "op1");
        assert!(!d.is_empty());
        assert!(kv.get("audit:epoch:00000000000000000001").is_some());
    }

    #[test]
    fn production_audit_sink_durable() {
        let dir = std::env::temp_dir().join(format!("cis-prod-audit-{}", std::process::id()));
        let path = dir.join("audit.jsonl");
        let _ = std::fs::remove_dir_all(&dir);
        let sink = ProductionAuditSink::new(&path);
        sink.record_sync(1, "write_file");
        drop(sink);
        let sink2 = ProductionAuditSink::new(&path);
        sink2.record_sync(2, "confirm_patch");
        let s = std::fs::read_to_string(&path).unwrap();
        assert!(s.contains("write_file"));
        assert!(s.contains("confirm_patch"));
    }

    #[test]
    fn production_audit_epoch_seal() {
        let kv = MemoryKv::new();
        let dir = std::env::temp_dir().join(format!("cis-epoch-{}", std::process::id()));
        let path = dir.join("audit.jsonl");
        let sink = ProductionAuditSink::with_options(&path, Duration::from_secs(0), 1);
        sink.record_sync(0, "op");
        let d1 = sink.maybe_seal_epoch(&kv).expect("seal");
        assert!(!d1.is_empty());
        sink.record_sync(0, "op2");
        let d2 = sink.maybe_seal_epoch(&kv).expect("seal2");
        assert_ne!(d1, d2);
        let v = verify_audit_chain(&kv);
        assert!(v.ok, "{}", v.message);
        assert_eq!(v.chain_length, 2);
    }

    #[test]
    fn production_audit_resume_from_kv() {
        let kv = MemoryKv::new();
        let d1 = append_audit_epoch(&kv, 1, "", "epoch1");
        append_audit_epoch(&kv, 2, &d1, "epoch2");
        let dir = std::env::temp_dir().join(format!("cis-resume-{}", std::process::id()));
        let sink = ProductionAuditSink::new(dir.join("audit.jsonl"));
        sink.resume_from_kv(&kv);
        sink.record_sync(0, "after_resume");
        let d3 = sink.maybe_seal_epoch(&kv).expect("seal after resume");
        let v = verify_audit_chain(&kv);
        assert!(v.ok, "{}", v.message);
        assert_eq!(v.chain_length, 3);
        assert_ne!(d3, d1);
    }

    #[test]
    fn verify_audit_chain_detects_tamper() {
        let kv = MemoryKv::new();
        let _ = append_audit_epoch(&kv, 1, "", "genesis");
        kv.set(
            "audit:epoch:00000000000000000001",
            br#"{"prev":"","digest":"bad","summary":"genesis"}"#.to_vec(),
        );
        let v = verify_audit_chain(&kv);
        assert!(!v.ok);
    }

    #[test]
    fn quota_429_semantics() {
        let q = QuotaTracker::new(1);
        assert!(q.acquire_query(1).is_ok());
        assert_eq!(q.acquire_query(1), Err(QuotaError::TooManyConcurrentQueries));
        q.release_query(1);
        assert!(q.acquire_query(1).is_ok());
    }

    #[test]
    fn quota_guard_raii() {
        let q = QuotaTracker::new(1);
        {
            let _g = QuotaGuard::acquire(&q, 7).unwrap();
            assert_eq!(q.acquire_query(7), Err(QuotaError::TooManyConcurrentQueries));
        }
        assert!(q.acquire_query(7).is_ok());
    }
}
