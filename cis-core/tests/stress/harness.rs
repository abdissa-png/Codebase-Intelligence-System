//! Shared stress-test harness (**Phase 4.2**).

use std::collections::HashSet;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex, OnceLock};
use std::thread;

use cis_core::{
    check_all_invariants_with_mode, CisMcpRuntime, FaultInjector, InvariantCheckMode,
    InvariantReport, NoOpFaultInjector, OptimisticPatcher, PathLeaseManager,
    SpeculativePathTracker,
};

pub struct StressConfig {
    pub n_threads: usize,
    pub ops_per_thread: usize,
}

impl StressConfig {
    pub fn fast() -> Self {
        Self {
            n_threads: 4,
            ops_per_thread: 12,
        }
    }

    pub fn full() -> Self {
        Self {
            n_threads: 16,
            ops_per_thread: 50,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OpRecord {
    pub thread: usize,
    pub op_index: usize,
    pub label: String,
}

pub struct StressHarness {
    pub rt: Arc<CisMcpRuntime>,
    pub dir: tempfile::TempDir,
    pub op_log: Arc<Mutex<Vec<OpRecord>>>,
    pub failures: Arc<Mutex<Vec<String>>>,
}

impl StressHarness {
    pub fn new() -> Self {
        Self::with_injector(Arc::new(NoOpFaultInjector))
    }

    pub fn with_injector(injector: Arc<dyn FaultInjector>) -> Self {
        std::env::set_var("CIS_WAL_MEMORY", "1");
        std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");
        std::env::set_var("CIS_SKIP_MERGE_RECOVER", "1");
        let dir = tempfile::tempdir().expect("tempdir");
        let rt = Arc::new(CisMcpRuntime::new_dev_with_fault_injector(
            &dir.path().to_string_lossy(),
            injector,
        ));
        Self {
            rt,
            dir,
            op_log: Arc::new(Mutex::new(Vec::new())),
            failures: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn repo(&self) -> &std::path::Path {
        self.dir.path()
    }

    pub fn log(&self, thread: usize, op_index: usize, label: impl Into<String>) {
        self.op_log.lock().unwrap().push(OpRecord {
            thread,
            op_index,
            label: label.into(),
        });
    }

    pub fn record_failure(&self, msg: impl Into<String>) {
        self.failures.lock().unwrap().push(msg.into());
    }

    pub fn invariant_report_with_mode(&self, mode: InvariantCheckMode) -> InvariantReport {
        let branch = self.rt.active_branch();
        let branches = [branch];
        let ctx = self.rt.invariant_context(&branches);
        check_all_invariants_with_mode(&ctx, mode)
    }

    pub fn invariant_report(&self) -> InvariantReport {
        self.invariant_report_with_mode(InvariantCheckMode::Strict)
    }

    pub fn invariants_dirty(&self) -> bool {
        !self.invariant_report().is_clean()
    }

    fn fail_invariants(
        &self,
        report: &InvariantReport,
        context: &str,
        shrink: Option<&mut dyn FnMut(&[OpRecord]) -> bool>,
    ) {
        if report.is_clean() {
            return;
        }
        self.dump_op_log();
        if let Some(repro) = shrink {
            shrink_op_log(self, repro);
        }
        assert!(
            false,
            "{context} ({} ops logged): {}",
            self.op_log.lock().unwrap().len(),
            report.summary()
        );
    }

    pub fn assert_invariants(&self) {
        let report = self.invariant_report();
        self.fail_invariants(&report, "invariant violation", None);
    }

    pub fn assert_invariants_shrink<F>(&self, mut reproduce: F)
    where
        F: FnMut(&[OpRecord]) -> bool,
    {
        let report = self.invariant_report();
        self.fail_invariants(&report, "invariant violation", Some(&mut reproduce));
    }

    /// Print the full op trace (used when invariant checks fail).
    pub fn dump_op_log(&self) {
        let log = self.op_log.lock().unwrap();
        eprintln!("stress op log ({} entries):", log.len());
        for op in log.iter() {
            eprintln!("  t{} op{}: {}", op.thread, op.op_index, op.label);
        }
    }

    /// Per-op invariant check: all threads record failures (no panic before quiescence).
    pub fn check_invariants_after_op(&self, thread: usize, mode: InvariantCheckMode) {
        static CHECK: OnceLock<Mutex<()>> = OnceLock::new();
        let _guard = CHECK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let report = self.invariant_report_with_mode(mode);
        if !report.is_clean() {
            self.record_failure(format!(
                "t{thread} per-op ({mode:?}): {}",
                report.summary()
            ));
        }
    }

    pub fn assert_invariants_after_op(&self) {
        static CHECK: OnceLock<Mutex<()>> = OnceLock::new();
        let _guard = CHECK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let report = self.invariant_report();
        self.fail_invariants(&report, "per-op invariant violation", None);
    }

    /// Lease/patcher alignment for isolated lease-storm scenarios (no graph).
    pub fn lease_storm_violation(
        leases: &PathLeaseManager,
        patcher: &OptimisticPatcher,
        spec: &SpeculativePathTracker,
    ) -> Option<String> {
        let patch_paths: HashSet<String> = patcher.all_open_paths().into_iter().collect();
        for (path, _session) in leases.active_paths() {
            if !patch_paths.contains(&path) {
                return Some(format!("lease without patch: {path}"));
            }
        }
        for (path, tracker_count) in spec.path_counts() {
            let patch_refs = patcher.path_refcounts().get(&path).copied().unwrap_or(0);
            if tracker_count != patch_refs {
                return Some(format!(
                    "spec tracker mismatch on {path}: tracker={tracker_count} patch={patch_refs}"
                ));
            }
        }
        None
    }

    pub fn check_lease_storm_after_op(
        &self,
        quiesce: &Barrier,
        gate: &Barrier,
        thread: usize,
        leases: &PathLeaseManager,
        patcher: &OptimisticPatcher,
        spec: &SpeculativePathTracker,
    ) {
        quiesce.wait();
        if let Some(msg) = Self::lease_storm_violation(leases, patcher, spec) {
            self.record_failure(format!("t{thread} lease per-op: {msg}"));
        }
        gate.wait();
    }

    /// End-of-op quiescence, optional `ri:` sync, invariant check on all threads, then gate
    /// so the next op cannot start until every thread finishes checking.
    pub fn quiesce_then_check_after_op(
        &self,
        quiesce: &Barrier,
        gate: &Barrier,
        thread: usize,
        mode: InvariantCheckMode,
        sync_before_check: bool,
    ) {
        quiesce.wait();
        if sync_before_check && thread == 0 {
            self.rt.sync_revision_index_from_graph();
        }
        if sync_before_check {
            gate.wait();
        }
        self.check_invariants_after_op(thread, mode);
        gate.wait();
    }

    pub fn quiesce_sync_then_check_after_op(
        &self,
        quiesce: &Barrier,
        gate: &Barrier,
        thread: usize,
        mode: InvariantCheckMode,
    ) {
        self.quiesce_then_check_after_op(quiesce, gate, thread, mode, true);
    }

    pub fn assert_lease_storm_invariants(
        leases: &PathLeaseManager,
        patcher: &OptimisticPatcher,
        spec: &SpeculativePathTracker,
    ) {
        if let Some(msg) = Self::lease_storm_violation(leases, patcher, spec) {
            panic!("{msg}");
        }
    }

    /// Per-op invariant check after all threads reach the quiescence barrier.
    pub fn quiesce_then_assert_after_op(
        &self,
        quiesce: &Barrier,
        gate: &Barrier,
        thread: usize,
    ) {
        self.quiesce_then_check_after_op(
            quiesce,
            gate,
            thread,
            InvariantCheckMode::Strict,
            false,
        );
    }

    pub fn run_barrier<F>(&self, n_threads: usize, f: F)
    where
        F: Fn(Arc<Self>, usize, Arc<Barrier>, Arc<Barrier>, Arc<Barrier>) + Send + Sync + 'static,
    {
        let harness = Arc::new(StressHarness {
            rt: Arc::clone(&self.rt),
            dir: tempfile::tempdir().expect("placeholder"),
            op_log: Arc::clone(&self.op_log),
            failures: Arc::clone(&self.failures),
        });
        let start = Arc::new(Barrier::new(n_threads));
        let quiesce = Arc::new(Barrier::new(n_threads));
        let sync_done = Arc::new(Barrier::new(n_threads));
        let func = Arc::new(f);
        let mut handles = Vec::with_capacity(n_threads);
        for t in 0..n_threads {
            let h = Arc::clone(&harness);
            let s = Arc::clone(&start);
            let q = Arc::clone(&quiesce);
            let sd = Arc::clone(&sync_done);
            let ff = Arc::clone(&func);
            handles.push(thread::spawn(move || ff(h, t, s, q, sd)));
        }
        for handle in handles {
            handle.join().expect("thread join");
        }
        let fails = harness.failures.lock().unwrap().clone();
        assert!(fails.is_empty(), "stress failures: {:?}", fails);
    }

    /// Non-panicking lease-storm alignment check (for shrink reproducers).
    pub fn lease_storm_invariants_dirty(
        leases: &PathLeaseManager,
        patcher: &OptimisticPatcher,
        spec: &SpeculativePathTracker,
    ) -> bool {
        Self::lease_storm_violation(leases, patcher, spec).is_some()
    }
}

pub fn seed_file(repo: &std::path::Path, rel: &str, content: &str) {
    let p = repo.join(rel);
    if let Some(parent) = p.parent() {
        let _ = fs::create_dir_all(parent);
    }
    fs::write(p, content).unwrap();
}

pub fn path_pool(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("stress/p_{}.py", i)).collect()
}

pub fn identity_cas_delay_injector(seed: u64) -> Arc<dyn FaultInjector> {
    Arc::new(cis_core::IdentityCasDelay::new(0, 3, seed))
}

pub static OP_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn next_op_id() -> usize {
    OP_COUNTER.fetch_add(1, Ordering::SeqCst)
}

pub fn shrink_op_log<F>(harness: &StressHarness, mut reproduce: F)
where
    F: FnMut(&[OpRecord]) -> bool,
{
    let log = harness.op_log.lock().unwrap().clone();
    if log.is_empty() {
        return;
    }
    let mut lo = 1usize;
    let mut hi = log.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if reproduce(&log[..mid]) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    eprintln!(
        "minimal reproducing prefix ({} ops): {:?}",
        lo,
        &log[..lo]
    );
}
