//! **Phase 3 — Rename benchmark** (FR-1.4 exit criterion)
//!
//! Drives a labeled corpus through two rounds of ingest (initial → edited) and measures:
//! - `false_positive_rate = false_merges / negative_cases`  (must be < 0.05)
//! - `missed_rename_rate  = missed_links / positive_cases`  (tracked; no hard gate yet)
//!
//! All cases use a fixed `RenameConfig` to avoid policy-defaults sensitivity.

use std::sync::Arc;

use cis_core::{
    apply_index_events_with_config, stable_id_bytes, stable_rev_id_bytes, EdgeResolution, EdgeType,
    FsChangeKind,
    GraphEdge, IndexEvent, IndexEventQueue, MemoryKv, MergeSagaOrchestrator, RenameConfig,
    RevisionStatus, SourceSpan, SourceType, WriteCoordinator,
};
use cis_wal::{BranchId, IdentityId, MutationLog, NodeRevisionId};

const BRANCH: BranchId = BranchId([0u8; 16]);

/// Fixed thresholds for reproducible benchmark runs.
/// Uses realistic defaults — name proximity threshold is deliberately high so that two
/// functions sharing only a path prefix don't produce a false name signal.
fn bench_cfg() -> RenameConfig {
    RenameConfig {
        rename_min_confidence: 0.55,
        body_similarity_threshold: 0.55,
        name_proximity_threshold: 0.85,
        window_days: 30,
    }
}

fn fresh_coord() -> (Arc<WriteCoordinator>, Arc<MemoryKv>) {
    let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
    let coord = Arc::new(WriteCoordinator::new(Arc::clone(&wal)));
    let kv = Arc::new(MemoryKv::new());
    let _ = coord.reconcile_on_startup(&MergeSagaOrchestrator::new(Arc::new(MemoryKv::new())));
    (coord, kv)
}

fn ingest(coord: &WriteCoordinator, kv: Arc<MemoryKv>, path: &str, src: &str) {
    let q = IndexEventQueue::new();
    let s = src.to_string();
    apply_index_events_with_config(
        &q,
        coord,
        kv,
        vec![IndexEvent { branch_id: BRANCH, path: path.into(), kind: FsChangeKind::Modified, old_path: None }],
        move |_| Ok(s.clone()),
        None,
        None,
        Some(bench_cfg()),
    )
    .expect("ingest");
}

fn identity_of(coord: &WriteCoordinator, path: &str, name: &str) -> IdentityId {
    let g = coord.graph().read();
    // Prefer live revision for this symbol (append-only may not use stable_rev_id).
    let suffix = format!("::{name}");
    if let Some(r) = g.revisions().find(|r| {
        r.file_path == path
            && matches!(
                r.status,
                RevisionStatus::Active | RevisionStatus::Speculative
            )
            && (r.qualified_name.ends_with(&suffix) || r.qualified_name == name)
    }) {
        return r.identity_id;
    }
    let rev_id = NodeRevisionId(stable_rev_id_bytes(BRANCH, path, name));
    g.get_revision(rev_id)
        .map(|r| r.identity_id)
        .unwrap_or_else(|| IdentityId(stable_id_bytes("id", path, name)))
}

fn proposed_identity(path: &str, name: &str) -> IdentityId {
    IdentityId(stable_id_bytes("id", path, name))
}

fn _has_renamed_from(coord: &WriteCoordinator, tomb_path: &str, tomb_name: &str) -> bool {
    let tomb_rev = NodeRevisionId(stable_rev_id_bytes(BRANCH, tomb_path, tomb_name));
    let g = coord.graph().read();
    g.outbound_edges(tomb_rev)
        .iter()
        .any(|e| e.ty == EdgeType::RenamedFrom)
}

// ── Positive cases: rename should be detected ────────────────────────────────

/// Rename `foo` → `bar` with identical body; same file.
#[test]
fn pos_same_file_identical_body() {
    let (coord, kv) = fresh_coord();
    ingest(&coord, Arc::clone(&kv), "m.py", "def foo():\n    return 1\n");
    let iid = proposed_identity("m.py", "foo");
    ingest(&coord, kv, "m.py", "def bar():\n    return 1\n");
    assert_eq!(identity_of(&coord, "m.py", "bar"), iid);
}

/// Rename with trivial body change (adds a comment line).
#[test]
fn pos_same_file_minor_edit() {
    let (coord, kv) = fresh_coord();
    ingest(
        &coord,
        Arc::clone(&kv),
        "m.py",
        "def compute():\n    x = 1\n    y = 2\n    return x + y\n",
    );
    let iid = proposed_identity("m.py", "compute");
    ingest(
        &coord,
        kv,
        "m.py",
        "def calculate():\n    x = 1\n    y = 2\n    #return x + y\n",
    );
    assert_eq!(identity_of(&coord, "m.py", "calculate"), iid);
}

/// Cross-file body move with matching body.
#[test]
fn pos_cross_file_body_move() {
    let body = "def helper():\n    a = 10\n    b = 20\n    return a * b\n";
    let (coord, kv) = fresh_coord();
    ingest(&coord, Arc::clone(&kv), "a.py", body);
    let iid = proposed_identity("a.py", "helper");
    // Tombstone via Deleted — empty Modified ingest does not reliably clear symbols.
    let q = IndexEventQueue::new();
    apply_index_events_with_config(
        &q,
        &coord,
        Arc::clone(&kv),
        vec![IndexEvent {
            branch_id: BRANCH,
            path: "a.py".into(),
            kind: FsChangeKind::Deleted,
            old_path: None,
        }],
        |_| Err(std::io::Error::new(std::io::ErrorKind::NotFound, "deleted")),
        None,
        None,
        Some(bench_cfg()),
    )
    .expect("delete a.py");
    ingest(&coord, kv, "b.py", body);
    assert_eq!(identity_of(&coord, "b.py", "helper"), iid);
}

/// Rename with 80% body overlap.
#[test]
fn pos_same_file_high_overlap() {
    let (coord, kv) = fresh_coord();
    ingest(
        &coord,
        Arc::clone(&kv),
        "m.py",
        "def process():\n    data = load()\n    clean = preprocess(data)\n    return clean\n",
    );
    let iid = proposed_identity("m.py", "process");
    ingest(
        &coord,
        kv,
        "m.py",
        "def transform():\n    data = load()\n    clean = preprocess(data)\n    return clean\n",
    );
    assert_eq!(identity_of(&coord, "m.py", "transform"), iid);
}

// ── Negative cases: no false merge should occur ───────────────────────────────

/// New function with completely different name and body — must stay separate.
#[test]
fn neg_unrelated_new_function() {
    let (coord, kv) = fresh_coord();
    ingest(&coord, Arc::clone(&kv), "m.py", "def foo():\n    return 1\n");
    ingest(
        &coord,
        kv,
        "m.py",
        "def completely_unrelated(x, y, z):\n    return x * y - z\n",
    );
    let rev = NodeRevisionId(stable_rev_id_bytes(BRANCH, "m.py", "completely_unrelated"));
    let g = coord.graph().read();
    let rev = g.get_revision(rev).expect("new function");
    assert_eq!(rev.identity_id, proposed_identity("m.py", "completely_unrelated"));
}

/// Two functions present at the same time with similar names but different bodies.
#[test]
fn neg_sibling_functions_not_linked() {
    let (coord, kv) = fresh_coord();
    let src = "def get_pos(x):\n    return x\ndef get_str_pos(x):\n    return str(x)\n";
    ingest(&coord, Arc::clone(&kv), "m.py", src);
    ingest(&coord, kv, "m.py", src);
    let iid_a = proposed_identity("m.py", "get_pos");
    let iid_b = proposed_identity("m.py", "get_str_pos");
    assert_ne!(iid_a, iid_b);
    assert_eq!(identity_of(&coord, "m.py", "get_pos"), iid_a);
    assert_eq!(identity_of(&coord, "m.py", "get_str_pos"), iid_b);
}

/// Dissimilar bodies — different control flow; no rename link expected.
#[test]
fn neg_dissimilar_body_splits_identity() {
    let (coord, kv) = fresh_coord();
    ingest(&coord, Arc::clone(&kv), "m.py", "def foo():\n    pass\n");
    ingest(
        &coord,
        kv,
        "m.py",
        "def bar(self, a, b, c):\n    return self.matrix[a][b] * c + a\n",
    );
    let rev = NodeRevisionId(stable_rev_id_bytes(BRANCH, "m.py", "bar"));
    let g = coord.graph().read();
    let bar = g.get_revision(rev).expect("bar");
    assert_eq!(bar.identity_id, proposed_identity("m.py", "bar"));
    assert!(bar.rename_source_id.is_none());
}

/// Empty file after clear: no active symbols should remain.
#[test]
fn neg_empty_file_no_active() {
    let (coord, kv) = fresh_coord();
    ingest(
        &coord,
        Arc::clone(&kv),
        "m.py",
        "def a(): pass\ndef b(): pass\n",
    );
    ingest(&coord, kv, "m.py", "");
    let g = coord.graph().read();
    // The $file hub symbol remains Active; only Function symbols should be tombstoned.
    let active_fns: Vec<_> = g
        .revisions()
        .filter(|r| {
            r.file_path == "m.py"
                && matches!(r.status, RevisionStatus::Active)
                && !r.qualified_name.ends_with(".py") // skip file-hub
        })
        .collect();
    assert!(active_fns.is_empty(), "all function symbols should be tombstoned after clear, got: {:?}", active_fns.iter().map(|r| &r.qualified_name).collect::<Vec<_>>());
}

/// Rename to identical name (no-op ingest): identity unchanged.
#[test]
fn neg_unchanged_ingest_preserves_identity() {
    let (coord, kv) = fresh_coord();
    let src = "def stable_fn():\n    return 0\n";
    ingest(&coord, Arc::clone(&kv), "m.py", src);
    let iid_before = identity_of(&coord, "m.py", "stable_fn");
    ingest(&coord, kv, "m.py", src);
    let iid_after = identity_of(&coord, "m.py", "stable_fn");
    assert_eq!(iid_before, iid_after);
}

// ── File delete → tombstone ───────────────────────────────────────────────────

#[test]
fn delete_event_tombstones_all_file_symbols() {
    let (coord, kv) = fresh_coord();
    ingest(&coord, Arc::clone(&kv), "m.py", "def a(): pass\ndef b(): pass\n");

    // Now send a Deleted event
    let q = IndexEventQueue::new();
    apply_index_events_with_config(
        &q,
        &coord,
        Arc::clone(&kv),
        vec![IndexEvent { branch_id: BRANCH, path: "m.py".into(), kind: FsChangeKind::Deleted, old_path: None }],
        |_| Err(std::io::Error::new(std::io::ErrorKind::NotFound, "deleted")),
        None,
        None,
        Some(bench_cfg()),
    )
    .expect("delete ingest");

    let g = coord.graph().read();
    let active_fns: Vec<_> = g
        .revisions()
        .filter(|r| {
            r.file_path == "m.py"
                && matches!(r.status, RevisionStatus::Active)
                && !r.qualified_name.ends_with(".py")
        })
        .collect();
    assert!(
        active_fns.is_empty(),
        "all function symbols should be tombstoned after delete, got: {:?}",
        active_fns.iter().map(|r| &r.qualified_name).collect::<Vec<_>>()
    );
}

#[test]
fn delete_then_recreate_same_name_reuses_identity() {
    // Identity continuity: within window_days, re-creating a same-named function after
    // file deletion should restore the original identity via name-proximity rename signal.
    let (coord, kv) = fresh_coord();
    let path = "m.py";
    let src = "def worker():\n    x = 1\n    return x\n";
    ingest(&coord, Arc::clone(&kv), path, src);
    let original_iid = proposed_identity(path, "worker");

    let q = IndexEventQueue::new();
    apply_index_events_with_config(
        &q,
        &coord,
        Arc::clone(&kv),
        vec![IndexEvent { branch_id: BRANCH, path: path.into(), kind: FsChangeKind::Deleted, old_path: None }],
        |_| Err(std::io::Error::new(std::io::ErrorKind::NotFound, "deleted")),
        None,
        None,
        Some(bench_cfg()),
    )
    .expect("delete ingest");

    ingest(&coord, Arc::clone(&kv), path, src);
    let new_iid = identity_of(&coord, path, "worker");
    assert_eq!(
        new_iid, original_iid,
        "same-named function re-created after delete should reuse original identity"
    );
}

#[test]
fn delete_then_recreate_different_name_fresh_identity() {
    let (coord, kv) = fresh_coord();
    let path = "m.py";
    ingest(&coord, Arc::clone(&kv), path, "def worker():\n    pass\n");

    let q = IndexEventQueue::new();
    apply_index_events_with_config(
        &q,
        &coord,
        Arc::clone(&kv),
        vec![IndexEvent { branch_id: BRANCH, path: path.into(), kind: FsChangeKind::Deleted, old_path: None }],
        |_| Err(std::io::Error::new(std::io::ErrorKind::NotFound, "deleted")),
        None,
        None,
        Some(bench_cfg()),
    )
    .expect("delete ingest");

    // Completely different name and body: fresh identity, no rename link
    let new_src = "def completely_new_fn():\n    result = [x**2 for x in range(10)]\n    return result\n";
    ingest(&coord, Arc::clone(&kv), path, new_src);
    let new_iid = identity_of(&coord, path, "completely_new_fn");
    let worker_iid = proposed_identity(path, "worker");
    assert_ne!(
        new_iid, worker_iid,
        "different-named function after delete should get fresh identity"
    );
}

#[test]
fn delete_clears_calls_edges_but_keeps_renamed_from() {
    let (coord, kv) = fresh_coord();
    let path = "m.py";
    // Regex ingest does not extract Calls edges (tree-sitter only). Seed Calls +
    // RENAMED_FROM manually to exercise tombstone_all_file_symbols edge cleanup.
    ingest(&coord, Arc::clone(&kv), path, "def worker():\n    return 1\n");

    let worker_rev = NodeRevisionId(stable_rev_id_bytes(BRANCH, path, "worker"));
    let callee_iid = IdentityId(stable_id_bytes("id", path, "worker"));
    let successor_iid = IdentityId(stable_id_bytes("id", path, "worker_v2"));

    {
        let mut g = coord.graph().write();
        let calls = GraphEdge {
            edge_id: stable_id_bytes("cal", path, "worker->callee"),
            ty: EdgeType::Calls,
            source_revision_id: worker_rev,
            target_identity_id: callee_iid,
            resolution: EdgeResolution {
                target_signature_hash: [0u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: SourceSpan::UNKNOWN,
        };
        let renamed = GraphEdge {
            edge_id: stable_id_bytes("rn", path, "worker"),
            ty: EdgeType::RenamedFrom,
            source_revision_id: worker_rev,
            target_identity_id: successor_iid,
            resolution: EdgeResolution {
                target_signature_hash: [0u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: SourceSpan::UNKNOWN,
        };
        g.replace_edges_for_revision(worker_rev, vec![calls, renamed])
            .expect("seed outbound edges");
        assert_eq!(
            g.outbound_edges(worker_rev).len(),
            2,
            "precondition: worker should have Calls + RENAMED_FROM before delete"
        );
    }

    let q = IndexEventQueue::new();
    apply_index_events_with_config(
        &q,
        &coord,
        Arc::clone(&kv),
        vec![IndexEvent {
            branch_id: BRANCH,
            path: path.into(),
            kind: FsChangeKind::Deleted,
            old_path: None,
        }],
        |_| Err(std::io::Error::new(std::io::ErrorKind::NotFound, "deleted")),
        None,
        None,
        Some(bench_cfg()),
    )
    .expect("delete ingest");

    let g = coord.graph().read();
    let worker = g.get_revision(worker_rev).expect("worker revision");
    assert!(
        matches!(worker.status, RevisionStatus::Tombstone),
        "worker should be tombstoned after file delete"
    );
    let outbound = g.outbound_edges(worker_rev);
    assert!(
        !outbound.iter().any(|e| e.ty == EdgeType::Calls),
        "Calls edges should be cleared on tombstone, got {:?}",
        outbound
    );
    assert!(
        outbound.iter().any(|e| e.ty == EdgeType::RenamedFrom),
        "RENAMED_FROM should survive file-delete tombstone cleanup, got {:?}",
        outbound
    );
}

#[test]
fn re_index_orphan_cleanup() {
    let (coord, kv) = fresh_coord();
    ingest(
        &coord,
        Arc::clone(&kv),
        "m.py",
        "def alpha(): pass\ndef beta(): pass\n",
    );
    // Re-ingest with only alpha remaining (beta removed)
    ingest(&coord, kv, "m.py", "def alpha(): pass\n");

    let g = coord.graph().read();
    let beta_rev = NodeRevisionId(stable_rev_id_bytes(BRANCH, "m.py", "beta"));
    let beta = g.get_revision(beta_rev).expect("beta revision must exist");
    assert!(
        matches!(beta.status, RevisionStatus::Tombstone),
        "removed symbol should be tombstoned, got {:?}",
        beta.status
    );
    let alpha_rev = NodeRevisionId(stable_rev_id_bytes(BRANCH, "m.py", "alpha"));
    let alpha = g.get_revision(alpha_rev).expect("alpha revision must exist");
    assert!(
        matches!(alpha.status, RevisionStatus::Active),
        "retained symbol should stay Active, got {:?}",
        alpha.status
    );
}

// ── Aggregate metrics assertion ────────────────────────────────────────────────

/// Run all labeled cases through a single harness and assert FP rate < 5%.
///
/// Positive cases: expected rename link; negative: expected fresh identity.
#[test]
fn benchmark_false_positive_rate_lt_5pct() {
    struct Case {
        label: &'static str,
        path_before: &'static str,
        before: &'static str,
        path_after: &'static str,
        after: &'static str,
        new_sym: &'static str,
        positive: bool, // true = expect same identity; false = expect fresh identity
    }

    let cases: &[Case] = &[
        // — positives —
        Case {
            label: "same_file_identical",
            path_before: "m.py", before: "def foo():\n    return 1\n",
            path_after: "m.py", after: "def bar():\n    return 1\n",
            new_sym: "bar", positive: true,
        },
        Case {
            label: "same_file_minor_edit",
            path_before: "m.py",
            before: "def compute():\n    x = 1\n    y = 2\n    return x + y\n",
            path_after: "m.py",
            after: "def calculate():\n    x = 1\n    y = 2\n    return x + y\n",
            new_sym: "calculate", positive: true,
        },
        Case {
            label: "same_file_80pct_overlap",
            path_before: "m.py",
            before: "def process():\n    data = load()\n    clean = preprocess(data)\n    return clean\n",
            path_after: "m.py",
            after: "def transform():\n    data = load()\n    clean = preprocess(data)\n    return clean\n",
            new_sym: "transform", positive: true,
        },
        // — negatives —
        Case {
            label: "dissimilar_body",
            path_before: "m.py", before: "def foo():\n    pass\n",
            path_after: "m.py",
            after: "def bar(self, a, b, c):\n    return self.matrix[a][b] * c + a\n",
            new_sym: "bar", positive: false,
        },
        Case {
            label: "sibling_unchanged",
            path_before: "m.py",
            before: "def get_pos(x):\n    return x\ndef get_str_pos(x):\n    return str(x)\n",
            path_after: "m.py",
            after: "def get_pos(x):\n    return x\ndef get_str_pos(x):\n    return str(x)\n",
            new_sym: "get_pos", positive: false,
        },
        Case {
            label: "totally_unrelated",
            path_before: "m.py", before: "def alpha():\n    return 1\n",
            path_after: "m.py",
            after: "def omega(n, m):\n    result = [i*m for i in range(n)]\n    return sum(result)\n",
            new_sym: "omega", positive: false,
        },
        Case {
            label: "empty_after_clear",
            path_before: "m.py", before: "def a(): pass\n",
            path_after: "m.py", after: "def unrelated_z(): return 99\n",
            new_sym: "unrelated_z", positive: false,
        },
        Case {
            label: "near_name_different_body",
            path_before: "m.py",
            before: "def update_cache(key, val):\n    self.cache[key] = val\n    self.dirty = True\n",
            path_after: "m.py",
            after: "def update_index(node, weight):\n    self.heap.push((weight, node))\n    self.mapping[node] = weight\n",
            new_sym: "update_index", positive: false,
        },
    ];

    let total_positive = cases.iter().filter(|c| c.positive).count();
    let total_negative = cases.iter().filter(|c| !c.positive).count();
    let mut missed = 0usize;
    let mut false_merges = 0usize;

    for c in cases {
        let (coord, kv) = fresh_coord();
        ingest(&coord, Arc::clone(&kv), c.path_before, c.before);
        let _original_iid = proposed_identity(c.path_before, {
            // extract the first def name from `before`
            c.before
                .lines()
                .find(|l| l.starts_with("def "))
                .and_then(|l| l.strip_prefix("def "))
                .and_then(|l| l.split('(').next())
                .unwrap_or("_")
        });

        if c.path_after != c.path_before {
            ingest(&coord, Arc::clone(&kv), c.path_before, "");
        }
        ingest(&coord, kv, c.path_after, c.after);

        let got_iid = identity_of(&coord, c.path_after, c.new_sym);
        let expected_iid = proposed_identity(c.path_before, {
            c.before
                .lines()
                .find(|l| l.starts_with("def "))
                .and_then(|l| l.strip_prefix("def "))
                .and_then(|l| l.split('(').next())
                .unwrap_or("_")
        });
        let new_expected_iid = proposed_identity(c.path_after, c.new_sym);

        if c.positive {
            if got_iid != expected_iid {
                eprintln!("[MISSED_RENAME] {}", c.label);
                missed += 1;
            }
        } else {
            // For a sibling that wasn't removed, we only check it stayed unchanged
            let first_name = c.before
                .lines()
                .find(|l| l.starts_with("def "))
                .and_then(|l| l.strip_prefix("def "))
                .and_then(|l| l.split('(').next())
                .unwrap_or("_");
            let first_iid = proposed_identity(c.path_before, first_name);
            if got_iid == first_iid && got_iid != new_expected_iid {
                eprintln!("[FALSE_MERGE] {}: got {:?} but expected fresh {:?}", c.label, got_iid, new_expected_iid);
                false_merges += 1;
            }
        }
    }

    let fp_rate = false_merges as f64 / total_negative.max(1) as f64;
    let miss_rate = missed as f64 / total_positive.max(1) as f64;
    eprintln!(
        "rename_benchmark: positives={} missed={} ({:.1}%)  negatives={} false_merges={} FP={:.1}%",
        total_positive, missed, miss_rate * 100.0,
        total_negative, false_merges, fp_rate * 100.0
    );

    assert!(
        fp_rate < 0.05,
        "false_positive_rate {:.1}% >= 5% — tighten thresholds or fix body scoring",
        fp_rate * 100.0
    );
}
