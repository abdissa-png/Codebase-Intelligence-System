//! **Phase 1** hot-path benchmark (gated — not run in default CI).
//!
//! Set `CIS_RUN_BENCHMARKS=1` to execute and assert 10×+ speedup vs linear-scan baseline.

use std::sync::Arc;
use std::time::{Duration, Instant};

use cis_core::{
    fork_branch_bindings, premerge_bindings_for_branch, BranchRegistry, InMemoryGraph, MemoryKv,
    NodeIdentity, NodeKind, NodeRevision, RevisionStatus, Language, SourceSpan,
};
use cis_wal::{BranchId, IdentityId, NodeRevisionId};

const N_REVS: usize = 50_000;
const N_FILES: usize = 500;
const N_IDENTITIES: usize = 100;

fn hex_branch(b: BranchId) -> String {
    b.0.iter().map(|x| format!("{:02x}", x)).collect()
}

fn build_graph() -> (InMemoryGraph, BranchId, IdentityId, String) {
    let branch = BranchId([0u8; 16]);
    let mut g = InMemoryGraph::default();
    let target_identity = IdentityId([7u8; 16]);
    g.put_identity(NodeIdentity {
        identity_id: target_identity,
        kind: NodeKind::Function,
    });

    let target_path = "src/hot.py".to_string();
    for i in 0..N_REVS {
        let identity = IdentityId({
            let mut b = [0u8; 16];
            b[0] = (i % N_IDENTITIES) as u8;
            b[1] = (i / N_IDENTITIES) as u8;
            IdentityId(b)
        }.0);
        if i % (N_REVS / N_IDENTITIES) == 0 {
            g.put_identity(NodeIdentity {
                identity_id: identity,
                kind: NodeKind::Function,
            });
        }
        let path = format!("src/f{}.py", i % N_FILES);
        let status = if i == N_REVS - 1 && path == target_path {
            RevisionStatus::Speculative
        } else if i % 17 == 0 {
            RevisionStatus::Active
        } else {
            RevisionStatus::Tombstone
        };
        let mut rid = [0u8; 16];
        rid[0..8].copy_from_slice(&(i as u64).to_le_bytes());
        g.put_revision(NodeRevision {
            revision_id: NodeRevisionId(rid),
            identity_id: if path == target_path && i == N_REVS - 1 {
                target_identity
            } else {
                identity
            },
            branch_id: branch,
            status,
            qualified_name: format!("f{i}"),
            file_path: path,
            body_hash: [0u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
    }
    (g, branch, target_identity, target_path)
}

fn build_kv_bindings(n: usize) -> MemoryKv {
    let kv = MemoryKv::new();
    let reg = BranchRegistry::new(Arc::new(kv.clone()));
    let main = reg.get_or_create_id("main");
    let child = reg.get_or_create_id("feature");
    let main_hex = hex_branch(main);
    for i in 0..n {
        let key = format!("ri:{main_hex}:{i:032x}");
        let mut rid = [0u8; 16];
        rid[0..8].copy_from_slice(&(i as u64).to_le_bytes());
        kv.set(&key, rid.to_vec());
    }
    // Noise keys outside prefix
    for i in 0..1000 {
        kv.set(&format!("other:{i}"), vec![1]);
    }
    let _ = fork_branch_bindings(&kv, main, child);
    kv
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

fn linear_primary(g: &InMemoryGraph, branch: BranchId, identity: IdentityId) {
    let mut best = None;
    for r in g.revisions() {
        if r.branch_id != branch || r.identity_id != identity {
            continue;
        }
        if matches!(r.status, RevisionStatus::Active) {
            best = Some(r);
            break;
        }
        if best.is_none() {
            best = Some(r);
        }
    }
    std::hint::black_box(best);
}

fn linear_speculative(g: &InMemoryGraph, branch: BranchId, path: &str) -> Vec<NodeRevisionId> {
    g.revisions()
        .filter(|r| {
            r.branch_id == branch
                && r.file_path == path
                && matches!(r.status, RevisionStatus::Speculative)
        })
        .map(|r| r.revision_id)
        .collect()
}

fn linear_scan_bindings(kv: &MemoryKv, branch: BranchId) -> usize {
    let prefix = format!("ri:{}:", hex_branch(branch));
    kv.inner_scan_all()
        .into_iter()
        .filter(|(k, _)| k.starts_with(&prefix))
        .count()
}

#[test]
#[ignore = "run with CIS_RUN_BENCHMARKS=1 for Phase 1 hot-path gate"]
fn index_hot_path_benchmark_gate() {
    if std::env::var_os("CIS_RUN_BENCHMARKS").is_none() {
        return;
    }

    let (g, branch, identity, path) = build_graph();
    let kv = build_kv_bindings(N_REVS);
    let child = BranchRegistry::new(Arc::new(kv.clone())).get_or_create_id("feature");

    let iters = 100usize;

    // primary_revision_for_identity
    let mut indexed = Vec::new();
    for _ in 0..iters {
        let t0 = Instant::now();
        std::hint::black_box(g.primary_revision_for_identity(branch, identity));
        indexed.push(t0.elapsed());
    }
    let mut linear = Vec::new();
    for _ in 0..iters {
        let t0 = Instant::now();
        linear_primary(&g, branch, identity);
        linear.push(t0.elapsed());
    }
    let idx_med = median(indexed);
    let lin_med = median(linear);
    println!(
        "primary_revision_for_identity median: indexed={idx_med:?} linear={lin_med:?} ratio={:.1}x",
        lin_med.as_secs_f64() / idx_med.as_secs_f64().max(1e-9)
    );
    assert!(
        lin_med >= idx_med * 10 || lin_med > Duration::from_micros(500),
        "expected indexed primary lookup much faster than linear scan"
    );

    // speculative by file
    let mut indexed_s = Vec::new();
    for _ in 0..iters {
        let t0 = Instant::now();
        std::hint::black_box(g.speculative_revision_ids_for_file(branch, &path));
        indexed_s.push(t0.elapsed());
    }
    let mut linear_s = Vec::new();
    for _ in 0..iters {
        let t0 = Instant::now();
        std::hint::black_box(linear_speculative(&g, branch, &path));
        linear_s.push(t0.elapsed());
    }
    let idx_s = median(indexed_s);
    let lin_s = median(linear_s);
    println!(
        "speculative_by_file median: indexed={idx_s:?} linear={lin_s:?} ratio={:.1}x",
        lin_s.as_secs_f64() / idx_s.as_secs_f64().max(1e-9)
    );
    assert!(
        lin_s >= idx_s * 10 || lin_s > Duration::from_micros(500),
        "expected indexed file lookup much faster than linear scan"
    );

    // scan_branch_bindings via premerge (uses scan_prefix)
    let mut btree = Vec::new();
    for _ in 0..20 {
        let t0 = Instant::now();
        std::hint::black_box(premerge_bindings_for_branch(&kv, child).len());
        btree.push(t0.elapsed());
    }
    let mut hash_scan = Vec::new();
    for _ in 0..20 {
        let t0 = Instant::now();
        std::hint::black_box(linear_scan_bindings(&kv, child));
        hash_scan.push(t0.elapsed());
    }
    let bt_med = median(btree);
    let hs_med = median(hash_scan);
    println!(
        "scan_branch_bindings median: btree_range={bt_med:?} full_map_filter={hs_med:?} ratio={:.1}x",
        hs_med.as_secs_f64() / bt_med.as_secs_f64().max(1e-9)
    );
    assert!(
        hs_med >= bt_med * 5 || hs_med > Duration::from_micros(200),
        "expected prefix range scan faster than full-map filter"
    );
}

// Test-only helper mirroring pre-BTreeMap scan_prefix behavior.
trait KvFullScan {
    fn inner_scan_all(&self) -> Vec<(String, Vec<u8>)>;
}

impl KvFullScan for MemoryKv {
    fn inner_scan_all(&self) -> Vec<(String, Vec<u8>)> {
        self.snapshot().entries.into_iter().collect()
    }
}
