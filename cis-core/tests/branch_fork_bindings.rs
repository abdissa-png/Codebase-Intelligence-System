//! **Phase 1.5** — forked branch sees 100% of parent `ri:` bindings without re-ingest.

use std::sync::Arc;

use cis_core::{
    fork_branch_bindings, premerge_bindings_for_branch, revision_binding_kv_key, BranchRegistry,
    MemoryKv,
};
use cis_wal::{BranchId, IdentityId, NodeRevisionId};

#[test]
fn forked_branch_inherits_all_parent_bindings() {
    let kv = Arc::new(MemoryKv::new());
    let reg = BranchRegistry::new(Arc::clone(&kv));
    let main = reg.get_or_create_id("main");
    let feature = reg.get_or_create_id("feature");

    for i in 0..12u8 {
        let identity = IdentityId([i; 16]);
        let revision = NodeRevisionId([i + 100; 16]);
        kv.set(
            &revision_binding_kv_key(main, identity),
            revision.0.to_vec(),
        );
    }

    let parent_bindings = premerge_bindings_for_branch(kv.as_ref(), main);
    assert_eq!(parent_bindings.len(), 12);

    let copied = fork_branch_bindings(kv.as_ref(), main, feature);
    assert_eq!(copied, 12);

    let child_bindings = premerge_bindings_for_branch(kv.as_ref(), feature);
    assert_eq!(child_bindings.len(), parent_bindings.len());
    for (identity, revision) in parent_bindings {
        let child_key = revision_binding_kv_key(feature, identity);
        let v = kv.get(&child_key).expect("child binding");
        assert_eq!(v, revision.0.to_vec());
    }
}

#[test]
fn fork_skips_existing_child_keys() {
    let kv = MemoryKv::new();
    let parent = BranchId([1u8; 16]);
    let child = BranchId([2u8; 16]);
    let identity = IdentityId([3u8; 16]);
    let parent_rev = NodeRevisionId([4u8; 16]);
    let child_rev = NodeRevisionId([5u8; 16]);
    kv.set(
        &revision_binding_kv_key(parent, identity),
        parent_rev.0.to_vec(),
    );
    kv.set(
        &revision_binding_kv_key(child, identity),
        child_rev.0.to_vec(),
    );
    assert_eq!(fork_branch_bindings(&kv, parent, child), 0);
    assert_eq!(
        kv.get(&revision_binding_kv_key(child, identity)),
        Some(child_rev.0.to_vec())
    );
}
