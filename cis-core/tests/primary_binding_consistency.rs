//! Graph primary and revision-index must agree: never bind tombstones.

use cis_core::{
    InMemoryGraph, Language, NodeIdentity, NodeKind, NodeRevision, RevisionStatus, SourceSpan,
};
use cis_wal::{BranchId, IdentityId, NodeRevisionId};

fn rid(x: u8) -> NodeRevisionId {
    let mut b = [0u8; 16];
    b[15] = x;
    NodeRevisionId(b)
}
fn iid(x: u8) -> IdentityId {
    let mut b = [0u8; 16];
    b[14] = x;
    IdentityId(b)
}

#[test]
fn primary_never_falls_back_to_tombstone() {
    let mut g = InMemoryGraph::default();
    let branch = BranchId([0u8; 16]);
    let id = iid(1);
    g.put_identity(NodeIdentity {
        identity_id: id,
        kind: NodeKind::Function,
    });
    let tomb = rid(1);
    g.put_revision(NodeRevision {
        revision_id: tomb,
        identity_id: id,
        branch_id: branch,
        status: RevisionStatus::Tombstone,
        qualified_name: "f".into(),
        file_path: "a.py".into(),
        body_hash: [1u8; 32],
        signature_hash: [2u8; 32],
        language: Language::Python,
        parent_revision_id: None,
        rename_source_id: None,
        tombstoned_at_ms: Some(1),
        span: SourceSpan::UNKNOWN,
    });
    assert!(
        g.primary_revision_for_identity(branch, id).is_none(),
        "tombstone-only identity must have no primary"
    );
}

#[test]
fn primary_prefers_active_over_speculative() {
    let mut g = InMemoryGraph::default();
    let branch = BranchId([0u8; 16]);
    let id = iid(2);
    g.put_identity(NodeIdentity {
        identity_id: id,
        kind: NodeKind::Function,
    });
    let spec = rid(1);
    let active = rid(2);
    for (rev, status) in [
        (spec, RevisionStatus::Speculative),
        (active, RevisionStatus::Active),
    ] {
        g.put_revision(NodeRevision {
            revision_id: rev,
            identity_id: id,
            branch_id: branch,
            status,
            qualified_name: "f".into(),
            file_path: "a.py".into(),
            body_hash: [1u8; 32],
            signature_hash: [2u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            tombstoned_at_ms: None,
            span: SourceSpan::UNKNOWN,
        });
    }
    let primary = g.primary_revision_for_identity(branch, id).unwrap();
    assert_eq!(primary.revision_id, active);
}
