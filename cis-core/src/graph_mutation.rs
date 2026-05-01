//! Typed summary of work entering `WriteCoordinator` (from `ASTSubtreeDiff` in the full system).
//!
//! **DERIVED:** diff taxonomy §01.5; this crate only carries **identity** needed for WAL + phases.

use cis_wal::NodeRevisionId;

#[derive(Debug, Clone)]
pub struct GraphMutationSet {
    pub affected_revisions: Vec<NodeRevisionId>,
    /// Canonical checksum over serialized graph ops (NFR-SEC4 / replay integrity).
    pub payload_checksum: [u8; 32],
}

impl GraphMutationSet {
    pub fn new(affected_revisions: Vec<NodeRevisionId>, payload_checksum: [u8; 32]) -> Self {
        Self {
            affected_revisions,
            payload_checksum,
        }
    }
}
