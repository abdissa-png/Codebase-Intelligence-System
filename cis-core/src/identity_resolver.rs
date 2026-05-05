//! **FR-1.11** — multi-signal rename confidence + **`RENAMED_FROM`** edge construction (**FR-1.12** tombstone aware).

use crate::graph::{EdgeResolution, EdgeType, GraphEdge, SourceType};
use cis_wal::{IdentityId, NodeRevisionId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenameSignalKind {
    LspRename,
    AstBodySimilarity,
    NameProximity,
}

#[derive(Debug, Clone)]
pub struct RenameEvidence {
    pub kind: RenameSignalKind,
    pub confidence: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct IdentityResolver {
    pub rename_min_confidence: f64,
}

impl Default for IdentityResolver {
    fn default() -> Self {
        Self {
            rename_min_confidence: 0.55,
        }
    }
}

impl IdentityResolver {
    pub fn from_policy(rename_min: f64) -> Self {
        Self {
            rename_min_confidence: rename_min,
        }
    }

    /// Fold independent signals (take max — conservative agreement that any strong signal counts).
    pub fn combined_confidence(signals: &[RenameEvidence]) -> f64 {
        signals
            .iter()
            .map(|s| s.confidence)
            .fold(0.0_f64, f64::max)
    }

    pub fn should_emit_rename(&self, signals: &[RenameEvidence]) -> bool {
        Self::combined_confidence(signals) >= self.rename_min_confidence
    }

    /// **`RENAMED_FROM`** from **tombstone** revision → successor **identity** (FR-1.11 evidence).
    pub fn renamed_from_edge(
        source_tombstone_revision: NodeRevisionId,
        successor_identity: IdentityId,
        evidence: RenameEvidence,
        edge_id: [u8; 16],
    ) -> GraphEdge {
        GraphEdge {
            edge_id,
            ty: EdgeType::RenamedFrom,
            source_revision_id: source_tombstone_revision,
            target_identity_id: successor_identity,
            resolution: EdgeResolution {
                target_signature_hash: [0u8; 32],
                resolver: match evidence.kind {
                    RenameSignalKind::LspRename => SourceType::Lsp,
                    RenameSignalKind::AstBodySimilarity => SourceType::Ast,
                    RenameSignalKind::NameProximity => SourceType::Textual,
                },
                last_validation_ms: 0,
            },
            anchor: crate::graph::SourceSpan::UNKNOWN,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combined_uses_max_signal() {
        let s = vec![
            RenameEvidence {
                kind: RenameSignalKind::NameProximity,
                confidence: 0.2,
            },
            RenameEvidence {
                kind: RenameSignalKind::AstBodySimilarity,
                confidence: 0.9,
            },
        ];
        assert!((IdentityResolver::combined_confidence(&s) - 0.9).abs() < 1e-9);
    }

    #[test]
    fn edge_kind_renamed_from() {
        let r = NodeRevisionId([1u8; 16]);
        let i = IdentityId([2u8; 16]);
        let e = IdentityResolver::renamed_from_edge(
            r,
            i,
            RenameEvidence {
                kind: RenameSignalKind::AstBodySimilarity,
                confidence: 0.8,
            },
            [7u8; 16],
        );
        assert_eq!(e.ty, EdgeType::RenamedFrom);
        assert_eq!(e.resolution.resolver, SourceType::Ast);
    }
}
