//! **FR-3** query / ranking: `build_context`, **`hybrid_search`**, cancellation (**FR-3.8**).

use std::cmp::Ordering;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;

use crate::ranking_policy::RankingPolicySnapshot;

/// Tokenizer port (**FR-3.7**): named tokenizer with char/4 fallback via **`tokenizer_mode`**.
pub trait Tokenizer: Send + Sync {
    fn count_tokens(&self, text: &str) -> usize;
}

pub struct CharApproxTokenizer;

impl Tokenizer for CharApproxTokenizer {
    fn count_tokens(&self, text: &str) -> usize {
        (text.len() + 3) / 4
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueryMeta {
    pub policy_version: String,
    pub query_latency_ms: u64,
    pub node_count: usize,
    pub stale_count: usize,
    /// Orphaned revision rows observed for the query subject.
    /// Serde alias keeps older clients that still send/expect `speculative_count` for this slot.
    #[serde(alias = "speculative_count")]
    pub orphaned_count: usize,
    pub pruned_low_confidence_count: usize,
    pub tokenizer_mode: String,
    pub speculative_excluded_from_vector: bool,
    pub degraded_modes: Vec<String>,
    pub retrieval_confidence: f64,
    pub signature_drift_count: usize,
    pub dangling_edge_count: usize,
    pub background_reconciliation_pending: bool,
    pub merge_in_progress: bool,
    /// **FR-4.11** — WAL log id (hex/decimal) when query uses a time-travel snapshot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_at_commit: Option<String>,
    /// When true, time-travel results bind identities via RIS but read **`NodeRevision`** data from the live graph.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub time_travel_uses_live_graph: bool,
    /// Ingest pipeline mode used to produce the current graph snapshot.
    pub ingest_mode: String,
    /// Last known indexed symbol count (graph revisions).
    pub indexed_symbol_count: usize,
    /// Last known indexed edge count.
    pub indexed_edge_count: usize,
    /// Human-readable reason when a query runs in degraded mode (e.g. semantic search without embeddings).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub degraded_reason: Option<String>,
    /// True when `.git/MERGE_HEAD` exists (read-only signal; ADR 0007).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub git_merge_in_progress: bool,
}

impl Default for QueryMeta {
    fn default() -> Self {
        Self {
            policy_version: "2.0".into(),
            query_latency_ms: 0,
            node_count: 0,
            stale_count: 0,
            orphaned_count: 0,
            pruned_low_confidence_count: 0,
            tokenizer_mode: "char_approximation".into(),
            speculative_excluded_from_vector: true,
            degraded_modes: vec![],
            retrieval_confidence: 1.0,
            signature_drift_count: 0,
            dangling_edge_count: 0,
            background_reconciliation_pending: false,
            merge_in_progress: false,
            query_at_commit: None,
            time_travel_uses_live_graph: false,
            ingest_mode: "regex".into(),
            indexed_symbol_count: 0,
            indexed_edge_count: 0,
            degraded_reason: None,
            git_merge_in_progress: false,
        }
    }
}

#[derive(Debug)]
pub struct ContextRanker {
    policy: RankingPolicySnapshot,
}

impl ContextRanker {
    pub fn new(policy: RankingPolicySnapshot) -> Self {
        Self { policy }
    }

    /// Structural axis weight from active policy (**§01.2**).
    pub fn structural_weight(&self) -> f64 {
        self.policy.axis_weights.structural_proximity
    }
}

/// **FR-3.5** — exclude speculative identities from vector pool before rerank (stub API).
#[allow(dead_code)]
pub fn hybrid_search_excludes_speculative(_speculative: bool) -> bool {
    true
}

#[derive(Debug, Default)]
pub struct DedupShell;

impl DedupShell {
    /// **FR-3.6** session read-your-writes: speculative from active session ranks above committed (stub).
    pub fn prefer_session_speculative(&self, _session_id: u64, is_speculative: bool) -> i32 {
        if is_speculative {
            1
        } else {
            0
        }
    }
}

/// **FR-3.8** — cooperative query cancellation (host/MCP sets flag; engines poll).
#[derive(Debug, Clone)]
pub struct QueryCancelFlag(pub Arc<AtomicBool>);

impl QueryCancelFlag {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    pub fn cancel(&self) {
        self.0.store(true, AtomicOrdering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(AtomicOrdering::SeqCst)
    }
}

impl Default for QueryCancelFlag {
    fn default() -> Self {
        Self::new()
    }
}

/// **FR-3.1** / **M-6** — budget truncation with trailing annotation (char tokenizer approximation).
pub fn build_context_truncated(
    text: &str,
    budget_tokens: usize,
    tok: &dyn Tokenizer,
) -> (String, bool, usize) {
    if tok.count_tokens(text) <= budget_tokens {
        return (text.to_string(), false, 0);
    }
    let total = tok.count_tokens(text);
    let mut lo = 0usize;
    let mut hi = text.len();
    while lo + 1 < hi {
        let mid = (lo + hi) / 2;
        if tok.count_tokens(&text[..mid]) <= budget_tokens {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    let kept = tok.count_tokens(&text[..lo]);
    let omitted = total.saturating_sub(kept);
    let mut s = text[..lo].to_string();
    s.push_str(&format!("\n// ... [truncated, {omitted} tokens omitted]"));
    (s, true, omitted)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HybridSearchCandidate {
    pub id: String,
    pub vector_score: f64,
    pub structural_score: f64,
    pub speculative: bool,
}

/// **FR-3.5** — drop speculative before rerank, then axis-weighted score; keep top **`vector_top_k`**.
pub fn hybrid_search_rerank(
    mut candidates: Vec<HybridSearchCandidate>,
    policy: &RankingPolicySnapshot,
    vector_top_k: usize,
) -> Vec<HybridSearchCandidate> {
    candidates.retain(|c| !c.speculative);
    let w_sem = policy.axis_weights.semantic_similarity;
    let w_str = policy.axis_weights.structural_proximity;
    candidates.sort_by(|a, b| {
        let sa = a.vector_score * w_sem + a.structural_score * w_str;
        let sb = b.vector_score * w_sem + b.structural_score * w_str;
        sb.partial_cmp(&sa).unwrap_or(Ordering::Equal)
    });
    candidates.truncate(vector_top_k.max(1));
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ranking_policy::RankingPolicy;

    #[test]
    fn ranker_reads_policy() {
        let p = RankingPolicy::default().snapshot();
        let r = ContextRanker::new(p);
        assert!((r.structural_weight() - 0.40).abs() < 1e-9);
    }

    #[test]
    fn char_tokenizer() {
        let t = CharApproxTokenizer;
        assert_eq!(t.count_tokens("abcd"), 1);
    }

    #[test]
    fn hybrid_search_stub_excludes_speculative() {
        assert!(super::hybrid_search_excludes_speculative(true));
    }

    #[test]
    fn truncation_appends_comment() {
        let t = CharApproxTokenizer;
        let body = "a".repeat(400);
        let (s, ob, om) = build_context_truncated(&body, 10, &t);
        assert!(ob);
        assert!(om > 0);
        assert!(s.contains("// ... [truncated,"));
    }

    #[test]
    fn hybrid_rerank_drops_speculative() {
        let p = RankingPolicy::default().snapshot();
        let c = hybrid_search_rerank(
            vec![
                HybridSearchCandidate {
                    id: "a".into(),
                    vector_score: 1.0,
                    structural_score: 0.0,
                    speculative: true,
                },
                HybridSearchCandidate {
                    id: "b".into(),
                    vector_score: 0.5,
                    structural_score: 0.5,
                    speculative: false,
                },
            ],
            &p,
            5,
        );
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].id, "b");
    }

    #[test]
    fn cancel_flag_flip() {
        let f = QueryCancelFlag::new();
        assert!(!f.is_cancelled());
        f.cancel();
        assert!(f.is_cancelled());
    }
}
