//! §01.1 confidence model (mechanical).

use crate::graph::{GraphEdge, SourceType};

#[inline]
pub fn clamp01(x: f64) -> f64 {
    x.clamp(0.0, 1.0)
}

/// `source_weight` **DERIVED** §01.1
#[inline]
pub fn source_weight(s: SourceType) -> f64 {
    match s {
        SourceType::Compiler => 1.0,
        SourceType::Lsp => 0.85,
        SourceType::Ast => 0.60,
        SourceType::Textual => 0.30,
    }
}

#[inline]
pub fn path_floor(s: SourceType) -> f64 {
    match s {
        SourceType::Compiler => 0.5,
        SourceType::Lsp => 0.4,
        SourceType::Ast => 0.25,
        SourceType::Textual => 0.1,
    }
}

#[inline]
pub fn confidence_overall(source: SourceType, freshness: f64, corroboration_boost: f64) -> f64 {
    clamp01(source_weight(source) * freshness * corroboration_boost)
}

/// `path_confidence(p) = max(FLOOR(min_source), ∏ edge_confidence)` **DERIVED** §01.1
pub fn path_confidence(edge_confs: &[f64], min_source: SourceType) -> f64 {
    let prod = edge_confs.iter().product::<f64>();
    prod.max(path_floor(min_source))
}

/// `freshness = max(MIN_FRESHNESS, exp(-Δt / τ))` **§01.1**
pub fn freshness_decay(last_validation_ms: i64, now_ms: u64, half_life_ms: u64) -> f64 {
    const MIN_FRESHNESS: f64 = 0.5;
    if half_life_ms == 0 {
        return 1.0;
    }
    let now = now_ms as i64;
    let delta = now.saturating_sub(last_validation_ms).max(0) as f64;
    let tau = half_life_ms as f64;
    (MIN_FRESHNESS.max((-delta / tau).exp())).clamp(0.0, 1.0)
}

/// Per-edge confidence from resolution metadata (**§01.1**).
pub fn edge_confidence(edge: &GraphEdge, now_ms: u64, half_life_ms: u64) -> f64 {
    let fresh = freshness_decay(edge.resolution.last_validation_ms, now_ms, half_life_ms);
    confidence_overall(edge.resolution.resolver, fresh, 1.0)
}

/// **FR-2.3**
pub fn node_confidence_from_inbound(inbound: &[f64], producer: SourceType) -> f64 {
    if inbound.is_empty() {
        source_weight(producer)
    } else {
        inbound.iter().copied().fold(0.0_f64, f64::max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_never_below_language_floor() {
        let p = path_confidence(&[0.01, 0.01], SourceType::Ast);
        assert!((p - path_floor(SourceType::Ast)).abs() < 1e-9);
    }

    #[test]
    fn empty_inbound_uses_producer() {
        let n = node_confidence_from_inbound(&[], SourceType::Lsp);
        assert!((n - 0.85).abs() < 1e-9);
    }

    #[test]
    fn freshness_never_below_min() {
        let f = freshness_decay(0, 10_000_000_000, 7 * 24 * 60 * 60 * 1000);
        assert!(f >= 0.5);
    }
}
