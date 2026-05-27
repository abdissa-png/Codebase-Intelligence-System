//! **RankingPolicy** — **§01.2**, **US-02**, **US-10**, **FR-1.13**, **§01.4** (vector HWM/LWM), **FR-1.7**, **FR-1.10**.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

/// Immutable policy captured **at query start** (**US-02.2**).
pub type RankingPolicySnapshot = RankingPolicy;

#[derive(Debug, Clone)]
pub struct RankingPolicy {
    pub version: String,
    pub axis_weights: AxisWeights,
    pub edge_type_weights: EdgeTypeWeights,
    pub conflict_resolution: ConflictResolution,
    pub max_hops: u32,
    pub min_path_confidence: f64,
    pub hybrid_search: HybridSearchPolicy,
    pub recency: RecencyPolicy,
    pub dedup: DedupPolicy,
    pub budget: BudgetPolicy,
    /// **US-10 / §01.6.1** — rename detection tunables.
    pub rename_min_confidence: f64,
    pub body_similarity_threshold: f64,
    pub name_proximity_threshold: f64,
    pub rename_detection_window_days: u32,
    /// **FR-1.13 / v2.6** — tombstone + BodyStore TTL alignment.
    pub tombstone_retention_days: u32,
    /// **FR-1.13** — GC pin for time-travel snapshots.
    pub time_travel_retention_days: u32,
    /// **FR-1.7** — WAL compaction threshold (bytes).
    pub wal_max_bytes: u64,
    /// **FR-1.10** — disk pressure threshold (% free space floor).
    pub disk_min_free_pct: u8,
    /// **§01.6** — abandoned merge auto-cancel.
    pub merge_ttl_hours: u32,
    /// **§01.4 / v2.6** — embedding queue backpressure.
    pub embedding_queue_hwm: u32,
    pub embedding_queue_lwm: u32,
    /// **v2.6** — vector degraded mode clears only after LWM held this long (seconds).
    pub vector_recovery_debounce_s: u32,
    /// **NFR-C1** — soft cap for embedding spend signaling (tokens/day).
    pub embedding_tokens_per_day: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct AxisWeights {
    pub structural_proximity: f64,
    pub edge_type: f64,
    pub semantic_similarity: f64,
    pub recency: f64,
}

impl Default for AxisWeights {
    fn default() -> Self {
        Self {
            structural_proximity: 0.40,
            edge_type: 0.30,
            semantic_similarity: 0.20,
            recency: 0.10,
        }
    }
}

/// Per **§01.2** default `edge_type_weights` table.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct EdgeTypeWeights {
    pub calls: f64,
    pub imports: f64,
    pub extends: f64,
    pub uses: f64,
    pub configures: f64,
    pub co_located: f64,
    pub test_of: f64,
    pub renamed_from: f64,
}

impl Default for EdgeTypeWeights {
    fn default() -> Self {
        Self {
            calls: 1.00,
            imports: 0.90,
            extends: 0.85,
            uses: 0.65,
            configures: 0.70,
            co_located: 0.40,
            test_of: 0.75,
            renamed_from: 0.80,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct HybridSearchPolicy {
    #[serde(default = "default_hybrid_structural_threshold")]
    pub structural_threshold: f64,
    #[serde(default = "default_vector_top_k")]
    pub vector_top_k: u32,
}

impl Default for HybridSearchPolicy {
    fn default() -> Self {
        Self {
            structural_threshold: default_hybrid_structural_threshold(),
            vector_top_k: default_vector_top_k(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct RecencyPolicy {
    #[serde(default)]
    pub source: RecencySource,
    #[serde(default = "default_recency_half_life_days")]
    pub half_life_days: u32,
}

impl Default for RecencyPolicy {
    fn default() -> Self {
        Self {
            source: RecencySource::default(),
            half_life_days: default_recency_half_life_days(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RecencySource {
    #[default]
    CommitterTime,
    AuthorTime,
    IndexTime,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct DedupPolicy {
    #[serde(default)]
    pub strategy: DedupStrategy,
    #[serde(default)]
    pub overlap_definition: OverlapDefinition,
}

impl Default for DedupPolicy {
    fn default() -> Self {
        Self {
            strategy: DedupStrategy::default(),
            overlap_definition: OverlapDefinition::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DedupStrategy {
    #[default]
    PreferDefinitionSite,
    PreferCallSite,
    PreferRecent,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum OverlapDefinition {
    #[default]
    AstSubtreeContainment,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct BudgetPolicy {
    #[serde(default)]
    pub overshoot_policy: BudgetOvershootPolicy,
    #[serde(default = "default_tokenizer_name")]
    pub tokenizer: String,
}

impl Default for BudgetPolicy {
    fn default() -> Self {
        Self {
            overshoot_policy: BudgetOvershootPolicy::default(),
            tokenizer: default_tokenizer_name(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum BudgetOvershootPolicy {
    #[default]
    TruncateLowestScored,
    Reject,
    ReturnPartialWithWarning,
}

impl Default for RankingPolicy {
    fn default() -> Self {
        Self {
            version: "2.0".into(),
            axis_weights: AxisWeights::default(),
            edge_type_weights: EdgeTypeWeights::default(),
            conflict_resolution: ConflictResolution::WeightedSum,
            max_hops: default_max_hops(),
            min_path_confidence: default_min_path_confidence(),
            hybrid_search: HybridSearchPolicy::default(),
            recency: RecencyPolicy::default(),
            dedup: DedupPolicy::default(),
            budget: BudgetPolicy::default(),
            rename_min_confidence: default_rename_min_confidence(),
            body_similarity_threshold: default_body_similarity_threshold(),
            name_proximity_threshold: default_name_proximity_threshold(),
            rename_detection_window_days: default_rename_detection_window_days(),
            tombstone_retention_days: default_tombstone_retention_days(),
            time_travel_retention_days: default_time_travel_retention_days(),
            wal_max_bytes: default_wal_max_bytes(),
            disk_min_free_pct: default_disk_min_free_pct(),
            merge_ttl_hours: default_merge_ttl_hours(),
            embedding_queue_hwm: default_embedding_queue_hwm(),
            embedding_queue_lwm: default_embedding_queue_lwm(),
            vector_recovery_debounce_s: default_vector_recovery_debounce_s(),
            embedding_tokens_per_day: default_embedding_tokens_per_day(),
        }
    }
}

fn default_max_hops() -> u32 {
    3
}

fn default_min_path_confidence() -> f64 {
    0.20
}

fn default_hybrid_structural_threshold() -> f64 {
    0.35
}

fn default_vector_top_k() -> u32 {
    50
}

fn default_recency_half_life_days() -> u32 {
    30
}

fn default_rename_min_confidence() -> f64 {
    0.5
}

fn default_body_similarity_threshold() -> f64 {
    0.7
}

fn default_name_proximity_threshold() -> f64 {
    0.85
}

fn default_rename_detection_window_days() -> u32 {
    30
}

fn default_tombstone_retention_days() -> u32 {
    90
}

fn default_time_travel_retention_days() -> u32 {
    90
}

fn default_wal_max_bytes() -> u64 {
    256 * 1024 * 1024
}

fn default_disk_min_free_pct() -> u8 {
    10
}

fn default_merge_ttl_hours() -> u32 {
    24
}

fn default_embedding_queue_hwm() -> u32 {
    5000
}

fn default_embedding_queue_lwm() -> u32 {
    1000
}

fn default_vector_recovery_debounce_s() -> u32 {
    30
}

fn default_embedding_tokens_per_day() -> u64 {
    1_000_000
}

fn default_tokenizer_name() -> String {
    "cl100k_base".into()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ConflictResolution {
    #[default]
    WeightedSum,
    StructuralFirst,
    SemanticFirst,
}

#[derive(Debug, Error)]
pub enum PolicyValidationError {
    #[error("axis_weights must sum to 1.0 ± 1e-6 (US-02.6), got {0}")]
    AxisWeightsSum(f64),
    #[error("probability-like field {field} out of range [0,1]: {value}")]
    ProbabilityOutOfRange { field: &'static str, value: f64 },
    #[error("embedding_queue_lwm ({lwm}) must be < embedding_queue_hwm ({hwm})")]
    EmbeddingQueueWatermarks { lwm: u32, hwm: u32 },
    #[error("disk_min_free_pct must be 1..=99, got {0}")]
    DiskMinFreePct(u8),
    #[error("tombstone_retention_days ({retention}) must be >= rename_detection_window_days ({window})")]
    TombstoneRetentionVsRenameWindow { retention: u32, window: u32 },
    #[error("tombstone_retention_days must be > 0, got {0}")]
    TombstoneRetentionZero(u32),
}

#[derive(Debug, Error)]
pub enum PolicyLoadError {
    #[error(transparent)]
    Yaml(#[from] serde_yaml::Error),
    #[error(transparent)]
    Validation(#[from] PolicyValidationError),
}

impl RankingPolicy {
    /// Load + **validate** so invalid files never become active policy (**US-02.3** pattern at call site).
    pub fn from_yaml_str(s: &str) -> Result<Self, PolicyLoadError> {
        let p: Self = serde_yaml::from_str(s)?;
        p.validate()?;
        Ok(p)
    }

    pub fn validate(&self) -> Result<(), PolicyValidationError> {
        let s = self.axis_weights.structural_proximity
            + self.axis_weights.edge_type
            + self.axis_weights.semantic_similarity
            + self.axis_weights.recency;
        if (s - 1.0).abs() > 1e-6 {
            return Err(PolicyValidationError::AxisWeightsSum(s));
        }

        check_prob("min_path_confidence", self.min_path_confidence)?;
        check_prob("hybrid_search.structural_threshold", self.hybrid_search.structural_threshold)?;
        check_prob("rename_min_confidence", self.rename_min_confidence)?;
        check_prob("body_similarity_threshold", self.body_similarity_threshold)?;
        check_prob("name_proximity_threshold", self.name_proximity_threshold)?;

        if self.embedding_queue_lwm >= self.embedding_queue_hwm {
            return Err(PolicyValidationError::EmbeddingQueueWatermarks {
                lwm: self.embedding_queue_lwm,
                hwm: self.embedding_queue_hwm,
            });
        }

        if self.disk_min_free_pct == 0 || self.disk_min_free_pct >= 100 {
            return Err(PolicyValidationError::DiskMinFreePct(
                self.disk_min_free_pct,
            ));
        }

        if self.tombstone_retention_days == 0 {
            return Err(PolicyValidationError::TombstoneRetentionZero(
                self.tombstone_retention_days,
            ));
        }

        if self.tombstone_retention_days < self.rename_detection_window_days {
            return Err(PolicyValidationError::TombstoneRetentionVsRenameWindow {
                retention: self.tombstone_retention_days,
                window: self.rename_detection_window_days,
            });
        }

        Ok(())
    }

    /// Merge extra edge-type weights from YAML map (`CALLS:` …) into **EdgeTypeWeights** defaults.
    /// Used when policy uses architecture doc **SCREAMING_SNAKE_CASE** keys.
    pub fn merge_edge_type_weight_map(&mut self, m: &HashMap<String, f64>) {
        merge_edge_weights_into(&mut self.edge_type_weights, m);
    }

    /// **US-02.2** — cheap clone of the in-flight snapshot.
    pub fn snapshot(&self) -> RankingPolicySnapshot {
        self.clone()
    }
}

fn merge_edge_weights_into(w: &mut EdgeTypeWeights, m: &HashMap<String, f64>) {
    for (k, v) in m {
        let u = k.to_ascii_uppercase();
        match u.as_str() {
            "CALLS" => w.calls = *v,
            "IMPORTS" => w.imports = *v,
            "EXTENDS" => w.extends = *v,
            "USES" => w.uses = *v,
            "CONFIGURES" => w.configures = *v,
            "CO_LOCATED" | "CO-LOCATED" | "COLOCATED" => w.co_located = *v,
            "TEST_OF" | "TEST-OF" => w.test_of = *v,
            "RENAMED_FROM" | "RENAMED-FROM" => w.renamed_from = *v,
            _ => {}
        }
    }
}

fn check_prob(field: &'static str, value: f64) -> Result<(), PolicyValidationError> {
    if !(0.0..=1.0).contains(&value) {
        return Err(PolicyValidationError::ProbabilityOutOfRange { field, value });
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(untagged)]
enum EdgeTypeWeightsYaml {
    Map(HashMap<String, f64>),
    Nested(EdgeTypeWeights),
}

impl Default for EdgeTypeWeightsYaml {
    fn default() -> Self {
        EdgeTypeWeightsYaml::Nested(EdgeTypeWeights::default())
    }
}

/// **§01.2** YAML may use `edge_type_weights: { CALLS: 1.0, ... }`. Serde cannot merge that into
/// struct fields named `calls` without a custom deserializer; we accept both via an intermediate
/// representation.
impl<'de> Deserialize<'de> for RankingPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            version: String,
            #[serde(default)]
            axis_weights: AxisWeights,
            #[serde(default)]
            edge_type_weights: EdgeTypeWeightsYaml,
            #[serde(default)]
            conflict_resolution: ConflictResolution,
            #[serde(default = "default_max_hops")]
            max_hops: u32,
            #[serde(default = "default_min_path_confidence")]
            min_path_confidence: f64,
            #[serde(default)]
            hybrid_search: HybridSearchPolicy,
            #[serde(default)]
            recency: RecencyPolicy,
            #[serde(default)]
            dedup: DedupPolicy,
            #[serde(default)]
            budget: BudgetPolicy,
            #[serde(default = "default_rename_min_confidence")]
            rename_min_confidence: f64,
            #[serde(default = "default_body_similarity_threshold")]
            body_similarity_threshold: f64,
            #[serde(default = "default_name_proximity_threshold")]
            name_proximity_threshold: f64,
            #[serde(default = "default_rename_detection_window_days")]
            rename_detection_window_days: u32,
            #[serde(default = "default_tombstone_retention_days")]
            tombstone_retention_days: u32,
            #[serde(default = "default_time_travel_retention_days")]
            time_travel_retention_days: u32,
            #[serde(default = "default_wal_max_bytes")]
            wal_max_bytes: u64,
            #[serde(default = "default_disk_min_free_pct")]
            disk_min_free_pct: u8,
            #[serde(default = "default_merge_ttl_hours")]
            merge_ttl_hours: u32,
            #[serde(default = "default_embedding_queue_hwm")]
            embedding_queue_hwm: u32,
            #[serde(default = "default_embedding_queue_lwm")]
            embedding_queue_lwm: u32,
            #[serde(default = "default_vector_recovery_debounce_s")]
            vector_recovery_debounce_s: u32,
            #[serde(default = "default_embedding_tokens_per_day")]
            embedding_tokens_per_day: u64,
        }

        let raw = Raw::deserialize(deserializer)?;
        let version = if raw.version.is_empty() {
            "2.0".into()
        } else {
            raw.version
        };

        let edge_type_weights = match raw.edge_type_weights {
            EdgeTypeWeightsYaml::Nested(n) => n,
            EdgeTypeWeightsYaml::Map(m) => {
                let mut w = EdgeTypeWeights::default();
                merge_edge_weights_into(&mut w, &m);
                w
            }
        };

        Ok(RankingPolicy {
            version,
            axis_weights: raw.axis_weights,
            edge_type_weights,
            conflict_resolution: raw.conflict_resolution,
            max_hops: raw.max_hops,
            min_path_confidence: raw.min_path_confidence,
            hybrid_search: raw.hybrid_search,
            recency: raw.recency,
            dedup: raw.dedup,
            budget: raw.budget,
            rename_min_confidence: raw.rename_min_confidence,
            body_similarity_threshold: raw.body_similarity_threshold,
            name_proximity_threshold: raw.name_proximity_threshold,
            rename_detection_window_days: raw.rename_detection_window_days,
            tombstone_retention_days: raw.tombstone_retention_days,
            time_travel_retention_days: raw.time_travel_retention_days,
            wal_max_bytes: raw.wal_max_bytes,
            disk_min_free_pct: raw.disk_min_free_pct,
            merge_ttl_hours: raw.merge_ttl_hours,
            embedding_queue_hwm: raw.embedding_queue_hwm,
            embedding_queue_lwm: raw.embedding_queue_lwm,
            vector_recovery_debounce_s: raw.vector_recovery_debounce_s,
            embedding_tokens_per_day: raw.embedding_tokens_per_day,
        })
    }
}

impl Serialize for RankingPolicy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("RankingPolicy", 22)?;
        s.serialize_field("version", &self.version)?;
        s.serialize_field("axis_weights", &self.axis_weights)?;
        s.serialize_field("edge_type_weights", &self.edge_type_weights)?;
        s.serialize_field("conflict_resolution", &self.conflict_resolution)?;
        s.serialize_field("max_hops", &self.max_hops)?;
        s.serialize_field("min_path_confidence", &self.min_path_confidence)?;
        s.serialize_field("hybrid_search", &self.hybrid_search)?;
        s.serialize_field("recency", &self.recency)?;
        s.serialize_field("dedup", &self.dedup)?;
        s.serialize_field("budget", &self.budget)?;
        s.serialize_field("rename_min_confidence", &self.rename_min_confidence)?;
        s.serialize_field(
            "body_similarity_threshold",
            &self.body_similarity_threshold,
        )?;
        s.serialize_field(
            "name_proximity_threshold",
            &self.name_proximity_threshold,
        )?;
        s.serialize_field(
            "rename_detection_window_days",
            &self.rename_detection_window_days,
        )?;
        s.serialize_field("tombstone_retention_days", &self.tombstone_retention_days)?;
        s.serialize_field(
            "time_travel_retention_days",
            &self.time_travel_retention_days,
        )?;
        s.serialize_field("wal_max_bytes", &self.wal_max_bytes)?;
        s.serialize_field("disk_min_free_pct", &self.disk_min_free_pct)?;
        s.serialize_field("merge_ttl_hours", &self.merge_ttl_hours)?;
        s.serialize_field("embedding_queue_hwm", &self.embedding_queue_hwm)?;
        s.serialize_field("embedding_queue_lwm", &self.embedding_queue_lwm)?;
        s.serialize_field(
            "vector_recovery_debounce_s",
            &self.vector_recovery_debounce_s,
        )?;
        s.serialize_field(
            "embedding_tokens_per_day",
            &self.embedding_tokens_per_day,
        )?;
        s.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_validates() {
        RankingPolicy::default().validate().unwrap();
    }

    #[test]
    fn partial_yaml_merges_defaults() {
        let yaml = r#"
version: "2.0"
axis_weights:
  structural_proximity: 0.5
  edge_type: 0.2
  semantic_similarity: 0.2
  recency: 0.1
"#;
        let p = RankingPolicy::from_yaml_str(yaml).unwrap();
        assert_eq!(p.max_hops, 3);
        assert_eq!(p.hybrid_search.vector_top_k, 50);
        assert_eq!(p.tombstone_retention_days, 90);
    }

    #[test]
    fn reject_bad_axis_sum() {
        let yaml = r#"
axis_weights:
  structural_proximity: 0.9
  edge_type: 0.9
  semantic_similarity: 0.0
  recency: 0.0
"#;
        assert!(RankingPolicy::from_yaml_str(yaml).is_err());
    }

    /// **US-10 / AC-10.1** — thresholds load from YAML.
    #[test]
    fn us_10_rename_thresholds_yaml() {
        let yaml = r#"
version: "t"
axis_weights:
  structural_proximity: 0.25
  edge_type: 0.25
  semantic_similarity: 0.25
  recency: 0.25
rename_min_confidence: 0.55
body_similarity_threshold: 0.72
name_proximity_threshold: 0.88
"#;
        let p = RankingPolicy::from_yaml_str(yaml).unwrap();
        assert!((p.rename_min_confidence - 0.55).abs() < 1e-9);
        assert!((p.body_similarity_threshold - 0.72).abs() < 1e-9);
        assert!((p.name_proximity_threshold - 0.88).abs() < 1e-9);
    }

    /// **§01.2** — doc-style `CALLS` map for edge_type_weights.
    #[test]
    fn section_01_2_edge_type_weights_map() {
        let yaml = r#"
version: "2.0"
axis_weights:
  structural_proximity: 0.25
  edge_type: 0.25
  semantic_similarity: 0.25
  recency: 0.25
edge_type_weights:
  CALLS: 1.0
  IMPORTS: 0.9
  TEST_OF: 0.5
"#;
        let p = RankingPolicy::from_yaml_str(yaml).unwrap();
        assert!((p.edge_type_weights.calls - 1.0).abs() < 1e-9);
        assert!((p.edge_type_weights.imports - 0.9).abs() < 1e-9);
        assert!((p.edge_type_weights.test_of - 0.5).abs() < 1e-9);
    }

    #[test]
    fn rejects_bad_watermarks() {
        let mut p = RankingPolicy::default();
        p.embedding_queue_lwm = 5000;
        p.embedding_queue_hwm = 1000;
        assert!(p.validate().is_err());
    }

    #[test]
    fn rejects_zero_tombstone_retention() {
        let mut p = RankingPolicy::default();
        p.tombstone_retention_days = 0;
        p.rename_detection_window_days = 0;
        assert!(matches!(
            p.validate(),
            Err(PolicyValidationError::TombstoneRetentionZero(0))
        ));
    }

    #[test]
    fn doc_policy_traceability_fr_1_13_fields_present() {
        let p = RankingPolicy::default();
        assert_eq!(p.tombstone_retention_days, 90);
        assert_eq!(p.time_travel_retention_days, 90);
        assert_eq!(p.rename_detection_window_days, 30);
    }
}
