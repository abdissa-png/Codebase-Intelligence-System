//! Append-only merge observability log (**Phase 2.5**).

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MergeMetricsRecord {
    pub merge_id_hex: String,
    pub started_at_ms: u64,
    pub phase_durations_ms: BTreeMap<String, u64>,
    pub edges_regenerated: usize,
    pub dangling_edges_removed: usize,
    pub signature_reresolved: usize,
    pub cardinality_violations: Vec<String>,
    pub resumed_from_phase: Option<String>,
    pub compensated: bool,
}

pub fn merge_metrics_path(cis_dir: &Path) -> std::path::PathBuf {
    cis_dir.join("merge_metrics.jsonl")
}

/// Append one JSON line to `.cis/merge_metrics.jsonl`.
pub fn append_merge_metrics(cis_dir: &Path, record: &MergeMetricsRecord) -> std::io::Result<()> {
    std::fs::create_dir_all(cis_dir)?;
    let path = merge_metrics_path(cis_dir);
    let line = serde_json::to_string(record).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })?;
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(f, "{line}")?;
    f.sync_all()?;
    Ok(())
}

pub fn read_merge_metrics(cis_dir: &Path, limit: usize) -> std::io::Result<Vec<MergeMetricsRecord>> {
    read_merge_metrics_filtered(cis_dir, limit, None)
}

pub fn read_merge_metrics_filtered(
    cis_dir: &Path,
    limit: usize,
    since_ms: Option<u64>,
) -> std::io::Result<Vec<MergeMetricsRecord>> {
    let path = merge_metrics_path(cis_dir);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = std::fs::read_to_string(path)?;
    let mut out = Vec::new();
    for line in content.lines().filter(|l| !l.trim().is_empty()) {
        if let Ok(rec) = serde_json::from_str::<MergeMetricsRecord>(line) {
            if since_ms.is_some_and(|s| rec.started_at_ms < s) {
                continue;
            }
            out.push(rec);
        }
    }
    if out.len() > limit {
        out = out.split_off(out.len().saturating_sub(limit));
    }
    Ok(out)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct MergeMetricsRollup {
    pub window_records: usize,
    pub avg_phase_ms: BTreeMap<String, f64>,
    pub p99_phase_ms: BTreeMap<String, u64>,
    pub compensation_rate: f64,
    pub resume_rate: f64,
    pub avg_edges_regenerated: f64,
    pub cardinality_violation_count: usize,
    pub slowest_merges: Vec<MergeMetricsRecord>,
}

fn total_duration_ms(rec: &MergeMetricsRecord) -> u64 {
    rec.phase_durations_ms.values().sum()
}

fn percentile_u64(mut values: Vec<u64>, p: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    let idx = ((values.len() as f64 - 1.0) * p).round() as usize;
    values[idx.min(values.len() - 1)]
}

pub fn rollup_merge_metrics(records: &[MergeMetricsRecord]) -> MergeMetricsRollup {
    if records.is_empty() {
        return MergeMetricsRollup::default();
    }
    let n = records.len();
    let compensated = records.iter().filter(|r| r.compensated).count();
    let resumed = records.iter().filter(|r| r.resumed_from_phase.is_some()).count();
    let cardinality_violation_count: usize = records
        .iter()
        .map(|r| r.cardinality_violations.len())
        .sum();
    let avg_edges = records.iter().map(|r| r.edges_regenerated).sum::<usize>() as f64 / n as f64;

    let mut phase_names = BTreeMap::new();
    for rec in records {
        for (phase, _) in &rec.phase_durations_ms {
            phase_names.insert(phase.clone(), ());
        }
    }
    let mut avg_phase_ms = BTreeMap::new();
    let mut p99_phase_ms = BTreeMap::new();
    for phase in phase_names.keys() {
        let mut vals: Vec<u64> = records
            .iter()
            .filter_map(|r| r.phase_durations_ms.get(phase).copied())
            .collect();
        if vals.is_empty() {
            continue;
        }
        let sum: u64 = vals.iter().sum();
        avg_phase_ms.insert(phase.clone(), sum as f64 / vals.len() as f64);
        p99_phase_ms.insert(phase.clone(), percentile_u64(vals, 0.99));
    }

    let mut by_duration: Vec<&MergeMetricsRecord> = records.iter().collect();
    by_duration.sort_by_key(|r| std::cmp::Reverse(total_duration_ms(r)));
    let slowest_merges: Vec<MergeMetricsRecord> = by_duration
        .into_iter()
        .take(5)
        .cloned()
        .collect();

    MergeMetricsRollup {
        window_records: n,
        avg_phase_ms,
        p99_phase_ms,
        compensation_rate: compensated as f64 / n as f64,
        resume_rate: resumed as f64 / n as f64,
        avg_edges_regenerated: avg_edges,
        cardinality_violation_count,
        slowest_merges,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut phases = BTreeMap::new();
        phases.insert("Classifying".into(), 12);
        let rec = MergeMetricsRecord {
            merge_id_hex: "ab".repeat(16),
            started_at_ms: 1_000,
            phase_durations_ms: phases,
            edges_regenerated: 3,
            dangling_edges_removed: 1,
            signature_reresolved: 0,
            cardinality_violations: vec![],
            resumed_from_phase: None,
            compensated: false,
        };
        append_merge_metrics(dir.path(), &rec).unwrap();
        let rows = read_merge_metrics(dir.path(), 10).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].merge_id_hex, rec.merge_id_hex);
    }

    #[test]
    fn rollup_computes_rates_and_slowest() {
        let mut phases_a = BTreeMap::new();
        phases_a.insert("A".into(), 100);
        let mut phases_b = BTreeMap::new();
        phases_b.insert("A".into(), 300);
        let records = vec![
            MergeMetricsRecord {
                merge_id_hex: "01".repeat(16),
                started_at_ms: 1,
                phase_durations_ms: phases_a,
                edges_regenerated: 2,
                dangling_edges_removed: 0,
                signature_reresolved: 0,
                cardinality_violations: vec![],
                resumed_from_phase: None,
                compensated: false,
            },
            MergeMetricsRecord {
                merge_id_hex: "02".repeat(16),
                started_at_ms: 2,
                phase_durations_ms: phases_b,
                edges_regenerated: 4,
                dangling_edges_removed: 0,
                signature_reresolved: 0,
                cardinality_violations: vec!["x".into()],
                resumed_from_phase: Some("B".into()),
                compensated: true,
            },
        ];
        let rollup = rollup_merge_metrics(&records);
        assert_eq!(rollup.window_records, 2);
        assert!((rollup.compensation_rate - 0.5).abs() < f64::EPSILON);
        assert!((rollup.resume_rate - 0.5).abs() < f64::EPSILON);
        assert_eq!(rollup.cardinality_violation_count, 1);
        assert_eq!(rollup.slowest_merges[0].merge_id_hex, "02".repeat(16));
    }
}
