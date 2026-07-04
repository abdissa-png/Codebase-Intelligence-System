//! Fast stress suite for PR CI (**Phase 4.2**).

#[path = "stress/harness.rs"]
mod harness;
#[path = "stress/replay.rs"]
mod replay;
#[path = "stress/scenarios.rs"]
mod scenarios;

use harness::{StressConfig, StressHarness};
use scenarios::run_all;

#[test]
fn stress_fast_all_scenarios() {
    let h = StressHarness::new();
    run_all(&h, &StressConfig::fast());
}

#[test]
fn stress_fast_write_confirm_revert() {
    let h = StressHarness::new();
    scenarios::scenario_write_confirm_revert(&h, &StressConfig::fast());
}

#[test]
fn stress_fast_identity_cas() {
    let h = StressHarness::new();
    scenarios::scenario_identity_cas_converges(&h, &StressConfig::fast());
}

#[test]
fn stress_fast_merge_preflight_race() {
    let h = StressHarness::new();
    scenarios::scenario_merge_preflight_vs_speculative(&h, &StressConfig::fast());
}

#[test]
fn stress_fast_ttl_sweep_vs_confirm() {
    let h = StressHarness::new();
    scenarios::scenario_ttl_sweep_vs_confirm(&h, &StressConfig::fast());
}

#[test]
fn stress_fast_wal_compaction_during_merge() {
    let h = StressHarness::new();
    scenarios::scenario_wal_compaction_during_merge(&h, &StressConfig::fast());
}

#[test]
fn stress_fast_tombstone_gc_rename() {
    let h = StressHarness::new();
    scenarios::scenario_tombstone_gc_during_rename(&h, &StressConfig::fast());
}

#[test]
fn stress_fast_branch_switch_write() {
    let h = StressHarness::new();
    scenarios::scenario_branch_switch_and_write(&h, &StressConfig::fast());
}

#[test]
fn stress_fast_lease_storm() {
    let h = StressHarness::new();
    scenarios::scenario_lease_storm(&h, &StressConfig::fast());
}

#[test]
fn stress_fast_crash_mid_promote() {
    let h = StressHarness::new();
    scenarios::scenario_crash_mid_promote_recovery(&h, &StressConfig::fast());
}

#[test]
fn stress_fast_background_worker_chaos() {
    let h = StressHarness::new();
    scenarios::scenario_background_worker_chaos(&h, &StressConfig::fast());
}
