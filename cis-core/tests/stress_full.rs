//! Full stress suite for nightly CI (**Phase 4.2**).

#[path = "stress/harness.rs"]
mod harness;
#[path = "stress/replay.rs"]
mod replay;
#[path = "stress/scenarios.rs"]
mod scenarios;

use harness::{StressConfig, StressHarness};
use scenarios::run_all;

#[test]
#[ignore = "nightly stress — run with: cargo test -p cis-core --test stress_full -- --ignored --release"]
fn stress_full_all_scenarios() {
    let h = StressHarness::new();
    run_all(&h, &StressConfig::full());
}

#[test]
#[ignore = "nightly stress"]
fn stress_full_write_confirm_revert() {
    let h = StressHarness::new();
    scenarios::scenario_write_confirm_revert(&h, &StressConfig::full());
}

#[test]
#[ignore = "nightly stress"]
fn stress_full_identity_cas() {
    let h = StressHarness::new();
    scenarios::scenario_identity_cas_converges(&h, &StressConfig::full());
}

#[test]
#[ignore = "nightly stress"]
fn stress_full_merge_preflight_race() {
    let h = StressHarness::new();
    scenarios::scenario_merge_preflight_vs_speculative(&h, &StressConfig::full());
}

#[test]
#[ignore = "nightly stress"]
fn stress_full_ttl_sweep_vs_confirm() {
    let h = StressHarness::new();
    scenarios::scenario_ttl_sweep_vs_confirm(&h, &StressConfig::full());
}

#[test]
#[ignore = "nightly stress"]
fn stress_full_wal_compaction_during_merge() {
    let h = StressHarness::new();
    scenarios::scenario_wal_compaction_during_merge(&h, &StressConfig::full());
}

#[test]
#[ignore = "nightly stress"]
fn stress_full_tombstone_gc_rename() {
    let h = StressHarness::new();
    scenarios::scenario_tombstone_gc_during_rename(&h, &StressConfig::full());
}

#[test]
#[ignore = "nightly stress"]
fn stress_full_branch_switch_write() {
    let h = StressHarness::new();
    scenarios::scenario_branch_switch_and_write(&h, &StressConfig::full());
}

#[test]
#[ignore = "nightly stress"]
fn stress_full_lease_storm() {
    let h = StressHarness::new();
    scenarios::scenario_lease_storm(&h, &StressConfig::full());
}

#[test]
#[ignore = "nightly stress"]
fn stress_full_crash_mid_promote() {
    let h = StressHarness::new();
    scenarios::scenario_crash_mid_promote_recovery(&h, &StressConfig::full());
}

#[test]
#[ignore = "nightly stress"]
fn stress_full_background_worker_chaos() {
    let h = StressHarness::new();
    scenarios::scenario_background_worker_chaos(&h, &StressConfig::full());
}
