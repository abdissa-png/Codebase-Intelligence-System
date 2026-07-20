//! Shared path to the [Pygame chess](https://github.com/abdissa-png/A-chess-game-using-Pygame) fixture repo.

use std::path::{Path, PathBuf};

/// Default: `cis/fixtures/chess_pygame` (one level above `cis-core`). Override with `CHESS_FIXTURE_ROOT`.
pub fn chess_fixture_root() -> PathBuf {
    if let Ok(p) = std::env::var("CHESS_FIXTURE_ROOT") {
        return PathBuf::from(p);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../fixtures/chess_pygame")
}

/// Returns `true` if the fixture directory exists.
///
/// When `CIS_REQUIRE_FIXTURES=1`, missing fixtures **panic** (CI must clone the fixture).
/// Otherwise logs a skip message and returns `false` for local ergonomics.
pub fn ensure_chess_fixture(root: &Path) -> bool {
    if root.is_dir() {
        return true;
    }
    if std::env::var_os("CIS_REQUIRE_FIXTURES").is_some_and(|v| v == "1") {
        panic!(
            "CIS_REQUIRE_FIXTURES=1 but chess fixture missing at {} — clone to fixtures/chess_pygame or set CHESS_FIXTURE_ROOT",
            root.display()
        );
    }
    eprintln!(
        "chess fixture: skip — clone to {} (or set CIS_REQUIRE_FIXTURES=1 in CI)",
        root.display()
    );
    false
}

/// Minimum expected `Calls` edge count in chess integration tests (default 100).
pub fn chess_min_edges() -> usize {
    std::env::var("CHESS_MIN_EDGES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
}
