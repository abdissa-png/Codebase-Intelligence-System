//! Shared path to the [Pygame chess](https://github.com/abdissa-png/A-chess-game-using-Pygame) fixture repo.

use std::path::PathBuf;

/// Default: `cis/fixtures/chess_pygame` (one level above `cis-core`). Override with `CHESS_FIXTURE_ROOT`.
pub fn chess_fixture_root() -> PathBuf {
    if let Ok(p) = std::env::var("CHESS_FIXTURE_ROOT") {
        return PathBuf::from(p);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../fixtures/chess_pygame")
}

/// Minimum expected `Calls` edge count in chess integration tests (default 100).
pub fn chess_min_edges() -> usize {
    std::env::var("CHESS_MIN_EDGES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
}
