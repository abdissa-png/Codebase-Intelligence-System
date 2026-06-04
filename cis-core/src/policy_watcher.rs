//! **US-02** — hot-reloadable policy holder + file reloader (debounce contract for FS notify integration).

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::ranking_policy::{PolicyLoadError, RankingPolicy, RankingPolicySnapshot};

/// Thread-safe active policy. **`try_update_from_yaml`** preserves the previous policy on error (**US-02.3**).
#[derive(Clone, Debug)]
pub struct ActiveRankingPolicy {
    inner: Arc<RwLock<RankingPolicy>>,
}

impl ActiveRankingPolicy {
    pub fn new(initial: RankingPolicy) -> Self {
        Self {
            inner: Arc::new(RwLock::new(initial)),
        }
    }

    pub fn with_system_default() -> Self {
        Self::new(RankingPolicy::default())
    }

    /// **US-02.2** — query snapshot (cheap clone).
    pub fn snapshot(&self) -> RankingPolicySnapshot {
        self.inner.read().unwrap().clone()
    }

    /// Replace only if YAML parses **and** validates; otherwise **no mutation**.
    pub fn try_update_from_yaml(&self, yaml: &str) -> Result<(), PolicyLoadError> {
        let next = RankingPolicy::from_yaml_str(yaml)?;
        *self.inner.write().unwrap() = next;
        Ok(())
    }

    pub fn current_version_label(&self) -> String {
        self.inner.read().unwrap().version.clone()
    }
}

/// Outcome of a disk reload attempt (structured logging / metrics hooks).
#[derive(Debug)]
pub enum PolicyReloadOutcome {
    Applied,
    RejectedInvalid(PolicyLoadError),
    ReadError(io::Error),
}

/// Reads `.cis/ranking_policy.yaml`-style path; call **`on_file_event`** from a debounced watcher.
#[derive(Debug)]
pub struct PolicyFileReloader {
    path: PathBuf,
    active: ActiveRankingPolicy,
    debounce: Duration,
    last_fired: RwLock<Option<Instant>>,
}

impl PolicyFileReloader {
    /// Fails if file is missing/unreadable, or if YAML is invalid (**strict** initial load).
    pub fn from_file(path: impl Into<PathBuf>) -> Result<Self, PolicyBootstrapError> {
        let path = path.into();
        let yaml = std::fs::read_to_string(&path).map_err(PolicyBootstrapError::Io)?;
        let initial = RankingPolicy::from_yaml_str(&yaml).map_err(PolicyBootstrapError::Invalid)?;
        Ok(Self {
            path,
            active: ActiveRankingPolicy::new(initial),
            debounce: Duration::from_millis(250),
            last_fired: RwLock::new(None),
        })
    }

    pub fn with_debounce(path: PathBuf, initial: RankingPolicy, debounce: Duration) -> Self {
        Self {
            path,
            active: ActiveRankingPolicy::new(initial),
            debounce,
            last_fired: RwLock::new(None),
        }
    }

    pub fn active(&self) -> ActiveRankingPolicy {
        self.active.clone()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Call when the file may have changed. Enforces **debounce** wall-clock (**US-02.1** 250ms intent).
    pub fn on_file_event(&self, now: Instant) -> Option<PolicyReloadOutcome> {
        let mut last = self.last_fired.write().unwrap();
        match *last {
            None => *last = Some(now),
            Some(t) if now.duration_since(t) < self.debounce => return None,
            Some(_) => *last = Some(now),
        }
        drop(last);
        Some(self.reload_now())
    }

    /// Immediate read + validate (no debounce) — tests / forced refresh.
    pub fn reload_now(&self) -> PolicyReloadOutcome {
        match std::fs::read_to_string(&self.path) {
            Err(e) => PolicyReloadOutcome::ReadError(e),
            Ok(s) => match self.active.try_update_from_yaml(&s) {
                Ok(()) => PolicyReloadOutcome::Applied,
                Err(err) => PolicyReloadOutcome::RejectedInvalid(err),
            },
        }
    }
}

#[derive(Debug)]
pub enum PolicyBootstrapError {
    Io(io::Error),
    Invalid(PolicyLoadError),
}

impl From<io::Error> for PolicyBootstrapError {
    fn from(e: io::Error) -> Self {
        PolicyBootstrapError::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_reload_leaves_policy_unchanged() {
        let good = r#"
version: "2.0-test"
axis_weights:
  structural_proximity: 0.5
  edge_type: 0.2
  semantic_similarity: 0.2
  recency: 0.1
"#;
        let p = ActiveRankingPolicy::new(RankingPolicy::from_yaml_str(good).unwrap());
        assert_eq!(p.current_version_label(), "2.0-test");
        let bad = r#"
axis_weights:
  structural_proximity: 0.9
  edge_type: 0.9
  semantic_similarity: 0.0
  recency: 0.0
"#;
        assert!(p.try_update_from_yaml(bad).is_err());
        assert_eq!(p.current_version_label(), "2.0-test");
    }

    #[test]
    fn policy_file_reloader_rejects_bad_file() {
        let dir = std::env::temp_dir().join(format!(
            "cis-ranking-policy-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("ranking_policy.yaml");
        std::fs::write(
            &f,
            r#"version: "v-good"
axis_weights:
  structural_proximity: 0.5
  edge_type: 0.2
  semantic_similarity: 0.2
  recency: 0.1
"#,
        )
        .unwrap();
        let r = PolicyFileReloader::from_file(&f).unwrap();
        assert_eq!(r.active().current_version_label(), "v-good");
        std::fs::write(
            &f,
            r#"axis_weights:
  structural_proximity: 1.0
  edge_type: 1.0
  semantic_similarity: 0.0
  recency: 0.0
"#,
        )
        .unwrap();
        let out = r.reload_now();
        assert!(matches!(out, PolicyReloadOutcome::RejectedInvalid(_)));
        assert_eq!(r.active().current_version_label(), "v-good");
    }

    #[test]
    fn debounce_suppresses_immediate_second_reload() {
        let dir = std::env::temp_dir().join(format!(
            "cis-debounce-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("p.yaml");
        std::fs::write(
            &f,
            r#"version: "a"
axis_weights:
  structural_proximity: 0.5
  edge_type: 0.2
  semantic_similarity: 0.2
  recency: 0.1
"#,
        )
        .unwrap();
        let r = PolicyFileReloader::from_file(&f).unwrap();
        std::fs::write(
            &f,
            r#"version: "b"
axis_weights:
  structural_proximity: 0.5
  edge_type: 0.2
  semantic_similarity: 0.2
  recency: 0.1
"#,
        )
        .unwrap();
        let t0 = Instant::now();
        assert!(r.on_file_event(t0).is_some());
        let t1 = t0 + Duration::from_millis(50);
        assert!(r.on_file_event(t1).is_none());
        let t2 = t0 + Duration::from_millis(300);
        assert!(r.on_file_event(t2).is_some());
        assert_eq!(r.active().current_version_label(), "b");
    }
}
