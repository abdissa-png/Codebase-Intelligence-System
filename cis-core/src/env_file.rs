//! Optional **`.env`** loader — sets vars only when not already present in the process environment.

use std::path::{Path, PathBuf};

/// Load key=value pairs from `.env` files (no quotes / export syntax).
/// Searches, in order: explicit `path`, `./.env`, `../.env` relative to `cwd`.
pub fn load_env_file(path: Option<&Path>) {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Some(p) = path {
        candidates.push(p.to_path_buf());
    }
    candidates.push(PathBuf::from(".env"));
    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.join(".env"));
        if let Some(parent) = cwd.parent() {
            candidates.push(parent.join(".env"));
        }
    }

    for p in candidates {
        if p.is_file() {
            apply_env_file(&p);
            return;
        }
    }
}

fn apply_env_file(path: &Path) {
    let Ok(content) = std::fs::read_to_string(path) else {
        return;
    };
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() || std::env::var_os(key).is_some() {
            continue;
        }
        let value = value.trim().trim_matches('"').trim_matches('\'');
        // SAFETY: called from main() before background threads spawn.
        unsafe {
            std::env::set_var(key, value);
        }
    }
    eprintln!("cis: loaded env file {:?}", path);
}
