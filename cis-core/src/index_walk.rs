//! Gitignore-aware repo walk for indexing and FS sync.

use std::path::{Path, PathBuf};

/// Whether `.gitignore` (and nested ignore files) are honored during indexing.
///
/// Set `CIS_INDEX_RESPECT_GITIGNORE=0` to restore the legacy recursive walk.
pub fn index_respect_gitignore() -> bool {
    !std::env::var_os("CIS_INDEX_RESPECT_GITIGNORE").is_some_and(|v| {
        v == "0" || v.eq_ignore_ascii_case("false")
    })
}

/// Built-in directory names skipped even when no `.gitignore` exists.
const BUILTIN_SKIP_DIRS: &[&str] = &[".git", ".cis"];

fn is_builtin_skip(name: &str, is_dir: bool) -> bool {
    is_dir && BUILTIN_SKIP_DIRS.contains(&name)
}

/// Recursively collect source files with any of the given extensions under `root`.
pub fn collect_source_files(root: &Path, extensions: &[&str], out: &mut Vec<PathBuf>) {
    if index_respect_gitignore() {
        let mut ig = GitignoreWalker::new(root);
        collect_source_files_gitignore(root, root, extensions, &mut ig, out);
    } else {
        collect_source_files_naive(root, extensions, out);
    }
}

fn collect_source_files_gitignore(
    root: &Path,
    dir: &Path,
    extensions: &[&str],
    ig: &mut GitignoreWalker,
    out: &mut Vec<PathBuf>,
) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    ig.push_dir(dir);
    for entry in entries.flatten() {
        let p = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        let is_dir = p.is_dir();
        if is_builtin_skip(&name, is_dir) {
            continue;
        }
        let rel = p
            .strip_prefix(root)
            .map(|r| r.to_string_lossy().replace('\\', "/"))
            .unwrap_or_default();
        if ig.is_ignored(&rel, is_dir) {
            continue;
        }
        if is_dir {
            collect_source_files_gitignore(root, &p, extensions, ig, out);
        } else if p
            .extension()
            .and_then(|x| x.to_str())
            .is_some_and(|ext| extensions.contains(&ext))
        {
            out.push(p);
        }
    }
    ig.pop_dir();
}

fn collect_source_files_naive(root: &Path, extensions: &[&str], out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            collect_source_files_naive(&p, extensions, out);
        } else if p
            .extension()
            .and_then(|x| x.to_str())
            .is_some_and(|ext| extensions.contains(&ext))
        {
            out.push(p);
        }
    }
}

#[derive(Debug, Clone)]
struct GitignoreRule {
    pattern: String,
    anchored: bool,
    dir_only: bool,
    negated: bool,
}

#[derive(Debug, Default)]
struct GitignoreWalker {
    rules: Vec<GitignoreRule>,
}

impl GitignoreWalker {
    fn new(root: &Path) -> Self {
        let mut ig = Self::default();
        let gitignore = root.join(".gitignore");
        if gitignore.is_file() {
            if let Ok(text) = std::fs::read_to_string(&gitignore) {
                ig.extend_rules(&text);
            }
        }
        ig
    }

    fn push_dir(&mut self, dir: &Path) {
        let gitignore = dir.join(".gitignore");
        if gitignore.is_file() {
            if let Ok(text) = std::fs::read_to_string(&gitignore) {
                self.extend_rules(&text);
            }
        }
    }

    fn pop_dir(&mut self) {
        // Nested .gitignore rules are kept for simplicity; git applies them globally
        // with path-relative semantics. Our matcher uses full repo-relative paths,
        // which matches common monorepo layouts well enough for indexing skips.
    }

    fn extend_rules(&mut self, text: &str) {
        for line in text.lines() {
            let line = line.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let mut negated = false;
            let mut pat = line;
            if let Some(rest) = pat.strip_prefix('!') {
                negated = true;
                pat = rest.trim();
            }
            if pat.is_empty() {
                continue;
            }
            let dir_only = pat.ends_with('/');
            if dir_only {
                pat = pat.trim_end_matches('/');
            }
            let anchored = !pat.starts_with('*') && !pat.contains('/');
            self.rules.push(GitignoreRule {
                pattern: pat.to_string(),
                anchored,
                dir_only,
                negated,
            });
        }
    }

    fn is_ignored(&self, rel: &str, is_dir: bool) -> bool {
        let mut ignored = false;
        for rule in &self.rules {
            if rule.dir_only && !is_dir {
                continue;
            }
            let matched = if rule.anchored {
                rel == rule.pattern
                    || rel.strip_prefix("./") == Some(&rule.pattern)
                    || rel.ends_with(&format!("/{}", rule.pattern))
                    || (is_dir && rel == rule.pattern.trim_end_matches('/'))
            } else {
                glob_match(&rule.pattern, rel)
                    || (is_dir && glob_match(&rule.pattern, &format!("{rel}/")))
            };
            if matched {
                ignored = !rule.negated;
            }
        }
        ignored
    }
}

fn glob_match(pattern: &str, text: &str) -> bool {
    let pat = pattern.replace('.', "\\.").replace('?', ".");
    let mut re = String::from("^");
    let chars: Vec<char> = pat.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '*' {
            if i + 1 < chars.len() && chars[i + 1] == '*' {
                re.push_str(".*");
                i += 2;
                if i < chars.len() && chars[i] == '/' {
                    i += 1;
                }
            } else {
                re.push_str("[^/]*");
                i += 1;
            }
        } else {
            re.push(chars[i]);
            i += 1;
        }
    }
    re.push('$');
    regex::Regex::new(&re)
        .map(|r| r.is_match(text))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Mutex;

    /// `CIS_INDEX_RESPECT_GITIGNORE` is process-global; lib tests run in parallel.
    static GITIGNORE_ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_gitignore_env<R>(respect: bool, f: impl FnOnce() -> R) -> R {
        let _guard = GITIGNORE_ENV_LOCK.lock().unwrap();
        let prev = std::env::var_os("CIS_INDEX_RESPECT_GITIGNORE");
        if respect {
            std::env::set_var("CIS_INDEX_RESPECT_GITIGNORE", "1");
        } else {
            std::env::set_var("CIS_INDEX_RESPECT_GITIGNORE", "0");
        }
        let out = f();
        match prev {
            Some(v) => std::env::set_var("CIS_INDEX_RESPECT_GITIGNORE", v),
            None => std::env::remove_var("CIS_INDEX_RESPECT_GITIGNORE"),
        }
        out
    }

    fn write_tree(root: &Path) {
        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(root.join("src/main.py"), "pass\n").unwrap();
        fs::create_dir_all(root.join(".venv/lib")).unwrap();
        fs::write(root.join(".venv/lib/site.py"), "# skip\n").unwrap();
        fs::create_dir_all(root.join("__pycache__")).unwrap();
        fs::write(root.join("__pycache__/mod.cpython-311.pyc"), "x").unwrap();
        fs::create_dir_all(root.join(".git/objects")).unwrap();
        fs::write(root.join(".git/objects/abc.py"), "# skip\n").unwrap();
        fs::create_dir_all(root.join(".cis/bodies")).unwrap();
        fs::write(root.join(".cis/bodies/x.py"), "# skip\n").unwrap();
    }

    #[test]
    fn collect_respects_gitignore() {
        with_gitignore_env(true, || {
            let tmp = tempfile::tempdir().unwrap();
            write_tree(tmp.path());
            fs::write(
                tmp.path().join(".gitignore"),
                ".venv/\n**/*__pycache__\n**/*.pyc\n",
            )
            .unwrap();

            let mut paths = Vec::new();
            collect_source_files(tmp.path(), &["py"], &mut paths);
            assert_eq!(paths.len(), 1, "paths: {:?}", paths);
            assert!(paths[0].ends_with("src/main.py"));
        });
    }

    #[test]
    fn collect_skips_builtin_dirs_without_gitignore() {
        with_gitignore_env(true, || {
            let tmp = tempfile::tempdir().unwrap();
            write_tree(tmp.path());

            let mut paths = Vec::new();
            collect_source_files(tmp.path(), &["py"], &mut paths);
            assert_eq!(paths.len(), 2, "paths: {:?}", paths);
            assert!(paths.iter().any(|p| p.ends_with("src/main.py")));
            assert!(paths.iter().any(|p| p.to_string_lossy().contains(".venv")));
            assert!(!paths.iter().any(|p| p.to_string_lossy().contains(".git/")));
            assert!(!paths.iter().any(|p| p.to_string_lossy().contains(".cis/")));
        });
    }

    #[test]
    fn collect_naive_when_disabled() {
        with_gitignore_env(false, || {
            let tmp = tempfile::tempdir().unwrap();
            write_tree(tmp.path());
            fs::write(tmp.path().join(".gitignore"), ".venv/\n").unwrap();

            let mut paths = Vec::new();
            collect_source_files(tmp.path(), &["py"], &mut paths);

            assert!(paths.len() > 1, "naive walk should include .venv: {:?}", paths);
            assert!(paths.iter().any(|p| p.to_string_lossy().contains(".venv")));
        });
    }

    #[test]
    fn glob_patterns() {
        assert!(glob_match("**/*__pycache__", "src/foo/__pycache__"));
        assert!(glob_match(".venv/**", ".venv/lib/site.py"));
        assert!(!glob_match(".venv/", "src/main.py"));
    }
}
