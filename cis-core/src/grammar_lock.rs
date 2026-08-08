//! **FR-1.1** — grammar pin file (`.cis/grammar.lock`).

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct GrammarLock {
    pub python: String,
    #[serde(default)]
    pub rust: Option<String>,
    #[serde(default)]
    pub typescript: Option<String>,
    #[serde(default)]
    pub go: Option<String>,
    #[serde(default)]
    pub java: Option<String>,
    #[serde(default)]
    pub cpp: Option<String>,
    #[serde(default)]
    pub javascript: Option<String>,
    #[serde(default)]
    pub csharp: Option<String>,
    #[serde(default)]
    pub c: Option<String>,
}

impl GrammarLock {
    pub fn from_yaml_bytes(bytes: &[u8]) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_lock() {
        let y = br#"
python: "tree-sitter-python@0.25"
rust: "pending"
"#;
        let g = GrammarLock::from_yaml_bytes(y).unwrap();
        assert!(g.python.contains("python"));
    }
}
