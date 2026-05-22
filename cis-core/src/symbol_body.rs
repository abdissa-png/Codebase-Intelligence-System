//! Resolve symbol source text with BodyStore → cis blob → disk fallback (**Phase 7**).

use std::path::Path;

use crate::body_blob::{load_body_blob_with_fallback, BodyBlobStore};
use crate::body_store::BodyStore;
use crate::graph::NodeRevision;
use crate::identity_resolution::body_snippet_for_span;
use crate::ingest::load_file_body;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BodySource {
    BodyStore,
    CisBlob,
    Disk,
    Stub,
}

pub fn resolve_revision_body(
    body_store: &BodyStore,
    blob_store: &dyn BodyBlobStore,
    cis_dir: &Path,
    repo_root: &Path,
    rev: &NodeRevision,
) -> (String, BodySource) {
    if let Some(bytes) = body_store.get(&rev.body_hash) {
        if let Ok(s) = String::from_utf8(bytes) {
            if !s.is_empty() {
                return (s, BodySource::BodyStore);
            }
        }
    }
    if let Ok(Some(bytes)) = load_body_blob_with_fallback(blob_store, cis_dir, &rev.body_hash) {
        if let Ok(s) = String::from_utf8(bytes.clone()) {
            if !s.is_empty() {
                body_store.put(rev.body_hash, bytes);
                return (s, BodySource::CisBlob);
            }
        }
    }
    if let Some(full) = load_file_body(body_store, &rev.file_path) {
        let snip = if rev.qualified_name == rev.file_path {
            full
        } else {
            body_snippet_for_span(&full, rev.span.start_line, rev.span.end_line)
        };
        if !snip.is_empty() {
            return (snip, BodySource::Disk);
        }
    }
    let abs = repo_root.join(&rev.file_path);
    if let Ok(full) = std::fs::read_to_string(&abs) {
        let snip = if rev.qualified_name == rev.file_path {
            full
        } else {
            body_snippet_for_span(&full, rev.span.start_line, rev.span.end_line)
        };
        if !snip.is_empty() {
            return (snip, BodySource::Disk);
        }
    }
    (
        format!(
            "{}\n{}\n// (body not available)\n",
            rev.qualified_name, rev.file_path
        ),
        BodySource::Stub,
    )
}

/// Convenience when only `cis_dir` is available (opens store per call).
pub fn resolve_revision_body_legacy(
    body_store: &BodyStore,
    cis_dir: &Path,
    repo_root: &Path,
    rev: &NodeRevision,
) -> (String, BodySource) {
    let store = crate::body_blob::open_body_blob_store(cis_dir);
    resolve_revision_body(body_store, store.as_ref(), cis_dir, repo_root, rev)
}
