//! **Phase 3** — `resolve_or_create`, tombstone window, body similarity (**FR-1.11**, **FR-1.12**).

use std::collections::HashSet;

use cis_wal::{BranchId, IdentityId, NodeRevisionId};

use crate::graph::{InMemoryGraph, NodeKind, NodeRevision, RevisionStatus};
use crate::identity_cas::IdentityProvisionalCas;
use crate::identity_resolver::{IdentityResolver, RenameEvidence, RenameSignalKind};
use crate::ranking_policy::RankingPolicy;
use crate::body_store::BodyStore;

/// Tunables for rename detection (from [`RankingPolicy`] / `.cis/ranking_policy.yaml`).
#[derive(Debug, Clone, Copy)]
pub struct RenameConfig {
    pub rename_min_confidence: f64,
    pub body_similarity_threshold: f64,
    pub name_proximity_threshold: f64,
    /// Tombstone candidates must be within this many days (wall clock).
    pub window_days: u32,
}

impl Default for RenameConfig {
    fn default() -> Self {
        let p = RankingPolicy::default();
        Self::from_policy(&p)
    }
}

impl RenameConfig {
    pub fn from_policy(p: &RankingPolicy) -> Self {
        Self {
            rename_min_confidence: p.rename_min_confidence,
            body_similarity_threshold: p.body_similarity_threshold,
            name_proximity_threshold: p.name_proximity_threshold,
            window_days: p.rename_detection_window_days,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolveOutcome {
    pub identity_id: IdentityId,
    /// When a rename was detected: tombstone revision + evidence for **`RENAMED_FROM`** edge.
    pub rename_link: Option<(NodeRevisionId, RenameEvidence)>,
}

/// Token-set Jaccard similarity in `[0, 1]`.
pub fn token_jaccard(a: &str, b: &str) -> f64 {
    let ta = token_set(a);
    let tb = token_set(b);
    if ta.is_empty() && tb.is_empty() {
        return 1.0;
    }
    if ta.is_empty() || tb.is_empty() {
        return 0.0;
    }
    let inter = ta.intersection(&tb).count();
    let union = ta.union(&tb).count();
    inter as f64 / union as f64
}

fn token_set(s: &str) -> HashSet<String> {
    s.split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|t| !t.is_empty())
        .map(|t| t.to_ascii_lowercase())
        .collect()
}

/// Tokens that do not distinguish stub bodies (`def foo(): pass`).
fn is_stop_token(t: &str) -> bool {
    matches!(
        t,
        "def"
            | "async"
            | "return"
            | "pass"
            | "none"
            | "true"
            | "false"
            | "self"
            | "cls"
            | "class"
    )
}

/// Leaf identifier for rename proximity: last `::` segment, then last `.` segment.
/// Handles production QNs like `app.py::compute` and `Board.py::Board.method`.
pub fn name_leaf(qn: &str) -> &str {
    let after_ns = qn.rsplit("::").next().unwrap_or(qn);
    after_ns.rsplit('.').next().unwrap_or(after_ns)
}

/// Simple name similarity on the leaf segment of `qualified_name`.
pub fn name_proximity(a: &str, b: &str) -> f64 {
    let sa = name_leaf(a);
    let sb = name_leaf(b);
    if sa == sb {
        return 1.0;
    }
    let max_len = sa.len().max(sb.len()).max(1);
    let dist = levenshtein(sa.chars().collect::<Vec<_>>().as_slice(), sb.chars().collect::<Vec<_>>().as_slice());
    1.0 - (dist as f64 / max_len as f64)
}

fn levenshtein(a: &[char], b: &[char]) -> usize {
    let m = b.len();
    let mut prev: Vec<usize> = (0..=m).collect();
    let mut cur = vec![0usize; m + 1];
    for (i, ca) in a.iter().enumerate() {
        cur[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            cur[j + 1] = (prev[j + 1] + 1)
                .min(cur[j] + 1)
                .min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[m]
}

/// Extract a best-effort body snippet for similarity.
///
/// When `end_line > start_line` the span is trusted (tree-sitter full node) and no
/// indent-continuation is applied — otherwise sibling class methods would bleed in.
/// Single-line spans (regex def-line only) still continue through the indented block.
///
/// `start_line == 0` is treated as unknown / non-file: returns empty. File hubs must
/// pass the full file content directly (ingest already does).
pub fn body_snippet_for_span(file_content: &str, start_line: u32, end_line: u32) -> String {
    if start_line == 0 {
        return String::new();
    }
    let lines: Vec<&str> = file_content.lines().collect();
    let start_idx = (start_line.saturating_sub(1)) as usize;
    let end_idx = (end_line.max(start_line).saturating_sub(1)) as usize;
    if start_idx >= lines.len() {
        return String::new();
    }
    let end_idx = end_idx.min(lines.len() - 1);
    // Accurate multi-line span: do not continue past end_line.
    if end_line > start_line {
        return lines[start_idx..=end_idx].join("\n");
    }
    let mut out: Vec<&str> = lines[start_idx..=end_idx].to_vec();
    let mut i = end_idx + 1;
    while i < lines.len() {
        let line = lines[i];
        if line.is_empty() {
            out.push(line);
            i += 1;
            continue;
        }
        let Some(first) = line.chars().next() else {
            break;
        };
        if first == ' ' || first == '\t' {
            out.push(line);
            i += 1;
        } else {
            break;
        }
    }
    out.join("\n")
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Active tombstones on `branch`, optionally restricted to `file_path`.
/// With `window_days == 0` (via [`scan_tombstone_window`]) no age filter is applied.
pub fn scan_tombstone_window<'a>(
    graph: &'a InMemoryGraph,
    branch: BranchId,
    file_path: Option<&str>,
) -> Vec<&'a NodeRevision> {
    scan_tombstone_window_with_days(graph, branch, file_path, 0)
}

/// Like [`scan_tombstone_window`] but honours a maximum age in days.
///
/// Tombstones without `tombstoned_at_ms` are treated as **older than the window**
/// (excluded when `window_days > 0`) so they are not permanent rename candidates.
pub fn scan_tombstone_window_with_days<'a>(
    graph: &'a InMemoryGraph,
    branch: BranchId,
    file_path: Option<&str>,
    window_days: u32,
) -> Vec<&'a NodeRevision> {
    let now = now_ms();
    let cutoff_ms: Option<u64> = if window_days > 0 {
        Some(now.saturating_sub(window_days as u64 * 86_400_000))
    } else {
        None
    };
    graph
        .tombstone_revisions_on_branch(branch, file_path)
        .into_iter()
        .filter(|r| {
            cutoff_ms
                .map(|cut| r.tombstoned_at_ms.map(|ts| ts >= cut).unwrap_or(false))
                .unwrap_or(true)
        })
        .collect()
}

/// Placeholder used when masking symbol leaf names for same-file body compare.
const RENAME_NAME_MASK: &str = "__cis_nm__";

/// Replace whole-identifier occurrences of `leaf` so a rename does not poison Jaccard.
fn mask_ident(body: &str, leaf: &str) -> String {
    if leaf.is_empty() {
        return body.to_string();
    }
    let mut out = String::with_capacity(body.len());
    let chars: Vec<char> = body.chars().collect();
    let leaf_chars: Vec<char> = leaf.chars().collect();
    let n = chars.len();
    let m = leaf_chars.len();
    let is_ident = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let mut i = 0;
    while i < n {
        if m > 0
            && i + m <= n
            && chars[i..i + m] == leaf_chars[..]
            && (i == 0 || !is_ident(chars[i - 1]))
            && (i + m >= n || !is_ident(chars[i + m]))
        {
            out.push_str(RENAME_NAME_MASK);
            i += m;
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

fn body_similarity_signal(
    new_body: &str,
    tombstone: &NodeRevision,
    body_store: Option<&BodyStore>,
    threshold: f64,
) -> Option<RenameEvidence> {
    body_similarity_signal_masked(new_body, None, tombstone, None, body_store, threshold)
}

/// Body similarity with optional leaf-name masking (same-file rename: `foo` → `bar`).
fn body_similarity_signal_masked(
    new_body: &str,
    new_leaf: Option<&str>,
    tombstone: &NodeRevision,
    old_leaf: Option<&str>,
    body_store: Option<&BodyStore>,
    threshold: f64,
) -> Option<RenameEvidence> {
    let old_body = body_store
        .and_then(|bs| bs.get(&tombstone.body_hash))
        .and_then(|v| String::from_utf8(v).ok())
        .unwrap_or_default();
    if old_body.is_empty() || new_body.is_empty() {
        return None;
    }
    let new_cmp = match new_leaf {
        Some(leaf) => mask_ident(new_body, leaf),
        None => new_body.to_string(),
    };
    let old_cmp = match old_leaf {
        Some(leaf) => mask_ident(&old_body, leaf),
        None => old_body,
    };
    // Stub bodies (`def x(): pass`) share stop-tokens only — reject as rename evidence.
    // Count after masking so the renamed identifier does not inflate stub token counts.
    // Same-file masked compare allows a single non-stop token (e.g. `return 1`);
    // unmasked / cross-file keeps the stricter >= 2 guard.
    let min_meaningful = if new_leaf.is_some() || old_leaf.is_some() {
        1
    } else {
        2
    };
    let new_meaningful = token_set(&new_cmp)
        .into_iter()
        .filter(|t| !is_stop_token(t) && t != RENAME_NAME_MASK)
        .count();
    let old_meaningful = token_set(&old_cmp)
        .into_iter()
        .filter(|t| !is_stop_token(t) && t != RENAME_NAME_MASK)
        .count();
    if new_meaningful < min_meaningful || old_meaningful < min_meaningful {
        return None;
    }
    // Same-file (masked): Jaccard over meaningful tokens only so stop-tokens (`def`,
    // `return`) cannot make `return 1` vs `return 2` look like a rename.
    // Cross-file: full token Jaccard (includes structural stop-tokens).
    let sim = if new_leaf.is_some() || old_leaf.is_some() {
        let a: HashSet<String> = token_set(&new_cmp)
            .into_iter()
            .filter(|t| !is_stop_token(t) && t != RENAME_NAME_MASK)
            .collect();
        let b: HashSet<String> = token_set(&old_cmp)
            .into_iter()
            .filter(|t| !is_stop_token(t) && t != RENAME_NAME_MASK)
            .collect();
        if a.is_empty() && b.is_empty() {
            1.0
        } else if a.is_empty() || b.is_empty() {
            0.0
        } else {
            let inter = a.intersection(&b).count();
            let union = a.union(&b).count();
            inter as f64 / union as f64
        }
    } else {
        token_jaccard(&new_cmp, &old_cmp)
    };
    if sim >= threshold {
        Some(RenameEvidence {
            kind: RenameSignalKind::AstBodySimilarity,
            confidence: sim,
        })
    } else {
        None
    }
}

fn name_proximity_signal(
    new_qn: &str,
    tombstone_qn: &str,
    threshold: f64,
) -> Option<RenameEvidence> {
    let sim = name_proximity(new_qn, tombstone_qn);
    if sim >= threshold {
        Some(RenameEvidence {
            kind: RenameSignalKind::NameProximity,
            confidence: sim,
        })
    } else {
        None
    }
}

fn tombstone_body_available(tombstone: &NodeRevision, body_store: Option<&BodyStore>) -> bool {
    body_store
        .and_then(|bs| bs.get(&tombstone.body_hash))
        .map(|v| !v.is_empty())
        .unwrap_or(false)
}

/// Same-file: when both bodies are available, require a body similarity signal so
/// near-names with unrelated bodies (`compute` → `compute2`) do not rename.
fn same_file_rename_allowed(
    candidate: &NodeRevision,
    path: &str,
    body_text: &str,
    signals: &[RenameEvidence],
    body_store: Option<&BodyStore>,
) -> bool {
    if candidate.file_path != path {
        return true;
    }
    let bodies_comparable =
        !body_text.is_empty() && tombstone_body_available(candidate, body_store);
    if !bodies_comparable {
        return true;
    }
    signals
        .iter()
        .any(|s| s.kind == RenameSignalKind::AstBodySimilarity)
}

/// Score tombstone `candidate` against a new symbol at `path` with `qualified_name` and `body_text`.
pub fn score_rename_candidate(
    candidate: &NodeRevision,
    path: &str,
    qualified_name: &str,
    body_text: &str,
    config: &RenameConfig,
    body_store: Option<&BodyStore>,
) -> Vec<RenameEvidence> {
    let mut signals = Vec::new();
    if candidate.file_path == path {
        if let Some(s) =
            name_proximity_signal(qualified_name, &candidate.qualified_name, config.name_proximity_threshold)
        {
            signals.push(s);
        }
    }
    // Same-file renames (e.g. `foo` → `bar`) often score ~0.5 token Jaccard; relax locally
    // and mask leaf names so the rename itself does not poison body similarity.
    let body_thresh = if candidate.file_path == path {
        config.body_similarity_threshold.min(0.4)
    } else {
        config.body_similarity_threshold
    };
    let body_signal = if candidate.file_path == path {
        body_similarity_signal_masked(
            body_text,
            Some(name_leaf(qualified_name)),
            candidate,
            Some(name_leaf(&candidate.qualified_name)),
            body_store,
            body_thresh,
        )
    } else {
        body_similarity_signal(body_text, candidate, body_store, body_thresh)
    };
    if let Some(s) = body_signal {
        signals.push(s);
    }
    signals
}

fn is_file_hub_revision(graph: &InMemoryGraph, rev: &NodeRevision) -> bool {
    graph.identity_kind(rev.identity_id) == Some(NodeKind::File)
        || rev.qualified_name == rev.file_path
}

/// Pick best tombstone rename candidate, if any.
///
/// `claimed` tombstones already taken by an earlier symbol in this ingest batch are skipped.
pub fn best_tombstone_rename(
    graph: &InMemoryGraph,
    branch: BranchId,
    path: &str,
    qualified_name: &str,
    body_text: &str,
    resolver: &IdentityResolver,
    config: &RenameConfig,
    body_store: Option<&BodyStore>,
    claimed: &HashSet<NodeRevisionId>,
) -> Option<(IdentityId, NodeRevisionId, RenameEvidence)> {
    let candidates = scan_tombstone_window_with_days(graph, branch, Some(path), config.window_days);
    let mut best: Option<(IdentityId, NodeRevisionId, RenameEvidence, f64)> = None;
    for cand in candidates {
        if claimed.contains(&cand.revision_id) {
            continue;
        }
        if is_file_hub_revision(graph, cand) {
            continue;
        }
        let signals = score_rename_candidate(cand, path, qualified_name, body_text, config, body_store);
        if !resolver.should_emit_rename(&signals) {
            continue;
        }
        if !same_file_rename_allowed(cand, path, body_text, &signals, body_store) {
            continue;
        }
        let conf = IdentityResolver::combined_confidence(&signals);
        let primary = signals
            .iter()
            .max_by(|a, b| a.confidence.partial_cmp(&b.confidence).unwrap_or(std::cmp::Ordering::Equal))
            .cloned()
            .unwrap_or(RenameEvidence {
                kind: RenameSignalKind::AstBodySimilarity,
                confidence: conf,
            });
        if best.as_ref().map(|(_, _, _, c)| conf > *c).unwrap_or(true) {
            best = Some((cand.identity_id, cand.revision_id, primary, conf));
        }
    }
    best.map(|(id, rev, ev, _)| (id, rev, ev))
}

/// Cross-file: tombstones anywhere on branch with high body similarity.
pub fn best_cross_file_rename(
    graph: &InMemoryGraph,
    branch: BranchId,
    path: &str,
    qualified_name: &str,
    body_text: &str,
    resolver: &IdentityResolver,
    config: &RenameConfig,
    body_store: Option<&BodyStore>,
    claimed: &HashSet<NodeRevisionId>,
) -> Option<(IdentityId, NodeRevisionId, RenameEvidence)> {
    let candidates = scan_tombstone_window_with_days(graph, branch, None, config.window_days)
        .into_iter()
        .filter(|c| c.file_path != path)
        .filter(|c| !claimed.contains(&c.revision_id))
        .filter(|c| !is_file_hub_revision(graph, c))
        .collect::<Vec<_>>();
    let mut best: Option<(IdentityId, NodeRevisionId, RenameEvidence, f64)> = None;
    for cand in candidates {
        let signals = score_rename_candidate(cand, path, qualified_name, body_text, config, body_store);
        if !signals.iter().any(|s| s.kind == RenameSignalKind::AstBodySimilarity) {
            continue;
        }
        if !resolver.should_emit_rename(&signals) {
            continue;
        }
        let conf = IdentityResolver::combined_confidence(&signals);
        let primary = signals
            .into_iter()
            .find(|s| s.kind == RenameSignalKind::AstBodySimilarity)
            .unwrap_or(RenameEvidence {
                kind: RenameSignalKind::AstBodySimilarity,
                confidence: conf,
            });
        if best.as_ref().map(|(_, _, _, c)| conf > *c).unwrap_or(true) {
            best = Some((cand.identity_id, cand.revision_id, primary, conf));
        }
    }
    best.map(|(id, rev, ev, _)| (id, rev, ev))
}

/// **FR-1.11** — resolve identity for a new symbol (tombstone rename or fresh id).
///
/// `claimed` accumulates tombstone revision ids already linked in this ingest batch.
pub fn resolve_or_create(
    graph: &InMemoryGraph,
    branch: BranchId,
    path: &str,
    qualified_name: &str,
    body_text: &str,
    proposed_identity: IdentityId,
    resolver: &IdentityResolver,
    config: &RenameConfig,
    body_store: Option<&BodyStore>,
    cas: Option<&IdentityProvisionalCas>,
    semantic_hash: [u8; 32],
    claimed: &mut HashSet<NodeRevisionId>,
) -> ResolveOutcome {
    if let Some((id, tomb_rev, ev)) = best_tombstone_rename(
        graph,
        branch,
        path,
        qualified_name,
        body_text,
        resolver,
        config,
        body_store,
        claimed,
    ) {
        claimed.insert(tomb_rev);
        return ResolveOutcome {
            identity_id: id,
            rename_link: Some((tomb_rev, ev)),
        };
    }

    if let Some((id, tomb_rev, ev)) = best_cross_file_rename(
        graph,
        branch,
        path,
        qualified_name,
        body_text,
        resolver,
        config,
        body_store,
        claimed,
    ) {
        claimed.insert(tomb_rev);
        return ResolveOutcome {
            identity_id: id,
            rename_link: Some((tomb_rev, ev)),
        };
    }

    if let Some(cas) = cas {
        if let Ok(Some(id)) = cas.try_allocate(branch, semantic_hash, proposed_identity) {
            return ResolveOutcome {
                identity_id: id,
                rename_link: None,
            };
        }
        if let Some(id) = cas.poll_ready(branch, semantic_hash) {
            return ResolveOutcome {
                identity_id: id,
                rename_link: None,
            };
        }
    }

    ResolveOutcome {
        identity_id: proposed_identity,
        rename_link: None,
    }
}

fn do_tombstone(
    graph: &mut InMemoryGraph,
    rids: Vec<NodeRevisionId>,
    absence: Option<&crate::deletion_absence::DeletionAbsenceStore>,
) {
    let ts = now_ms();
    for rid in rids {
        if let Some(rev) = graph.get_revision(rid).cloned() {
            if let Some(store) = absence {
                store.mark_deleted(rev.branch_id, rev.identity_id);
            }
            let mut updated = rev;
            updated.status = RevisionStatus::Tombstone;
            updated.tombstoned_at_ms = Some(ts);
            graph.put_revision(updated);
        }
    }
}

/// Plant branch-local tombstones that hide inherited (other-branch) live symbols on `file_path`
/// whose qualified names are not in `retained`. Parent revisions keep their own `branch_id`.
///
/// Also writes durable [`crate::deletion_absence`] markers so hiding survives tombstone GC.
fn plant_inherited_file_tombstones(
    graph: &mut InMemoryGraph,
    branch: BranchId,
    file_path: &str,
    retained_qualified_names: &HashSet<String>,
    absence: Option<&crate::deletion_absence::DeletionAbsenceStore>,
) {
    let inherited: Vec<NodeRevision> = graph
        .revisions()
        .filter(|r| {
            r.branch_id != branch
                && r.file_path == file_path
                && matches!(
                    r.status,
                    RevisionStatus::Active | RevisionStatus::Speculative
                )
                && !retained_qualified_names.contains(&r.qualified_name)
        })
        .cloned()
        .collect();

    let ts = now_ms();
    for parent_rev in inherited {
        if let Some(store) = absence {
            store.mark_deleted(branch, parent_rev.identity_id);
        }
        if let Some(local) = graph.primary_revision_for_identity(branch, parent_rev.identity_id) {
            if matches!(
                local.status,
                RevisionStatus::Active | RevisionStatus::Speculative | RevisionStatus::Tombstone
            ) {
                // Local tombstone (from `do_tombstone`) already hides; absence marker is enough
                // for post-GC durability. Skip planting a duplicate `$tomb:` row.
                continue;
            }
        }
        let tomb_rid = NodeRevisionId(crate::index_model::stable_rev_id_bytes(
            branch,
            file_path,
            &format!("$tomb:{}", parent_rev.qualified_name),
        ));
        if graph.get_revision(tomb_rid).is_some() {
            if let Some(existing) = graph.get_revision(tomb_rid).cloned() {
                if !matches!(existing.status, RevisionStatus::Tombstone) {
                    let mut updated = existing;
                    updated.status = RevisionStatus::Tombstone;
                    updated.tombstoned_at_ms = Some(ts);
                    graph.put_revision(updated);
                }
            }
            continue;
        }
        graph.put_revision(NodeRevision {
            revision_id: tomb_rid,
            identity_id: parent_rev.identity_id,
            branch_id: branch,
            status: RevisionStatus::Tombstone,
            qualified_name: parent_rev.qualified_name.clone(),
            file_path: file_path.to_string(),
            body_hash: parent_rev.body_hash,
            signature_hash: parent_rev.signature_hash,
            language: parent_rev.language,
            parent_revision_id: Some(parent_rev.revision_id),
            rename_source_id: None,
            span: parent_rev.span,
            tombstoned_at_ms: Some(ts),
        });
    }
}

/// Tombstone active/speculative revisions for `file_path` whose qualified name is not in
/// `retained_names`, and plant branch-local tombstones that hide **inherited** symbols
/// removed from this file (parent revisions keep their own `branch_id`).
///
/// When `absence` is provided, durable `deleted:` KV markers are written so deletions
/// survive tombstone GC.
pub fn tombstone_orphaned_file_symbols(
    graph: &mut InMemoryGraph,
    branch: BranchId,
    file_path: &str,
    retained_qualified_names: &HashSet<String>,
) {
    tombstone_orphaned_file_symbols_with_absence(
        graph,
        branch,
        file_path,
        retained_qualified_names,
        None,
    );
}

/// Like [`tombstone_orphaned_file_symbols`] with an optional absence store.
pub fn tombstone_orphaned_file_symbols_with_absence(
    graph: &mut InMemoryGraph,
    branch: BranchId,
    file_path: &str,
    retained_qualified_names: &HashSet<String>,
    absence: Option<&crate::deletion_absence::DeletionAbsenceStore>,
) {
    let to_tombstone: Vec<NodeRevisionId> = graph
        .revisions()
        .filter(|r| {
            r.branch_id == branch
                && r.file_path == file_path
                && matches!(
                    r.status,
                    RevisionStatus::Active | RevisionStatus::Speculative
                )
                && !retained_qualified_names.contains(&r.qualified_name)
        })
        .map(|r| r.revision_id)
        .collect();
    do_tombstone(graph, to_tombstone, absence);
    plant_inherited_file_tombstones(
        graph,
        branch,
        file_path,
        retained_qualified_names,
        absence,
    );
}

/// Tombstone **all** active revisions for `file_path` (used when the file is deleted).
/// Also plants local tombstones for inherited symbols and clears outbound Calls/Imports;
/// RENAMED_FROM edges are preserved so rename detection still works within the window.
pub fn tombstone_all_file_symbols(
    graph: &mut InMemoryGraph,
    branch: BranchId,
    file_path: &str,
) {
    tombstone_all_file_symbols_with_absence(graph, branch, file_path, None);
}

/// Like [`tombstone_all_file_symbols`] with an optional absence store.
pub fn tombstone_all_file_symbols_with_absence(
    graph: &mut InMemoryGraph,
    branch: BranchId,
    file_path: &str,
    absence: Option<&crate::deletion_absence::DeletionAbsenceStore>,
) {
    let to_tombstone: Vec<NodeRevisionId> = graph
        .revisions()
        .filter(|r| {
            r.branch_id == branch
                && r.file_path == file_path
                && matches!(
                    r.status,
                    RevisionStatus::Active | RevisionStatus::Speculative
                )
        })
        .map(|r| r.revision_id)
        .collect();
    do_tombstone(graph, to_tombstone.clone(), absence);
    plant_inherited_file_tombstones(graph, branch, file_path, &HashSet::new(), absence);
    for rid in to_tombstone {
        // Retain RENAMED_FROM edges (needed for cross-file rename detection).
        // Clear Calls/Imports — stale edges from a deleted symbol.
        let remaining: Vec<_> = graph
            .outbound_edges(rid)
            .iter()
            .filter(|e| e.ty == crate::graph::EdgeType::RenamedFrom)
            .cloned()
            .collect();
        let _ = graph.replace_edges_for_revision(rid, remaining);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{
        Language, NodeIdentity, NodeKind, NodeRevision, RevisionStatus, SourceSpan,
    };

    fn rev(
        rid: u8,
        iid: u8,
        path: &str,
        qn: &str,
        status: RevisionStatus,
        body_hash: [u8; 32],
    ) -> NodeRevision {
        NodeRevision {
            revision_id: NodeRevisionId([rid; 16]),
            identity_id: IdentityId([iid; 16]),
            branch_id: BranchId([0u8; 16]),
            status,
            qualified_name: qn.into(),
            file_path: path.into(),
            body_hash,
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: if matches!(status, RevisionStatus::Tombstone) {
                Some(now_ms())
            } else {
                None
            },
        }
    }

    #[test]
    fn token_jaccard_identical() {
        assert!((token_jaccard("a = 1\nreturn a", "a = 1\nreturn a") - 1.0).abs() < 1e-9);
    }

    #[test]
    fn name_leaf_strips_path_and_class() {
        assert_eq!(name_leaf("app.py::compute"), "compute");
        assert_eq!(name_leaf("Board.py::Board.getStrPosition"), "getStrPosition");
        assert_eq!(name_leaf("m.compute"), "compute");
    }

    #[test]
    fn body_snippet_unknown_span_is_empty() {
        let src = "def a():\n    return 1\n\ndef b():\n    return 2\n";
        assert!(body_snippet_for_span(src, 0, 0).is_empty());
    }

    #[test]
    fn body_snippet_trusts_multiline_span() {
        let src = "class C:\n    def a(self):\n        return 1\n\n    def b(self):\n        return 2\n";
        // lines 2-3 = method a only (1-based)
        let snip = body_snippet_for_span(src, 2, 3);
        assert!(snip.contains("def a"), "{snip}");
        assert!(!snip.contains("def b"), "must not bleed into sibling: {snip}");
    }

    #[test]
    fn cross_file_move_via_resolve_or_create() {
        let mut g = InMemoryGraph::default();
        let branch = BranchId([0u8; 16]);
        let body = "def big_helper():\n    x = 1\n    y = 2\n    return x + y\n";
        let body_hash = [7u8; 32];
        let kv = std::sync::Arc::new(crate::MemoryKv::new());
        let bs = BodyStore::new(std::sync::Arc::clone(&kv));
        bs.put(body_hash, body.as_bytes().to_vec());

        let i_old = IdentityId([1u8; 16]);
        g.put_identity(NodeIdentity {
            identity_id: i_old,
            kind: NodeKind::Function,
        });
        g.put_revision(rev(
            1,
            1,
            "a.py",
            "a.py::big_helper",
            RevisionStatus::Tombstone,
            body_hash,
        ));

        let resolver = IdentityResolver::from_policy(0.3);
        let config = RenameConfig {
            rename_min_confidence: 0.3,
            body_similarity_threshold: 0.3,
            name_proximity_threshold: 0.85,
            window_days: 30,
        };
        let mut claimed = HashSet::new();
        let out = resolve_or_create(
            &g,
            branch,
            "b.py",
            "b.py::big_helper",
            body,
            IdentityId([2u8; 16]),
            &resolver,
            &config,
            Some(&bs),
            None,
            [0u8; 32],
            &mut claimed,
        );
        assert_eq!(out.identity_id, i_old, "cross-file body match should rename");
        assert!(out.rename_link.is_some());
    }

    #[test]
    fn same_file_thin_body_rename_masks_leaf_names() {
        let mut g = InMemoryGraph::default();
        let path = "m.py";
        let body = "def foo():\n    return 1\n";
        let body_hash = [9u8; 32];
        let kv = std::sync::Arc::new(crate::MemoryKv::new());
        let bs = BodyStore::new(std::sync::Arc::clone(&kv));
        bs.put(body_hash, body.as_bytes().to_vec());

        let i_old = IdentityId([1u8; 16]);
        g.put_identity(NodeIdentity {
            identity_id: i_old,
            kind: NodeKind::Function,
        });
        g.put_revision(rev(
            1,
            1,
            path,
            "m.py::foo",
            RevisionStatus::Tombstone,
            body_hash,
        ));

        let resolver = IdentityResolver::from_policy(0.55);
        let config = RenameConfig::default();
        let mut claimed = HashSet::new();
        let out = resolve_or_create(
            &g,
            BranchId([0u8; 16]),
            path,
            "m.py::bar",
            "def bar():\n    return 1\n",
            IdentityId([2u8; 16]),
            &resolver,
            &config,
            Some(&bs),
            None,
            [0u8; 32],
            &mut claimed,
        );
        assert_eq!(out.identity_id, i_old);
        assert!(out.rename_link.is_some());
    }

    #[test]
    fn same_file_rename_preserves_identity() {
        let mut g = InMemoryGraph::default();
        let path = "m.py";
        let body = "def foo():\n    x = 1\n    y = 2\n    return x + y\n";
        let body_hash = [9u8; 32];
        let kv = std::sync::Arc::new(crate::MemoryKv::new());
        let bs = BodyStore::new(std::sync::Arc::clone(&kv));
        bs.put(body_hash, body.as_bytes().to_vec());

        let i_old = IdentityId([1u8; 16]);
        g.put_identity(NodeIdentity {
            identity_id: i_old,
            kind: NodeKind::Function,
        });
        let tomb = rev(1, 1, path, "m.foo", RevisionStatus::Tombstone, body_hash);
        g.put_revision(tomb);

        let resolver = IdentityResolver::from_policy(0.5);
        let config = RenameConfig {
            rename_min_confidence: 0.5,
            body_similarity_threshold: 0.4,
            name_proximity_threshold: 0.3,
            window_days: 30,
        };
        let new_body = "def bar():\n    x = 1\n    y = 2\n    return x + y\n";
        let mut claimed = HashSet::new();
        let out = resolve_or_create(
            &g,
            BranchId([0u8; 16]),
            path,
            "m.bar",
            new_body,
            i_old,
            &resolver,
            &config,
            Some(&bs),
            None,
            [0u8; 32],
            &mut claimed,
        );
        assert_eq!(out.identity_id, i_old);
        assert!(out.rename_link.is_some());
        assert_eq!(claimed.len(), 1);
    }

    #[test]
    fn stub_bodies_do_not_false_rename() {
        let mut g = InMemoryGraph::default();
        let path = "m.py";
        let body = "def foo():\n    pass\n";
        let body_hash = [9u8; 32];
        let kv = std::sync::Arc::new(crate::MemoryKv::new());
        let bs = BodyStore::new(std::sync::Arc::clone(&kv));
        bs.put(body_hash, body.as_bytes().to_vec());
        let i_old = IdentityId([1u8; 16]);
        g.put_identity(NodeIdentity {
            identity_id: i_old,
            kind: NodeKind::Function,
        });
        g.put_revision(rev(1, 1, path, "m.foo", RevisionStatus::Tombstone, body_hash));
        let resolver = IdentityResolver::default();
        let config = RenameConfig::default();
        let mut claimed = HashSet::new();
        let out = resolve_or_create(
            &g,
            BranchId([0u8; 16]),
            path,
            "m.bar",
            "def bar():\n    pass\n",
            IdentityId([2u8; 16]),
            &resolver,
            &config,
            Some(&bs),
            None,
            [0u8; 32],
            &mut claimed,
        );
        assert_ne!(out.identity_id, i_old, "stubs must not merge");
        assert!(out.rename_link.is_none());
    }

    #[test]
    fn untimestamped_tombstone_excluded_from_window() {
        let mut g = InMemoryGraph::default();
        let path = "m.py";
        let mut tomb = rev(1, 1, path, "m.foo", RevisionStatus::Tombstone, [1u8; 32]);
        tomb.tombstoned_at_ms = None;
        g.put_revision(tomb);
        let found = scan_tombstone_window_with_days(&g, BranchId([0u8; 16]), Some(path), 30);
        assert!(found.is_empty(), "missing timestamp must be out of window");
    }

    #[test]
    fn different_bodies_do_not_false_rename() {
        let mut g = InMemoryGraph::default();
        let path = "m.py";
        let body_hash = [9u8; 32];
        let kv = std::sync::Arc::new(crate::MemoryKv::new());
        let bs = BodyStore::new(std::sync::Arc::clone(&kv));
        bs.put(body_hash, b"def foo(): pass".to_vec());

        let i_old = IdentityId([2u8; 16]);
        g.put_identity(NodeIdentity {
            identity_id: i_old,
            kind: NodeKind::Function,
        });
        g.put_revision(rev(
            2,
            2,
            path,
            "m.foo",
            RevisionStatus::Tombstone,
            body_hash,
        ));

        let resolver = IdentityResolver::default();
        let config = RenameConfig::default();
        let proposed = IdentityId([99u8; 16]);
        let mut claimed = HashSet::new();
        let out = resolve_or_create(
            &g,
            BranchId([0u8; 16]),
            path,
            "m.bar",
            "class CompletelyDifferent:\n    def run(self):\n        return 99\n",
            proposed,
            &resolver,
            &config,
            Some(&bs),
            None,
            [1u8; 32],
            &mut claimed,
        );
        assert_ne!(out.identity_id, i_old);
        assert!(out.rename_link.is_none());
    }
}
