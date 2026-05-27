//! **FR-4.11** — WAL checkpoint → **`ris:`** epoch mapping for time-travel queries.
//!
//! MCP **`commit_hash`** may be:
//! - a CIS **`wal_log_id`**: shorter hex (optional `0x`) or decimal.
//!
//! **Time-travel limitation:** `find_symbol_at` / `build_context_at` combine an RIS binding snapshot with the
//! **live** [`InMemoryGraph`](crate::graph::InMemoryGraph). Revisions removed after the anchor may be missing;
//! see [`QueryMeta::time_travel_uses_live_graph`].

use std::path::Path;
use std::sync::Arc;

use cis_wal::BranchId;

use crate::kv::MemoryKv;
use crate::revision_cow::RevisionIndexCow;

fn branch_hex(branch: BranchId) -> String {
    branch
        .0
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

/// KV: `tt:log:{branch_hex}:{log_id:020}` → epoch (u64 LE) into **`ris:`** snapshot.
pub fn wal_log_anchor_key(branch: BranchId, log_id: u64) -> String {
    format!("tt:log:{}:{:020}", branch_hex(branch), log_id)
}

fn epoch_seq_key(branch: BranchId) -> String {
    format!("tt:seq:{}", branch_hex(branch))
}

fn read_u64_le(v: &[u8]) -> Option<u64> {
    if v.len() != 8 {
        return None;
    }
    let mut a = [0u8; 8];
    a.copy_from_slice(v);
    Some(u64::from_le_bytes(a))
}

/// Monotonic per-branch epoch for RIS snapshots.
pub fn next_ris_epoch(kv: &MemoryKv, branch: BranchId) -> u64 {
    let k = epoch_seq_key(branch);
    let cur = kv.get(&k).as_deref().and_then(read_u64_le).unwrap_or(0);
    let next = cur.saturating_add(1).max(1);
    kv.set(&k, next.to_le_bytes().to_vec());
    next
}

/// Call when a WAL row reaches **`COMMITTED`**: write **`ris:`** + **`tt:log:`** mapping.
pub fn record_committed_snapshot(
    ri: &RevisionIndexCow,
    kv: &MemoryKv,
    branch: BranchId,
    log_id: u64,
    cis_dir: Option<&Path>,
) {
    let epoch = next_ris_epoch(kv, branch);
    ri.persist_ris_snapshot(epoch, cis_dir);
    kv.set(
        &wal_log_anchor_key(branch, log_id),
        epoch.to_le_bytes().to_vec(),
    );
}

/// Backward-compatible wrapper without metadata store path.
pub fn record_committed_snapshot_legacy(
    ri: &RevisionIndexCow,
    kv: &MemoryKv,
    branch: BranchId,
    log_id: u64,
) {
    record_committed_snapshot(ri, kv, branch, log_id, None);
}

pub fn resolve_epoch_for_wal_log(kv: &MemoryKv, branch: BranchId, log_id: u64) -> Option<u64> {
    let v = kv.get(&wal_log_anchor_key(branch, log_id))?;
    read_u64_le(&v)
}

pub fn overlay_at_wal_log(
    kv: &Arc<MemoryKv>,
    branch: BranchId,
    log_id: u64,
) -> Option<Arc<RevisionIndexCow>> {
    overlay_at_wal_log_at(kv, branch, log_id, None)
}

pub fn overlay_at_wal_log_at(
    kv: &Arc<MemoryKv>,
    branch: BranchId,
    log_id: u64,
    cis_dir: Option<&Path>,
) -> Option<Arc<RevisionIndexCow>> {
    let epoch = resolve_epoch_for_wal_log(kv, branch, log_id)?;
    RevisionIndexCow::from_ris_snapshot_at(branch, Arc::clone(kv), epoch, cis_dir)
}

/// KV: **`tt:git:{40_hex_oid}`** → **`wal_log_id`** (u64 LE) for MCP **`commit_hash`** indirection.
pub fn git_oid_anchor_key(git_oid_lower_hex: &str) -> String {
    format!("tt:git:{}", git_oid_lower_hex.trim().to_lowercase())
}

/// Register a Git commit OID → CIS WAL row id (call from indexer when head is known).
pub fn record_git_oid_wal_log(kv: &MemoryKv, git_oid_hex: &str, wal_log_id: u64) -> Result<(), &'static str> {
    let t = git_oid_hex.trim().to_lowercase();
    if t.len() != 40 || !t.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err("git_oid must be 40 lowercase hex characters");
    }
    kv.set(&git_oid_anchor_key(&t), wal_log_id.to_le_bytes().to_vec());
    Ok(())
}

pub fn resolve_wal_log_from_git_oid(kv: &MemoryKv, git_oid_hex: &str) -> Option<u64> {
    let t = git_oid_hex.trim().to_lowercase();
    if t.len() != 40 || !t.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let v = kv.get(&git_oid_anchor_key(&t))?;
    read_u64_le(&v)
}

/// MCP **`commit_hash`**: Git OID (**40 hex**, indexed) or CIS **`wal_log_id`**.
pub fn resolve_commit_anchor_to_wal_log(kv: &MemoryKv, commit_hash: &str) -> Result<u64, &'static str> {
    let t = commit_hash.trim();
    if t.is_empty() {
        return Err("empty commit_hash");
    }
    let stripped = t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")).unwrap_or(t);
    if stripped.len() == 40 && stripped.chars().all(|c| c.is_ascii_hexdigit()) {
        return resolve_wal_log_from_git_oid(kv, stripped)
            .ok_or("git commit_oid not indexed (record_git_oid_wal_log)");
    }
    parse_wal_log_anchor(t)
}

/// Parse MCP **`commit_hash`** as WAL log id.
pub fn parse_wal_log_anchor(s: &str) -> Result<u64, &'static str> {
    let t = s.trim();
    if t.is_empty() {
        return Err("empty commit_hash");
    }
    let t = t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")).unwrap_or(t);
    if t.chars().all(|c| c.is_ascii_hexdigit()) && !t.is_empty() {
        u64::from_str_radix(t, 16).map_err(|_| "commit_hash hex overflow")
    } else {
        t.parse::<u64>().map_err(|_| "commit_hash must be wal log id (hex or decimal)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cis_wal::{IdentityId, NodeRevisionId};

    #[test]
    fn git_oid_maps_to_wal_log() {
        let kv = Arc::new(MemoryKv::new());
        record_git_oid_wal_log(&kv, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 99).unwrap();
        assert_eq!(
            resolve_commit_anchor_to_wal_log(&kv, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Ok(99)
        );
        assert_eq!(
            resolve_commit_anchor_to_wal_log(&kv, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Ok(99)
        );
    }

    #[test]
    fn anchor_roundtrip_and_parse() {
        let kv = Arc::new(MemoryKv::new());
        let b = BranchId([3u8; 16]);
        let ri = RevisionIndexCow::root(b, Arc::clone(&kv));
        let i = IdentityId([9u8; 16]);
        let r = NodeRevisionId([8u8; 16]);
        ri.bind(i, r);
        record_committed_snapshot(&ri, &kv, b, 7, None);
        assert_eq!(resolve_epoch_for_wal_log(&kv, b, 7), Some(1));
        assert_eq!(parse_wal_log_anchor("7"), Ok(7));
        assert_eq!(parse_wal_log_anchor("0xa"), Ok(10));
        let loaded = overlay_at_wal_log(&kv, b, 7).unwrap();
        assert_eq!(loaded.lookup(i), Some(r));
    }
}
