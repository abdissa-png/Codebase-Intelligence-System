//! **`RevisionIndexCow`** — COW chain + **`ris:`** checkpoints (**FR-2.6**, **FR-4.11**).

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use cis_wal::{BranchId, IdentityId, NodeRevisionId};

use crate::kv::MemoryKv;
use crate::revision_index::revision_binding_kv_key;

const COMPACT_MAX_DEPTH: usize = 10;

fn parse_hex16(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Overlay resolved by walking **local** overrides then **parent** chain (**FR-2.6**).
#[derive(Debug)]
pub struct RevisionIndexCow {
    branch_id: BranchId,
    kv: Arc<MemoryKv>,
    parent: Option<Arc<RevisionIndexCow>>,
    local: Mutex<HashMap<IdentityId, NodeRevisionId>>,
}

impl RevisionIndexCow {
    pub fn root(branch_id: BranchId, kv: Arc<MemoryKv>) -> Arc<Self> {
        Arc::new(Self {
            branch_id,
            kv,
            parent: None,
            local: Mutex::new(HashMap::new()),
        })
    }

    /// **Cold start:** load existing `ri:{branch}:*` bindings from KV into the root overlay.
    pub fn root_hydrated(branch_id: BranchId, kv: Arc<MemoryKv>) -> Arc<Self> {
        let hex_branch = branch_id
            .0
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();
        let prefix = format!("ri:{}:", hex_branch);
        let mut local = HashMap::new();
        for (k, v) in kv.scan_prefix(&prefix) {
            let parts: Vec<&str> = k.split(':').collect();
            if parts.len() != 3 {
                continue;
            }
            let Some(identity) = parse_hex16(parts[2]) else {
                continue;
            };
            if v.len() != 16 {
                continue;
            }
            let mut rev = [0u8; 16];
            rev.copy_from_slice(&v);
            local.insert(IdentityId(identity), NodeRevisionId(rev));
        }
        Arc::new(Self {
            branch_id,
            kv,
            parent: None,
            local: Mutex::new(local),
        })
    }

    /// **Feature branch** overlay: inherits lookups from **`parent`** until locally rebound.
    pub fn fork(parent: Arc<RevisionIndexCow>, branch_id: BranchId) -> Arc<Self> {
        Arc::new(Self {
            branch_id,
            kv: Arc::clone(&parent.kv),
            parent: Some(parent),
            local: Mutex::new(HashMap::new()),
        })
    }

    pub fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub fn chain_depth(&self) -> usize {
        let mut d = 0;
        let mut p = &self.parent;
        while let Some(x) = p {
            d += 1;
            p = &x.parent;
        }
        d
    }

    /// All `identity_id → revision_id` pairs visible from this overlay (parent chain folded).
    pub fn resolved_bindings(&self) -> Vec<(IdentityId, NodeRevisionId)> {
        let mut merged = HashMap::new();
        Self::collect_bindings(self, &mut merged);
        merged.into_iter().collect()
    }

    pub fn lookup(&self, identity_id: IdentityId) -> Option<NodeRevisionId> {
        let key = revision_binding_kv_key(self.branch_id, identity_id);
        if let Some(v) = self.kv.get(&key) {
            if v.len() == 16 {
                let mut a = [0u8; 16];
                a.copy_from_slice(&v);
                let rid = NodeRevisionId(a);
                let _ = self.local.lock().unwrap().insert(identity_id, rid);
                return Some(rid);
            }
        }
        if let Some(r) = self.local.lock().unwrap().get(&identity_id).copied() {
            return Some(r);
        }
        self.parent.as_ref()?.lookup(identity_id)
    }

    /// **§01.6.6** — write-through to `ri:{branch}:{identity}` on this overlay’s branch id.
    pub fn bind(&self, identity_id: IdentityId, revision_id: NodeRevisionId) {
        self.local.lock().unwrap().insert(identity_id, revision_id);
        let key = revision_binding_kv_key(self.branch_id, identity_id);
        self.kv.set(&key, revision_id.0.to_vec());
    }

    /// Remove `ri:{branch}:{identity}` when the identity has no live revision.
    pub fn unbind(&self, identity_id: IdentityId) {
        self.local.lock().unwrap().remove(&identity_id);
        let key = revision_binding_kv_key(self.branch_id, identity_id);
        self.kv.delete(&key);
    }

    /// Flatten parent chain into **`self`** when depth exceeds policy (**v2.5** compaction).
    pub fn compacted_if_deep(self: Arc<Self>) -> Arc<Self> {
        if self.chain_depth() <= COMPACT_MAX_DEPTH {
            return self;
        }
        let mut merged = HashMap::new();
        Self::collect_bindings(&self, &mut merged);
        Arc::new(RevisionIndexCow {
            branch_id: self.branch_id,
            kv: Arc::clone(&self.kv),
            parent: None,
            local: Mutex::new(merged),
        })
    }

    fn collect_bindings(this: &RevisionIndexCow, out: &mut HashMap<IdentityId, NodeRevisionId>) {
        if let Some(p) = &this.parent {
            Self::collect_bindings(p, out);
        }
        for (&k, &v) in this.local.lock().unwrap().iter() {
            out.insert(k, v);
        }
    }

    /// Persist **resolved** bindings for time-travel materialization (**`ris:{branch}:{epoch}`**).
    pub fn persist_ris_snapshot(&self, epoch: u64, cis_dir: Option<&Path>) {
        let mut merged = HashMap::new();
        Self::collect_bindings(self, &mut merged);
        let key = ris_snapshot_kv_key(self.branch_id, epoch);
        let pairs: Vec<([u8; 16], [u8; 16])> = merged
            .into_iter()
            .map(|(i, r)| (i.0, r.0))
            .collect();
        let payload = serde_json::to_vec(&pairs).expect("ris json");
        self.kv.set(&key, payload.clone());
        #[cfg(feature = "body-sqlite")]
        if let Some(cis) = cis_dir {
            if crate::metadata_store::metadata_backend_from_env()
                == crate::metadata_store::MetadataBackendKind::Sqlite
            {
                if let Ok(store) = crate::metadata_store::MetadataStore::open(cis) {
                    let _ = store.put_ris_snapshot(self.branch_id, epoch, &payload);
                }
            }
        }
        let _ = cis_dir;
    }

    fn ris_payload_bytes(
        branch_id: BranchId,
        kv: &MemoryKv,
        epoch: u64,
        cis_dir: Option<&Path>,
    ) -> Option<Vec<u8>> {
        let key = ris_snapshot_kv_key(branch_id, epoch);
        if let Some(b) = kv.get(&key) {
            return Some(b);
        }
        #[cfg(feature = "body-sqlite")]
        if let Some(cis) = cis_dir {
            if crate::metadata_store::metadata_backend_from_env()
                == crate::metadata_store::MetadataBackendKind::Sqlite
            {
                if let Ok(store) = crate::metadata_store::MetadataStore::open(cis) {
                    if let Ok(Some(b)) = store.get_ris_snapshot(branch_id, epoch) {
                        return Some(b);
                    }
                }
            }
        }
        let _ = cis_dir;
        None
    }

    /// Load snapshot into a **root** index (no parent).
    pub fn from_ris_snapshot(
        branch_id: BranchId,
        kv: Arc<MemoryKv>,
        epoch: u64,
    ) -> Option<Arc<Self>> {
        Self::from_ris_snapshot_at(branch_id, kv, epoch, None)
    }

    /// Like [`from_ris_snapshot`] with SQLite metadata fallback when `cis_dir` is set.
    pub fn from_ris_snapshot_at(
        branch_id: BranchId,
        kv: Arc<MemoryKv>,
        epoch: u64,
        cis_dir: Option<&Path>,
    ) -> Option<Arc<Self>> {
        let bytes = Self::ris_payload_bytes(branch_id, kv.as_ref(), epoch, cis_dir)?;
        let pairs: Vec<([u8; 16], [u8; 16])> = serde_json::from_slice(&bytes).ok()?;
        let mut m = HashMap::new();
        for (a, b) in pairs {
            m.insert(IdentityId(a), NodeRevisionId(b));
        }
        Some(Arc::new(RevisionIndexCow {
            branch_id,
            kv,
            parent: None,
            local: Mutex::new(m),
        }))
    }
}

pub fn ris_snapshot_kv_key(branch_id: BranchId, epoch: u64) -> String {
    let hex = branch_id
        .0
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    format!("ris:{}:{:020}", hex, epoch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cow_lookup_falls_through_parent() {
        let kv = Arc::new(MemoryKv::new());
        let base = RevisionIndexCow::root(BranchId([1u8; 16]), Arc::clone(&kv));
        let i = IdentityId([9u8; 16]);
        let r = NodeRevisionId([8u8; 16]);
        base.bind(i, r);
        let feat = RevisionIndexCow::fork(Arc::clone(&base), BranchId([2u8; 16]));
        assert_eq!(feat.lookup(i), Some(r));
    }

    #[test]
    fn child_override_wins() {
        let kv = Arc::new(MemoryKv::new());
        let base = RevisionIndexCow::root(BranchId([1u8; 16]), Arc::clone(&kv));
        let i = IdentityId([9u8; 16]);
        base.bind(i, NodeRevisionId([1u8; 16]));
        let feat = RevisionIndexCow::fork(Arc::clone(&base), BranchId([2u8; 16]));
        feat.bind(i, NodeRevisionId([2u8; 16]));
        assert_eq!(feat.lookup(i), Some(NodeRevisionId([2u8; 16])));
    }

    #[test]
    fn compaction_flattens_deep_chain() {
        let kv = Arc::new(MemoryKv::new());
        let mut cur = RevisionIndexCow::root(BranchId([1u8; 16]), Arc::clone(&kv));
        let i = IdentityId([3u8; 16]);
        cur.bind(i, NodeRevisionId([4u8; 16]));
        for b in 2u8..=12 {
            cur = RevisionIndexCow::fork(cur, BranchId([b; 16]));
        }
        assert!(cur.chain_depth() > COMPACT_MAX_DEPTH);
        let flat = RevisionIndexCow::compacted_if_deep(cur);
        assert!(flat.chain_depth() <= COMPACT_MAX_DEPTH);
        assert_eq!(flat.lookup(i), Some(NodeRevisionId([4u8; 16])));
    }

    #[test]
    fn ris_roundtrip() {
        let kv = Arc::new(MemoryKv::new());
        let idx = RevisionIndexCow::root(BranchId([7u8; 16]), Arc::clone(&kv));
        idx.bind(IdentityId([5u8; 16]), NodeRevisionId([6u8; 16]));
        idx.persist_ris_snapshot(42, None);
        let loaded = RevisionIndexCow::from_ris_snapshot(BranchId([7u8; 16]), Arc::clone(&kv), 42).unwrap();
        assert_eq!(
            loaded.lookup(IdentityId([5u8; 16])),
            Some(NodeRevisionId([6u8; 16]))
        );
    }
}
