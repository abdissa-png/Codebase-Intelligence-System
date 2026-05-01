//! Deterministic **`chunk_id`** = hash(identity ‖ revision ‖ chunk_index) (**SP-4**).

use cis_wal::{IdentityId, NodeRevisionId};

#[inline]
fn fnv1a128(data: &[u8], seed: u128) -> u128 {
    let mut h = seed;
    const P: u128 = 0x100000001b3;
    for &b in data {
        h ^= b as u128;
        h = h.wrapping_mul(P);
    }
    h
}

/// **DESIGNED:** dual FNV-1a streams → 32-byte id (dependency-free; stable across retries).
pub fn chunk_id(identity_id: IdentityId, revision_id: NodeRevisionId, chunk_index: u32) -> [u8; 32] {
    let mut buf = [0u8; 36];
    buf[..16].copy_from_slice(&identity_id.0);
    buf[16..32].copy_from_slice(&revision_id.0);
    buf[32..].copy_from_slice(&chunk_index.to_le_bytes());
    let h1 = fnv1a128(&buf, 0xcbf29ce484222325);
    let h2 = fnv1a128(&buf, 0x84222325cbf29ce4);
    let mut out = [0u8; 32];
    out[..16].copy_from_slice(&h1.to_le_bytes());
    out[16..].copy_from_slice(&h2.to_le_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_across_calls() {
        let i = IdentityId([3u8; 16]);
        let r = NodeRevisionId([5u8; 16]);
        assert_eq!(chunk_id(i, r, 0), chunk_id(i, r, 0));
        assert_ne!(chunk_id(i, r, 0), chunk_id(i, r, 1));
    }
}
