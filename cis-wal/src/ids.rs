//! Stable binary UUIDs — **DERIVED** from architecture (`UUID` fields) without pulling `uuid` + `getrandom`
//! (keeps MSRV / toolchain constraints predictable in restricted CI).

use serde::{Deserialize, Serialize};

/// **`NodeRevision.revision_id`** in the architecture (opaque UUID octets).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeRevisionId(pub [u8; 16]);

/// Merge / saga correlation id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MergeId(pub [u8; 16]);

/// **`NodeIdentity.identity_id`**
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdentityId(pub [u8; 16]);

/// Stable branch id (FR-1.6 `branch_id` on `NodeRevision`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BranchId(pub [u8; 16]);

impl IdentityId {
    #[inline]
    pub const fn from_bytes(b: [u8; 16]) -> Self {
        Self(b)
    }
}

impl BranchId {
    #[inline]
    pub const fn from_bytes(b: [u8; 16]) -> Self {
        Self(b)
    }
}

impl NodeRevisionId {
    #[inline]
    pub const fn from_bytes(b: [u8; 16]) -> Self {
        Self(b)
    }
}

impl MergeId {
    #[inline]
    pub const fn from_bytes(b: [u8; 16]) -> Self {
        Self(b)
    }
}
