//! Process-global `CIS_*` store env helpers for dual JSON/SQLite MCP tests.

use std::sync::Mutex;

/// `CIS_*` backend env vars are process-global; serialize tests that read/write them.
pub static CIS_ENV_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreProfile {
    Json,
    Sqlite,
}

impl StoreProfile {
    pub fn all() -> [StoreProfile; 2] {
        [StoreProfile::Json, StoreProfile::Sqlite]
    }

    pub fn name(self) -> &'static str {
        match self {
            StoreProfile::Json => "json",
            StoreProfile::Sqlite => "sqlite",
        }
    }
}

pub fn clear_store_env() {
    std::env::remove_var("CIS_SKIP_WORKSPACE_LOAD");
    std::env::remove_var("CIS_FORCE_REINDEX");
    std::env::remove_var("CIS_SKIP_MERGE_RECOVER");
    std::env::remove_var("CIS_BODY_BACKEND");
    std::env::remove_var("CIS_METADATA_BACKEND");
    std::env::remove_var("CIS_GRAPH_BACKEND");
    std::env::remove_var("CIS_KV_BACKEND");
    std::env::remove_var("CIS_KV_JSON_EXPORT");
    std::env::remove_var("CIS_WAL_BACKEND");
    std::env::remove_var("CIS_WAL_JSON_EXPORT");
    std::env::remove_var("CIS_VECTOR_BACKEND");
    std::env::remove_var("CIS_VECTOR_JSON_EXPORT");
    std::env::remove_var("CIS_DEFER_VECTOR_LOAD");
    std::env::remove_var("CIS_GRAPH_JSON_EXPORT");
    std::env::remove_var("CIS_WAL_MEMORY");
    std::env::remove_var("CIS_SQLITE_UNIFIED");
}

pub fn apply_profile(profile: StoreProfile) {
    std::env::set_var("CIS_ALLOW_DEFAULT_SESSION", "1");
    match profile {
        StoreProfile::Json => {
            std::env::set_var("CIS_GRAPH_BACKEND", "json");
            std::env::set_var("CIS_KV_BACKEND", "json");
            std::env::set_var("CIS_WAL_BACKEND", "json");
            std::env::set_var("CIS_VECTOR_BACKEND", "json");
            std::env::set_var("CIS_BODY_BACKEND", "file");
            std::env::set_var("CIS_METADATA_BACKEND", "json");
            std::env::set_var("CIS_WAL_MEMORY", "1");
        }
        StoreProfile::Sqlite => {
            std::env::set_var("CIS_GRAPH_BACKEND", "sqlite");
            std::env::set_var("CIS_KV_BACKEND", "sqlite");
            std::env::set_var("CIS_WAL_BACKEND", "sqlite");
            std::env::set_var("CIS_VECTOR_BACKEND", "sqlite");
            std::env::set_var("CIS_BODY_BACKEND", "sqlite");
            std::env::set_var("CIS_METADATA_BACKEND", "sqlite");
            std::env::set_var("CIS_SKIP_MERGE_RECOVER", "1");
        }
    }
}
