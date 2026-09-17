//! Graph + vector + KV JSON snapshots under `.cis/` (**Phase 1** persistence).
//!
//! See **`docs/adr/0001-graph-persistence.md`**.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use cis_wal::{DurableMutationLog, MutationLog, MutationLogStore};
use serde::{Deserialize, Serialize};

use crate::coordinator::{CoordinatorPersistence, WriteCoordinator};
use crate::graph_store::save_graph_with_backend;
use crate::graph::{GraphSnapshot, InMemoryGraph, GRAPH_SNAPSHOT_VERSION};
use crate::shared_graph::SharedInMemoryGraph;
use crate::kv::{durable_kv_subset_for_persist, KvSnapshot, MemoryKv};
use crate::revision_cow::RevisionIndexCow;
use crate::vector_store::{InMemoryVectorStore, VectorStoreSnapshot, VECTOR_STORE_SNAPSHOT_VERSION};
use cis_wal::BranchId;

pub const VECTOR_SNAPSHOT_VERSION: u32 = 2;
pub const VECTOR_SNAPSHOT_VERSION_V1: u32 = 1;
pub const KV_SNAPSHOT_VERSION: u32 = 1;

/// Default CIS metadata directory relative to a repository root.
pub fn cis_dir(repo_root: impl AsRef<Path>) -> PathBuf {
    repo_root.as_ref().join(".cis")
}

pub fn wal_path(cis: impl AsRef<Path>) -> PathBuf {
    cis.as_ref().join("wal.json")
}

pub fn graph_snapshot_path(cis: impl AsRef<Path>) -> PathBuf {
    cis.as_ref().join("graph.json")
}

pub fn vector_snapshot_path(cis: impl AsRef<Path>) -> PathBuf {
    cis.as_ref().join("vector.json")
}

pub fn kv_snapshot_path(cis: impl AsRef<Path>) -> PathBuf {
    cis.as_ref().join("kv.json")
}

/// When `CIS_REINDEX_PERSIST=0`, skip automatic snapshot rewrites during incremental ingest
/// (coordinator commit + [`crate::fs_sync::reindex_python_paths_on_coordinator`]).
pub fn snapshot_persist_enabled() -> bool {
    !std::env::var_os("CIS_REINDEX_PERSIST").is_some_and(|v| v == "0")
}

/// Store backend env keys filled by [`apply_mcp_sqlite_store_defaults`].
pub const MCP_SQLITE_STORE_ENV_KEYS: &[&str] = &[
    "CIS_GRAPH_BACKEND",
    "CIS_KV_BACKEND",
    "CIS_WAL_BACKEND",
    "CIS_VECTOR_BACKEND",
    "CIS_BODY_BACKEND",
    "CIS_METADATA_BACKEND",
];

/// `cisd --mcp` sqlite store profile: set each key to `sqlite` only if unset.
///
/// Shell and `.env` (via [`load_env_file`]) win. Tests and [`crate::CisMcpRuntime::new_dev`]
/// do not call this, so they keep JSON/`file` defaults.
///
/// Returns how many keys were filled. No-op without the `body-sqlite` feature.
pub fn apply_mcp_sqlite_store_defaults() -> usize {
    #[cfg(feature = "body-sqlite")]
    {
        let mut n = 0usize;
        for key in MCP_SQLITE_STORE_ENV_KEYS {
            if std::env::var_os(key).is_none() {
                std::env::set_var(key, "sqlite");
                n += 1;
            }
        }
        n
    }
    #[cfg(not(feature = "body-sqlite"))]
    {
        0
    }
}

static ATOMIC_WRITE_SEQ: AtomicU64 = AtomicU64::new(0);

/// On-disk vector snapshot (v2: content-addressed store; v1 legacy chunk-only).
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum VectorSnapshot {
    V2(VectorStoreSnapshot),
    V1 {
        version: u32,
        chunks: Vec<([u8; 32], Vec<f32>)>,
    },
}

impl VectorSnapshot {
    pub fn from_store(store: &InMemoryVectorStore) -> Self {
        VectorSnapshot::V2(store.export_snapshot())
    }

    pub fn restore_into(&self, store: &InMemoryVectorStore) {
        match self {
            VectorSnapshot::V2(snap) => store.restore_snapshot(snap),
            VectorSnapshot::V1 { chunks, .. } => store.replace_all(chunks.clone()),
        }
    }

    pub fn chunk_count(&self) -> usize {
        match self {
            VectorSnapshot::V2(s) => s.chunks.len(),
            VectorSnapshot::V1 { chunks, .. } => chunks.len(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KvSnapshotFile {
    pub version: u32,
    pub entries: std::collections::BTreeMap<String, Vec<u8>>,
}

/// Atomic JSON write: unique `*.tmp` → `fsync` → `rename`.
///
/// Writes **compact** JSON (not pretty). Pretty snapshots inflated `.cis/graph.json` to
/// multi‑MB whitespace and made warm MCP startup parse/rebuild take tens of seconds.
pub fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("snap");
    let seq = ATOMIC_WRITE_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp = path.with_file_name(format!("{stem}.{}.{seq}.tmp", std::process::id()));
    let _ = fs::remove_file(&tmp);
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        serde_json::to_writer(&mut f, value).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, e.to_string())
        })?;
        f.flush()?;
        if !std::env::var_os("CIS_SNAPSHOT_FSYNC").is_some_and(|v| {
            v == "0" || v.eq_ignore_ascii_case("false")
        }) {
            f.sync_all()?;
        }
    }
    if let Err(e) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(())
}

/// When true, [`load_state_from_cis_dir`] skips `vector.json` (load later for MCP warm start).
pub fn defer_vector_snapshot_load() -> bool {
    std::env::var_os("CIS_DEFER_VECTOR_LOAD").is_some_and(|v| {
        v == "1" || v.eq_ignore_ascii_case("true")
    })
}

pub fn load_graph_snapshot(path: &Path) -> io::Result<InMemoryGraph> {
    let t0 = std::time::Instant::now();
    let bytes = fs::read(path)?;
    let t_read = t0.elapsed();
    let snap: GraphSnapshot = serde_json::from_slice(&bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let t_parse = t0.elapsed();
    if snap.version != GRAPH_SNAPSHOT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported graph snapshot version {} (want {})",
                snap.version, GRAPH_SNAPSHOT_VERSION
            ),
        ));
    }
    let g = InMemoryGraph::from_snapshot(snap)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    eprintln!(
        "cisd: graph.json read={:.1}s parse={:.1}s rebuild={:.1}s total={:.1}s ({} bytes)",
        t_read.as_secs_f64(),
        (t_parse - t_read).as_secs_f64(),
        (t0.elapsed() - t_parse).as_secs_f64(),
        t0.elapsed().as_secs_f64(),
        bytes.len()
    );
    Ok(g)
}

pub fn save_graph_snapshot(path: &Path, graph: &InMemoryGraph) -> io::Result<()> {
    write_json_atomic(path, &graph.to_snapshot())
}

pub fn load_vector_snapshot(path: &Path) -> io::Result<VectorSnapshot> {
    let f = File::open(path)?;
    let snap: VectorSnapshot = serde_json::from_reader(f)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    match &snap {
        VectorSnapshot::V2(s) if s.version != VECTOR_STORE_SNAPSHOT_VERSION => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported vector snapshot version {} (want {})",
                    s.version, VECTOR_STORE_SNAPSHOT_VERSION
                ),
            ));
        }
        VectorSnapshot::V1 { version, .. } if *version != VECTOR_SNAPSHOT_VERSION_V1 => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported vector snapshot version {} (want {})",
                    version, VECTOR_SNAPSHOT_VERSION_V1
                ),
            ));
        }
        _ => {}
    }
    Ok(snap)
}

pub fn save_vector_snapshot(path: &Path, store: &InMemoryVectorStore) -> io::Result<()> {
    write_json_atomic(path, &VectorSnapshot::from_store(store))
}

pub fn save_kv_snapshot(path: &Path, kv: &MemoryKv) -> io::Result<()> {
    if kv.is_sqlite_backed() && !crate::kv::kv_json_export_enabled() {
        return Ok(());
    }
    let subset = durable_kv_subset_for_persist(&kv.snapshot());
    let file = KvSnapshotFile {
        version: KV_SNAPSHOT_VERSION,
        entries: subset.entries,
    };
    write_json_atomic(path, &file)
}

pub fn load_kv_snapshot(path: &Path, kv: &MemoryKv) -> io::Result<usize> {
    let f = File::open(path)?;
    let file: KvSnapshotFile = serde_json::from_reader(f)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if file.version != KV_SNAPSHOT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported kv snapshot version {} (want {})",
                file.version, KV_SNAPSHOT_VERSION
            ),
        ));
    }
    let n = file.entries.len();
    kv.merge_snapshot(&KvSnapshot {
        entries: file.entries,
    });
    Ok(n)
}

/// Open the workspace KV: SQLite when `CIS_KV_BACKEND=sqlite`, otherwise RAM (`kv.json` loaded by callers).
///
/// If the SQLite `kv` table is empty and `kv.json` exists, durable keys are imported once.
pub fn open_workspace_kv(cis: &Path) -> MemoryKv {
    match crate::kv::kv_backend_from_env() {
        crate::kv::KvBackendKind::Sqlite => {
            #[cfg(feature = "body-sqlite")]
            {
                match MemoryKv::open_sqlite(cis) {
                    Ok(kv) => {
                        if kv.durable_row_count() == 0 {
                            let kpath = kv_snapshot_path(cis);
                            if kpath.is_file() {
                                if let Err(e) = load_kv_snapshot(&kpath, &kv) {
                                    eprintln!(
                                        "cis: kv.json import into sqlite failed ({e}); starting empty"
                                    );
                                }
                            }
                        }
                        return kv;
                    }
                    Err(e) => {
                        eprintln!(
                            "cis: CIS_KV_BACKEND=sqlite open failed ({e}); using memory+json"
                        );
                    }
                }
            }
            #[cfg(not(feature = "body-sqlite"))]
            {
                eprintln!(
                    "cis: CIS_KV_BACKEND=sqlite requires --features body-sqlite; using memory+json"
                );
            }
        }
        crate::kv::KvBackendKind::Json => {}
    }
    MemoryKv::new()
}

/// Load persisted graph/vector when present; missing files are OK.
pub fn load_state_from_cis_dir(
    cis: &Path,
    graph: &mut InMemoryGraph,
    vector: &InMemoryVectorStore,
) -> PersistenceLoadReport {
    let _ = vector;
    let mut report = PersistenceLoadReport::default();
    let t_graph = std::time::Instant::now();
    match crate::graph_store::load_graph_with_backend(cis, graph) {
        Ok(true) => {
            report.graph_loaded = true;
            report.graph_identities = graph.identity_count();
            report.graph_revisions = graph.revision_count();
            report.graph_edges = graph.edge_count();
        }
        Ok(false) => {}
        Err(e) => report.graph_error = Some(e.to_string()),
    }
    if !report.graph_loaded {
        let gpath = graph_snapshot_path(cis);
        if gpath.exists() {
            match load_graph_snapshot(&gpath) {
                Ok(g) => {
                    report.graph_loaded = true;
                    report.graph_identities = g.identity_count();
                    report.graph_revisions = g.revision_count();
                    report.graph_edges = g.edge_count();
                    *graph = g;
                }
                Err(e) => report.graph_error = Some(e.to_string()),
            }
        }
    }
    if report.graph_loaded {
        eprintln!(
            "cisd: loaded graph ({} revs, {} edges) in {:.1}s",
            report.graph_revisions,
            report.graph_edges,
            t_graph.elapsed().as_secs_f64()
        );
    }
    report
}

/// Load vectors from the active backend (`vectors.db` or `vector.json`).
pub fn load_vector_into(cis: &Path, vector: &InMemoryVectorStore) -> PersistenceLoadReport {
    #[cfg(feature = "body-sqlite")]
    {
        if crate::sqlite_vector::vector_backend_from_env()
            == crate::sqlite_vector::VectorBackendKind::Sqlite
        {
            match crate::sqlite_vector::SqliteVectorStore::open(cis) {
                Ok(sql) => {
                    let mut report = PersistenceLoadReport::default();
                    match sql.load_into(vector) {
                        Ok(n) => {
                            report.vector_loaded = n > 0 || sql.vector_count() > 0;
                            report.vector_chunks = n;
                        }
                        Err(e) => report.vector_error = Some(e.to_string()),
                    }
                    vector.set_persist(std::sync::Arc::new(sql));
                    return report;
                }
                Err(e) => eprintln!(
                    "cis: CIS_VECTOR_BACKEND=sqlite load failed ({e}); trying vector.json"
                ),
            }
        }
    }
    let mut report = PersistenceLoadReport::default();
    let vpath = vector_snapshot_path(cis);
    if !vpath.exists() {
        return report;
    }
    match load_vector_snapshot(&vpath) {
        Ok(snap) => {
            report.vector_loaded = true;
            report.vector_chunks = snap.chunk_count();
            snap.restore_into(vector);
        }
        Err(e) => report.vector_error = Some(e.to_string()),
    }
    report
}

/// Persist graph, vector, and durable KV subset for a workspace.
pub fn save_workspace_snapshots(
    cis: &Path,
    graph: &InMemoryGraph,
    vector: &InMemoryVectorStore,
    kv: &MemoryKv,
) -> io::Result<()> {
    fs::create_dir_all(cis)?;
    if crate::graph_store::graph_backend_from_env() == crate::graph_store::GraphBackendKind::Sqlite {
        // Phase 4: SQL is source of truth. Never write_normalized from an empty/partial overlay.
        if crate::graph_store::graph_json_export_enabled() && graph.revision_count() > 0 {
            let json = crate::graph_store::JsonGraphStore::new(cis.to_path_buf());
            let _ = crate::graph_store::GraphStore::save_snapshot(&json, graph);
        }
    } else {
        save_graph_with_backend(cis, graph)?;
    }
    if vector_sqlite_enabled() {
        #[cfg(feature = "body-sqlite")]
        {
            if let Err(e) = crate::sqlite_vector::SqliteVectorStore::open(cis)
                .and_then(|sql| sql.checkpoint())
            {
                eprintln!("cis: sqlite vector checkpoint failed ({e})");
            }
            if crate::sqlite_vector::vector_json_export_enabled() && vector.len() > 0 {
                save_vector_snapshot(&vector_snapshot_path(cis), vector)?;
            }
        }
    } else {
        save_vector_snapshot(&vector_snapshot_path(cis), vector)?;
    }
    if wal_sqlite_enabled() {
        #[cfg(feature = "body-sqlite")]
        {
            if let Err(e) = crate::sqlite_wal::SqliteMutationLog::checkpoint_file(cis) {
                eprintln!("cis: sqlite WAL checkpoint failed ({e})");
            }
        }
    }
    save_kv_snapshot(&kv_snapshot_path(cis), kv)?;
    Ok(())
}

/// Load graph/vector/KV into detached buffers (bootstrap path).
pub fn load_workspace_into(
    repo_root: &Path,
    graph: &SharedInMemoryGraph,
    vector: &InMemoryVectorStore,
    kv: &MemoryKv,
    revision_index: &RevisionIndexCow,
    branch: BranchId,
) -> PersistenceLoadReport {
    let cis = cis_dir(repo_root);
    let mut report = PersistenceLoadReport::default();
    if !cis.is_dir() {
        return report;
    }
    if crate::graph_store::graph_backend_from_env()
        == crate::graph_store::GraphBackendKind::Sqlite
    {
        report.graph_loaded = true;
    } else {
        let mut g = graph.write();
        let sub = load_state_from_cis_dir(&cis, &mut *g, vector);
        report = sub;
    }
    let vsub = attach_vector_store(&cis, vector);
    report.vector_loaded = vsub.vector_loaded;
    report.vector_chunks = vsub.vector_chunks;
    report.vector_error = vsub.vector_error;
    if kv.is_sqlite_backed() {
        let n = kv.durable_row_count();
        if n > 0 {
            report.kv_loaded = true;
            report.kv_entries = n;
        }
    } else {
        let kpath = kv_snapshot_path(&cis);
        if kpath.exists() {
            match load_kv_snapshot(&kpath, kv) {
                Ok(n) => {
                    report.kv_loaded = true;
                    report.kv_entries = n;
                }
                Err(e) => report.kv_error = Some(e.to_string()),
            }
        }
    }
    if let Ok(n) = hydrate_ris_from_metadata_store(&cis, kv) {
        report.kv_entries = report.kv_entries.saturating_add(n);
    }
    if report.graph_loaded
        && crate::graph_store::graph_backend_from_env()
            != crate::graph_store::GraphBackendKind::Sqlite
    {
        let bindings: Vec<_> = {
            let g = graph.read();
            g.revisions()
                .filter(|r| r.branch_id == branch)
                .map(|r| (r.identity_id, r.revision_id))
                .collect()
        };
        for (identity_id, revision_id) in bindings {
            revision_index.bind(identity_id, revision_id);
        }
    }
    report
}

/// Load `ris:` snapshots from `.cis/store.db` into KV for dual-read time travel.
fn hydrate_ris_from_metadata_store(cis: &Path, kv: &MemoryKv) -> io::Result<usize> {
    if crate::metadata_store::metadata_backend_from_env()
        != crate::metadata_store::MetadataBackendKind::Sqlite
    {
        return Ok(0);
    }
    #[cfg(feature = "body-sqlite")]
    {
        use crate::metadata_store::MetadataStore;
        use crate::revision_cow::ris_snapshot_kv_key;
        use cis_wal::BranchId;

        let store = MetadataStore::open(cis)?;
        let mut n = 0usize;
        for (branch, epoch, payload) in store.list_ris_snapshots()? {
            let key = ris_snapshot_kv_key(BranchId(branch), epoch);
            if kv.get(&key).is_none() {
                kv.set(&key, payload);
                n += 1;
            }
        }
        Ok(n)
    }
    #[cfg(not(feature = "body-sqlite"))]
    Ok(0)
}

/// Open the workspace mutation log: SQLite when `CIS_WAL_BACKEND=sqlite`, else `wal.json`.
///
/// `CIS_WAL_MEMORY=1` returns an in-memory log (tests). Empty `wal.db` imports `wal.json` once.
pub fn open_workspace_wal(cis: &Path) -> io::Result<Arc<dyn MutationLogStore>> {
    if std::env::var_os("CIS_WAL_MEMORY").is_some_and(|v| v == "1") {
        return Ok(Arc::new(MutationLog::new()));
    }
    #[cfg(feature = "body-sqlite")]
    {
        if crate::sqlite_wal::wal_backend_from_env() == crate::sqlite_wal::WalBackendKind::Sqlite {
            return crate::sqlite_wal::SqliteMutationLog::open(cis)
                .map(|w| Arc::new(w) as Arc<dyn MutationLogStore>);
        }
    }
    #[cfg(not(feature = "body-sqlite"))]
    {
        if std::env::var_os("CIS_WAL_BACKEND")
            .is_some_and(|v| v == "sqlite" || v == "sqlite3")
        {
            eprintln!(
                "cis: CIS_WAL_BACKEND=sqlite requires --features body-sqlite; using wal.json"
            );
        }
    }
    let path = wal_path(cis);
    DurableMutationLog::open(&path).map(|w| Arc::new(w) as Arc<dyn MutationLogStore>)
}

/// Attach SQLite vector persistence (write-through) and/or load `vector.json`.
pub fn attach_vector_store(cis: &Path, vector: &InMemoryVectorStore) -> PersistenceLoadReport {
    #[cfg(feature = "body-sqlite")]
    {
        if crate::sqlite_vector::vector_backend_from_env()
            == crate::sqlite_vector::VectorBackendKind::Sqlite
        {
            match crate::sqlite_vector::SqliteVectorStore::open(cis) {
                Ok(sql) => {
                    if sql.is_empty() {
                        let vpath = vector_snapshot_path(cis);
                        if vpath.is_file() {
                            match load_vector_snapshot(&vpath) {
                                Ok(snap) => {
                                    if let Err(e) = import_vector_snapshot_sql(&sql, &snap) {
                                        eprintln!(
                                            "cis: vector.json import into sqlite failed ({e})"
                                        );
                                    }
                                }
                                Err(e) => eprintln!(
                                    "cis: vector.json import into sqlite failed ({e})"
                                ),
                            }
                        }
                    }
                    let mut report = PersistenceLoadReport::default();
                    if !defer_vector_snapshot_load() {
                        match sql.load_into(vector) {
                            Ok(n) => {
                                report.vector_loaded = n > 0 || sql.vector_count() > 0;
                                report.vector_chunks = n;
                            }
                            Err(e) => report.vector_error = Some(e.to_string()),
                        }
                    }
                    vector.set_persist(std::sync::Arc::new(sql));
                    return report;
                }
                Err(e) => eprintln!(
                    "cis: CIS_VECTOR_BACKEND=sqlite open failed ({e}); using vector.json"
                ),
            }
        }
    }
    #[cfg(not(feature = "body-sqlite"))]
    {
        if std::env::var_os("CIS_VECTOR_BACKEND")
            .is_some_and(|v| v == "sqlite" || v == "sqlite3")
        {
            eprintln!(
                "cis: CIS_VECTOR_BACKEND=sqlite requires --features body-sqlite; using vector.json"
            );
        }
    }
    if defer_vector_snapshot_load() {
        let vpath = vector_snapshot_path(cis);
        if vpath.exists() {
            eprintln!("cisd: deferring vector.json load (CIS_DEFER_VECTOR_LOAD)");
        }
        return PersistenceLoadReport::default();
    }
    load_vector_into(cis, vector)
}

#[cfg(feature = "body-sqlite")]
fn import_vector_snapshot_sql(
    sql: &crate::sqlite_vector::SqliteVectorStore,
    snap: &VectorSnapshot,
) -> io::Result<()> {
    match snap {
        VectorSnapshot::V2(s) => sql.import_snapshot(s),
        VectorSnapshot::V1 { chunks, .. } => {
            let s = crate::vector_store::VectorStoreSnapshot {
                version: VECTOR_STORE_SNAPSHOT_VERSION,
                chunks: chunks
                    .iter()
                    .map(|(c, _)| crate::vector_store::VectorChunkRecord {
                        chunk_id: *c,
                        body_hash: *c,
                    })
                    .collect(),
                vectors: chunks
                    .iter()
                    .map(|(c, e)| crate::vector_store::VectorBodyRecord {
                        body_hash: *c,
                        embedding: e.clone(),
                        model_id: "legacy".into(),
                    })
                    .collect(),
            };
            sql.import_snapshot(&s)
        }
    }
}

fn vector_sqlite_enabled() -> bool {
    #[cfg(feature = "body-sqlite")]
    {
        return crate::sqlite_vector::vector_backend_from_env()
            == crate::sqlite_vector::VectorBackendKind::Sqlite;
    }
    #[cfg(not(feature = "body-sqlite"))]
    false
}

fn wal_sqlite_enabled() -> bool {
    #[cfg(feature = "body-sqlite")]
    {
        return crate::sqlite_wal::wal_backend_from_env()
            == crate::sqlite_wal::WalBackendKind::Sqlite;
    }
    #[cfg(not(feature = "body-sqlite"))]
    false
}

/// Open durable WAL + snapshot-backed **`WriteCoordinator`** for a repository root.
pub fn open_persisted_coordinator(
    repo_root: impl AsRef<Path>,
) -> std::io::Result<Arc<WriteCoordinator>> {
    let cis = cis_dir(&repo_root);
    std::fs::create_dir_all(&cis)?;
    let t0 = std::time::Instant::now();
    let wal = open_workspace_wal(&cis)?;
    eprintln!("cisd: wal open in {:.1}s", t0.elapsed().as_secs_f64());
    let t1 = std::time::Instant::now();
    let coord = Arc::new(WriteCoordinator::open(
        wal,
        Some(CoordinatorPersistence { cis_dir: cis }),
    ));
    eprintln!(
        "cisd: graph/vector snapshot load in {:.1}s (defer_vector={})",
        t1.elapsed().as_secs_f64(),
        defer_vector_snapshot_load()
    );
    Ok(coord)
}

#[derive(Debug, Default, Clone)]
pub struct PersistenceLoadReport {
    pub graph_loaded: bool,
    pub vector_loaded: bool,
    pub kv_loaded: bool,
    pub graph_identities: usize,
    pub graph_revisions: usize,
    pub graph_edges: usize,
    pub vector_chunks: usize,
    pub kv_entries: usize,
    pub graph_error: Option<String>,
    pub vector_error: Option<String>,
    pub kv_error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use cis_wal::{BranchId, IdentityId, NodeRevisionId};

    use crate::graph::{
        EdgeResolution, EdgeType, GraphEdge, Language, NodeIdentity, NodeKind, NodeRevision,
        RevisionStatus, SourceSpan, SourceType,
    };
    use crate::revision_index::revision_binding_kv_key;

    fn id(b: u8) -> IdentityId {
        let mut x = [0u8; 16];
        x[15] = b;
        IdentityId(x)
    }

    fn rid(b: u8) -> NodeRevisionId {
        let mut x = [0u8; 16];
        x[14] = b;
        NodeRevisionId(x)
    }

    #[test]
    fn graph_snapshot_roundtrip_on_disk() {
        let dir = std::env::temp_dir().join(format!("cis-graph-snap-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.json");

        let mut g = InMemoryGraph::default();
        let i = id(1);
        g.put_identity(NodeIdentity {
            identity_id: i,
            kind: NodeKind::Function,
        });
        let r = rid(1);
        g.put_revision(NodeRevision {
            revision_id: r,
            identity_id: i,
            branch_id: BranchId([0u8; 16]),
            status: RevisionStatus::Active,
            qualified_name: "f".into(),
            file_path: "a.py".into(),
            body_hash: [1u8; 32],
            signature_hash: [2u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
                tombstoned_at_ms: None,
            span: SourceSpan::UNKNOWN,
        });
        let edge = GraphEdge {
            edge_id: [9u8; 16],
            ty: EdgeType::Calls,
            source_revision_id: r,
            target_identity_id: id(2),
            resolution: EdgeResolution {
                target_signature_hash: [3u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: SourceSpan::UNKNOWN,
        };
        g.replace_edges_for_revision(r, vec![edge]).unwrap();

        save_graph_snapshot(&path, &g).unwrap();
        let g2 = load_graph_snapshot(&path).unwrap();
        assert_eq!(g2.identity_count(), 1);
        assert_eq!(g2.revision_count(), 1);
        assert_eq!(g2.edge_count(), 1);
        assert!(g2.source_identities_targeting(id(2)).contains(&i));
    }

    #[test]
    fn chess_fixture_workspace_loads() {
        use crate::revision_cow::RevisionIndexCow;
        use crate::shared_graph::SharedInMemoryGraph;
        use crate::vector_store::InMemoryVectorStore;

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../_tmp_chess_pygame");
        if !cis_dir(&root).is_dir() {
            return;
        }
        let graph = SharedInMemoryGraph::new(InMemoryGraph::default());
        let vector = InMemoryVectorStore::new();
        let kv = MemoryKv::new();
        let branch = BranchId([0u8; 16]);
        let ri = RevisionIndexCow::root_hydrated(branch, std::sync::Arc::new(kv.clone()));
        let rep = load_workspace_into(
            &root,
            &graph,
            &vector,
            &kv,
            ri.as_ref(),
            branch,
        );
        assert!(rep.graph_loaded, "graph.json should load");
        assert!(rep.kv_loaded, "kv.json should load");
        assert!(graph.read().revision_count() > 50);

        let saga = crate::saga::MergeSagaOrchestrator::new(std::sync::Arc::new(kv.clone()));
        let merge = crate::merge_control::MergeControl::new(std::sync::Arc::new(kv.clone()));
        let gate = crate::merge_gate::MergeRecoveryGate::new(std::sync::Arc::new(kv.clone()));
        let body = crate::body_store::BodyStore::new(std::sync::Arc::new(kv.clone()));
        let mut g = graph.write();
        let _ = crate::merge_engine::recover_inflight_merges(
            &mut g,
            &kv,
            &body,
            &saga,
            &merge,
            &vector,
            &gate,
            None,
        );
    }

    #[test]
    fn kv_snapshot_filters_ephemeral_keys() {
        let dir = std::env::temp_dir().join(format!("cis-kv-snap-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let kv = MemoryKv::new();
        let b = BranchId([0u8; 16]);
        let ident = id(3);
        let rev = rid(4);
        kv.set(
            &revision_binding_kv_key(b, ident),
            rev.0.to_vec(),
        );
        kv.set("merge_lock:deadbeef", vec![1]);
        kv.set("wal:deadbeef", vec![1]);
        save_kv_snapshot(&dir.join("kv.json"), &kv).unwrap();
        let kv2 = MemoryKv::new();
        let n = load_kv_snapshot(&dir.join("kv.json"), &kv2).unwrap();
        assert_eq!(n, 2);
        assert!(kv2.get("wal:deadbeef").is_none());
        assert_eq!(kv2.get("merge_lock:deadbeef"), Some(vec![1]));
        assert_eq!(kv2.get(&revision_binding_kv_key(b, ident)), Some(rev.0.to_vec()));
    }

    #[cfg(feature = "body-sqlite")]
    #[test]
    fn sqlite_kv_skips_json_unless_exported() {
        std::env::remove_var("CIS_KV_JSON_EXPORT");
        let dir = tempfile::tempdir().unwrap();
        let kv = MemoryKv::open_sqlite(dir.path()).unwrap();
        kv.set("ri:aa:01", vec![1]);
        let path = dir.path().join("kv.json");
        save_kv_snapshot(&path, &kv).unwrap();
        assert!(!path.exists(), "sqlite KV must not rewrite kv.json by default");

        std::env::set_var("CIS_KV_JSON_EXPORT", "1");
        save_kv_snapshot(&path, &kv).unwrap();
        std::env::remove_var("CIS_KV_JSON_EXPORT");
        assert!(path.is_file());
        let kv2 = MemoryKv::new();
        assert_eq!(load_kv_snapshot(&path, &kv2).unwrap(), 1);
        assert_eq!(kv2.get("ri:aa:01"), Some(vec![1]));
    }

    #[cfg(feature = "body-sqlite")]
    #[test]
    fn sqlite_kv_imports_kv_json_when_empty() {
        let dir = tempfile::tempdir().unwrap();
        let json_kv = MemoryKv::new();
        json_kv.set("ri:aa:01", vec![7]);
        json_kv.set("eto:skip", vec![9]);
        save_kv_snapshot(&dir.path().join("kv.json"), &json_kv).unwrap();

        std::env::set_var("CIS_KV_BACKEND", "sqlite");
        let imported = open_workspace_kv(dir.path());
        std::env::remove_var("CIS_KV_BACKEND");
        assert!(imported.is_sqlite_backed());
        assert_eq!(imported.get("ri:aa:01"), Some(vec![7]));
        assert!(imported.get("eto:skip").is_none());
    }

    #[cfg(feature = "body-sqlite")]
    #[test]
    fn sqlite_vector_snapshots_skip_json_unless_exported() {
        std::env::remove_var("CIS_VECTOR_JSON_EXPORT");
        std::env::set_var("CIS_VECTOR_BACKEND", "sqlite");
        let dir = tempfile::tempdir().unwrap();
        let vector = InMemoryVectorStore::new();
        let sql = crate::sqlite_vector::SqliteVectorStore::open(dir.path()).unwrap();
        vector.set_persist(std::sync::Arc::new(sql));
        vector.register([1u8; 32], [2u8; 32]);
        vector.set_embedding([2u8; 32], vec![0.5], "m");
        let graph = InMemoryGraph::default();
        let kv = MemoryKv::new();
        save_workspace_snapshots(dir.path(), &graph, &vector, &kv).unwrap();
        assert!(
            !vector_snapshot_path(dir.path()).exists(),
            "sqlite vectors must not rewrite vector.json by default"
        );
        assert!(crate::sqlite_vector::vectors_db_path(dir.path()).is_file());

        std::env::set_var("CIS_VECTOR_JSON_EXPORT", "1");
        save_workspace_snapshots(dir.path(), &graph, &vector, &kv).unwrap();
        std::env::remove_var("CIS_VECTOR_JSON_EXPORT");
        std::env::remove_var("CIS_VECTOR_BACKEND");
        assert!(vector_snapshot_path(dir.path()).is_file());
    }

    #[cfg(feature = "body-sqlite")]
    #[test]
    fn mcp_sqlite_defaults_fill_only_unset() {
        let prev: Vec<(String, Option<std::ffi::OsString>)> = MCP_SQLITE_STORE_ENV_KEYS
            .iter()
            .map(|k| ((*k).to_string(), std::env::var_os(k)))
            .collect();
        for k in MCP_SQLITE_STORE_ENV_KEYS {
            std::env::remove_var(k);
        }
        let filled = apply_mcp_sqlite_store_defaults();
        assert_eq!(filled, MCP_SQLITE_STORE_ENV_KEYS.len());
        for k in MCP_SQLITE_STORE_ENV_KEYS {
            assert_eq!(std::env::var(k).as_deref(), Ok("sqlite"));
        }
        std::env::set_var("CIS_GRAPH_BACKEND", "json");
        assert_eq!(apply_mcp_sqlite_store_defaults(), 0);
        assert_eq!(std::env::var("CIS_GRAPH_BACKEND").unwrap(), "json");
        for (k, v) in prev {
            match v {
                Some(val) => std::env::set_var(&k, val),
                None => std::env::remove_var(&k),
            }
        }
    }
}
