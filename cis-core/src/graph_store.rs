//! Graph persistence backends (**ADR 0005**): JSON snapshot + optional SQLite (normalized rows).

use std::io;
use std::path::{Path, PathBuf};

use cis_wal::{BranchId, IdentityId, NodeRevisionId};

use crate::graph::{
    GraphEdge, GraphSnapshot, InMemoryGraph, NodeIdentity, NodeRevision, GRAPH_SNAPSHOT_VERSION,
};
use crate::persistence::{graph_snapshot_path, load_graph_snapshot, save_graph_snapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphBackendKind {
    Json,
    Sqlite,
}

/// Read `CIS_GRAPH_BACKEND=json|sqlite` (default `json`).
pub fn graph_backend_from_env() -> GraphBackendKind {
    match std::env::var_os("CIS_GRAPH_BACKEND") {
        Some(v) if v == "sqlite" => GraphBackendKind::Sqlite,
        _ => GraphBackendKind::Json,
    }
}

/// When `CIS_GRAPH_BACKEND=sqlite`, still export `graph.json` unless `CIS_GRAPH_JSON_EXPORT=0`.
pub fn graph_json_export_enabled() -> bool {
    match std::env::var_os("CIS_GRAPH_JSON_EXPORT") {
        Some(v) if v == "0" || v == "false" => false,
        _ => true,
    }
}

pub fn graph_db_path(cis: &Path) -> PathBuf {
    cis.join("graph.db")
}

pub trait GraphStore: Send + Sync {
    fn load_into(&self, graph: &mut InMemoryGraph) -> io::Result<bool>;
    fn save_snapshot(&self, graph: &InMemoryGraph) -> io::Result<()>;
    fn upsert_identity(&self, id: &NodeIdentity) -> io::Result<()> {
        let _ = id;
        Ok(())
    }
    fn upsert_revision(&self, rev: &NodeRevision) -> io::Result<()> {
        let _ = rev;
        Ok(())
    }
    fn upsert_edges(&self, source: NodeRevisionId, edges: &[GraphEdge]) -> io::Result<()> {
        let _ = (source, edges);
        Ok(())
    }
    /// Batch upsert affected revisions (+ optional blob refresh) in one connection when supported.
    fn apply_delta(&self, graph: &InMemoryGraph, affected: &[NodeRevisionId]) -> io::Result<()> {
        let mut seen_identities = std::collections::HashSet::new();
        for rid in affected {
            if let Some(rev) = graph.get_revision(*rid) {
                if seen_identities.insert(rev.identity_id) {
                    if let Some(kind) = graph.identity_kind(rev.identity_id) {
                        self.upsert_identity(&NodeIdentity {
                            identity_id: rev.identity_id,
                            kind,
                        })?;
                    }
                }
                self.upsert_revision(rev)?;
                let edges: Vec<_> = graph.outbound_edges(*rid).into_iter().cloned().collect();
                self.upsert_edges(*rid, &edges)?;
            }
        }
        Ok(())
    }
    fn tombstone_file(&self, branch: BranchId, path: &str) -> io::Result<()> {
        let _ = (branch, path);
        Ok(())
    }
}

pub struct JsonGraphStore {
    cis_dir: PathBuf,
}

impl JsonGraphStore {
    pub fn new(cis_dir: PathBuf) -> Self {
        Self { cis_dir }
    }
}

impl GraphStore for JsonGraphStore {
    fn load_into(&self, graph: &mut InMemoryGraph) -> io::Result<bool> {
        let path = graph_snapshot_path(&self.cis_dir);
        if !path.is_file() {
            return Ok(false);
        }
        *graph = load_graph_snapshot(&path)?;
        Ok(true)
    }

    fn save_snapshot(&self, graph: &InMemoryGraph) -> io::Result<()> {
        save_graph_snapshot(&graph_snapshot_path(&self.cis_dir), graph)
    }
}

#[cfg(feature = "body-sqlite")]
mod sqlite {
    use super::*;
    use rusqlite::{params, Connection};

    use crate::graph::{EdgeType, Language, NodeKind, RevisionStatus, SourceSpan, SourceType};

    #[derive(serde::Serialize, serde::Deserialize)]
    struct RevisionExtra {
        parent_revision_id: Option<NodeRevisionId>,
        rename_source_id: Option<IdentityId>,
        span: SourceSpan,
        tombstoned_at_ms: Option<u64>,
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct EdgeExtra {
        resolution_target_sig: [u8; 32],
        resolution_resolver: u8,
        resolution_last_validation_ms: i64,
        anchor: SourceSpan,
    }

    const SCHEMA: &str = "
        CREATE TABLE IF NOT EXISTS graph_snapshot (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            version INTEGER NOT NULL,
            payload BLOB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS identities (
            identity_id BLOB PRIMARY KEY,
            kind INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS revisions (
            revision_id BLOB PRIMARY KEY,
            identity_id BLOB NOT NULL,
            branch_id BLOB NOT NULL,
            status INTEGER NOT NULL,
            qualified_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            body_hash BLOB NOT NULL,
            signature_hash BLOB NOT NULL,
            language INTEGER NOT NULL,
            extra BLOB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_revisions_branch_file ON revisions(branch_id, file_path);
        CREATE TABLE IF NOT EXISTS edges (
            edge_id BLOB PRIMARY KEY,
            source_revision_id BLOB NOT NULL,
            target_identity_id BLOB NOT NULL,
            ty INTEGER NOT NULL,
            extra BLOB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_revision_id);
        CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_identity_id);
    ";

    pub struct SqliteGraphStore {
        path: PathBuf,
    }

    impl SqliteGraphStore {
        pub fn open(cis_dir: &Path) -> io::Result<Self> {
            std::fs::create_dir_all(cis_dir)?;
            let path = graph_db_path(cis_dir);
            let conn = Connection::open(&path).map_err(|e| io::Error::other(e.to_string()))?;
            conn.execute_batch(SCHEMA)
                .map_err(|e| io::Error::other(e.to_string()))?;
            Ok(Self { path })
        }

        fn with_conn<F, T>(&self, f: F) -> io::Result<T>
        where
            F: FnOnce(&Connection) -> io::Result<T>,
        {
            let conn = Connection::open(&self.path).map_err(|e| io::Error::other(e.to_string()))?;
            f(&conn)
        }

        fn revision_count(conn: &Connection) -> io::Result<i64> {
            conn.query_row("SELECT COUNT(*) FROM revisions", [], |r| r.get(0))
                .map_err(|e| io::Error::other(e.to_string()))
        }

        fn edge_count(conn: &Connection) -> io::Result<i64> {
            conn.query_row("SELECT COUNT(*) FROM edges", [], |r| r.get(0))
                .map_err(|e| io::Error::other(e.to_string()))
        }

        fn blob_edge_count(conn: &Connection) -> io::Result<Option<usize>> {
            let mut stmt = conn
                .prepare("SELECT payload FROM graph_snapshot WHERE id = 1")
                .map_err(|e| io::Error::other(e.to_string()))?;
            let mut rows = stmt
                .query([])
                .map_err(|e| io::Error::other(e.to_string()))?;
            let Some(row) = rows.next().map_err(|e| io::Error::other(e.to_string()))? else {
                return Ok(None);
            };
            let payload: Vec<u8> = row.get(0).map_err(|e| io::Error::other(e.to_string()))?;
            let snap: GraphSnapshot = serde_json::from_slice(&payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            Ok(Some(snap.edges.len()))
        }

        fn load_from_blob(conn: &Connection, graph: &mut InMemoryGraph) -> io::Result<bool> {
            let mut stmt = conn
                .prepare("SELECT payload FROM graph_snapshot WHERE id = 1")
                .map_err(|e| io::Error::other(e.to_string()))?;
            let mut rows = stmt
                .query([])
                .map_err(|e| io::Error::other(e.to_string()))?;
            let Some(row) = rows.next().map_err(|e| io::Error::other(e.to_string()))? else {
                return Ok(false);
            };
            let payload: Vec<u8> = row.get(0).map_err(|e| io::Error::other(e.to_string()))?;
            let snap: GraphSnapshot = serde_json::from_slice(&payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            *graph = InMemoryGraph::from_snapshot(snap)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(true)
        }

        fn kind_to_i64(kind: NodeKind) -> i64 {
            match kind {
                NodeKind::Class => 1,
                NodeKind::Function => 2,
                NodeKind::File => 3,
                NodeKind::Config => 4,
                NodeKind::Stub => 5,
                NodeKind::Test => 6,
            }
        }

        fn kind_from_i64(kind: i64) -> NodeKind {
            match kind {
                1 => NodeKind::Class,
                2 => NodeKind::Function,
                3 => NodeKind::File,
                4 => NodeKind::Config,
                5 => NodeKind::Stub,
                6 => NodeKind::Test,
                _ => NodeKind::Function,
            }
        }

        fn insert_identity(conn: &Connection, id: &NodeIdentity) -> io::Result<()> {
            conn.execute(
                "INSERT OR REPLACE INTO identities (identity_id, kind) VALUES (?1, ?2)",
                params![id.identity_id.0.as_slice(), Self::kind_to_i64(id.kind)],
            )
            .map_err(|e| io::Error::other(e.to_string()))?;
            Ok(())
        }

        fn insert_revision(conn: &Connection, r: &NodeRevision) -> io::Result<()> {
            let extra = RevisionExtra {
                parent_revision_id: r.parent_revision_id,
                rename_source_id: r.rename_source_id,
                span: r.span,
                tombstoned_at_ms: r.tombstoned_at_ms,
            };
            let extra_bytes = serde_json::to_vec(&extra)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            conn.execute(
                "INSERT OR REPLACE INTO revisions (revision_id, identity_id, branch_id, status, qualified_name,
                 file_path, body_hash, signature_hash, language, extra)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params![
                    r.revision_id.0.as_slice(),
                    r.identity_id.0.as_slice(),
                    r.branch_id.0.as_slice(),
                    match r.status {
                        RevisionStatus::Active => 0i64,
                        RevisionStatus::Speculative => 1,
                        RevisionStatus::Tombstone => 2,
                        RevisionStatus::Orphaned => 3,
                    },
                    r.qualified_name,
                    r.file_path,
                    r.body_hash.as_slice(),
                    r.signature_hash.as_slice(),
                    match r.language {
                        Language::TypeScript => 1i64,
                        _ => 0,
                    },
                    extra_bytes,
                ],
            )
            .map_err(|e| io::Error::other(e.to_string()))?;
            Ok(())
        }

        fn insert_edge(conn: &Connection, e: &GraphEdge) -> io::Result<()> {
            let ex = EdgeExtra {
                resolution_target_sig: e.resolution.target_signature_hash,
                resolution_resolver: match e.resolution.resolver {
                    SourceType::Lsp => 1,
                    SourceType::Ast => 2,
                    SourceType::Textual => 3,
                    _ => 0,
                },
                resolution_last_validation_ms: e.resolution.last_validation_ms,
                anchor: e.anchor,
            };
            let extra_bytes = serde_json::to_vec(&ex)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            conn.execute(
                "INSERT OR REPLACE INTO edges (edge_id, source_revision_id, target_identity_id, ty, extra)
                 VALUES (?1,?2,?3,?4,?5)",
                params![
                    e.edge_id.as_slice(),
                    e.source_revision_id.0.as_slice(),
                    e.target_identity_id.0.as_slice(),
                    match e.ty {
                        EdgeType::Calls => 0i64,
                        EdgeType::Imports => 1,
                        EdgeType::Uses => 2,
                        EdgeType::Extends => 3,
                        EdgeType::Configures => 4,
                        EdgeType::CoLocated => 5,
                        EdgeType::TestOf => 6,
                        EdgeType::RenamedFrom => 7,
                    },
                    extra_bytes,
                ],
            )
            .map_err(|e| io::Error::other(e.to_string()))?;
            Ok(())
        }

        fn load_normalized(conn: &Connection, graph: &mut InMemoryGraph) -> io::Result<()> {
            let mut snap = GraphSnapshot {
                version: GRAPH_SNAPSHOT_VERSION,
                identities: Vec::new(),
                revisions: Vec::new(),
                edges: Vec::new(),
            };

            let mut id_stmt = conn
                .prepare("SELECT identity_id, kind FROM identities")
                .map_err(|e| io::Error::other(e.to_string()))?;
            let id_rows = id_stmt
                .query_map([], |row| {
                    let id: Vec<u8> = row.get(0)?;
                    let kind: i64 = row.get(1)?;
                    Ok((id, kind))
                })
                .map_err(|e| io::Error::other(e.to_string()))?;
            for row in id_rows {
                let (id, kind) = row.map_err(|e| io::Error::other(e.to_string()))?;
                if id.len() != 16 {
                    continue;
                }
                let mut ib = [0u8; 16];
                ib.copy_from_slice(&id);
                snap.identities.push(NodeIdentity {
                    identity_id: IdentityId(ib),
                    kind: Self::kind_from_i64(kind),
                });
            }

            let mut rev_stmt = conn
                .prepare(
                    "SELECT revision_id, identity_id, branch_id, status, qualified_name, file_path,
                            body_hash, signature_hash, language, extra FROM revisions",
                )
                .map_err(|e| io::Error::other(e.to_string()))?;
            let rev_rows = rev_stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, Vec<u8>>(6)?,
                        row.get::<_, Vec<u8>>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, Vec<u8>>(9)?,
                    ))
                })
                .map_err(|e| io::Error::other(e.to_string()))?;
            for row in rev_rows {
                let (rid, iid, bid, st, qn, fp, bh, sh, lang, extra) =
                    row.map_err(|e| io::Error::other(e.to_string()))?;
                if rid.len() != 16 || iid.len() != 16 || bid.len() != 16 || bh.len() != 32 || sh.len() != 32
                {
                    continue;
                }
                let mut rb = [0u8; 16];
                rb.copy_from_slice(&rid);
                let mut ib = [0u8; 16];
                ib.copy_from_slice(&iid);
                let mut bb = [0u8; 16];
                bb.copy_from_slice(&bid);
                let mut body_hash = [0u8; 32];
                body_hash.copy_from_slice(&bh);
                let mut signature_hash = [0u8; 32];
                signature_hash.copy_from_slice(&sh);
                let ex: RevisionExtra = serde_json::from_slice(&extra)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                snap.revisions.push(NodeRevision {
                    revision_id: NodeRevisionId(rb),
                    identity_id: IdentityId(ib),
                    branch_id: BranchId(bb),
                    status: match st {
                        0 => RevisionStatus::Active,
                        1 => RevisionStatus::Speculative,
                        2 => RevisionStatus::Tombstone,
                        _ => RevisionStatus::Orphaned,
                    },
                    qualified_name: qn,
                    file_path: fp,
                    body_hash,
                    signature_hash,
                    language: match lang {
                        1 => Language::TypeScript,
                        _ => Language::Python,
                    },
                    parent_revision_id: ex.parent_revision_id,
                    rename_source_id: ex.rename_source_id,
                    span: ex.span,
                    tombstoned_at_ms: ex.tombstoned_at_ms,
                });
            }

            let mut edge_stmt = conn
                .prepare(
                    "SELECT edge_id, source_revision_id, target_identity_id, ty, extra FROM edges",
                )
                .map_err(|e| io::Error::other(e.to_string()))?;
            let edge_rows = edge_stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Vec<u8>>(4)?,
                    ))
                })
                .map_err(|e| io::Error::other(e.to_string()))?;
            for row in edge_rows {
                let (eid, src, tgt, ty, extra) = row.map_err(|e| io::Error::other(e.to_string()))?;
                if eid.len() != 16 || src.len() != 16 || tgt.len() != 16 {
                    continue;
                }
                let mut eb = [0u8; 16];
                eb.copy_from_slice(&eid);
                let mut sb = [0u8; 16];
                sb.copy_from_slice(&src);
                let mut tb = [0u8; 16];
                tb.copy_from_slice(&tgt);
                let ex: EdgeExtra = serde_json::from_slice(&extra)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                snap.edges.push(GraphEdge {
                    edge_id: eb,
                    ty: match ty {
                        0 => EdgeType::Calls,
                        1 => EdgeType::Imports,
                        2 => EdgeType::Uses,
                        3 => EdgeType::Extends,
                        4 => EdgeType::Configures,
                        5 => EdgeType::CoLocated,
                        6 => EdgeType::TestOf,
                        _ => EdgeType::RenamedFrom,
                    },
                    source_revision_id: NodeRevisionId(sb),
                    target_identity_id: IdentityId(tb),
                    resolution: crate::graph::EdgeResolution {
                        target_signature_hash: ex.resolution_target_sig,
                        resolver: match ex.resolution_resolver {
                            1 => SourceType::Lsp,
                            2 => SourceType::Ast,
                            3 => SourceType::Textual,
                            _ => SourceType::Compiler,
                        },
                        last_validation_ms: ex.resolution_last_validation_ms,
                    },
                    anchor: ex.anchor,
                });
            }

            *graph = InMemoryGraph::from_snapshot(snap)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(())
        }

        fn write_blob(conn: &Connection, graph: &InMemoryGraph) -> io::Result<()> {
            let snap = graph.to_snapshot();
            let payload = serde_json::to_vec(&snap)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            conn.execute(
                "INSERT INTO graph_snapshot (id, version, payload) VALUES (1, ?1, ?2)
                 ON CONFLICT(id) DO UPDATE SET version = excluded.version, payload = excluded.payload",
                params![snap.version, payload],
            )
            .map_err(|e| io::Error::other(e.to_string()))?;
            Ok(())
        }

        fn write_normalized(conn: &Connection, graph: &InMemoryGraph) -> io::Result<()> {
            let snap = graph.to_snapshot();
            conn.execute_batch("DELETE FROM edges; DELETE FROM revisions; DELETE FROM identities;")
                .map_err(|e| io::Error::other(e.to_string()))?;
            for id in &snap.identities {
                Self::insert_identity(conn, id)?;
            }
            for r in &snap.revisions {
                Self::insert_revision(conn, r)?;
            }
            for e in &snap.edges {
                Self::insert_edge(conn, e)?;
            }
            Ok(())
        }

        /// Force reload from blob snapshot and rewrite normalized tables.
        pub fn rebuild_normalized_from_blob(&self) -> io::Result<InMemoryGraph> {
            self.with_conn(|conn| {
                let mut graph = InMemoryGraph::default();
                if !Self::load_from_blob(conn, &mut graph)? {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "graph.db has no blob snapshot (graph_snapshot row)",
                    ));
                }
                Self::write_normalized(conn, &graph)?;
                Ok(graph)
            })
        }
    }

    impl GraphStore for SqliteGraphStore {
        fn load_into(&self, graph: &mut InMemoryGraph) -> io::Result<bool> {
            self.with_conn(|conn| {
                if Self::revision_count(conn)? > 0 {
                    Self::load_normalized(conn, graph)?;
                    if let Ok(Some(blob_n)) = Self::blob_edge_count(conn) {
                        let norm_n = Self::edge_count(conn)? as usize;
                        if norm_n != blob_n {
                            // Prefer normalized rows (source of truth after incremental deltas);
                            // refresh the blob so a later load does not discard them.
                            eprintln!(
                                "cis: graph.db normalized edge count ({norm_n}) != blob ({blob_n}); refreshing blob from normalized"
                            );
                            Self::write_blob(conn, graph)?;
                        }
                    }
                    return Ok(true);
                }
                if Self::load_from_blob(conn, graph)? {
                    return Ok(true);
                }
                Ok(false)
            })
        }

        fn save_snapshot(&self, graph: &InMemoryGraph) -> io::Result<()> {
            self.with_conn(|conn| {
                let tx = conn
                    .unchecked_transaction()
                    .map_err(|e| io::Error::other(e.to_string()))?;
                Self::write_normalized(&tx, graph)?;
                Self::write_blob(&tx, graph)?;
                tx.commit().map_err(|e| io::Error::other(e.to_string()))?;
                Ok(())
            })
        }

        fn upsert_identity(&self, id: &NodeIdentity) -> io::Result<()> {
            self.with_conn(|conn| Self::insert_identity(conn, id))
        }

        fn upsert_revision(&self, rev: &NodeRevision) -> io::Result<()> {
            self.with_conn(|conn| Self::insert_revision(conn, rev))
        }

        fn upsert_edges(&self, source: NodeRevisionId, edges: &[GraphEdge]) -> io::Result<()> {
            self.with_conn(|conn| {
                conn.execute(
                    "DELETE FROM edges WHERE source_revision_id = ?1",
                    params![source.0.as_slice()],
                )
                .map_err(|e| io::Error::other(e.to_string()))?;
                for e in edges {
                    Self::insert_edge(conn, e)?;
                }
                Ok(())
            })
        }

        fn apply_delta(&self, graph: &InMemoryGraph, affected: &[NodeRevisionId]) -> io::Result<()> {
            self.with_conn(|conn| {
                let tx = conn
                    .unchecked_transaction()
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let mut seen_identities = std::collections::HashSet::new();
                for rid in affected {
                    let Some(rev) = graph.get_revision(*rid) else {
                        continue;
                    };
                    if seen_identities.insert(rev.identity_id) {
                        if let Some(kind) = graph.identity_kind(rev.identity_id) {
                            Self::insert_identity(
                                &tx,
                                &NodeIdentity {
                                    identity_id: rev.identity_id,
                                    kind,
                                },
                            )?;
                        }
                    }
                    Self::insert_revision(&tx, rev)?;
                    tx.execute(
                        "DELETE FROM edges WHERE source_revision_id = ?1",
                        params![rid.0.as_slice()],
                    )
                    .map_err(|e| io::Error::other(e.to_string()))?;
                    for e in graph.outbound_edges(*rid) {
                        Self::insert_edge(&tx, e)?;
                    }
                }
                // Keep blob in sync so load_into mismatch logic cannot discard deltas.
                Self::write_blob(&tx, graph)?;
                tx.commit().map_err(|e| io::Error::other(e.to_string()))?;
                Ok(())
            })
        }

        fn tombstone_file(&self, branch: BranchId, path: &str) -> io::Result<()> {
            self.with_conn(|conn| {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let mut stmt = conn
                    .prepare(
                        "SELECT revision_id, extra FROM revisions WHERE branch_id = ?1 AND file_path = ?2",
                    )
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let rows: Vec<(Vec<u8>, Vec<u8>)> = stmt
                    .query_map(params![branch.0.as_slice(), path], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })
                    .map_err(|e| io::Error::other(e.to_string()))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| io::Error::other(e.to_string()))?;
                drop(stmt);
                for (rev_id, extra_bytes) in rows {
                    let mut extra: RevisionExtra = if extra_bytes.is_empty() {
                        RevisionExtra {
                            parent_revision_id: None,
                            rename_source_id: None,
                            span: SourceSpan::UNKNOWN,
                            tombstoned_at_ms: None,
                        }
                    } else {
                        serde_json::from_slice(&extra_bytes).unwrap_or(RevisionExtra {
                            parent_revision_id: None,
                            rename_source_id: None,
                            span: SourceSpan::UNKNOWN,
                            tombstoned_at_ms: None,
                        })
                    };
                    extra.tombstoned_at_ms = Some(now_ms);
                    let new_extra = serde_json::to_vec(&extra)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                    conn.execute(
                        "UPDATE revisions SET status = 2, extra = ?1 WHERE revision_id = ?2",
                        params![new_extra, rev_id],
                    )
                    .map_err(|e| io::Error::other(e.to_string()))?;
                }
                Ok(())
            })
        }
    }
}

#[cfg(feature = "body-sqlite")]
pub use sqlite::SqliteGraphStore;

pub fn open_graph_store(cis_dir: &Path) -> Box<dyn GraphStore> {
    match graph_backend_from_env() {
        GraphBackendKind::Json => Box::new(JsonGraphStore::new(cis_dir.to_path_buf())),
        GraphBackendKind::Sqlite => {
            #[cfg(feature = "body-sqlite")]
            {
                match SqliteGraphStore::open(cis_dir) {
                    Ok(s) => return Box::new(s),
                    Err(e) => eprintln!("cis: CIS_GRAPH_BACKEND=sqlite open failed ({e}); using json"),
                }
            }
            #[cfg(not(feature = "body-sqlite"))]
            eprintln!("cis: CIS_GRAPH_BACKEND=sqlite requires --features body-sqlite; using json");
            Box::new(JsonGraphStore::new(cis_dir.to_path_buf()))
        }
    }
}

/// Load graph via active backend; fall back to JSON if SQLite empty.
pub fn load_graph_with_backend(cis: &Path, graph: &mut InMemoryGraph) -> io::Result<bool> {
    let store = open_graph_store(cis);
    if store.load_into(graph)? {
        return Ok(true);
    }
    if graph_backend_from_env() == GraphBackendKind::Sqlite {
        let json = JsonGraphStore::new(cis.to_path_buf());
        return json.load_into(graph);
    }
    Ok(false)
}

/// Incremental upsert for sqlite backend; full snapshot for json.
pub fn save_graph_delta(
    cis: &Path,
    graph: &InMemoryGraph,
    affected: &[NodeRevisionId],
) -> io::Result<()> {
    if graph_backend_from_env() == GraphBackendKind::Sqlite && !affected.is_empty() {
        let store = open_graph_store(cis);
        return store.apply_delta(graph, affected);
    }
    save_graph_with_backend(cis, graph)
}

pub fn save_graph_with_backend(cis: &Path, graph: &InMemoryGraph) -> io::Result<()> {
    let store = open_graph_store(cis);
    store.save_snapshot(graph)?;
    if graph_backend_from_env() == GraphBackendKind::Sqlite && graph_json_export_enabled() {
        let json = JsonGraphStore::new(cis.to_path_buf());
        let _ = json.save_snapshot(graph);
    }
    Ok(())
}

#[derive(Debug, Default)]
pub struct MigrateGraphReport {
    pub identities: usize,
    pub revisions: usize,
    pub edges: usize,
    pub graph_json_stripped: bool,
}

/// Import `graph.json` into SQLite graph store.
pub fn migrate_graph_json_to_sqlite(
    repo_root: impl AsRef<Path>,
    compact: bool,
    rebuild_normalized: bool,
) -> io::Result<MigrateGraphReport> {
    migration_strict_sqlite_enabled()?;
    let cis = crate::persistence::cis_dir(&repo_root);
    let path = graph_snapshot_path(&cis);
    #[cfg(feature = "body-sqlite")]
    {
        let store = SqliteGraphStore::open(&cis)?;
        if rebuild_normalized {
            return rebuild_graph_normalized(&cis, compact);
        }
        if !path.is_file() {
            return Ok(MigrateGraphReport::default());
        }
        let graph = load_graph_snapshot(&path)?;
        let mut rep = MigrateGraphReport {
            identities: graph.identity_count(),
            revisions: graph.revision_count(),
            edges: graph.edge_count(),
            graph_json_stripped: false,
        };
        store.save_snapshot(&graph)?;
        if compact {
            std::fs::remove_file(&path).map_err(|e| io::Error::other(e.to_string()))?;
            rep.graph_json_stripped = true;
        }
        return Ok(rep);
    }
    #[cfg(not(feature = "body-sqlite"))]
    {
        let _ = (compact, rebuild_normalized, path);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "migrate-graph requires --features body-sqlite",
        ))
    }
}

/// Rebuild normalized SQLite rows from the blob snapshot in `graph.db`.
#[cfg(feature = "body-sqlite")]
pub fn rebuild_graph_normalized(cis: &Path, compact: bool) -> io::Result<MigrateGraphReport> {
    migration_strict_sqlite_enabled()?;
    let path = graph_snapshot_path(cis);
    let store = SqliteGraphStore::open(cis)?;
    let graph = match store.rebuild_normalized_from_blob() {
        Ok(g) => g,
        Err(e) if e.kind() == io::ErrorKind::NotFound && path.is_file() => {
            let g = load_graph_snapshot(&path)?;
            store.save_snapshot(&g)?;
            g
        }
        Err(e) => return Err(e),
    };
    let mut rep = MigrateGraphReport {
        identities: graph.identity_count(),
        revisions: graph.revision_count(),
        edges: graph.edge_count(),
        graph_json_stripped: false,
    };
    if compact && path.is_file() {
        std::fs::remove_file(&path).map_err(|e| io::Error::other(e.to_string()))?;
        rep.graph_json_stripped = true;
    }
    Ok(rep)
}

#[cfg(not(feature = "body-sqlite"))]
pub fn rebuild_graph_normalized(_cis: &Path, _compact: bool) -> io::Result<MigrateGraphReport> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "rebuild-graph-normalized requires --features body-sqlite",
    ))
}

fn migration_strict_sqlite_enabled() -> io::Result<()> {
    if std::env::var_os("CIS_MIGRATION_STRICT").is_some_and(|v| v == "1") {
        #[cfg(not(feature = "body-sqlite"))]
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "CIS_MIGRATION_STRICT=1 requires body-sqlite feature",
            ));
        }
    }
    Ok(())
}

pub fn migration_strict_body_sqlite() -> io::Result<()> {
    if !std::env::var_os("CIS_MIGRATION_STRICT").is_some_and(|v| v == "1") {
        return Ok(());
    }
    #[cfg(not(feature = "body-sqlite"))]
    {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "CIS_MIGRATION_STRICT=1 requires body-sqlite feature",
        ));
    }
    #[cfg(feature = "body-sqlite")]
    {
        if graph_backend_from_env() == GraphBackendKind::Sqlite
            || std::env::var_os("CIS_BODY_BACKEND").is_some_and(|v| v == "sqlite")
        {
            return Ok(());
        }
    }
    Ok(())
}
