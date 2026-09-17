//! Graph persistence backends (**ADR 0005**): JSON snapshot + optional SQLite (normalized rows).

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use cis_wal::{BranchId, IdentityId, NodeRevisionId};

/// Incremented by [`GraphStore::load_into`]. Phase 4 MCP sqlite boot must not bump this.
pub static LOAD_INTO_CALLS: AtomicU64 = AtomicU64::new(0);

use crate::graph::{
    EdgeType, GraphEdge, GraphSnapshot, InMemoryGraph, NodeIdentity, NodeRevision,
    GRAPH_SNAPSHOT_VERSION,
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

/// When `CIS_GRAPH_BACKEND=sqlite`, also write `graph.json` if `CIS_GRAPH_JSON_EXPORT=1`.
///
/// Default is off: the JSON export is a full-graph copy and was the checkpoint tax
/// this refactor is removing. Opt in for migration / debug dumps.
pub fn graph_json_export_enabled() -> bool {
    std::env::var_os("CIS_GRAPH_JSON_EXPORT").is_some_and(|v| {
        v == "1" || v.eq_ignore_ascii_case("true")
    })
}

/// Opt-in RAM vs SQL query compare on `find_symbol` / `get_callers` (`CIS_GRAPH_SHADOW=1`).
///
/// Phase 4: MCP queries return SQL. Shadow still compares when RAM overlay is populated.
pub fn graph_shadow_enabled() -> bool {
    std::env::var_os("CIS_GRAPH_SHADOW").is_some_and(|v| {
        v == "1" || v.eq_ignore_ascii_case("true")
    })
}

/// Compare RAM vs SQL [`crate::graph_view::GraphView`] results (Phase 3 shadow reads).
#[cfg(feature = "body-sqlite")]
pub fn shadow_graph_view_diffs(
    ram: &InMemoryGraph,
    sql: &SqliteGraphStore,
    chain: &[BranchId],
    needle: &str,
    caller_target: Option<IdentityId>,
) -> Vec<String> {
    use crate::graph_view::GraphView;

    let mut diffs = Vec::new();
    let ram_qn: Vec<_> = GraphView::find_revisions_qn_contains(ram, chain, needle, 0)
        .into_iter()
        .map(|r| r.revision_id)
        .collect();
    let sql_qn: Vec<_> = GraphView::find_revisions_qn_contains(sql, chain, needle, 0)
        .into_iter()
        .map(|r| r.revision_id)
        .collect();
    if ram_qn != sql_qn {
        diffs.push(format!(
            "find_qn_contains needle={needle:?} ram={ram_qn:?} sql={sql_qn:?}"
        ));
    }
    if let Some(id) = ram_qn.first() {
        if GraphView::get_revision(ram, *id) != GraphView::get_revision(sql, *id) {
            diffs.push(format!("get_revision mismatch for {id:?}"));
        }
        if GraphView::outbound_edges(ram, *id) != GraphView::outbound_edges(sql, *id) {
            diffs.push(format!("outbound_edges mismatch for {id:?}"));
        }
    }
    if let Some(target) = caller_target {
        let ram_in: Vec<_> = GraphView::inbound_edges_to(ram, target, Some(EdgeType::Calls))
            .into_iter()
            .map(|(_, e)| e.edge_id)
            .collect();
        let sql_in: Vec<_> = GraphView::inbound_edges_to(sql, target, Some(EdgeType::Calls))
            .into_iter()
            .map(|(_, e)| e.edge_id)
            .collect();
        if ram_in != sql_in {
            diffs.push(format!("inbound_calls ram={ram_in:?} sql={sql_in:?}"));
        }
    }
    diffs
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
        LOAD_INTO_CALLS.fetch_add(1, Ordering::SeqCst);
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
    use rusqlite::{params, Connection, OptionalExtension};

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
        CREATE INDEX IF NOT EXISTS idx_revisions_branch_identity ON revisions(branch_id, identity_id);
        CREATE INDEX IF NOT EXISTS idx_revisions_body_hash ON revisions(body_hash);
        CREATE INDEX IF NOT EXISTS idx_revisions_branch_qn ON revisions(branch_id, qualified_name);
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

    const FTS_SCHEMA: &str = "
        CREATE VIRTUAL TABLE IF NOT EXISTS revisions_fts USING fts5(
            qualified_name,
            content='revisions',
            content_rowid='rowid',
            tokenize='trigram'
        );
        CREATE TRIGGER IF NOT EXISTS revisions_fts_ai AFTER INSERT ON revisions BEGIN
            INSERT INTO revisions_fts(rowid, qualified_name) VALUES (new.rowid, new.qualified_name);
        END;
        CREATE TRIGGER IF NOT EXISTS revisions_fts_ad AFTER DELETE ON revisions BEGIN
            INSERT INTO revisions_fts(revisions_fts, rowid, qualified_name)
                VALUES('delete', old.rowid, old.qualified_name);
        END;
        CREATE TRIGGER IF NOT EXISTS revisions_fts_au AFTER UPDATE ON revisions BEGIN
            INSERT INTO revisions_fts(revisions_fts, rowid, qualified_name)
                VALUES('delete', old.rowid, old.qualified_name);
            INSERT INTO revisions_fts(rowid, qualified_name) VALUES (new.rowid, new.qualified_name);
        END;
    ";

    pub struct SqliteGraphStore {
        conn: std::sync::Mutex<Connection>,
    }

    impl SqliteGraphStore {
        pub fn open(cis_dir: &Path) -> io::Result<Self> {
            std::fs::create_dir_all(cis_dir)?;
            let path = graph_db_path(cis_dir);
            let conn = Connection::open(&path).map_err(|e| io::Error::other(e.to_string()))?;
            conn.busy_timeout(std::time::Duration::from_millis(5000))
                .map_err(|e| io::Error::other(e.to_string()))?;
            // journal_mode=WAL returns a row; pragma_update handles that.
            conn.pragma_update(None, "journal_mode", "WAL")
                .map_err(|e| io::Error::other(e.to_string()))?;
            conn.pragma_update(None, "synchronous", "NORMAL")
                .map_err(|e| io::Error::other(e.to_string()))?;
            conn.execute_batch(SCHEMA)
                .map_err(|e| io::Error::other(e.to_string()))?;
            Self::ensure_fts(&conn);
            Ok(Self {
                conn: std::sync::Mutex::new(conn),
            })
        }

        fn ensure_fts(conn: &Connection) {
            if conn.execute_batch(FTS_SCHEMA).is_err() {
                return;
            }
            let rev_n: i64 = conn
                .query_row("SELECT COUNT(*) FROM revisions", [], |r| r.get(0))
                .unwrap_or(0);
            let fts_n: i64 = conn
                .query_row("SELECT COUNT(*) FROM revisions_fts", [], |r| r.get(0))
                .unwrap_or(0);
            if rev_n > 0 && fts_n == 0 {
                let _ = conn.execute(
                    "INSERT INTO revisions_fts(revisions_fts) VALUES('rebuild')",
                    [],
                );
            }
        }

        fn with_conn<F, T>(&self, f: F) -> io::Result<T>
        where
            F: FnOnce(&Connection) -> io::Result<T>,
        {
            let conn = self
                .conn
                .lock()
                .map_err(|e| io::Error::other(format!("graph.db lock: {e}")))?;
            f(&conn)
        }

        fn blob16(v: &[u8]) -> Option<[u8; 16]> {
            (v.len() == 16).then(|| {
                let mut a = [0u8; 16];
                a.copy_from_slice(v);
                a
            })
        }

        fn blob32(v: &[u8]) -> Option<[u8; 32]> {
            (v.len() == 32).then(|| {
                let mut a = [0u8; 32];
                a.copy_from_slice(v);
                a
            })
        }

        fn revision_from_parts(
            rid: Vec<u8>,
            iid: Vec<u8>,
            bid: Vec<u8>,
            st: i64,
            qn: String,
            fp: String,
            bh: Vec<u8>,
            sh: Vec<u8>,
            lang: i64,
            extra: Vec<u8>,
        ) -> io::Result<Option<NodeRevision>> {
            let Some(rb) = Self::blob16(&rid) else {
                return Ok(None);
            };
            let Some(ib) = Self::blob16(&iid) else {
                return Ok(None);
            };
            let Some(bb) = Self::blob16(&bid) else {
                return Ok(None);
            };
            let Some(body_hash) = Self::blob32(&bh) else {
                return Ok(None);
            };
            let Some(signature_hash) = Self::blob32(&sh) else {
                return Ok(None);
            };
            let ex: RevisionExtra = serde_json::from_slice(&extra)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            Ok(Some(NodeRevision {
                revision_id: NodeRevisionId(rb),
                identity_id: IdentityId(ib),
                branch_id: BranchId(bb),
                status: RevisionStatus::from_i64(st),
                qualified_name: qn,
                file_path: fp,
                body_hash,
                signature_hash,
                language: Language::from_i64(lang),
                parent_revision_id: ex.parent_revision_id,
                rename_source_id: ex.rename_source_id,
                span: ex.span,
                tombstoned_at_ms: ex.tombstoned_at_ms,
            }))
        }

        fn edge_from_parts(
            eid: Vec<u8>,
            src: Vec<u8>,
            tgt: Vec<u8>,
            ty: i64,
            extra: Vec<u8>,
        ) -> io::Result<Option<GraphEdge>> {
            let Some(eb) = Self::blob16(&eid) else {
                return Ok(None);
            };
            let Some(sb) = Self::blob16(&src) else {
                return Ok(None);
            };
            let Some(tb) = Self::blob16(&tgt) else {
                return Ok(None);
            };
            let ex: EdgeExtra = serde_json::from_slice(&extra)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            Ok(Some(GraphEdge {
                edge_id: eb,
                ty: EdgeType::from_i64(ty),
                source_revision_id: NodeRevisionId(sb),
                target_identity_id: IdentityId(tb),
                resolution: crate::graph::EdgeResolution {
                    target_signature_hash: ex.resolution_target_sig,
                    resolver: SourceType::from_u8(ex.resolution_resolver),
                    last_validation_ms: ex.resolution_last_validation_ms,
                },
                anchor: ex.anchor,
            }))
        }

        const REV_COLS: &'static str = "revision_id, identity_id, branch_id, status, qualified_name, file_path,
            body_hash, signature_hash, language, extra";

        fn map_revision_tuple(
            rid: Vec<u8>,
            iid: Vec<u8>,
            bid: Vec<u8>,
            st: i64,
            qn: String,
            fp: String,
            bh: Vec<u8>,
            sh: Vec<u8>,
            lang: i64,
            extra: Vec<u8>,
        ) -> io::Result<Option<NodeRevision>> {
            Self::revision_from_parts(rid, iid, bid, st, qn, fp, bh, sh, lang, extra)
        }

        fn like_contains(needle: &str) -> String {
            let escaped = needle
                .replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_");
            format!("%{escaped}%")
        }

        fn fts_phrase(needle: &str) -> String {
            format!("\"{}\"", needle.replace('"', "\"\""))
        }

        fn fts_ready(conn: &Connection) -> bool {
            conn.query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'revisions_fts'",
                [],
                |r| r.get::<_, i64>(0),
            )
            .optional()
            .ok()
            .flatten()
            .is_some()
        }

        /// Indexed PK lookup — does not hydrate the graph.
        pub fn query_revision(&self, id: NodeRevisionId) -> io::Result<Option<NodeRevision>> {
            self.with_conn(|conn| {
                let mut stmt = conn
                    .prepare(&format!("SELECT {} FROM revisions WHERE revision_id = ?1", Self::REV_COLS))
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let mut rows = stmt
                    .query(params![id.0.as_slice()])
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let Some(row) = rows.next().map_err(|e| io::Error::other(e.to_string()))? else {
                    return Ok(None);
                };
                Self::map_revision_tuple(
                    row.get(0).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(1).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(2).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(3).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(4).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(5).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(6).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(7).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(8).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(9).map_err(|e| io::Error::other(e.to_string()))?,
                )
            })
        }

        pub fn query_outbound_edges(&self, id: NodeRevisionId) -> io::Result<Vec<GraphEdge>> {
            self.with_conn(|conn| {
                let mut stmt = conn
                    .prepare(
                        "SELECT edge_id, source_revision_id, target_identity_id, ty, extra
                         FROM edges WHERE source_revision_id = ?1 ORDER BY edge_id",
                    )
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let iter = stmt
                    .query_map(params![id.0.as_slice()], |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            row.get::<_, Vec<u8>>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, Vec<u8>>(4)?,
                        ))
                    })
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let mut out = Vec::new();
                for r in iter {
                    let t = r.map_err(|e| io::Error::other(e.to_string()))?;
                    if let Some(e) = Self::edge_from_parts(t.0, t.1, t.2, t.3, t.4)? {
                        out.push(e);
                    }
                }
                Ok(out)
            })
        }

        pub fn query_primary_revision(
            &self,
            branch_id: BranchId,
            identity_id: IdentityId,
        ) -> io::Result<Option<NodeRevision>> {
            self.with_conn(|conn| {
                let sql = format!(
                    "SELECT {} FROM revisions
                     WHERE branch_id = ?1 AND identity_id = ?2 AND status IN (0, 1)
                     ORDER BY CASE status WHEN 0 THEN 0 ELSE 1 END, revision_id
                     LIMIT 1",
                    Self::REV_COLS
                );
                let mut stmt = conn
                    .prepare(&sql)
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let mut rows = stmt
                    .query(params![branch_id.0.as_slice(), identity_id.0.as_slice()])
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let Some(row) = rows.next().map_err(|e| io::Error::other(e.to_string()))? else {
                    return Ok(None);
                };
                Self::map_revision_tuple(
                    row.get(0).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(1).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(2).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(3).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(4).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(5).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(6).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(7).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(8).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(9).map_err(|e| io::Error::other(e.to_string()))?,
                )
            })
        }

        pub fn query_revision_ids_for_file(
            &self,
            branch_id: BranchId,
            file_path: &str,
        ) -> io::Result<Vec<NodeRevisionId>> {
            self.with_conn(|conn| {
                let mut stmt = conn
                    .prepare("SELECT revision_id FROM revisions WHERE branch_id = ?1 AND file_path = ?2 ORDER BY revision_id")
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let iter = stmt
                    .query_map(params![branch_id.0.as_slice(), file_path], |row| {
                        row.get::<_, Vec<u8>>(0)
                    })
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let mut out = Vec::new();
                for r in iter {
                    let v = r.map_err(|e| io::Error::other(e.to_string()))?;
                    if let Some(id) = Self::blob16(&v) {
                        out.push(NodeRevisionId(id));
                    }
                }
                Ok(out)
            })
        }

        pub fn query_tombstone_revision(
            &self,
            branch_id: BranchId,
            identity_id: IdentityId,
        ) -> io::Result<Option<NodeRevision>> {
            self.with_conn(|conn| {
                let sql = format!(
                    "SELECT {} FROM revisions
                     WHERE branch_id = ?1 AND identity_id = ?2 AND status = 2
                     ORDER BY revision_id LIMIT 1",
                    Self::REV_COLS
                );
                let mut stmt = conn
                    .prepare(&sql)
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let mut rows = stmt
                    .query(params![branch_id.0.as_slice(), identity_id.0.as_slice()])
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let Some(row) = rows.next().map_err(|e| io::Error::other(e.to_string()))? else {
                    return Ok(None);
                };
                Self::map_revision_tuple(
                    row.get(0).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(1).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(2).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(3).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(4).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(5).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(6).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(7).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(8).map_err(|e| io::Error::other(e.to_string()))?,
                    row.get(9).map_err(|e| io::Error::other(e.to_string()))?,
                )
            })
        }

        pub fn query_revision_ids_for_body_hash(
            &self,
            body_hash: &[u8; 32],
        ) -> io::Result<Vec<NodeRevisionId>> {
            self.with_conn(|conn| {
                let mut stmt = conn
                    .prepare("SELECT revision_id FROM revisions WHERE body_hash = ?1")
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let iter = stmt
                    .query_map(params![body_hash.as_slice()], |row| row.get::<_, Vec<u8>>(0))
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let mut out = Vec::new();
                for r in iter {
                    let v = r.map_err(|e| io::Error::other(e.to_string()))?;
                    if let Some(id) = Self::blob16(&v) {
                        out.push(NodeRevisionId(id));
                    }
                }
                Ok(out)
            })
        }

        pub fn query_index_counts(&self) -> io::Result<crate::graph_view::GraphIndexCounts> {
            self.with_conn(|conn| {
                let revisions: i64 = conn
                    .query_row("SELECT COUNT(*) FROM revisions", [], |r| r.get(0))
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let identities: i64 = conn
                    .query_row("SELECT COUNT(*) FROM identities", [], |r| r.get(0))
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let edges: i64 = conn
                    .query_row("SELECT COUNT(*) FROM edges", [], |r| r.get(0))
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let active_revisions: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM revisions WHERE status = 0",
                        [],
                        |r| r.get(0),
                    )
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let active_outbound_edges: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM edges e
                         JOIN revisions r ON r.revision_id = e.source_revision_id
                         WHERE r.status = 0",
                        [],
                        |r| r.get(0),
                    )
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let indexable_files: i64 = conn
                    .query_row(
                        "SELECT COUNT(DISTINCT file_path) FROM revisions WHERE status = 0",
                        [],
                        |r| r.get(0),
                    )
                    .map_err(|e| io::Error::other(e.to_string()))?;
                Ok(crate::graph_view::GraphIndexCounts {
                    active_revisions: active_revisions as usize,
                    active_outbound_edges: active_outbound_edges as usize,
                    indexable_files: indexable_files as usize,
                    identities: identities as usize,
                    revisions: revisions as usize,
                    edges: edges as usize,
                })
            })
        }

        pub fn durable_revision_count(&self) -> io::Result<usize> {
            self.with_conn(|conn| Ok(Self::revision_count(conn)? as usize))
        }

        pub fn query_identity_ids_on_branch(&self, branch_id: BranchId) -> io::Result<Vec<IdentityId>> {
            self.with_conn(|conn| {
                let mut stmt = conn
                    .prepare("SELECT DISTINCT identity_id FROM revisions WHERE branch_id = ?1")
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let iter = stmt
                    .query_map(params![branch_id.0.as_slice()], |row| row.get::<_, Vec<u8>>(0))
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let mut out = Vec::new();
                for r in iter {
                    let v = r.map_err(|e| io::Error::other(e.to_string()))?;
                    if let Some(id) = Self::blob16(&v) {
                        out.push(IdentityId(id));
                    }
                }
                out.sort_by(|a, b| a.0.cmp(&b.0));
                Ok(out)
            })
        }

        pub fn query_revisions_on_branches(
            &self,
            branches: &[BranchId],
        ) -> io::Result<Vec<NodeRevision>> {
            self.with_conn(|conn| {
                let sql_all = format!("SELECT {} FROM revisions", Self::REV_COLS);
                let sql_one = format!(
                    "SELECT {} FROM revisions WHERE branch_id = ?1",
                    Self::REV_COLS
                );
                let mut out = Vec::new();
                let map_row = |row: &rusqlite::Row| {
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
                };
                if branches.is_empty() {
                    let mut stmt = conn
                        .prepare(&sql_all)
                        .map_err(|e| io::Error::other(e.to_string()))?;
                    let iter = stmt
                        .query_map([], map_row)
                        .map_err(|e| io::Error::other(e.to_string()))?;
                    for r in iter {
                        let t = r.map_err(|e| io::Error::other(e.to_string()))?;
                        if let Some(rev) =
                            Self::map_revision_tuple(t.0, t.1, t.2, t.3, t.4, t.5, t.6, t.7, t.8, t.9)?
                        {
                            out.push(rev);
                        }
                    }
                    return Ok(out);
                }
                let mut stmt = conn
                    .prepare(&sql_one)
                    .map_err(|e| io::Error::other(e.to_string()))?;
                for branch in branches {
                    let iter = stmt
                        .query_map(params![branch.0.as_slice()], map_row)
                        .map_err(|e| io::Error::other(e.to_string()))?;
                    for r in iter {
                        let t = r.map_err(|e| io::Error::other(e.to_string()))?;
                        if let Some(rev) =
                            Self::map_revision_tuple(t.0, t.1, t.2, t.3, t.4, t.5, t.6, t.7, t.8, t.9)?
                        {
                            out.push(rev);
                        }
                    }
                }
                Ok(out)
            })
        }

        pub fn query_count_revisions_with_status(
            &self,
            chain: &[BranchId],
            identity_id: IdentityId,
            status: RevisionStatus,
        ) -> io::Result<usize> {
            if chain.is_empty() {
                return Ok(0);
            }
            let st = status.to_i64();
            let mut total = 0usize;
            self.with_conn(|conn| {
                let mut stmt = conn
                    .prepare(
                        "SELECT COUNT(*) FROM revisions
                         WHERE branch_id = ?1 AND identity_id = ?2 AND status = ?3",
                    )
                    .map_err(|e| io::Error::other(e.to_string()))?;
                for branch in chain {
                    let n: i64 = stmt
                        .query_row(
                            params![branch.0.as_slice(), identity_id.0.as_slice(), st],
                            |row| row.get(0),
                        )
                        .map_err(|e| io::Error::other(e.to_string()))?;
                    total += n as usize;
                }
                Ok(())
            })?;
            Ok(total)
        }

        /// Load one file (all branches) plus branch tombstones into `graph` without a full hydrate.
        pub fn hydrate_working_set(
            &self,
            graph: &mut InMemoryGraph,
            branch: BranchId,
            file_path: &str,
        ) -> io::Result<usize> {
            self.with_conn(|conn| {
                let sql = format!(
                    "SELECT {} FROM revisions WHERE file_path = ?1
                     OR (branch_id = ?2 AND status = 2)",
                    Self::REV_COLS
                );
                let mut stmt = conn
                    .prepare(&sql)
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let iter = stmt
                    .query_map(params![file_path, branch.0.as_slice()], |row| {
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
                let mut revs = Vec::new();
                for r in iter {
                    let t = r.map_err(|e| io::Error::other(e.to_string()))?;
                    if let Some(rev) =
                        Self::map_revision_tuple(t.0, t.1, t.2, t.3, t.4, t.5, t.6, t.7, t.8, t.9)?
                    {
                        revs.push(rev);
                    }
                }
                let mut ident_ids: std::collections::HashSet<IdentityId> =
                    std::collections::HashSet::new();
                let mut rids: Vec<NodeRevisionId> = Vec::new();
                for rev in &revs {
                    ident_ids.insert(rev.identity_id);
                    rids.push(rev.revision_id);
                }
                for iid in &ident_ids {
                    let kind = conn
                        .query_row(
                            "SELECT kind FROM identities WHERE identity_id = ?1",
                            params![iid.0.as_slice()],
                            |row| row.get::<_, i64>(0),
                        )
                        .ok()
                        .map(NodeKind::from_i64)
                        .unwrap_or(NodeKind::Function);
                    graph.put_identity(NodeIdentity {
                        identity_id: *iid,
                        kind,
                    });
                }
                for rev in revs {
                    graph.put_revision(rev);
                }
                for rid in &rids {
                    let mut estmt = conn
                        .prepare(
                            "SELECT edge_id, source_revision_id, target_identity_id, ty, extra
                             FROM edges WHERE source_revision_id = ?1 ORDER BY edge_id",
                        )
                        .map_err(|e| io::Error::other(e.to_string()))?;
                    let eiter = estmt
                        .query_map(params![rid.0.as_slice()], |row| {
                            Ok((
                                row.get::<_, Vec<u8>>(0)?,
                                row.get::<_, Vec<u8>>(1)?,
                                row.get::<_, Vec<u8>>(2)?,
                                row.get::<_, i64>(3)?,
                                row.get::<_, Vec<u8>>(4)?,
                            ))
                        })
                        .map_err(|e| io::Error::other(e.to_string()))?;
                    let mut edges = Vec::new();
                    for er in eiter {
                        let t = er.map_err(|e| io::Error::other(e.to_string()))?;
                        if let Some(e) = Self::edge_from_parts(t.0, t.1, t.2, t.3, t.4)? {
                            edges.push(e);
                        }
                    }
                    drop(estmt);
                    if !edges.is_empty() {
                        let _ = graph.replace_edges_for_revision(*rid, edges);
                    }
                }
                Ok(rids.len())
            })
        }

        pub fn query_identity_kind(&self, id: IdentityId) -> io::Result<Option<NodeKind>> {
            self.with_conn(|conn| {
                let mut stmt = conn
                    .prepare("SELECT kind FROM identities WHERE identity_id = ?1")
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let mut rows = stmt
                    .query(params![id.0.as_slice()])
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let Some(row) = rows.next().map_err(|e| io::Error::other(e.to_string()))? else {
                    return Ok(None);
                };
                let kind: i64 = row.get(0).map_err(|e| io::Error::other(e.to_string()))?;
                Ok(Some(NodeKind::from_i64(kind)))
            })
        }

        pub fn query_revisions_qn_contains(
            &self,
            chain: &[BranchId],
            needle: &str,
            limit: usize,
        ) -> io::Result<Vec<NodeRevision>> {
            if chain.is_empty() {
                return Ok(Vec::new());
            }
            let pattern = Self::like_contains(needle);
            let mut out = Vec::new();
            if needle.chars().count() >= 3 {
                match self.query_revisions_qn_fts(chain, needle) {
                    Ok(rows) => out = rows,
                    Err(_) => self.with_conn(|conn| {
                        Self::query_revisions_qn_like(conn, chain, &pattern, &mut out)
                    })?,
                }
            } else {
                self.with_conn(|conn| {
                    Self::query_revisions_qn_like(conn, chain, &pattern, &mut out)
                })?;
            }
            crate::graph_view::sort_revisions_qn(&mut out);
            if limit > 0 {
                out.truncate(limit);
            }
            Ok(out)
        }

        fn query_revisions_qn_fts(
            &self,
            chain: &[BranchId],
            needle: &str,
        ) -> io::Result<Vec<NodeRevision>> {
            let phrase = Self::fts_phrase(needle);
            let mut out = Vec::new();
            self.with_conn(|conn| {
                if !Self::fts_ready(conn) {
                    return Err(io::Error::other("revisions_fts missing"));
                }
                let sql = format!(
                    "SELECT {} FROM revisions
                     WHERE branch_id = ?1
                       AND rowid IN (SELECT rowid FROM revisions_fts WHERE revisions_fts MATCH ?2)",
                    Self::REV_COLS
                );
                let mut stmt = conn
                    .prepare(&sql)
                    .map_err(|e| io::Error::other(e.to_string()))?;
                for branch in chain {
                    let iter = stmt
                        .query_map(params![branch.0.as_slice(), phrase.as_str()], |row| {
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
                    for r in iter {
                        let t = r.map_err(|e| io::Error::other(e.to_string()))?;
                        if let Some(rev) =
                            Self::map_revision_tuple(t.0, t.1, t.2, t.3, t.4, t.5, t.6, t.7, t.8, t.9)?
                        {
                            out.push(rev);
                        }
                    }
                }
                Ok(())
            })?;
            Ok(out)
        }

        fn query_revisions_qn_like(
            conn: &Connection,
            chain: &[BranchId],
            pattern: &str,
            out: &mut Vec<NodeRevision>,
        ) -> io::Result<()> {
            let sql = format!(
                "SELECT {} FROM revisions
                 WHERE branch_id = ?1 AND qualified_name LIKE ?2 ESCAPE '\\'",
                Self::REV_COLS
            );
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| io::Error::other(e.to_string()))?;
            for branch in chain {
                let iter = stmt
                    .query_map(params![branch.0.as_slice(), pattern], |row| {
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
                for r in iter {
                    let t = r.map_err(|e| io::Error::other(e.to_string()))?;
                    if let Some(rev) =
                        Self::map_revision_tuple(t.0, t.1, t.2, t.3, t.4, t.5, t.6, t.7, t.8, t.9)?
                    {
                        out.push(rev);
                    }
                }
            }
            Ok(())
        }

        pub fn query_inbound_edges(
            &self,
            target: IdentityId,
            ty: Option<EdgeType>,
        ) -> io::Result<Vec<(NodeRevision, GraphEdge)>> {
            self.with_conn(|conn| {
                let mut stmt = conn
                    .prepare(
                        "SELECT r.revision_id, r.identity_id, r.branch_id, r.status, r.qualified_name, r.file_path,
                                r.body_hash, r.signature_hash, r.language, r.extra,
                                e.edge_id, e.source_revision_id, e.target_identity_id, e.ty, e.extra
                         FROM edges e
                         JOIN revisions r ON r.revision_id = e.source_revision_id
                         WHERE e.target_identity_id = ?1 AND (e.ty = ?2 OR ?3 = 1)",
                    )
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let ty_i = ty.map(|t| t.to_i64()).unwrap_or(-1);
                let any = i64::from(ty.is_none());
                let iter = stmt
                    .query_map(params![target.0.as_slice(), ty_i, any], |row| {
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
                            row.get::<_, Vec<u8>>(10)?,
                            row.get::<_, Vec<u8>>(11)?,
                            row.get::<_, Vec<u8>>(12)?,
                            row.get::<_, i64>(13)?,
                            row.get::<_, Vec<u8>>(14)?,
                        ))
                    })
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let mut out = Vec::new();
                for r in iter {
                    let t = r.map_err(|e| io::Error::other(e.to_string()))?;
                    let Some(rev) =
                        Self::map_revision_tuple(t.0, t.1, t.2, t.3, t.4, t.5, t.6, t.7, t.8, t.9)?
                    else {
                        continue;
                    };
                    let Some(edge) = Self::edge_from_parts(t.10, t.11, t.12, t.13, t.14)? else {
                        continue;
                    };
                    out.push((rev, edge));
                }
                crate::graph_view::sort_inbound(&mut out);
                Ok(out)
            })
        }

        fn revision_count(conn: &Connection) -> io::Result<i64> {
            conn.query_row("SELECT COUNT(*) FROM revisions", [], |r| r.get(0))
                .map_err(|e| io::Error::other(e.to_string()))
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

        fn insert_identity(conn: &Connection, id: &NodeIdentity) -> io::Result<()> {
            conn.execute(
                "INSERT OR REPLACE INTO identities (identity_id, kind) VALUES (?1, ?2)",
                params![id.identity_id.0.as_slice(), id.kind.to_i64()],
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
                    r.status.to_i64(),
                    r.qualified_name,
                    r.file_path,
                    r.body_hash.as_slice(),
                    r.signature_hash.as_slice(),
                    r.language.to_i64(),
                    extra_bytes,
                ],
            )
            .map_err(|e| io::Error::other(e.to_string()))?;
            Ok(())
        }

        fn insert_edge(conn: &Connection, e: &GraphEdge) -> io::Result<()> {
            let ex = EdgeExtra {
                resolution_target_sig: e.resolution.target_signature_hash,
                resolution_resolver: e.resolution.resolver.to_u8(),
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
                    e.ty.to_i64(),
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
                    kind: NodeKind::from_i64(kind),
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
                    status: RevisionStatus::from_i64(st),
                    qualified_name: qn,
                    file_path: fp,
                    body_hash,
                    signature_hash,
                    language: Language::from_i64(lang),
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
                    ty: EdgeType::from_i64(ty),
                    source_revision_id: NodeRevisionId(sb),
                    target_identity_id: IdentityId(tb),
                    resolution: crate::graph::EdgeResolution {
                        target_signature_hash: ex.resolution_target_sig,
                        resolver: SourceType::from_u8(ex.resolution_resolver),
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
            LOAD_INTO_CALLS.fetch_add(1, Ordering::SeqCst);
            self.with_conn(|conn| {
                if Self::revision_count(conn)? > 0 {
                    // Normalized rows are the source of truth. Do not rewrite the
                    // legacy JSON blob on load — that copy is the checkpoint tax.
                    Self::load_normalized(conn, graph)?;
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
                let existing = Self::revision_count(conn)?;
                if graph.revision_count() == 0 && existing > 0 {
                    // Phase 4: RAM overlay is empty at boot. Never DELETE+rewrite SQL from it.
                    return Ok(());
                }
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

    impl crate::graph_view::GraphView for SqliteGraphStore {
        fn get_revision(&self, id: NodeRevisionId) -> Option<NodeRevision> {
            self.query_revision(id).ok().flatten()
        }

        fn outbound_edges(&self, id: NodeRevisionId) -> Vec<GraphEdge> {
            self.query_outbound_edges(id).unwrap_or_default()
        }

        fn primary_revision_for_identity(
            &self,
            branch_id: BranchId,
            identity_id: IdentityId,
        ) -> Option<NodeRevision> {
            self.query_primary_revision(branch_id, identity_id)
                .ok()
                .flatten()
        }

        fn revision_ids_for_file(&self, branch_id: BranchId, file_path: &str) -> Vec<NodeRevisionId> {
            self.query_revision_ids_for_file(branch_id, file_path)
                .unwrap_or_default()
        }

        fn identity_kind(&self, id: IdentityId) -> Option<NodeKind> {
            self.query_identity_kind(id).ok().flatten()
        }

        fn tombstone_revision_for_identity(
            &self,
            branch_id: BranchId,
            identity_id: IdentityId,
        ) -> Option<NodeRevision> {
            self.query_tombstone_revision(branch_id, identity_id)
                .ok()
                .flatten()
        }

        fn revision_ids_for_body_hash(&self, body_hash: &[u8; 32]) -> Vec<NodeRevisionId> {
            self.query_revision_ids_for_body_hash(body_hash)
                .unwrap_or_default()
        }

        fn find_revisions_qn_contains(
            &self,
            chain: &[BranchId],
            needle: &str,
            limit: usize,
        ) -> Vec<NodeRevision> {
            self.query_revisions_qn_contains(chain, needle, limit)
                .unwrap_or_default()
        }

        fn inbound_edges_to(
            &self,
            target: IdentityId,
            ty: Option<EdgeType>,
        ) -> Vec<(NodeRevision, GraphEdge)> {
            self.query_inbound_edges(target, ty).unwrap_or_default()
        }

        fn index_counts(&self) -> crate::graph_view::GraphIndexCounts {
            self.query_index_counts().unwrap_or_default()
        }

        fn identity_ids_on_branch(&self, branch: BranchId) -> Vec<IdentityId> {
            self.query_identity_ids_on_branch(branch).unwrap_or_default()
        }

        fn count_revisions_with_status(
            &self,
            chain: &[BranchId],
            identity_id: IdentityId,
            status: RevisionStatus,
        ) -> usize {
            self.query_count_revisions_with_status(chain, identity_id, status)
                .unwrap_or(0)
        }

        fn revisions_on_branches(&self, branches: &[BranchId]) -> Vec<NodeRevision> {
            self.query_revisions_on_branches(branches)
                .unwrap_or_default()
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

#[cfg(all(test, feature = "body-sqlite"))]
mod sqlite_store_tests {
    use super::*;
    use crate::graph::{
        EdgeResolution, EdgeType, GraphEdge, Language, NodeIdentity, NodeKind, NodeRevision,
        RevisionStatus, SourceSpan, SourceType,
    };
    use cis_wal::{BranchId, IdentityId, NodeRevisionId};

    fn rev(i: u8, lang: Language) -> NodeRevision {
        NodeRevision {
            revision_id: NodeRevisionId([i; 16]),
            identity_id: IdentityId([i; 16]),
            branch_id: BranchId([0u8; 16]),
            status: RevisionStatus::Active,
            qualified_name: format!("f{i}"),
            file_path: format!("f{i}.rs"),
            body_hash: [i; 32],
            signature_hash: [i; 32],
            language: lang,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        }
    }

    fn seed_graph(langs: &[Language]) -> InMemoryGraph {
        let mut g = InMemoryGraph::default();
        for (i, lang) in langs.iter().copied().enumerate() {
            let b = (i + 1) as u8;
            g.put_identity(NodeIdentity {
                identity_id: IdentityId([b; 16]),
                kind: NodeKind::Function,
            });
            let r = rev(b, lang);
            let rid = r.revision_id;
            g.put_revision(r);
            let edge = GraphEdge {
                edge_id: [b; 16],
                ty: EdgeType::Calls,
                source_revision_id: rid,
                target_identity_id: IdentityId([b; 16]),
                resolution: EdgeResolution {
                    target_signature_hash: [b; 32],
                    resolver: SourceType::Ast,
                    last_validation_ms: 0,
                },
                anchor: SourceSpan::UNKNOWN,
            };
            g.replace_edges_for_revision(rid, vec![edge]).unwrap();
        }
        g
    }

    #[test]
    fn sqlite_roundtrip_preserves_every_language() {
        let dir = tempfile::tempdir().unwrap();
        let cis = dir.path();
        let langs = [
            Language::Unknown,
            Language::Python,
            Language::TypeScript,
            Language::Go,
            Language::Rust,
            Language::Java,
            Language::Cpp,
            Language::JavaScript,
            Language::CSharp,
            Language::C,
        ];
        let g = seed_graph(&langs);
        let store = SqliteGraphStore::open(cis).unwrap();
        store.save_snapshot(&g).unwrap();
        let mut loaded = InMemoryGraph::default();
        assert!(store.load_into(&mut loaded).unwrap());
        for (i, lang) in langs.iter().copied().enumerate() {
            let rid = NodeRevisionId([(i + 1) as u8; 16]);
            let got = loaded.get_revision(rid).expect("revision");
            assert_eq!(got.language, lang, "language mismatch for {lang:?}");
        }
        assert_eq!(loaded.edge_count(), langs.len());
    }

    #[test]
    fn apply_delta_does_not_require_blob_refresh_to_reload() {
        let dir = tempfile::tempdir().unwrap();
        let cis = dir.path();
        let store = SqliteGraphStore::open(cis).unwrap();
        let mut g = seed_graph(&[Language::Rust]);
        store.save_snapshot(&g).unwrap();

        let rid = NodeRevisionId([2; 16]);
        g.put_identity(NodeIdentity {
            identity_id: IdentityId([2; 16]),
            kind: NodeKind::Function,
        });
        g.put_revision(rev(2, Language::Go));
        store.apply_delta(&g, &[rid]).unwrap();

        let mut loaded = InMemoryGraph::default();
        assert!(store.load_into(&mut loaded).unwrap());
        assert_eq!(
            loaded.get_revision(rid).unwrap().language,
            Language::Go,
            "delta must survive without rewriting graph_snapshot blob"
        );
        assert_eq!(
            loaded.get_revision(NodeRevisionId([1; 16])).unwrap().language,
            Language::Rust
        );
    }

    #[test]
    fn graph_view_sql_matches_ram_without_hydrate() {
        use crate::graph_view::GraphView;

        let dir = tempfile::tempdir().unwrap();
        let g = seed_graph(&[Language::Rust, Language::Go]);
        let store = SqliteGraphStore::open(dir.path()).unwrap();
        store.save_snapshot(&g).unwrap();

        let chain = [BranchId([0u8; 16])];
        let diffs = super::shadow_graph_view_diffs(
            &g,
            &store,
            &chain,
            "f",
            Some(IdentityId([1; 16])),
        );
        assert!(diffs.is_empty(), "shadow diffs: {diffs:?}");

        let rid = NodeRevisionId([1; 16]);
        assert_eq!(
            GraphView::get_revision(&g, rid),
            GraphView::get_revision(&store, rid)
        );
        assert_eq!(
            GraphView::outbound_edges(&g, rid),
            GraphView::outbound_edges(&store, rid)
        );
        assert_eq!(
            GraphView::primary_revision_for_identity(&g, chain[0], IdentityId([1; 16]))
                .map(|r| r.revision_id),
            GraphView::primary_revision_for_identity(&store, chain[0], IdentityId([1; 16]))
                .map(|r| r.revision_id)
        );
        assert_eq!(
            GraphView::revision_ids_for_file(&g, chain[0], "f1.rs"),
            GraphView::revision_ids_for_file(&store, chain[0], "f1.rs")
        );
        assert_eq!(
            GraphView::identity_kind(&g, IdentityId([1; 16])),
            GraphView::identity_kind(&store, IdentityId([1; 16]))
        );
    }

    #[test]
    fn sql_primary_prefers_active_then_lowest_speculative() {
        use crate::graph_view::GraphView;

        let dir = tempfile::tempdir().unwrap();
        let branch = BranchId([0u8; 16]);
        let ident = IdentityId([9; 16]);
        let mut g = InMemoryGraph::default();
        g.put_identity(NodeIdentity {
            identity_id: ident,
            kind: NodeKind::Function,
        });
        let mut tomb = rev(1, Language::Python);
        tomb.identity_id = ident;
        tomb.status = RevisionStatus::Tombstone;
        tomb.qualified_name = "pkg.dead".into();
        g.put_revision(tomb);
        let mut spec_hi = rev(5, Language::Python);
        spec_hi.identity_id = ident;
        spec_hi.status = RevisionStatus::Speculative;
        spec_hi.qualified_name = "pkg.spec_hi".into();
        g.put_revision(spec_hi);
        let mut spec_lo = rev(2, Language::Python);
        spec_lo.identity_id = ident;
        spec_lo.status = RevisionStatus::Speculative;
        spec_lo.qualified_name = "pkg.spec_lo".into();
        g.put_revision(spec_lo);

        let store = SqliteGraphStore::open(dir.path()).unwrap();
        store.save_snapshot(&g).unwrap();
        let sql_p = GraphView::primary_revision_for_identity(&store, branch, ident).unwrap();
        assert_eq!(sql_p.revision_id, NodeRevisionId([2; 16]));
        assert_eq!(sql_p.status, RevisionStatus::Speculative);
        assert_eq!(
            GraphView::primary_revision_for_identity(&g, branch, ident)
                .unwrap()
                .revision_id,
            sql_p.revision_id
        );

        let mut active = rev(4, Language::Python);
        active.identity_id = ident;
        active.status = RevisionStatus::Active;
        active.qualified_name = "pkg.live".into();
        g.put_revision(active);
        store.save_snapshot(&g).unwrap();
        let sql_live = GraphView::primary_revision_for_identity(&store, branch, ident).unwrap();
        assert_eq!(sql_live.revision_id, NodeRevisionId([4; 16]));
        assert_eq!(sql_live.status, RevisionStatus::Active);
    }

    #[test]
    fn json_export_opt_in_only() {
        std::env::remove_var("CIS_GRAPH_JSON_EXPORT");
        assert!(!graph_json_export_enabled());
        std::env::set_var("CIS_GRAPH_JSON_EXPORT", "1");
        assert!(graph_json_export_enabled());
        std::env::remove_var("CIS_GRAPH_JSON_EXPORT");
    }

    #[test]
    fn empty_overlay_save_does_not_wipe_sql() {
        use crate::graph_view::GraphView;
        let dir = tempfile::tempdir().unwrap();
        let mut g = InMemoryGraph::default();
        g.put_identity(NodeIdentity {
            identity_id: IdentityId([1; 16]),
            kind: NodeKind::Function,
        });
        g.put_revision(rev(1, Language::Python));
        let store = SqliteGraphStore::open(dir.path()).unwrap();
        store.save_snapshot(&g).unwrap();
        assert_eq!(store.durable_revision_count().unwrap(), 1);
        store.save_snapshot(&InMemoryGraph::default()).unwrap();
        assert_eq!(store.durable_revision_count().unwrap(), 1);
        assert!(GraphView::get_revision(&store, NodeRevisionId([1; 16])).is_some());
    }

    #[test]
    fn working_set_hydrate_is_not_full_load_into() {
        let dir = tempfile::tempdir().unwrap();
        let mut g = InMemoryGraph::default();
        g.put_identity(NodeIdentity {
            identity_id: IdentityId([1; 16]),
            kind: NodeKind::Function,
        });
        let mut a = rev(1, Language::Python);
        a.file_path = "a.py".into();
        g.put_revision(a);
        let mut b = rev(2, Language::Python);
        b.file_path = "b.py".into();
        g.put_revision(b);
        let store = SqliteGraphStore::open(dir.path()).unwrap();
        store.save_snapshot(&g).unwrap();
        let before = LOAD_INTO_CALLS.load(std::sync::atomic::Ordering::SeqCst);
        let mut overlay = InMemoryGraph::default();
        let n = store
            .hydrate_working_set(&mut overlay, BranchId([9; 16]), "a.py")
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(overlay.revision_count(), 1);
        assert_eq!(
            LOAD_INTO_CALLS.load(std::sync::atomic::Ordering::SeqCst),
            before
        );
    }

    #[test]
    fn fts_substring_matches_ram_contains() {
        use crate::graph_view::GraphView;

        let dir = tempfile::tempdir().unwrap();
        let mut g = InMemoryGraph::default();
        g.put_identity(NodeIdentity {
            identity_id: IdentityId([1; 16]),
            kind: NodeKind::Function,
        });
        let mut r = rev(1, Language::Python);
        r.qualified_name = "pkg.alpha_helper".into();
        g.put_revision(r);
        g.put_identity(NodeIdentity {
            identity_id: IdentityId([2; 16]),
            kind: NodeKind::Function,
        });
        let mut r2 = rev(2, Language::Python);
        r2.qualified_name = "pkg.other".into();
        g.put_revision(r2);

        let store = SqliteGraphStore::open(dir.path()).unwrap();
        store.save_snapshot(&g).unwrap();
        let chain = [BranchId([0u8; 16])];
        let needle = "lph";
        let ram: Vec<_> = GraphView::find_revisions_qn_contains(&g, &chain, needle, 0)
            .into_iter()
            .map(|x| x.revision_id)
            .collect();
        let sql: Vec<_> = GraphView::find_revisions_qn_contains(&store, &chain, needle, 0)
            .into_iter()
            .map(|x| x.revision_id)
            .collect();
        assert_eq!(ram, sql, "FTS substring hits must match RAM contains");
        assert_eq!(ram.len(), 1);
        assert_eq!(ram[0], NodeRevisionId([1; 16]));
    }
}
