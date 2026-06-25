//! Durable graph delete queue (**Phase 3.4**), modeled on [`VectorCleanupQueue`].

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use cis_wal::{BranchId, NodeRevisionId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeleteReason {
    TombstoneExpired,
    OrphanedRevision,
    AdminPurge,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteJob {
    pub revision_id: NodeRevisionId,
    pub branch_id: BranchId,
    pub reason: DeleteReason,
}

#[derive(Debug)]
pub struct GraphDeleteQueue {
    inner: Mutex<VecDeque<DeleteJob>>,
    persist_path: Option<PathBuf>,
}

#[derive(Serialize, Deserialize)]
struct DeleteQueueFile {
    jobs: Vec<DeleteJob>,
}

impl GraphDeleteQueue {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            persist_path: None,
        }
    }

    pub fn open_persistent(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        if path.exists() {
            let f = File::open(&path)?;
            let d: DeleteQueueFile = serde_json::from_reader(f)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            return Ok(Self {
                inner: Mutex::new(d.jobs.into()),
                persist_path: Some(path),
            });
        }
        let s = Self {
            inner: Mutex::new(VecDeque::new()),
            persist_path: Some(path),
        };
        s.flush_disk()?;
        Ok(s)
    }

    fn flush_disk(&self) -> io::Result<()> {
        let path = match &self.persist_path {
            Some(p) => p,
            None => return Ok(()),
        };
        let jobs: Vec<DeleteJob> = self.inner.lock().unwrap().iter().cloned().collect();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("del.tmp");
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            serde_json::to_writer_pretty(&mut f, &DeleteQueueFile { jobs })?;
            f.flush()?;
            f.sync_all()?;
        }
        fs::rename(&tmp, path)?;
        Ok(())
    }

    pub fn enqueue(&self, job: DeleteJob) {
        self.inner.lock().unwrap().push_back(job);
        let _ = self.flush_disk();
    }

    pub fn dequeue(&self) -> Option<DeleteJob> {
        let x = self.inner.lock().unwrap().pop_front();
        let _ = self.flush_disk();
        x
    }

    pub fn depth(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}

impl Default for GraphDeleteQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifo_delete_jobs() {
        let q = GraphDeleteQueue::new();
        q.enqueue(DeleteJob {
            revision_id: NodeRevisionId([1u8; 16]),
            branch_id: BranchId([2u8; 16]),
            reason: DeleteReason::TombstoneExpired,
        });
        assert_eq!(q.depth(), 1);
        assert!(q.dequeue().is_some());
        assert_eq!(q.depth(), 0);
    }
}
