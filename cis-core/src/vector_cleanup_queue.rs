//! Durable **`VectorCleanupQueue`** (DLQ) for failed vector deletes during **`cancel_merge`** (v2.6).
//!
//! Optional JSON snapshot (`vector_dlq.json`): atomic replace via temp file + `rename`, **fsync** before rename.

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct VectorCleanupQueue {
    inner: Mutex<VecDeque<[u8; 32]>>,
    persist_path: Option<PathBuf>,
}

#[derive(Serialize, Deserialize)]
struct DlqFile {
    chunks: Vec<String>,
}

impl VectorCleanupQueue {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            persist_path: None,
        }
    }

    /// Load or create file-backed queue (**FR-1.16** durable DLQ).
    pub fn open_persistent(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        if path.exists() {
            let f = File::open(&path)?;
            let d: DlqFile = serde_json::from_reader(f)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let mut deque = VecDeque::new();
            for s in d.chunks {
                deque.push_back(
                    parse_hex32(&s)
                        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad chunk hex"))?,
                );
            }
            return Ok(Self {
                inner: Mutex::new(deque),
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
        let chunks: Vec<String> = self
            .inner
            .lock()
            .unwrap()
            .iter()
            .map(hex_lower32)
            .collect();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("dlq.tmp");
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            serde_json::to_writer_pretty(&mut f, &DlqFile { chunks })?;
            f.flush()?;
            f.sync_all()?;
        }
        fs::rename(&tmp, path)?;
        Ok(())
    }

    pub fn enqueue_delete(&self, chunk_id: [u8; 32]) {
        self.inner.lock().unwrap().push_back(chunk_id);
        let _ = self.flush_disk();
    }

    pub fn dequeue(&self) -> Option<[u8; 32]> {
        let x = self.inner.lock().unwrap().pop_front();
        let _ = self.flush_disk();
        x
    }

    pub fn depth(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}

impl Default for VectorCleanupQueue {
    fn default() -> Self {
        Self::new()
    }
}

fn hex_lower32(b: &[u8; 32]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn parse_hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifo() {
        let q = VectorCleanupQueue::new();
        q.enqueue_delete([1u8; 32]);
        q.enqueue_delete([2u8; 32]);
        assert_eq!(q.dequeue(), Some([1u8; 32]));
        assert_eq!(q.dequeue(), Some([2u8; 32]));
    }

    #[test]
    fn persistent_survives_reopen() {
        let dir = std::env::temp_dir().join(format!("cis-dlq-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("vector_dlq.json");
        {
            let q = VectorCleanupQueue::open_persistent(&path).unwrap();
            q.enqueue_delete([9u8; 32]);
            q.enqueue_delete([8u8; 32]);
        }
        let q2 = VectorCleanupQueue::open_persistent(&path).unwrap();
        assert_eq!(q2.depth(), 2);
        assert_eq!(q2.dequeue(), Some([9u8; 32]));
        assert_eq!(q2.dequeue(), Some([8u8; 32]));
    }
}
