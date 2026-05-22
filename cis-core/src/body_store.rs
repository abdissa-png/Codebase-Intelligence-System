//! Content-addressed **`body:`** prefix (**C-4**, **FR-1.13** alignment).

use std::sync::Arc;

use crate::kv::MemoryKv;

fn hex32(b: &[u8; 32]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

#[derive(Debug, Clone)]
pub struct BodyStore {
    kv: Arc<MemoryKv>,
}

impl BodyStore {
    pub fn new(kv: Arc<MemoryKv>) -> Self {
        Self { kv }
    }

    pub fn put(&self, body_hash: [u8; 32], content: Vec<u8>) {
        let key = format!("body:{}", hex32(&body_hash));
        self.kv.set(&key, content);
    }

    pub fn get(&self, body_hash: &[u8; 32]) -> Option<Vec<u8>> {
        self.kv.get(&format!("body:{}", hex32(body_hash)))
    }

    pub fn has(&self, body_hash: &[u8; 32]) -> bool {
        self.get(body_hash).is_some()
    }

    pub fn delete(&self, body_hash: &[u8; 32]) {
        self.kv.delete(&format!("body:{}", hex32(body_hash)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_roundtrip() {
        let kv = Arc::new(MemoryKv::new());
        let bs = BodyStore::new(kv);
        let h = [7u8; 32];
        bs.put(h, b"hello".to_vec());
        assert_eq!(bs.get(&h), Some(b"hello".to_vec()));
    }
}
