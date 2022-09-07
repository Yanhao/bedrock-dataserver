use std::collections::HashMap;
use std::sync::RwLock;

pub struct KvStore {
    data: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }
}
