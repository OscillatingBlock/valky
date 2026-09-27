use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

pub trait Store: Send + Sync {
    fn get(&self, key: &Key) -> anyhow::Result<Option<Value>>;
    fn set(&self, key: Key, value: Value) -> anyhow::Result<()>;
}

pub struct ServerStore {
    store: Arc<RwLock<HashMap<Key, Value>>>,
}

impl ServerStore {
    pub fn new() -> Self {
        Self {
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[derive(Eq, Debug, PartialEq, Hash, Clone, Serialize, Deserialize)]
pub struct Key(String);
#[derive(Clone, Serialize, Deserialize)]
pub struct Value(Bytes);

impl Store for ServerStore {
    fn get(&self, key: &Key) -> anyhow::Result<Option<Value>> {
        let guard = match self.store.read() {
            Err(_) => anyhow::bail!("read lock poisoned"),
            Ok(guard) => guard,
        };

        Ok(guard.get(key).cloned())
    }

    fn set(&self, key: Key, value: Value) -> anyhow::Result<()> {
        let mut guard = match self.store.write() {
            Err(_) => anyhow::bail!("write lock poisoned"),
            Ok(guard) => guard,
        };
        guard.insert(key, value);
        Ok(())
    }
}
