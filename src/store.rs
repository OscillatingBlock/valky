use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub trait Store: Send + Sync {
    fn get(&self, key: &Key) -> anyhow::Result<Option<Value>>;
    fn set(&self, key: Key, value: Value) -> anyhow::Result<()>;
}

#[derive(Default)]
pub struct ServerStore {
    store: Arc<RwLock<HashMap<Key, Value>>>,
}

#[derive(Eq, Debug, PartialEq, Hash, Clone, Serialize, Deserialize)]
pub struct Key(pub String);

impl Key {
    pub fn from_string(key: String) -> Key {
        Key(key)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Value(Bytes);

impl Value {
    pub fn from_bytes(val: Bytes) -> Value {
        Value(val)
    }
}

impl Store for ServerStore {
    fn get(&self, key: &Key) -> anyhow::Result<Option<Value>> {
        let guard = self
            .store
            .read()
            .map_err(|e| anyhow::anyhow!("store read lock poisoned {e}"))?;
        Ok(guard.get(key).cloned())
    }

    fn set(&self, key: Key, value: Value) -> anyhow::Result<()> {
        let mut guard = self
            .store
            .write()
            .map_err(|e| anyhow::anyhow!("store write lock poisoned {e}"))?;
        guard.insert(key, value);
        Ok(())
    }
}
