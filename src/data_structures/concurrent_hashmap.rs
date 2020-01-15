use crate::types::{Key, ReturnValue, Value};
use evmap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use tokio::sync::Mutex;

impl Serialize for ConcurrentHashMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut clone = HashMap::new();
        self.reader.handle().for_each(|k, v| {
            if v.len() > 0 {
                clone.insert(k.clone(), v.last().cloned().unwrap());
            }
        });
        clone.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ConcurrentHashMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut s: HashMap<Key, Value> = Deserialize::deserialize(deserializer)?;
        Ok(ConcurrentHashMap::from(&mut s))
    }
}

#[derive(Debug)]
pub struct ConcurrentHashMap {
    reader: evmap::ReadHandleFactory<Key, Value>,
    writer: Mutex<evmap::WriteHandle<Key, Value>>,
}

impl Default for ConcurrentHashMap {
    fn default() -> Self {
        ConcurrentHashMap::new()
    }
}

impl ConcurrentHashMap {
    pub fn new() -> ConcurrentHashMap {
        let (reader, writer) = evmap::new();
        ConcurrentHashMap {
            reader: reader.factory(),
            writer: Mutex::new(writer),
        }
    }

    fn from(other: &mut HashMap<Key, Value>) -> ConcurrentHashMap {
        let (reader, mut writer) = evmap::new();
        for (key, value) in other.drain() {
            writer.insert(key, value);
        }
        writer.refresh();
        ConcurrentHashMap {
            reader: reader.factory(),
            writer: Mutex::new(writer),
        }
    }

    pub async fn insert(&self, key: Key, value: Value) {
        let mut guard = self.writer.lock().await;
        guard.update(key, value);
        guard.refresh();
    }

    pub fn read(&self, key: &Key) -> Option<Value> {
        self.reader
            .handle()
            .get_and(key, |e| e.last().cloned())
            .flatten()
    }

    pub async fn remove(&self, key: Key) {
        let mut guard = self.writer.lock().await;
        guard.empty(key);
        guard.refresh();
    }

    pub async fn flush_all(&self) {
        let mut guard = self.writer.lock().await;
        guard.purge();
        guard.refresh();
    }
}
