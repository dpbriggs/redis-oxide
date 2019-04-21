use crate::types::{Key, State, UpdateRes, UpdateState};

#[derive(Debug, Clone)]
pub enum MiscOps {
    Keys, // TODO: Add optional glob
    Exists(Vec<Key>),
    Pong,
    FlushAll,
}

impl UpdateState for MiscOps {
    fn update(self, engine: State) -> UpdateRes {
        // match self {}
        match self {
            MiscOps::Pong => UpdateRes::StringRes(b"PONG".to_vec()),
            MiscOps::FlushAll => {
                engine.kv.write().unwrap().clear();
                engine.sets.write().unwrap().clear();
                engine.lists.write().unwrap().clear();
                UpdateRes::Ok
            }
            MiscOps::Exists(keys) => UpdateRes::UIntRes(
                keys.iter()
                    .map(|key| engine.kv.read().unwrap().contains_key(key))
                    .filter(|exists| *exists)
                    .count(),
            ),
            MiscOps::Keys => {
                let mut kv_keys: Vec<Key> = engine.kv.read().unwrap().keys().cloned().collect();
                let mut set_keys: Vec<Key> = engine.sets.read().unwrap().keys().cloned().collect();
                let mut list_keys: Vec<Key> =
                    engine.lists.read().unwrap().keys().cloned().collect();
                kv_keys.append(&mut set_keys);
                kv_keys.append(&mut list_keys);
                UpdateRes::MultiStringRes(kv_keys)
            }
        }
    }
}
