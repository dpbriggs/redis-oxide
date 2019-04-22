use crate::types::{InteractionRes, Key, State, StateInteration};

#[derive(Debug, Clone)]
pub enum MiscOps {
    Keys, // TODO: Add optional glob
    Exists(Vec<Key>),
    Pong,
    FlushAll,
}

impl StateInteration for MiscOps {
    fn interact(self, engine: State) -> InteractionRes {
        // match self {}
        match self {
            MiscOps::Pong => InteractionRes::StringRes(b"PONG".to_vec()),
            MiscOps::FlushAll => {
                engine.kv.write().unwrap().clear();
                engine.sets.write().unwrap().clear();
                engine.lists.write().unwrap().clear();
                InteractionRes::Ok
            }
            MiscOps::Exists(keys) => InteractionRes::UIntRes(
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
                InteractionRes::MultiStringRes(kv_keys)
            }
        }
    }
}
