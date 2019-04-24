use crate::types::{Count, InteractionRes, Key, State, StateInteration};

#[derive(Debug, Clone)]
pub enum MiscOps {
    Keys, // TODO: Add optional glob
    Exists(Vec<Key>),
    Pong,
    FlushAll,
}

impl StateInteration for MiscOps {
    fn interact(self, state: State) -> InteractionRes {
        // match self {}
        match self {
            MiscOps::Pong => InteractionRes::StringRes(b"PONG".to_vec()),
            MiscOps::FlushAll => {
                state.kv.write().clear();
                state.sets.write().clear();
                state.lists.write().clear();
                InteractionRes::Ok
            }
            MiscOps::Exists(keys) => InteractionRes::IntRes(
                keys.iter()
                    .map(|key| state.kv.read().contains_key(key))
                    .filter(|exists| *exists)
                    .count() as Count,
            ),
            MiscOps::Keys => {
                let mut kv_keys: Vec<Key> = state.kv.read().keys().cloned().collect();
                let mut set_keys: Vec<Key> = state.sets.read().keys().cloned().collect();
                let mut list_keys: Vec<Key> = state.lists.read().keys().cloned().collect();
                kv_keys.append(&mut set_keys);
                kv_keys.append(&mut list_keys);
                InteractionRes::MultiStringRes(kv_keys)
            }
        }
    }
}
