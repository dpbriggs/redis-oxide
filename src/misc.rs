use crate::types::{Count, Index, Key, ReturnValue, StateRef, StateStoreRef, Value};
// use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum MiscOps {
    Keys, // TODO: Add optional glob
    Exists(Vec<Key>),
    Pong,
    FlushAll,
    FlushDB,
    // SwapDB(Index, Index),  // TODO: Need to figure out how to best sync clients.
    Echo(Value),
    PrintCmds,
    Select(Index),
}

macro_rules! create_commands_list {
    ($($ops:ident),*) => {
        {
            let mut res = Vec::new();
            $(
                let tmp = $ops.iter().cloned().map(|s| s.into_bytes()).collect();
                res.push(ReturnValue::MultiStringRes(tmp));
            )*
            ReturnValue::Array(res)
        }
    };
}

lazy_static! {
    static ref ALL_COMMANDS: ReturnValue = {
        use crate::bloom::OP_VARIANTS as BLOOM_VARIANTS;
        use crate::hashes::OP_VARIANTS as HASH_VARIANTS;
        use crate::keys::OP_VARIANTS as KEY_VARIANTS;
        use crate::lists::OP_VARIANTS as LIST_VARIANTS;
        use crate::sets::OP_VARIANTS as SET_VARIANTS;
        use crate::sorted_sets::OP_VARIANTS as ZSET_VARIANTS;
        create_commands_list!(
            KEY_VARIANTS,
            LIST_VARIANTS,
            HASH_VARIANTS,
            SET_VARIANTS,
            ZSET_VARIANTS,
            BLOOM_VARIANTS
        )
    };
}

pub async fn misc_interact(
    misc_op: MiscOps,
    state: &mut StateRef,
    state_store: StateStoreRef,
) -> ReturnValue {
    match misc_op {
        MiscOps::Pong => ReturnValue::StringRes(b"PONG".to_vec()),
        MiscOps::FlushAll => {
            let clear = |state: &StateRef| {
                state.kv.write().clear();
                state.sets.write().clear();
                state.lists.write().clear();
                state.hashes.write().clear();
                state.zsets.write().clear();
                state.blooms.write().clear();
            };
            let state_guard = state_store.states.lock();
            for state in state_guard.values() {
                clear(state);
            }
            ReturnValue::Ok
        }
        MiscOps::FlushDB => {
            *state = Default::default();
            ReturnValue::Ok
        }
        MiscOps::Exists(keys) => ReturnValue::IntRes(
            keys.iter()
                .map(|key| state.kv.read().contains_key(key))
                .filter(|exists| *exists)
                .count() as Count,
        ),
        MiscOps::Keys => {
            let mut kv_keys: Vec<Key> = state.kv.read().keys().cloned().collect();
            let mut set_keys: Vec<Key> = state.sets.read().keys().cloned().collect();
            let mut list_keys: Vec<Key> = state.lists.read().keys().cloned().collect();
            let mut hash_keys: Vec<Key> = state.hashes.read().keys().cloned().collect();
            let mut zset_keys: Vec<Key> = state.zsets.read().keys().cloned().collect();
            let mut bloom_keys: Vec<Key> = state.blooms.read().keys().cloned().collect();
            kv_keys.append(&mut set_keys);
            kv_keys.append(&mut list_keys);
            kv_keys.append(&mut hash_keys);
            kv_keys.append(&mut zset_keys);
            kv_keys.append(&mut bloom_keys);
            ReturnValue::MultiStringRes(kv_keys)
        }
        MiscOps::PrintCmds => (*ALL_COMMANDS).clone(),
        MiscOps::Select(index) => {
            let state_store = state_store.get_or_create(index);
            *state = state_store;
            ReturnValue::Ok
        }
        MiscOps::Echo(val) => ReturnValue::StringRes(val),
    }
}
