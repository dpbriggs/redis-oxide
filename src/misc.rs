use crate::types::{Count, Key, ReturnValue, StateRef};

#[derive(Debug, Clone)]
pub enum MiscOps {
    Keys, // TODO: Add optional glob
    Exists(Vec<Key>),
    Pong,
    FlushAll,
    PrintCmds(bool),
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
        // let mut res = Vec::new();
        create_commands_list!(
            KEY_VARIANTS,
            LIST_VARIANTS,
            HASH_VARIANTS,
            SET_VARIANTS,
            ZSET_VARIANTS,
            BLOOM_VARIANTS
        )
        // res.push(
        //     HASH_VARIANTS
        //         .iter()
        //         .cloned()
        //         .map(|s| s.into_bytes())
        //         .collect(),
        // );
        // res.push(
        //     SET_VARIANTS
        //         .iter()
        //         .cloned()
        //         .map(|s| s.into_bytes())
        //         .collect(),
        // );
        // res.push(
        //     ZSET_VARIANTS
        //         .iter()
        //         .cloned()
        //         .map(|s| s.into_bytes())
        //         .collect(),
        // );
        // res.push(
        //     BLOOM_VARIANTS
        //         .iter()
        //         .cloned()
        //         .map(|s| s.into_bytes())
        //         .collect(),
        // );
        // res
    };
}

pub async fn misc_interact(misc_op: MiscOps, state: StateRef) -> ReturnValue {
    match misc_op {
        MiscOps::Pong => ReturnValue::StringRes(b"PONG".to_vec()),
        MiscOps::FlushAll => {
            state.kv.write().clear();
            state.sets.write().clear();
            state.lists.write().clear();
            state.hashes.write().clear();
            state.zsets.write().clear();
            state.blooms.write().clear();
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
            kv_keys.append(&mut set_keys);
            kv_keys.append(&mut list_keys);
            ReturnValue::MultiStringRes(kv_keys)
        }
        MiscOps::PrintCmds(_) => (*ALL_COMMANDS).clone(),
    }
}
