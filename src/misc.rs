use std::sync::Arc;

use crate::scripting::{Program, ScriptingBridge};
use crate::types::{Count, Index, Key, RedisValueRef, ReturnValue, StateRef, StateStoreRef, Value};

op_variants! {
    MiscOps,
    Keys(), // TODO: Add optional glob
    Exists(Vec<Key>),
    Pong(),
    FlushAll(),
    FlushDB(),
    // SwapDB(Index, Index),  // TODO: Need to figure out how to best sync clients.
    Echo(Value),
    PrintCmds(),
    Select(Index),
    Script(Value),
    EmbeddedScript(Value, Vec<RedisValueRef>),
    Info()
}

macro_rules! create_commands_list {
    ($($ops:ident),*) => {
        {
            let mut res = Vec::new();
            $(
                let tmp = $ops.iter().cloned().map(|s| s.into()).collect();
                res.push(ReturnValue::MultiStringRes(tmp));
            )*
            ReturnValue::Array(res)
        }
    };
}

/// Easily get all keys out of each passed type.
macro_rules! get_all_keys {
    ($state:expr, $($type:ident),*) => {
        {
            let mut all = Vec::new();
            $(
                all.extend($state.$type.iter().map(|r| r.key().clone()));
            )*
            all
        }
    }
}

lazy_static! {
    static ref ALL_COMMANDS: ReturnValue = {
        use crate::keys::OP_VARIANTS as KEY_VARIANTS;
        use crate::sets::OP_VARIANTS as SET_VARIANTS;
        use crate::lists::OP_VARIANTS as LIST_VARIANTS;
        use crate::hashes::OP_VARIANTS as HASH_VARIANTS;
        use crate::sorted_sets::OP_VARIANTS as ZSET_VARIANTS;
        use crate::bloom::OP_VARIANTS as BLOOM_VARIANTS;
        use crate::stack::OP_VARIANTS as STACK_VARIANTS;
        use crate::hyperloglog::OP_VARIANTS as HYPERLOGLOG_VARIANTS;
        create_commands_list!(
            KEY_VARIANTS,
            LIST_VARIANTS,
            HASH_VARIANTS,
            SET_VARIANTS,
            ZSET_VARIANTS,
            BLOOM_VARIANTS,
            STACK_VARIANTS,
            HYPERLOGLOG_VARIANTS,
            OP_VARIANTS // Misc variants
        )
    };
}

pub async fn misc_interact(
    misc_op: MiscOps,
    state: &mut StateRef,
    state_store: StateStoreRef,
    scripting_bridge: Arc<ScriptingBridge>,
) -> ReturnValue {
    match misc_op {
        MiscOps::Pong() => ReturnValue::StringRes(Value::from_static(b"PONG")),
        MiscOps::FlushAll() => {
            let clear = |state: &StateRef| {
                state.kv.clear();
                state.sets.clear();
                state.lists.clear();
                state.hashes.clear();
                state.zsets.clear();
                state.blooms.clear();
            };
            for state in state_store.states.iter_mut() {
                clear(&state);
            }
            // let state_guard = state_store.states.lock();
            // for state in state_guard.values() {
            //     clear(state);
            // }
            ReturnValue::Ok
        }
        MiscOps::FlushDB() => {
            *state = Default::default();
            ReturnValue::Ok
        }
        MiscOps::Exists(keys) => ReturnValue::IntRes(
            keys.iter()
                .map(|key| state.kv.contains_key(key))
                .filter(|exists| *exists)
                .count() as Count,
        ),
        MiscOps::Keys() => {
            let kv_keys = get_all_keys!(state, kv, sets, lists, hashes, zsets, blooms);
            ReturnValue::MultiStringRes(kv_keys)
        }
        MiscOps::PrintCmds() => (*ALL_COMMANDS).clone(),
        MiscOps::Select(index) => {
            let state_store = state_store.get_or_create(index);
            *state = state_store;
            ReturnValue::Ok
        }
        MiscOps::Echo(val) => ReturnValue::StringRes(val),
        MiscOps::Info() => {
            let info: String = [
                concat!("redis_version", ":", env!("CARGO_PKG_VERSION")),
                "arch_bits:64",
            ]
            .join("\r\n");
            ReturnValue::StringRes(info.into())
        }
        MiscOps::Script(program) => {
            let prog_str = String::from_utf8_lossy(&program).to_string();
            let res = scripting_bridge
                .handle_script_cmd(Program::String(prog_str))
                .await;
            ReturnValue::Ident(res)
        }
        MiscOps::EmbeddedScript(fn_name, fn_args) => {
            // We need to send the program over the scripting bridge
            // and wait for the result
            let fn_name = String::from_utf8_lossy(&fn_name).to_string();
            let res = scripting_bridge
                .handle_script_cmd(Program::Function(fn_name, fn_args))
                .await;
            ReturnValue::Ident(res)
        }
    }
}
