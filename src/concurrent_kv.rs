use crate::logger::LOGGER;
use crate::op_variants;
use crate::types::{Count, Key, ReturnValue, StateRef, Value};

op_variants! {
    ConcKeyOps,
    CSet(Key, Value),
    CGet(Key),
    Del(Key)
}

pub async fn conc_key_interact(conc_key_op: ConcKeyOps, state: StateRef) -> ReturnValue {
    match conc_key_op {
        ConcKeyOps::CSet(key, value) => {
            state.concurrent_kv.insert(key, value).await;
            ReturnValue::Ok
        }
        ConcKeyOps::CGet(key) => state
            .concurrent_kv
            .read(&key)
            .map(ReturnValue::StringRes)
            .unwrap_or(ReturnValue::Nil),
        ConcKeyOps::Del(key) => {
            state.concurrent_kv.remove(key).await;
            ReturnValue::Ok
        }
    }
}
