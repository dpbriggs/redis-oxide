use crate::blocking::{KeyBlocking, YieldingFn};
use crate::data_structures::receipt_map::Receipt;
use crate::types::{Key, ReturnValue, StateRef, UTimeout};
use std::future::Future;
use std::time::Duration;
use tokio::time;

pub async fn blocking_key_timeout(
    f: YieldingFn,
    state: StateRef,
    key: Key,
    seconds: UTimeout,
) -> ReturnValue {
    let receipt = state.get_receipt();
    let kb = KeyBlocking::new(f, state.clone(), key.clone(), receipt);
    timeout(kb, seconds, state, receipt).await
}

async fn timeout<T: Future<Output = ReturnValue>>(
    fut: T,
    secs: UTimeout,
    state: StateRef,
    receipt: Receipt,
) -> ReturnValue {
    match time::timeout(Duration::from_secs(secs as u64), fut).await {
        Ok(ret) => ret,
        Err(_) => {
            let mut rm = state.reciept_map.lock();
            rm.timeout_receipt(receipt);
            ReturnValue::Nil
        }
    }
}
