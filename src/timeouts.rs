use crate::data_structures::receipt_map::Receipt;
use crate::types::{StateRef, UTimeout, Key, ReturnValue};
use crate::blocking::{KeyBlocking, YieldingFn};
use std::future::Future;
use std::time::Duration;
// use tokio::timer::Interval;
use tokio::timer::Timeout;

pub async fn blocking_key_timeout(f: YieldingFn, state: StateRef, key: Key, seconds: UTimeout) -> ReturnValue {
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
    match Timeout::new(fut, Duration::from_secs(secs as u64)).await {
        Ok(ret) => ret,
        Err(_) => {
            let mut rm = state.reciept_map.lock();
            rm.timeout_receipt(receipt);
            ReturnValue::Nil
        }
    }
}
