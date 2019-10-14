use crate::data_structures::receipt_map::Receipt;
use crate::types::{Count, StateRef, UTimeout, Key, InteractionRes, ReturnValue};
use crate::blocking::{KeyBlocking, YieldingFn};
use std::future::Future;
use std::time::Duration;
// use tokio::timer::Interval;
use tokio::timer::Timeout;

// #[derive(Debug, Clone)]
// pub enum TimeoutUnit {
//     Seconds(Count),
// }

// impl TimeoutUnit {
//     fn to_millis(&self) -> u64 {
//         let conv = match self {
//             TimeoutUnit::Seconds(t) => *t * 1000,
//         };
//         conv as u64
//     }
//     fn is_zero(&self) -> bool {
//         let v = match self {
//             TimeoutUnit::Seconds(t) => *t,
//         };
//         v == 0
//     }
// }

// #[derive(Debug, Clone)]
// pub struct RecieptTimeOut {
//     receipt: Receipt,
//     state: StateRef,
//     duration: TimeoutUnit,
// }

// impl RecieptTimeOut {
//     pub fn new(receipt: Receipt, state: StateRef, duration: TimeoutUnit) -> RecieptTimeOut {
//         RecieptTimeOut {
//             receipt,
//             state,
//             duration,
//         }
//     }

//     pub async fn start(&self) {
//         if self.duration.is_zero() {
//             return;
//         }
//         let mut interval = Interval::new_interval(Duration::from_millis(self.duration.to_millis()));
//         interval.next().await;
//         let mut rm = self.state.reciept_map.lock();
//         rm.timeout_receipt(self.receipt);
//     }
// }

pub async fn blocking_key_timeout(f: YieldingFn, state: StateRef, key: Key, seconds: UTimeout) -> InteractionRes {
    let receipt = state.get_receipt();
    let kb = KeyBlocking::new(f, state.clone(), key.clone(), receipt);
    timeout(kb, seconds, state, receipt).await
}

async fn timeout<T: Future<Output = ReturnValue>>(
    fut: T,
    secs: UTimeout,
    state: StateRef,
    receipt: Receipt,
) -> InteractionRes {
    match Timeout::new(fut, Duration::from_secs(secs as u64)).await {
        Ok(ret) => ret.into(),
        Err(_) => {
            let mut rm = state.reciept_map.lock();
            rm.timeout_receipt(receipt);
            ReturnValue::Nil.into()
        }
    }
}
