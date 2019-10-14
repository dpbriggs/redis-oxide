use crate::data_structures::receipt_map::Receipt;
use crate::types::{Count, StateRef};
use tokio::timer::Interval;
use std::time::Duration;

#[derive(Debug)]
pub enum TimeoutUnit {
    Seconds(Count),
}

impl TimeoutUnit {
    fn to_millis(&self) -> u64 {
        let conv = match self {
            TimeoutUnit::Seconds(t) => *t * 1000,
        };
        conv as u64
    }
    fn is_zero(&self) -> bool {
        let v = match self {
            TimeoutUnit::Seconds(t) => *t
        };
        v == 0
    }
}

#[derive(Debug)]
pub struct RecieptTimeOut {
    receipt: Receipt,
    state: StateRef,
    duration: TimeoutUnit,
}

impl RecieptTimeOut {
    pub fn new(receipt: Receipt, state: StateRef, duration: TimeoutUnit) -> RecieptTimeOut {
        RecieptTimeOut {
            receipt,
            state,
            duration,
        }
    }

    pub async fn start(&self) {
        if self.duration.is_zero() {
            return;
        }
        let mut interval = Interval::new_interval(Duration::from_millis(self.duration.to_millis()));
        interval.next().await;
        let mut rm = self.state.reciept_map.lock();
        rm.timeout_receipt(self.receipt);
    }
}
