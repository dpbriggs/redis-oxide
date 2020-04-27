use crate::data_structures::receipt_map::{KeyTypes, Receipt};
use crate::types::{Key, ReturnValue, StateRef};

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::Waker;
use std::task::{Context, Poll};

pub type YieldingFn = Box<dyn Fn() -> Option<ReturnValue> + Send>;

#[derive(Default, Debug)]
pub struct WakerStore {
    wakers: HashMap<Key, Vec<Waker>>,
}

pub struct KeyBlocking {
    f: Box<dyn Fn() -> Option<ReturnValue> + Send>,
    state: StateRef,
    key: Key,
    receipt: Receipt,
}

impl KeyBlocking {
    pub fn new(f: YieldingFn, state: StateRef, key: Key, receipt: Receipt) -> KeyBlocking {
        KeyBlocking {
            f,
            state,
            key,
            receipt,
        }
    }
}

impl Future for KeyBlocking {
    type Output = ReturnValue;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.state.receipt_timed_out(self.receipt) {
            return Poll::Ready(ReturnValue::Nil);
        }
        match (self.f)() {
            Some(ret) => Poll::Ready(ret),
            None => {
                let mut rm = self.state.reciept_map.lock();
                rm.insert(self.receipt, cx.waker().clone(), KeyTypes::list(&self.key));
                Poll::Pending
            }
        }
    }
}
