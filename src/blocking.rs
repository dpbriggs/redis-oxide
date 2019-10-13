use crate::types::{InteractionRes, Key, ReturnValue, StateRef};

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::Waker;
use std::task::{Context, Poll};

#[derive(Default, Debug)]
pub struct WakerStore {
    wakers: HashMap<Key, Vec<Waker>>,
}

impl WakerStore {
    pub fn add(&mut self, key: &[u8], waker: Waker) {
        self.wakers.entry(key.to_vec()).or_default().push(waker);
    }

    pub fn wake(&mut self, key: &[u8]) {
        self.wakers
            .get_mut(key)
            .map(|vec| vec.pop().map(|wake| wake.wake()));
    }
}

pub struct KeyBlocking {
    f: Box<dyn Fn() -> Option<ReturnValue> + Send>,
    state: StateRef,
    key: Key,
}

impl KeyBlocking {
    pub fn interaction_res(
        f: Box<dyn Fn() -> Option<ReturnValue> + Send>,
        state: StateRef,
        key: Key,
    ) -> InteractionRes {
        InteractionRes::Blocking(Box::new(KeyBlocking { f, state, key }))
    }
}

impl Future for KeyBlocking {
    type Output = ReturnValue;
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match (self.f)() {
            Some(ret) => Poll::Ready(ret),
            None => {
                self.state.sleep_list(&self.key, cx.waker().clone());
                Poll::Pending
            }
        }
    }
}
