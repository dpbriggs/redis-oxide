use crate::types::{InteractionRes, ReturnValue};

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct NilBlocking(Box<dyn Fn() -> Option<ReturnValue> + Send>);

impl NilBlocking {
    #[allow(dead_code)] // We don't have blocking atm
    pub fn new(checker: Box<dyn Fn() -> Option<ReturnValue> + Send>) -> NilBlocking {
        NilBlocking(checker)
    }
    pub fn interaction_res(checker: Box<dyn Fn() -> Option<ReturnValue> + Send>) -> InteractionRes {
        InteractionRes::Blocking(Box::new(NilBlocking(checker)))
    }
}

impl Future for NilBlocking {
    type Output = ReturnValue;
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.0() {
            Some(res) => Poll::Ready(res),
            None => {
                use crate::logger::LOGGER;
                info!(LOGGER, "here");
                Poll::Pending
            }
        }
    }
}
