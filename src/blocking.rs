use crate::types::{InteractionRes, ReturnValue};

use futures::future::Future;
use futures::{Async, Poll};

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
    type Item = ReturnValue;
    type Error = ();
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.0() {
            Some(res) => Ok(Async::Ready(res)),
            None => {
                use crate::logger::LOGGER;
                info!(LOGGER, "here");
                Ok(Async::NotReady)
            }
        }
    }
}
