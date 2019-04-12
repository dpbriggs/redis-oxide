#![feature(const_fn)]
#![feature(const_str_len)]
#![deny(unsafe_code)]
#![feature(await_macro, async_await, futures_api)]
#[macro_use]
extern crate nom;
#[cfg(test)]
extern crate pretty_assertions;
extern crate promptly;
extern crate shlex;

#[macro_use]
extern crate combine;

mod asyncresp;
mod engine;
mod ops;
mod resp;
mod server;
mod types;

use self::engine::Engine;
use self::server::server;

fn main() {
    let engine = Engine::default();
    server(engine).expect("server failed to start!");
}
