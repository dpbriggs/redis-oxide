#![feature(const_str_as_bytes, const_fn, const_str_len)]
#![deny(unsafe_code)]
#![feature(await_macro, async_await, futures_api)]
#[macro_use]
extern crate nom;
#[cfg(test)]
extern crate pretty_assertions;
extern crate promptly;
extern crate shlex;

#[cfg(test)]
extern crate proptest;

#[macro_use]
extern crate slog;
extern crate sloggers;

#[macro_use]
extern crate combine;

mod asyncresp;
mod engine;
mod logger;
mod ops;
mod resp;
mod server;
mod types;

use self::logger::LOGGER;
use self::server::server;
use self::types::Engine;

fn main() {
    info!(LOGGER, "initializing engine...");
    let engine = Engine::default();
    info!(LOGGER, "starting server...");
    server(engine).expect("server failed to start!");
}
