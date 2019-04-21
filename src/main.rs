#![deny(unsafe_code)]
extern crate bincode;
#[cfg(test)]
extern crate pretty_assertions;
extern crate promptly;
extern crate shlex;

#[macro_use]
extern crate structopt;

#[macro_use]
extern crate serde_derive;

#[cfg(test)]
extern crate proptest;

#[macro_use]
extern crate slog;
extern crate sloggers;

#[macro_use]
extern crate combine;

use structopt::StructOpt;

mod asyncresp;
mod engine;
mod logger;
mod ops;
mod server;
mod startup;
mod types;

use self::logger::LOGGER;
use self::server::server;
use self::startup::{startup_message, Config};
use self::types::Engine;

fn main() {
    let opt = Config::from_args();
    println!("{:?}", opt);
    startup_message(&opt);
    info!(LOGGER, "initializing engine...");
    let engine = Engine::default();
    info!(LOGGER, "starting server...");
    server(engine).expect("server failed to start!");
}
