#![deny(unsafe_code)]
extern crate bincode;
#[cfg(test)]
extern crate pretty_assertions;
extern crate promptly;
extern crate shlex;

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
mod keys;
mod lists;
mod logger;
mod misc;
mod ops;
mod server;
mod sets;
mod startup;
mod state;
mod types;

use self::logger::LOGGER;
use self::server::server;
use self::startup::{startup_message, Config};
use self::types::State;

fn main() {
    let opt = Config::from_args();
    startup_message(&opt);
    info!(LOGGER, "Initializing State...");
    let engine = State::default();
    info!(LOGGER, "Starting Server...");
    server(engine).expect("server failed to start!");
}
