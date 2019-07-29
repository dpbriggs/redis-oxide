#![deny(unsafe_code)]
#![feature(concat_idents)]
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

extern crate rmp_serde as rmps;

use structopt::StructOpt;

mod asyncresp;
mod blocking;
mod database;
mod hashes;
mod keys;
mod lists;
mod logger;
#[macro_use]
mod macros;
mod misc;
mod ops;
mod server;
mod sets;
mod startup;
mod state;
mod types;

use self::database::{get_dump_file, load_state};
use self::logger::LOGGER;
use self::server::server;
use self::startup::{startup_message, Config};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Config::from_args();
    startup_message(&opt);
    info!(LOGGER, "Initializing State...");
    info!(LOGGER, "Opening Datafile...");
    let dump_file = get_dump_file(&opt);
    let state = load_state(dump_file.clone(), &opt)?;
    info!(LOGGER, "Starting Server...");
    if let Err(e) = server(state, dump_file, opt) {
        error!(LOGGER, "Server failed to start! {:?}", e);
    }
    Ok(())
}
