use redis_oxide::database::save_state_interval;
use redis_oxide::database::{get_dump_file, load_state};
use redis_oxide::logger::LOGGER;
use redis_oxide::server::socket_listener;
use redis_oxide::startup::{startup_message, Config};
#[macro_use]
extern crate slog;

use structopt::StructOpt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Config::from_args();
    startup_message(&opt);
    info!(LOGGER, "Initializing State...");
    info!(LOGGER, "Opening Datafile...");
    let dump_file = get_dump_file(&opt);
    let state = load_state(dump_file.clone(), &opt)?;
    info!(LOGGER, "Starting Server...");
    tokio::spawn(save_state_interval(state.clone(), dump_file.clone()));
    socket_listener(state.clone(), dump_file.clone(), opt).await;
    Ok(())
}
