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
    // 1. Get the args.
    let opt = Config::from_args();
    // 2. Print the fancy logo.
    startup_message(&opt);
    // 3. Get the database file, making folders if necessary.
    info!(LOGGER, "Initializing State...");
    let dump_file = get_dump_file(&opt);
    // 4. Load database state if it exists.
    info!(LOGGER, "Opening Datafile...");
    let state = load_state(dump_file.clone(), &opt)?;
    // 5. Spawn the save-occasionally service.
    info!(LOGGER, "Starting Server...");
    if !opt.memory_only {
        info!(LOGGER, "Spawning database saving task...");
        tokio::spawn(save_state_interval(state.clone(), dump_file.clone()));
    } else {
        warn!(
            LOGGER,
            "Database is in memory-only mode. STATE WILL NOT BE SAVED!"
        );
    }
    // 6. Start the server! It will start listening for connections.
    socket_listener(state.clone(), dump_file.clone(), opt).await;
    Ok(())
}
