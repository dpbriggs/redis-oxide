use redis_oxide::database::{get_dump_file, load_state};
use redis_oxide::logger::LOGGER;
use redis_oxide::scripting::{handle_redis_cmd, ScriptingBridge};
use redis_oxide::server::socket_listener;
use redis_oxide::startup::{startup_message, Config};
use redis_oxide::{database::save_state_interval, scripting::ScriptingEngine};
use tokio::sync::mpsc::channel;
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
    // 6. Create the channels for scripting
    let (prog_string_sx, prog_string_rx) = channel(12);
    let (cmd_result_sx, cmd_result_rx) = channel(12);

    let scripting_engine =
        ScriptingEngine::new(prog_string_rx, cmd_result_sx, state.clone(), &opt)?;

    info!(LOGGER, "ScriptingEngine main loop started");
    std::thread::spawn(|| scripting_engine.main_loop());

    let scripting_bridge = ScriptingBridge::new(prog_string_sx);

    tokio::spawn(handle_redis_cmd(
        cmd_result_rx,
        state.clone(),
        dump_file.clone(),
        scripting_bridge.clone(),
    ));

    // 7. Start the server! It will start listening for connections.
    socket_listener(state.clone(), dump_file.clone(), opt, scripting_bridge).await;
    Ok(())
}
