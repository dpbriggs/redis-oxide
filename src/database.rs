use crate::logger::LOGGER;
use crate::startup::Config;
use crate::types::{DumpFile, StateStore, StateStoreRef};
use directories::ProjectDirs;
use parking_lot::Mutex;
use std::error::Error;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::task;
use tokio::time::interval;

const SAVE_STATE_PERIOD_SEC: u64 = 60;
const SAVE_STATE_PERIOD: u64 = SAVE_STATE_PERIOD_SEC * 1000;

/// Convenience macro to panic with error messages.
macro_rules! fatal_panic {
    ($msg:expr) => {{
        error!(LOGGER, "{}", $msg);
        println!("{}", $msg);
        panic!("Fatal Error, cannot continue...");
    }};
    ($msg:expr, $err:expr) => {{
        error!(LOGGER, "{} {}", $msg, $err);
        println!("{}", $msg);
        panic!("Fatal Error, cannot continue...");
    }};
}

/// Dump the current state to the dump_file
fn dump_state(state: StateStoreRef, dump_file: &mut File) -> Result<(), Box<dyn Error>> {
    dump_file.seek(SeekFrom::Start(0))?;
    rmps::encode::write(dump_file, &state)
        .map_err(|e| fatal_panic!("Could not write state!", e.to_string()))
        .unwrap();
    Ok(())
}

/// Load state from the dump_file
pub fn load_state(dump_file: DumpFile, config: &Config) -> Result<StateStoreRef, Box<dyn Error>> {
    let mut contents = dump_file.lock();
    if contents.metadata()?.len() == 0 {
        return Ok(Arc::new(StateStore::default()));
    }

    contents.seek(SeekFrom::Start(0))?;
    let mut state_store: StateStore = rmps::decode::from_read(&*contents)?;
    state_store.commands_threshold = config.ops_until_save;
    state_store.memory_only = config.memory_only;

    Ok(Arc::new(state_store))
}

/// Make the data directory (directory where the dump file lives)
fn make_data_dir(data_dir: &Path) {
    match std::fs::create_dir_all(&data_dir) {
        Ok(_) => {
            info!(
                LOGGER,
                "Created config dir path {}",
                data_dir.to_string_lossy()
            );
        }
        Err(e) => {
            let err_msg = format!(
                "Error! Cannot create path {}, error {}",
                data_dir.to_string_lossy(),
                e.to_string()
            );
            fatal_panic!(err_msg);
        }
    }
}

fn default_data_dir() -> PathBuf {
    let data_dir = ProjectDirs::from("ca", "dpbriggs", "redis-oxide").expect("to fetch a default");
    let mut p = PathBuf::new();
    p.push(data_dir.data_dir());
    p
}

/// Get the dump file
///
/// Panics if a data directory cannot be found, or file cannot be opened.
pub fn get_dump_file(config: &Config) -> DumpFile {
    let data_dir: PathBuf = match &config.data_dir {
        Some(dir) => dir.to_path_buf(),
        None => default_data_dir(),
    };
    if !data_dir.exists() {
        make_data_dir(&data_dir);
    }

    let dump_file = data_dir.join("dump.rodb");
    info!(LOGGER, "Dump File Location: {:?}", dump_file);
    let opened_file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .append(false)
        .open(dump_file)
    {
        Ok(f) => f,
        Err(e) => fatal_panic!(format!("Failed to open dump file! {}", e.to_string())),
    };
    Arc::new(Mutex::new(opened_file))
}

pub fn save_state(state: StateStoreRef, dump_file: DumpFile) {
    info!(
        LOGGER,
        "Saving state ({}s or >={} ops ran)...", SAVE_STATE_PERIOD_SEC, state.commands_threshold
    );
    match dump_file.try_lock() {
        Some(mut file) => {
            if let Err(e) = task::block_in_place(|| dump_state(state, &mut file)) {
                fatal_panic!("FAILED TO DUMP STATE!", e.to_string());
            }
        }
        None => debug!(
            LOGGER,
            "Failed to save state! Someone else is currently writing..."
        ),
    }
}

/// Save the current State to Dumpfile.
///
/// Panics if state fails to dump.
pub async fn save_state_interval(state: StateStoreRef, dump_file: DumpFile) {
    let mut interval = interval(Duration::from_millis(SAVE_STATE_PERIOD));
    loop {
        interval.tick().await;
        let commands_ran_since_save = state.commands_ran_since_save.load(Ordering::SeqCst);
        if commands_ran_since_save != 0 {
            state.commands_ran_since_save.store(0, Ordering::SeqCst);
            save_state(state.clone(), dump_file.clone());
        }
    }
}
