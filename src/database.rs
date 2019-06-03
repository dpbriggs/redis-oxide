use crate::logger::LOGGER;
use crate::startup::Config;
use crate::types::{Database, DumpFile, State};
use directories::ProjectDirs;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::convert::From;
use std::error::Error;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::prelude::*;
use tokio::timer::Interval;

const SAVE_STATE_PERIOD: u64 = 60 * 100;

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

/// Convenience macro to deserialize parts of State.
macro_rules! from_bytes {
    ($bytez:expr) => {{
        let de = Deserialize::deserialize(&mut Deserializer::new(&$bytez[..]));
        let des_atmp = match de {
            Ok(da) => da,
            Err(_) if $bytez.len() == 0 => Default::default(),
            Err(e) => fatal_panic!("failed to deserialize! {}", e.description()),
        };
        Arc::new(RwLock::new(des_atmp))
    }};
}

/// Convenience macro to serialize parts of State
macro_rules! to_bytes {
    ($item:expr) => {{
        let mut buf = Vec::new();
        $item
            .serialize(&mut Serializer::new(&mut buf))
            .and_then(|_| Ok(buf))
            .unwrap()
    }};
}

/// Trait to create a State from a Database instance (i.e. load from dump_file)
impl From<Database> for State {
    fn from(db: Database) -> State {
        State {
            kv: from_bytes!(db.kv),
            sets: from_bytes!(db.sets),
            lists: from_bytes!(db.lists),
            hashes: from_bytes!(db.hashes),
        }
    }
}

/// Dump the current state to the dump_file
fn dump_state(state: &State, dump_file: &mut File) -> Result<(), Box<Error>> {
    let db = Database {
        kv: to_bytes!(*state.kv.read()),
        sets: to_bytes!(&*state.sets.read()),
        lists: to_bytes!(&*state.lists.read()),
        hashes: to_bytes!(&*state.hashes.read()),
    };
    dump_file.seek(SeekFrom::Start(0))?;
    rmps::encode::write(dump_file, &db)
        .map_err(|e| fatal_panic!("Could not write state!", e.description()))
        .unwrap();
    Ok(())
}

/// Load state from the dump_file
pub fn load_state(dump_file: DumpFile) -> Result<State, Box<Error>> {
    let mut contents = dump_file.lock();
    if contents.metadata()?.len() == 0 {
        return Ok(State::default());
    }

    contents.seek(SeekFrom::Start(0))?;
    let database: Database = rmps::decode::from_read(&*contents)?;

    Ok(State::from(database))
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
                e.description()
            );
            fatal_panic!(err_msg);
        }
    }
}

/// Get the dump file
///
/// Panics if a data directory cannot be found, or file cannot be opened.
pub fn get_dump_file(config: &Config) -> DumpFile {
    let data_dir: PathBuf = match &config.data_dir {
        Some(dir) => dir.to_path_buf(),
        None => match ProjectDirs::from("ca", "dpbriggs", "redis-oxide") {
            Some(dir2) => {
                let mut p = PathBuf::new();
                p.push(dir2.data_dir());
                p
            }
            None => fatal_panic!("Could not get a data_dir!"),
        },
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
        Err(e) => fatal_panic!(format!("Failed to open dump file! {}", e.description())),
    };
    Arc::new(Mutex::new(opened_file))
}

/// Save the current State to Dumpfile.
///
/// Panics if state fails to dump.
pub fn save_state(state: State, dump_file: DumpFile) -> impl Future<Item = (), Error = ()> {
    let dump_file_clone = dump_file.clone();
    Interval::new(Instant::now(), Duration::from_millis(SAVE_STATE_PERIOD))
        .skip(1)
        .for_each(move |_| {
            info!(LOGGER, "Saving state...");
            match dump_file_clone.try_lock() {
                Some(mut file) => {
                    if let Err(e) = dump_state(&state, &mut file) {
                        fatal_panic!("FAILED TO DUMP STATE!", e.description());
                    }
                }
                None => info!(
                    LOGGER,
                    "Failed to save state! Someone else is currently writing..."
                ),
            };
            Ok(())
        })
        .map_err(|e| error!(LOGGER, "save state failed; err={:?}", e))
}
