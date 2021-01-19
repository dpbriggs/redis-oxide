use structopt::StructOpt;

use crate::logger::LOGGER;
use std::path::PathBuf;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "redis-oxide",
    about = "A multi-threaded implementation of redis written in rust 🦀"
)]
pub struct Config {
    /// Database Dump File Directory Location
    #[structopt(short = "d", long = "dump-file", parse(from_os_str))]
    pub data_dir: Option<PathBuf>,
    /// Don't show the starting graphic
    #[structopt(short = "g", long = "no-graphic")]
    pub dont_show_graphic: bool,
    #[structopt(short = "s", long = "ops-until-save", default_value = "10000")]
    pub ops_until_save: u64,
    #[structopt(short = "p", long = "port", default_value = "6379")]
    pub port: u64,
    /// Run in memory only mode. Don't save database state to disk
    #[structopt(short = "m", long = "memory-only")]
    pub memory_only: bool,
    #[structopt(short = "f", long = "scripts-dir")]
    pub scripts_dir: Option<std::path::PathBuf>,
}

pub fn startup_message(config: &Config) {
    if !config.dont_show_graphic {
        info!(
            LOGGER,
            r#"
____/\\\\\\\\\_____   _______/\\\\\______   ____________________
 __/\\\///////\\\___   _____/\\\///\\\____   ____________________
  _\/\\\_____\/\\\___   ___/\\\/__\///\\\__   ____________________
   _\/\\\\\\\\\\\/____   __/\\\______\//\\\_   ____/\\\\\\\\\______
    _\/\\\//////\\\____   _\/\\\_______\/\\\_   __/\\\/__////\______
     _\/\\\____\//\\\___   _\//\\\______/\\\__   _______///\/________
      _\/\\\_____\//\\\__   __\///\\\__/\\\____   _____/\\\/__________
       _\/\\\______\//\\\_   ____\///\\\\\/_____   ___/\\\/____________
        _\///________\///__   ______\/////_______   __\/////////________
"#
        );
    }
    info!(LOGGER, "Redis Oxide starting...");
}
