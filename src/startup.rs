use structopt::StructOpt;

use crate::logger::LOGGER;
use std::path::PathBuf;

#[derive(Debug, StructOpt)]
#[structopt(name = "redis-oxide", about = "The Rusty Redis Clone")]
#[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
pub struct Config {
    /// Database Dump File (default: )
    #[structopt(short = "d", long = "dump-file", parse(from_os_str))]
    output: Option<PathBuf>,
    /// Don't show the starting graphic
    #[structopt(short = "g", long = "no-graphic")]
    dont_show_graphic: bool,
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
    _\/\\\//////\\\____   _\/\\\_______\/\\\_   __/\\\/____//\\_____
     _\/\\\____\//\\\___   _\//\\\______/\\\__   ________//\\________
      _\/\\\_____\//\\\__   __\///\\\__/\\\____   _____/\\\/__________
       _\/\\\______\//\\\_   ____\///\\\\\/_____   ___/\\\/____________
        _\///________\///__   ______\/////_______   __\///////////////__
"#
        );
    }
    info!(LOGGER, "Redis Oxide starting...");
}
