use lazy_static::lazy_static;
use slog::Logger;
use sloggers::terminal::{Destination, TerminalLoggerBuilder};
#[allow(unused_imports)] // Emacs is convinced this is unused.
use sloggers::types::{Severity, SourceLocation};
use sloggers::Build;

#[cfg(debug_assertions)]
fn get_logger() -> Logger {
    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Debug);
    builder.destination(Destination::Stdout);

    builder.build().unwrap()
}

#[cfg(not(debug_assertions))]
fn get_logger() -> Logger {
    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Info);
    builder.destination(Destination::Stdout);
    builder.source_location(SourceLocation::None);

    let logger = builder.build().unwrap();
    logger
}

lazy_static! {
    pub static ref LOGGER: Logger = get_logger();
}
