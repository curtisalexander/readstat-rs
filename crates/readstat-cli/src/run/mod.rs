//! CLI command dispatch.

mod convert;
mod metadata;
mod preview;
mod support;

use crate::cli::{ReadStatCli, ReadStatCliCommands};
use readstat::ReadStatError;

pub fn run(cli: ReadStatCli) -> Result<(), ReadStatError> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    match cli.command {
        cmd @ ReadStatCliCommands::Metadata { .. } => metadata::run(cmd),
        cmd @ ReadStatCliCommands::Preview { .. } => preview::run(cmd),
        cmd @ ReadStatCliCommands::Convert { .. } => convert::run(cmd),
    }
}
