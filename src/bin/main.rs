use clap::Parser;
use simple_logger::SimpleLogger;
use veles::{Command, Args};

fn main() {
    SimpleLogger::new().init().unwrap();

    let args = Args::parse();

    match args.command {
        Command::Commit { file } => veles::commit(file),
        Command::Uncommit { hash, output } => veles::uncommit(hash, output),
        Command::Status => veles::status(),
        Command::Storage { command } => veles::storage(&command),
        Command::Index { command } => veles::index(&command),
        Command::Add { file } => veles::add(file),
        Command::Submit { message } => veles::submit(message),
        Command::Log => veles::log(),
        Command::Manifest => veles::manifest(),
        Command::Server => veles::server(),
    }
    .unwrap()
}