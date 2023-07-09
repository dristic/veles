use std::path::PathBuf;

use clap::{Parser, Subcommand};
use log::LevelFilter;
use simple_logger::SimpleLogger;

use veles::{client::VelesClient, error::VelesError, storage::VelesStore};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[arg(short, long)]
    pub debug: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    Init,
    Add {
        file: PathBuf,
    },
    Status,
    Changesets,
    Changes,
    Commit {
        #[arg(short, long)]
        file: String,
    },
    Uncommit {
        #[arg(long)]
        hash: String,

        #[arg(short, long)]
        output: Option<String>,
    },
    Submit {
        #[arg(short, long)]
        message: String,
    },
    Log,
    Manifest,
    Storage {
        #[command(subcommand)]
        command: StorageCmd,
    },
    Server,
}

#[derive(Subcommand)]
pub enum StorageCmd {
    Get {
        #[arg(short, long)]
        key: String,
    },
    Put {
        #[arg(short, long)]
        key: String,

        #[arg(short, long)]
        value: String,
    },
    Compact,
}

pub fn storage(command: &StorageCmd) -> Result<(), VelesError> {
    let mut veles = VelesStore::new()?;

    match command {
        StorageCmd::Get { key } => {
            let value = veles.get(key)?;
            println!("{}: {}", key, value);
        }
        StorageCmd::Put { key, value } => {
            veles.put(key, value)?;
            println!("Put {}: {}", key, value);
        }
        StorageCmd::Compact => {
            veles.compact()?;
        }
    }

    Ok(())
}

fn main() {
    let args = Cli::parse();

    let log_level = if args.debug {
        LevelFilter::Info
    } else {
        LevelFilter::Error
    };

    SimpleLogger::new().with_level(log_level).init().unwrap();

    let result = match args.command {
        // Official commands.
        Command::Init => init(),
        Command::Status => status(),
        Command::Changesets => changesets(),
        Command::Changes => changes(),

        // WIP commands.
        Command::Commit { file } => veles::commit(file),
        Command::Uncommit { hash, output } => veles::uncommit(hash, output),
        Command::Storage { command } => storage(&command),
        Command::Add { file } => add(file),
        Command::Submit { message } => veles::submit(message),
        Command::Log => veles::log(),
        Command::Manifest => veles::manifest(),
        Command::Server => veles::server(),
    };

    if let Err(e) = result {
        println!("{}", e);
    }
}

fn init() -> Result<(), VelesError> {
    let result = VelesClient::init();

    if result.is_ok() {
        println!("Repository initialized successfully.");
    }

    result
}

fn status() -> Result<(), VelesError> {
    let client = VelesClient::new()?;

    client.status()
}

fn changesets() -> Result<(), VelesError> {
    let client = VelesClient::new()?;

    client.changesets()
}

fn add(file: PathBuf) -> Result<(), VelesError> {
    let client = VelesClient::new()?;
    let file_path = PathBuf::from(&file);

    client.add(file_path)?;

    println!("Added {:?}", file);
    Ok(())
}

fn changes() -> Result<(), VelesError> {
    let client = VelesClient::new()?;

    client.changes()
}
