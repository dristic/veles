use std::{io::Write, path::PathBuf};

use clap::{Parser, Subcommand};
use log::{error, LevelFilter};
use simple_logger::SimpleLogger;

use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use veles::{
    client::{ChangeListEntry, IndexState, VelesClient},
    error::VelesError,
};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    debug: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Init,
    Config {
        #[arg(short, long)]
        username: Option<String>,
    },
    Add {
        file: PathBuf,
    },
    Remove {
        file: PathBuf,
    },
    Changes,
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
    Submit {
        #[arg(short, long)]
        description: String,
    },
    Changelog,
    Sync,
    Server,
}

#[derive(Subcommand)]
enum TaskCommand {
    Create { name: String },
    Delete { name: String },
}

fn main() {
    let cli = Cli::parse();

    let log_level = if cli.debug {
        LevelFilter::Info
    } else {
        LevelFilter::Error
    };

    SimpleLogger::new().with_level(log_level).init().unwrap();

    let result = match cli.command {
        Command::Init => init(),
        Command::Config { username } => config(username),
        Command::Add { file } => add(file),
        Command::Remove { file: _ } => todo!(),
        Command::Changes => changes(),
        Command::Task { command: _ } => todo!(),
        Command::Submit { description } => submit(description),
        Command::Changelog => changelog(),
        Command::Sync => cli.sync(),
        Command::Server => todo!(),
    };

    if let Err(e) = result {
        println!("{}", e);

        if cli.debug {
            error!("{:?}", e);
        }
    }
}

fn init() -> Result<(), VelesError> {
    let result = VelesClient::init();

    if result.is_ok() {
        println!("Repository initialized successfully.");
    }

    result
}

fn config(username: Option<String>) -> Result<(), VelesError> {
    let should_output = username.is_none();
    let mut client = VelesClient::new()?;
    let config = client.config(username)?;

    if should_output {
        println!("{}", config);
    } else {
        println!("Config updated.");
    }

    Ok(())
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
    let changes = client.changes()?;

    let mut stdout = StandardStream::stdout(ColorChoice::Always);

    writeln!(&mut stdout, "On /main")?;
    writeln!(&mut stdout)?;

    // Added header
    stdout.set_color(ColorSpec::new().set_bold(true).set_fg(Some(Color::Green)))?;
    writeln!(&mut stdout, "Added:")?;

    // Added files
    stdout.set_color(&ColorSpec::new())?;
    let added: Vec<&ChangeListEntry> = changes
        .iter()
        .filter(|item| item.state == IndexState::Added)
        .collect();
    for file in added {
        writeln!(&mut stdout, "\t{:?}", file.path)?;
    }
    writeln!(&mut stdout)?;

    // Untracked header
    stdout.set_color(ColorSpec::new().set_bold(true).set_fg(Some(Color::Blue)))?;
    writeln!(&mut stdout, "Untracked:")?;

    // Untracked files
    stdout.set_color(&ColorSpec::new())?;
    let untracked: Vec<&ChangeListEntry> = changes
        .iter()
        .filter(|item| item.state == IndexState::Untracked)
        .collect();
    for file in untracked {
        writeln!(&mut stdout, "\t{:?}", file.path)?;
    }

    Ok(())
}

fn changelog() -> Result<(), VelesError> {
    let client = VelesClient::new()?;
    let changesets = client.changesets()?;

    let mut stdout = StandardStream::stdout(ColorChoice::Always);

    for changeset in changesets {
        stdout.set_color(ColorSpec::new().set_bold(true).set_fg(Some(Color::Cyan)))?;
        writeln!(&mut stdout, "Changeset {}", changeset.id)?;
        stdout.set_color(&ColorSpec::new())?;
        writeln!(&mut stdout, "Author: {}", changeset.user)?;
        writeln!(&mut stdout)?;
        writeln!(&mut stdout, "\t{}", changeset.description)?;
        writeln!(&mut stdout)?;
    }

    Ok(())
}

fn submit(description: String) -> Result<(), VelesError> {
    let client = VelesClient::new()?;

    let changeset_id = client.submit(description)?;

    let mut stdout = StandardStream::stdout(ColorChoice::Always);
    stdout.set_color(ColorSpec::new().set_bold(true).set_fg(Some(Color::Green)))?;
    writeln!(&mut stdout, "Submitted changeset with ID {}", changeset_id)?;

    Ok(())
}

impl Cli {
    pub fn sync(&self) -> Result<(), VelesError> {
        let client = VelesClient::new()?;

        

        Ok(())
    }
}
