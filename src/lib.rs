use std::{
    collections::HashMap,
    ffi::OsString,
    fs::OpenOptions,
    io::{Read, Write},
    path::PathBuf,
};

use error::VelesError;
use flate2::read::GzDecoder;
use ring::digest;
use rusqlite::Connection;

use crate::util::DirIterator;

pub mod client;
pub mod config;
pub mod core;
pub mod error;
pub mod protocol;
pub mod repo;
pub mod storage;
pub mod util;

pub trait Finalize {
    fn finalize(self) -> Result<String, VelesError>;
}

#[derive(Clone, Debug, PartialEq)]
struct VelesNode {
    name: OsString,
    items: Vec<VelesNode>,
    hash: Option<String>,
}

pub fn manifest() -> Result<(), VelesError> {
    let mut nodes = HashMap::new();

    let iter = DirIterator::from_ignorefile(".", ".velesignore", true)?;
    for path in iter {
        if path.is_dir() {
            nodes.insert(
                path.clone(),
                VelesNode {
                    name: path.file_name().unwrap().to_owned(),
                    items: Vec::new(),
                    hash: None,
                },
            );
        }

        if let Some(parent) = path.parent() {
            let parent = parent.to_owned();
            if !nodes.contains_key(&parent) {
                nodes.insert(
                    parent.clone(),
                    VelesNode {
                        name: parent.file_name().unwrap_or_default().to_owned(),
                        items: Vec::new(),
                        hash: None,
                    },
                );
            }

            // let hash = if path.is_file() {
            //     Some(do_commit(path.clone())?)
            // } else {
            //     None
            // };
            let hash = None;

            let node = nodes.get_mut(&parent).unwrap();
            node.items.push(VelesNode {
                name: path.file_name().unwrap().to_owned(),
                items: Vec::new(),
                hash,
            });
        }
    }

    println!("Built node tree: {:?}", nodes);

    let mut ref_hash = String::new();
    let mut hashes: HashMap<OsString, String> = HashMap::new();
    let mut nodes: Vec<VelesNode> = nodes.into_values().collect();
    let mut i = 0;
    while !nodes.is_empty() {
        let node = &nodes[i];

        let ready = node
            .items
            .iter()
            .all(|n| n.hash.is_some() || hashes.contains_key(&n.name));
        if ready {
            let mut contents = String::new();

            for item in &node.items {
                let item_hash = item
                    .hash
                    .as_ref()
                    .unwrap_or_else(|| hashes.get(&item.name).unwrap());
                contents.push_str(&format!("{} {}\n", item_hash, item.name.to_string_lossy()));
            }

            let mut context = digest::Context::new(&digest::SHA256);
            context.update(contents.as_bytes());
            let digest = context.finish();
            let hex_digest = hex::encode(digest);

            hashes.insert(node.name.clone(), hex_digest[..40].to_owned());

            println!("Adding tree {}:\n{}", hex_digest[..40].to_owned(), contents);

            nodes.remove(i);

            if nodes.is_empty() {
                ref_hash = hex_digest[..40].to_owned();
            }

            if i == nodes.len() {
                i = 0;
            }
        } else {
            i += 1;

            if i == nodes.len() {
                i = 0;
            }
        }
    }

    let conn = Connection::open(".veles/veles.db3")?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS refs (
            name TEXT PRIMARY KEY,
            revision INTEGER NOT NULL
        )",
        (),
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS changes (
            id INTEGER PRIMARY KEY,
            tree TEXT NOT NULL,
            description TEXT NOT NULL
        )",
        (),
    )?;

    conn.execute(
        "INSERT INTO changes (id, tree, description) VALUES (NULL, ?1, ?2)",
        (&ref_hash, "Testing 123."),
    )?;

    let mut statement = conn.prepare("SELECT id FROM changes WHERE tree = ?1")?;
    let id: i32 = statement.query_row([&ref_hash], |row| row.get(0))?;

    conn.execute(
        "INSERT OR REPLACE INTO refs (name, revision) VALUES ('main', ?1)",
        [id],
    )?;

    let mut statement = conn.prepare("SELECT revision FROM refs WHERE name = 'main'")?;
    let revision: i32 = statement.query_row([], |row| row.get(0))?;

    let mut statement = conn.prepare("SELECT tree FROM changes WHERE id = ?1")?;
    let new_hash: String = statement.query_row([revision], |row| row.get(0))?;

    println!("Main is now at revision {} : {}", revision, new_hash);

    Ok(())
}

pub struct VelesChange {
    pub id: u32,
    pub user: String,
    pub description: String,
}

pub fn uncommit(hash: String, output: Option<String>) -> Result<(), VelesError> {
    let path = PathBuf::from(".veles/").join(&hash[..2]);
    let file_path = path.join(&hash[2..]);
    let file = OpenOptions::new().read(true).open(file_path)?;
    let mut decoder = GzDecoder::new(file);

    let mut buf = Vec::new();
    decoder.read_to_end(&mut buf)?;

    if let Some(out) = output {
        let mut new_file = OpenOptions::new().create(true).write(true).open(out)?;
        new_file.write_all(&buf)?;
    } else if let Ok(str) = String::from_utf8(buf) {
        println!("{}", str);
    } else {
        println!("Failed to parse data as utf-8");
    }

    Ok(())
}
