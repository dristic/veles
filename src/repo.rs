use std::{
    fs::{self, OpenOptions},
    path::PathBuf,
};

use flate2::{write, Compression};
use ring::digest;

use crate::{error::VelesError, Finalize, VelesChange};

pub struct Object {
    temp_path: PathBuf,
    context: digest::Context,
    encoder: write::GzEncoder<std::fs::File>,
}

impl Object {
    pub fn new() -> Result<Object, VelesError> {
        let temp_path = PathBuf::from(".veles/tempfile");
        let temp_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&temp_path)?;

        let context = digest::Context::new(&digest::SHA256);
        let encoder = write::GzEncoder::new(temp_file, Compression::default());

        Ok(Object {
            temp_path,
            context,
            encoder,
        })
    }
}

impl std::io::Write for Object {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.encoder.write(buf)?;
        self.context.update(&buf[..len]);

        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        todo!()
    }
}

impl Finalize for Object {
    fn finalize(self) -> Result<String, VelesError> {
        let digest = self.context.finish();
        let hex_digest = hex::encode(digest);
        let _ = self.encoder.finish()?;

        let path = PathBuf::from(".veles/objects/").join(&hex_digest[..2]);
        fs::create_dir_all(&path)?;

        let new_file_path = path.join(&hex_digest[2..40]);
        fs::rename(&self.temp_path, &new_file_path)?;

        Ok(hex_digest)
    }
}

pub struct VelesRepo {
    db: rusqlite::Connection,
}

impl VelesRepo {
    pub fn new() -> Result<VelesRepo, VelesError> {
        let path = PathBuf::from(".veles/veles.db3");
        let initialized = path.exists();
        let db = rusqlite::Connection::open(".veles/veles.db3").unwrap();

        if !initialized {
            let result = VelesRepo::initialize(&db);
            if result.is_err() {
                let _ = fs::remove_file(path);
                return Err(result.unwrap_err());
            }
        }

        Ok(VelesRepo {
            db,
        })
    }

    fn initialize(db: &rusqlite::Connection) -> Result<(), VelesError> {
        db.execute(
            "CREATE TABLE IF NOT EXISTS changesets (
                id INTEGER PRIMARY KEY,
                user TEXT NOT NULL,
                description TEXT NOT NULL,
                tree TEXT NOT NULL
            )",
            (),
        )?;

        db.execute(
            "CREATE TABLE IF NOT EXISTS file_trees (
                tree_id INTEGER PRIMARY KEY,
                tree_name TEXT
            )",
            (),
        )?;

        db.execute(
            "CREATE TABLE IF NOT EXISTS file_tree_nodes (
                node_id INTEGER PRIMARY KEY,
                tree_id INTEGER,
                file_id INTEGER,
                parent_node_id INTEGER,
                action TEXT,
                timestamp DATETIME,
                FOREIGN KEY (tree_id) REFERENCES file_trees(tree_id),
                FOREIGN KEY (file_id) REFERENCES files(file_id),
                FOREIGN KEY (parent_node_id) REFERENCES file_tree_nodes(node_id)
            )",
            (),
        )?;

        Ok(())
    }

    pub fn submit(&self, user: &str, description: &str) -> Result<(), VelesError> {
        self.db.execute(
            "INSERT INTO changesets (id, user, description, tree) VALUES (NULL, ?1, ?2, ?3)",
            (user, description, "hash"),
        )?;

        Ok(())
    }

    pub fn changesets(&self) -> Result<Vec<VelesChange>, VelesError> {
        let mut statement = self.db.prepare("SELECT id, user, description FROM changesets")?;
        let change_iter = statement.query_map([], |row| {
            Ok(VelesChange {
                id: row.get(0)?,
                user: row.get(1)?,
                description: row.get(2)?,
            })
        })?;

        let result: Result<Vec<_>, _> = change_iter.collect();
        let changesets = result?;

        Ok(changesets)
    }

    pub fn new_object(&self) -> Result<Object, VelesError> {
        Object::new()
    }
}
