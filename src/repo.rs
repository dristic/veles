use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
};

use flate2::{write, Compression};
use ring::digest;

use crate::{
    dao::{self, VelesDAO},
    error::VelesError,
    Changeset, Finalize, VelesChange,
};

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
    dao: VelesDAO,
}

impl VelesRepo {
    pub fn new() -> Result<VelesRepo, VelesError> {
        let dao = VelesDAO::new()?;

        // Guarantee the main task always exists.
        dao.insert_task("main")?;

        Ok(VelesRepo { dao })
    }

    pub fn create_task(&self, name: &str) -> Result<(), VelesError> {
        self.dao.insert_task(name)?;

        Ok(())
    }

    pub fn submit(&self, changeset: &Changeset) -> Result<i64, VelesError> {
        // Get the contextual information we need.
        let task = self.dao.get_task("main")?;
        let latest = self.dao.get_latest_changeset(task.task_id)?;
        let previous_changeset = latest.as_ref().map_or(0, |latest| latest.changeset_id);

        let mut tree = HashMap::new();

        // Read in the tree data if it exists.
        if let Some(latest) = latest {
            let path = PathBuf::from(".veles/objects/")
                .join(&latest.tree_hash[..2])
                .join(&latest.tree_hash[2..40]);
            let tree_contents = fs::read_to_string(&path)?;

            for line in tree_contents.lines() {
                let (file, hash) = line.split_at(line.find(' ').unwrap());
                tree.insert(file.to_string(), hash.to_string());
            }
        }

        // Now add in the changeset data.
        for (file, hash) in &changeset.changes {
            tree.insert(file.to_string(), hash.to_string());
        }

        // Create the tree file.
        let mut tree_contents = String::new();
        for (file, hash) in tree {
            tree_contents.push_str(&format!("{} {}\n", file, hash));
        }

        let mut obj_writer = Object::new()?;
        obj_writer.write_all(tree_contents.as_bytes())?;
        let tree_hash = obj_writer.finalize()?;

        // Finally build the new changeset and submit it.
        self.dao.insert_changeset(&dao::Changeset {
            changeset_id: -1,
            previous_changeset,
            task_id: task.task_id,
            user: changeset.owner.clone(),
            description: changeset.description.clone(),
            tree_hash,
        })
    }

    pub fn changesets(&self) -> Result<Vec<VelesChange>, VelesError> {
        self.dao.get_changesets()
    }

    pub fn new_object(&self) -> Result<Object, VelesError> {
        Object::new()
    }
}
