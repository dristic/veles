use std::{fs, path::PathBuf};

use rusqlite::named_params;

use crate::{error::VelesError, VelesChange};

pub struct Task {
    pub task_id: i32,
    pub name: String,
}

pub struct Changeset {
    pub changeset_id: i32,
    pub previous_changeset: i32,
    pub task_id: i32,
    pub user: String,
    pub description: String,
    pub tree_hash: String,
}

pub struct VelesDAO {
    db: rusqlite::Connection,
}

impl VelesDAO {
    pub fn new() -> Result<VelesDAO, VelesError> {
        let path = PathBuf::from(".veles/veles.db3");
        let initialized = path.exists();
        let db = rusqlite::Connection::open(".veles/veles.db3")?;

        if !initialized {
            let result = VelesDAO::initialize(&db);
            if result.is_err() {
                let _ = fs::remove_file(path);
                return Err(result.unwrap_err());
            }
        }

        Ok(VelesDAO { db })
    }

    fn initialize(db: &rusqlite::Connection) -> Result<(), VelesError> {
        db.execute(
            "CREATE TABLE IF NOT EXISTS tasks (
                task_id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            );",
            (),
        )?;

        db.execute(
            "CREATE UNIQUE INDEX idx_task_name
             ON tasks(name);",
            (),
        )?;

        db.execute(
            "CREATE TABLE IF NOT EXISTS changesets (
                changeset_id INTEGER PRIMARY KEY,
                previous_changeset INTEGER NOT NULL,
                task_id INTEGER NOT NULL,
                user TEXT NOT NULL,
                description TEXT NOT NULL,
                tree_hash TEXT NOT NULL,
                FOREIGN KEY(task_id) REFERENCES tasks(task_id)
            )",
            (),
        )?;

        Ok(())
    }

    pub fn insert_task(&self, name: &str) -> Result<(), VelesError> {
        self.db.execute(
            "INSERT OR IGNORE INTO tasks (task_id, name) VALUES (NULL, :name)",
            named_params! { ":name": name },
        )?;

        Ok(())
    }

    pub fn get_task(&self, name: &str) -> Result<Task, VelesError> {
        let mut statement = self
            .db
            .prepare("SELECT task_id, name FROM tasks WHERE name = ?")?;
        let task = statement.query_row([name], |row| {
            let task_id: i32 = row.get(0)?;
            let name: String = row.get(1)?;

            Ok(Task { task_id, name })
        })?;

        Ok(task)
    }

    pub fn get_latest_changeset(&self, task_id: i32) -> Result<Option<Changeset>, VelesError> {
        let mut statement = self.db.prepare(
            "
            SELECT * from changesets
            WHERE task_id = ?
            ORDER BY changeset_id DESC
            LIMIT 1
        ",
        )?;

        let result = statement.query_row([task_id], |row| {
            let changeset_id: i32 = row.get(0)?;
            let previous_changeset: i32 = row.get(1)?;
            let task_id: i32 = row.get(2)?;
            let user: String = row.get(3)?;
            let description: String = row.get(4)?;
            let tree_hash: String = row.get(5)?;

            Ok(Changeset {
                changeset_id,
                previous_changeset,
                task_id,
                user,
                description,
                tree_hash,
            })
        });

        match result {
            Ok(changeset) => Ok(Some(changeset)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub fn insert_changeset(&self, changeset: &Changeset) -> Result<i64, VelesError> {
        self.db.execute(
            "
            INSERT INTO changesets VALUES (
                NULL,
                :previous_changeset,
                :task_id,
                :user,
                :description,
                :tree_hash
            )",
            named_params! {
                ":previous_changeset": changeset.previous_changeset,
                ":task_id": changeset.task_id,
                ":user": changeset.user,
                ":description": changeset.description,
                ":tree_hash": changeset.tree_hash,
            },
        )?;

        Ok(self.db.last_insert_rowid())
    }

    pub fn get_changesets(&self) -> Result<Vec<VelesChange>, VelesError> {
        let mut statement = self
            .db
            .prepare("SELECT changeset_id, user, description FROM changesets")?;
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
}
