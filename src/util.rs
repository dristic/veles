use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::error::VelesError;

/// Iterates over all files and directories in a given path.
pub struct DirIterator {
    stack: Vec<PathBuf>,
    pos: usize,
}

impl DirIterator {
    /// Iterates over everything in a directory but will ignore
    /// any items that match the paths in the given ignore file.
    pub fn from_ignorefile<P: AsRef<Path>>(
        base: P,
        ignore: P,
        include_dirs: bool,
    ) -> Result<DirIterator, VelesError> {
        let mut stack = Vec::new();
        let base_path = base.as_ref().to_path_buf();
        let ignore = ignore.as_ref().to_path_buf();

        let filter: Vec<PathBuf> = if ignore.exists() {
            let ignore_data = fs::read_to_string(ignore)?;
            ignore_data
                .lines()
                .map(|line| base_path.join(line))
                .collect()
        } else {
            Vec::new()
        };

        DirIterator::visit(base.as_ref(), &filter, &mut stack, include_dirs)?;

        Ok(DirIterator { stack, pos: 0 })
    }

    fn visit(
        path: &Path,
        filter: &[PathBuf],
        stack: &mut Vec<PathBuf>,
        include_dirs: bool,
    ) -> Result<(), VelesError> {
        if path.is_dir() {
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let path = entry.path();

                if filter.iter().any(|p| path.starts_with(p)) {
                    continue;
                }

                if path.is_dir() {
                    if include_dirs {
                        stack.push(path.to_path_buf());
                    }
                    DirIterator::visit(&path, filter, stack, include_dirs)?;
                } else {
                    stack.push(path.to_path_buf());
                }
            }
        }

        Ok(())
    }
}

impl Iterator for DirIterator {
    type Item = PathBuf;

    fn next<'a>(&mut self) -> Option<Self::Item> {
        let pos = self.pos;
        self.pos += 1;
        self.stack.get(pos).cloned()
    }
}
