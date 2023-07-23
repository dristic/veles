use core::fmt;
use std::{fs, path::Path};

use serde::{Deserialize, Serialize};

use crate::error::VelesError;

#[derive(Serialize, Deserialize)]
pub struct VelesConfig {
    pub user: User,
}

#[derive(Serialize, Deserialize)]
pub struct User {
    pub name: Option<String>,
}

impl VelesConfig {
    pub fn load(path: &Path) -> Result<VelesConfig, VelesError> {
        if !path.exists() {
            Ok(VelesConfig {
                user: User { name: None },
            })
        } else {
            let contents = fs::read_to_string(path)?;
            let config = toml::from_str(&contents)?;

            Ok(config)
        }
    }

    pub fn save(&self, path: &Path) -> Result<(), VelesError> {
        let contents = toml::to_string(&self)?;
        fs::write(path, &contents)?;

        Ok(())
    }
}

impl fmt::Display for VelesConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = toml::to_string(&self).unwrap();
        write!(f, "{}", str)
    }
}
