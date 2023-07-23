use std::io::Write;

use serde::{Deserialize, Serialize};

use crate::{error::VelesError, repo::VelesRepo, Finalize, VelesChange};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct VelesMessage {
    pub command: VelesCommand,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum VelesCommand {
    SubmitStart { owner: String, description: String },
    FileWrite { data: Vec<u8> },
    SubmitFinalize,
}

pub trait VelesProtocol {
    fn send_message(&self, message: &VelesMessage) -> Result<(), VelesError>;
}

pub struct LocalTransport {
    repo: VelesRepo,
}

impl LocalTransport {
    pub fn new() -> Result<LocalTransport, VelesError> {
        Ok(LocalTransport {
            repo: VelesRepo::new()?,
        })
    }

    pub fn changesets(&self) -> Result<Vec<VelesChange>, VelesError> {
        self.repo.changesets()
    }

    pub fn submit(&self, user: &str, description: &str) -> Result<(), VelesError> {
        self.repo.submit(user, description)
    }

    pub fn send_object(&self) -> Result<impl Write + Finalize, VelesError> {
        self.repo.new_object()
    }
}

impl VelesProtocol for LocalTransport {
    fn send_message(&self, _message: &VelesMessage) -> Result<(), VelesError> {
        Ok(())
    }
}
