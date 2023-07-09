use crate::error::VelesError;

pub struct VelesMessage {
    pub command: VelesCommand,
}

pub enum VelesCommand {
    Status,
}

pub trait VelesProtocol {
    fn send_message(&self, message: &VelesMessage) -> Result<(), VelesError>;
}

#[derive(Default)]
pub struct LocalTransport {}

impl VelesProtocol for LocalTransport {
    fn send_message(&self, _message: &VelesMessage) -> Result<(), VelesError> {
        Ok(())
    }
}
