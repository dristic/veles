use std::array::TryFromSliceError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum VelesError {
    #[error("IO error")]
    IOError(#[from] std::io::Error),

    #[error("Serialization error")]
    SerializationError(#[from] bincode::Error),

    #[error("Try from slice error")]
    SliceError(#[from] TryFromSliceError),

    #[error("CRC check failed")]
    CorruptedData,

    #[error("DB Error")]
    DBError(#[from] rusqlite::Error),

    #[error("Data not found")]
    NotFound,
}
