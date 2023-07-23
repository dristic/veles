use std::array::TryFromSliceError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum VelesError {
    #[error("Repository not initialized")]
    NotInitialized,

    #[error("IO error")]
    IOError(#[from] std::io::Error),

    #[error("TOML parse error")]
    TOMLParseError(#[from] toml::de::Error),

    #[error("TOML serialize error")]
    TOMLSerializeError(#[from] toml::ser::Error),

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
