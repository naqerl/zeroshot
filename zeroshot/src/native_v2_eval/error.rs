use std::path::PathBuf;

use thiserror::Error;

/// Named failures from manifest validation, storage, scoring, and synthetic execution.
#[derive(Debug, Error)]
pub enum EvalError {
    #[error("eval manifest is not valid TOML: {0}")]
    ManifestToml(#[from] toml::de::Error),
    #[error("eval manifest is invalid: {0}")]
    Manifest(String),
    #[error("eval id {0:?} is invalid")]
    InvalidId(String),
    #[error("eval {0} was not found")]
    NotFound(String),
    #[error("eval store path {path} could not be used: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("stored eval data at {path} is invalid: {message}")]
    Data { path: PathBuf, message: String },
    #[error("eval scorer {0:?} is not registered")]
    UnknownScorer(String),
    #[error("eval scorer {0:?} is already registered")]
    DuplicateScorer(String),
    #[error("eval sample runner failed: {0}")]
    Runner(String),
    #[error("eval judge response is invalid: {0}")]
    Judge(String),
    #[error("eval report is unavailable for {0}")]
    ReportUnavailable(String),
    #[error("could not encode eval data: {0}")]
    Json(#[from] serde_json::Error),
    #[error("eval command failed: {0}")]
    Command(String),
}

impl EvalError {
    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    pub(crate) fn data(path: impl Into<PathBuf>, message: impl Into<String>) -> Self {
        Self::Data {
            path: path.into(),
            message: message.into(),
        }
    }
}
