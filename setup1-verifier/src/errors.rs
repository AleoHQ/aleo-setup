use phase1_coordinator::CoordinatorError;
use snarkvm_dpc::{AddressError, ViewKeyError};

#[derive(Debug, Error)]
pub enum VerifierError {
    #[error("{}: {}", _0, _1)]
    Crate(&'static str, String),

    #[error("Coordinator Error {}", _0)]
    CoordinatorError(CoordinatorError),

    #[error("Failed to download a challenge at {}", _0)]
    FailedChallengeDownload(String),

    #[error("Failed to lock a chunk")]
    FailedLock,

    #[error("Request {} sent to {} errored", _0, _1)]
    FailedRequest(String, String),

    #[error("Failed to download a response at {}", _0)]
    FailedResponseDownload(String),

    #[error("Failed to upload a new challenge file to {}", _0)]
    FailedChallengeUpload(String),

    #[error("The coordinator failed to verify the uploaded challenge file at chunk {}", _0)]
    FailedVerification(u64),

    #[error("Failed to join the queue")]
    FailedToJoinQueue,

    #[error("Mismatched response hashes")]
    MismatchedResponseHashes,

    #[error("Next challenge file missing stored response hash")]
    MissingStoredResponseHash,
}

impl From<anyhow::Error> for VerifierError {
    fn from(error: anyhow::Error) -> Self {
        VerifierError::Crate("anyhow", format!("{:?}", error))
    }
}

impl From<CoordinatorError> for VerifierError {
    fn from(error: CoordinatorError) -> Self {
        VerifierError::CoordinatorError(error)
    }
}

impl From<hex::FromHexError> for VerifierError {
    fn from(error: hex::FromHexError) -> Self {
        VerifierError::Crate("hex", format!("{:?}", error))
    }
}

impl From<reqwest::Error> for VerifierError {
    fn from(error: reqwest::Error) -> Self {
        VerifierError::Crate("reqwest", format!("{:?}", error))
    }
}

impl From<std::io::Error> for VerifierError {
    fn from(error: std::io::Error) -> Self {
        VerifierError::Crate("std::io", format!("{:?}", error))
    }
}

impl From<serde_json::Error> for VerifierError {
    fn from(error: serde_json::Error) -> Self {
        VerifierError::Crate("serde_json", format!("{:?}", error))
    }
}

impl From<AddressError> for VerifierError {
    fn from(error: AddressError) -> Self {
        VerifierError::Crate("snarkos", format!("{:?}", error))
    }
}

impl From<ViewKeyError> for VerifierError {
    fn from(error: ViewKeyError) -> Self {
        VerifierError::Crate("snarkos", format!("{:?}", error))
    }
}
