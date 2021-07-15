use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SignedContributedData {
    pub data: Value,
    pub signature: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SignedVerifiedData {
    pub data: Value,
    pub signature: String,
}

#[derive(Serialize, Deserialize, Debug, Hash, Clone, PartialEq, Eq)]
pub struct LockResponse {
    /// The chunk id
    #[serde(alias = "chunkId")]
    pub chunk_id: u64,

    /// Indicator if the chunk was locked
    pub locked: bool,

    /// The participant id related to the lock
    #[serde(alias = "participantId")]
    pub participant_id: String,

    /// The locator of the previous response
    #[serde(alias = "previousResponseLocator")]
    pub previous_response_locator: String,

    /// The locator of the challenge file that the participant will download
    #[serde(alias = "challengeLocator")]
    pub challenge_locator: String,

    /// The locator where the participant will upload their completed contribution.
    #[serde(alias = "responseLocator")]
    pub response_locator: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ContributedData {
    pub challenge_hash: String,
    pub response_hash: String,
    pub contribution_duration: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct VerifiedData {
    pub challenge_hash: String,
    pub response_hash: String,
    pub new_challenge_hash: String,
    pub verification_duration: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ContributionUploadUrl {
    pub chunk_id: String,
    pub participant_id: String,
    pub write_url: String,
}
