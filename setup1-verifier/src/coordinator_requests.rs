use crate::{errors::VerifierError, objects::LockResponse, utils::AleoAuthentication, verifier::Verifier};
use snarkos_toolkit::account::{Address, ViewKey};

use reqwest::Client;
use std::str::FromStr;
use tracing::{debug, error, info};

impl Verifier {
    ///
    /// Attempts to join the coordinator queue
    ///
    /// On success, this function returns `true`.
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub(crate) async fn join_queue(&self) -> Result<bool, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;

        let view_key = ViewKey::from_str(&self.view_key)?;
        let aleo_address = Address::from_view_key(&view_key)?.to_string();

        let method = "post";
        let path = "/v1/queue/verifier/join";

        let authentication = AleoAuthentication::authenticate(&view_key, &method, &path)?;

        info!("Attempting to join as verifier join the queue as {}", aleo_address);

        match Client::new()
            .post(&format!("{}{}", &coordinator_api_url, &path))
            .header("Authorization", authentication.to_string())
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Verifier failed to join the queue");
                    return Err(VerifierError::FailedToJoinQueue);
                }

                // Parse the lock response
                let queue_response = serde_json::from_value::<bool>(response.json().await?)?;
                info!("{} joined the queue with status {}", aleo_address, queue_response);
                Ok(queue_response)
            }
            Err(_) => {
                error!("Request ({}) to join the queue failed", path);
                return Err(VerifierError::FailedRequest(
                    path.to_string(),
                    coordinator_api_url.to_string(),
                ));
            }
        }
    }

    ///
    /// Attempts to acquire the lock on a chunk.
    ///
    /// On success, this function returns the `LockResponse`.
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub(crate) async fn lock_chunk(&self) -> Result<LockResponse, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "post";
        let path = "/v1/verifier/try_lock";

        let view_key = ViewKey::from_str(&self.view_key)?;
        let authentication = AleoAuthentication::authenticate(&view_key, &method, &path)?;

        info!("Verifier attempting to lock a chunk");

        match Client::new()
            .post(&format!("{}{}", &coordinator_api_url, &path))
            .header("Authorization", authentication.to_string())
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Verifier failed to acquire a lock on a chunk");
                    return Err(VerifierError::FailedLock);
                }

                // Parse the lock response
                let json_response = response.json().await?;
                let lock_response = serde_json::from_value::<LockResponse>(json_response)?;
                debug!("Decoded verifier lock response: {:#?}", lock_response);
                info!("Verifier locked chunk {}", lock_response.chunk_id);

                Ok(lock_response)
            }
            Err(_) => {
                error!("Request ({}) to lock a chunk failed", path);
                return Err(VerifierError::FailedRequest(
                    path.to_string(),
                    coordinator_api_url.to_string(),
                ));
            }
        }
    }

    ///
    /// Attempts to run verification  on the coordinator in the
    /// current round for a given `chunk_id`.
    ///
    /// This assumes that a valid challenge file has already been
    /// uploaded to the coordinator at the given `verified_locator`.
    ///
    /// On success, the coordinator returns an { "status": "ok" }
    /// response.
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub(crate) async fn coordinator_verify_contribution(&self, chunk_id: u64) -> Result<String, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "post";
        let path = format!("/v1/verifier/try_verify/{}", chunk_id);

        let view_key = ViewKey::from_str(&self.view_key)?;

        info!("Verifier running verification of a contribution at chunk {}", chunk_id);

        let signature_path = format!("{}", path.replace("./", ""));
        let authentication = AleoAuthentication::authenticate(&view_key, &method, &signature_path)?;
        match Client::new()
            .post(&format!("{}{}", &coordinator_api_url, &path))
            .header("Authorization", authentication.to_string())
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Failed to verify the challenge at chunk {}", chunk_id);
                    return Err(VerifierError::FailedVerification(chunk_id));
                }

                info!("Verifier successfully verified a contribution on chunk {}", chunk_id);

                Ok(response.text().await?)
            }
            Err(_) => {
                error!("Request ({}) to verify a contribution failed.", path);
                return Err(VerifierError::FailedRequest(
                    path.to_string(),
                    coordinator_api_url.to_string(),
                ));
            }
        }
    }

    ///
    /// Attempts to download the unverified response file from the coordinator at
    /// a given `response_locator`
    ///
    /// On success, this function returns the full response file buffer.
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub(crate) async fn download_response_file(&self, response_locator: &str) -> Result<Vec<u8>, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "get";
        let path = format!("/v1/download/response/{}", response_locator);

        let view_key = ViewKey::from_str(&self.view_key)?;

        info!("Verifier downloading a response file at {} ", response_locator);

        let signature_path = format!("{}", path.replace("./", ""));
        let authentication = AleoAuthentication::authenticate(&view_key, &method, &signature_path)?;
        match Client::new()
            .get(&format!("{}{}", &coordinator_api_url, &path))
            .header("Authorization", authentication.to_string())
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Failed to download the response file {}", response_locator);
                    return Err(VerifierError::FailedResponseDownload(response_locator.to_string()));
                }

                info!("Verifier downloaded the response file {} ", response_locator);

                Ok(response.bytes().await?.to_vec())
            }
            Err(_) => {
                error!("Request ({}) to download a response file failed.", path);
                return Err(VerifierError::FailedRequest(
                    path.to_string(),
                    coordinator_api_url.to_string(),
                ));
            }
        }
    }

    ///
    /// Attempts to download the challenge file from the coordinator at
    /// a given `challenge_locator`
    ///
    /// On success, this function returns the full challenge file buffer.
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub(crate) async fn download_challenge_file(&self, challenge_locator: &str) -> Result<Vec<u8>, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "get";
        let path = format!("/v1/download/challenge/{}", challenge_locator);

        let view_key = ViewKey::from_str(&self.view_key)?;

        info!("Verifier downloading a challenge file at {} ", challenge_locator);

        let signature_path = format!("{}", path.replace("./", ""));
        let authentication = AleoAuthentication::authenticate(&view_key, &method, &signature_path)?;
        match Client::new()
            .get(&format!("{}{}", &coordinator_api_url, &path))
            .header("Authorization", authentication.to_string())
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Failed to download the challenge file {}", challenge_locator);
                    return Err(VerifierError::FailedChallengeDownload(challenge_locator.to_string()));
                }

                info!("Verifier downloaded the challenge file {} ", challenge_locator);

                Ok(response.bytes().await?.to_vec())
            }
            Err(_) => {
                error!("Request ({}) to download a challenge file failed.", path);
                return Err(VerifierError::FailedRequest(
                    path.to_string(),
                    coordinator_api_url.to_string(),
                ));
            }
        }
    }

    ///
    /// Attempts to upload the next challenge file to the coordinator
    /// at a given `next_challenge_locator`
    ///
    /// On success, this function returns an `ok` status
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub(crate) async fn upload_next_challenge_locator_file(
        &self,
        next_challenge_locator: &str,
        signature_and_next_challenge_file_bytes: Vec<u8>,
    ) -> Result<String, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "post";
        let path = format!("/v1/upload/challenge/{}", next_challenge_locator);

        let view_key = ViewKey::from_str(&self.view_key)?;

        let signature_path = format!("{}", path.replace("./", ""));
        let authentication = AleoAuthentication::authenticate(&view_key, &method, &signature_path)?;

        info!(
            "Verifier uploading a response with size {} to {} ",
            signature_and_next_challenge_file_bytes.len(),
            next_challenge_locator
        );

        match Client::new()
            .post(&format!("{}{}", &coordinator_api_url, &path))
            .header("Authorization", authentication.to_string())
            .header("Content-Type", "application/octet-stream")
            .body(signature_and_next_challenge_file_bytes)
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Failed to upload the new challenge file {}", next_challenge_locator);
                    return Err(VerifierError::FailedChallengeUpload(next_challenge_locator.to_string()));
                }

                info!("Verifier uploaded the next challenge file {} ", next_challenge_locator);

                Ok(response.text().await?)
            }
            Err(_) => {
                error!("Request ({}) to upload a new challenge file failed.", path);
                return Err(VerifierError::FailedRequest(
                    path.to_string(),
                    coordinator_api_url.to_string(),
                ));
            }
        }
    }
}
