use crate::{
    cli::commands::contribute::ContributeOptions,
    errors::ContributeError,
    objects::{AleoSetupKeys, LockResponse},
    tasks::Tasks,
    utils::{
        create_parameters_for_chunk,
        get_authorization_value,
        read_from_file,
        remove_file_if_exists,
        sign_contribution_state,
        UploadMode,
    },
};

#[cfg(feature = "azure")]
use crate::utils::upload_file_to_azure_async;

use age::DecryptError;
use phase1::helpers::converters::CurveKind;
use phase1_cli::contribute;
use phase1_coordinator::{
    environment::Environment,
    objects::{Chunk, Participant, Round},
};
use setup1_shared::structures::PublicSettings;
use setup_utils::calculate_hash;
use snarkos_toolkit::account::{Address, PrivateKey, ViewKey};
use zexe_algebra::{Bls12_377, PairingEngine, BW6_761};

use anyhow::{Context, Result};
use chrono::Duration;
use indicatif::{ProgressBar, ProgressStyle};
use lazy_static::lazy_static;
use panic_control::{spawn_quiet, ThreadResultExt};
use rand::{CryptoRng, Rng};
use secrecy::{ExposeSecret, SecretString, SecretVec};
use setup_utils::derive_rng_from_seed;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::File,
    io::{Read, Write},
    ops::Deref,
    path::Path,
    str::FromStr,
    sync::{Arc, RwLock},
};
use tokio::time::{sleep, Instant};
use tracing::{debug, error, info, warn};
use url::Url;

const CHALLENGE_FILENAME: &str = "challenge";
const CHALLENGE_HASH_FILENAME: &str = "challenge.hash";
const RESPONSE_FILENAME: &str = "response";
const RESPONSE_HASH_FILENAME: &str = "response.hash";

const DELAY_AFTER_ERROR_DURATION_SECS: i64 = 60;
const DELAY_WAIT_FOR_PIPELINE_SECS: i64 = 5;
const DELAY_POLL_CEREMONY_SECS: i64 = 5;
const HEARTBEAT_POLL_SECS: i64 = 30;

lazy_static! {
    static ref PIPELINE: RwLock<HashMap<PipelineLane, VecDeque<LockResponse>>> = {
        let mut map = HashMap::new();
        map.insert(PipelineLane::Download, VecDeque::new());
        map.insert(PipelineLane::Process, VecDeque::new());
        map.insert(PipelineLane::Upload, VecDeque::new());
        RwLock::new(map)
    };
    static ref TASKS: RwLock<Tasks> = RwLock::new(Tasks::default());
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PipelineLane {
    Download,
    Process,
    Upload,
}

impl std::fmt::Display for PipelineLane {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone)]
pub struct Contribute {
    pub server_url: Url,
    /// Public key id for this contributor: e.g.
    /// `aleo1h7pwa3dh2egahqj7yvq7f7e533lr0ueysaxde2ktmtu2pxdjvqfqsj607a`
    pub participant_id: String,
    pub private_key: String,
    seed: Arc<SecretVec<u8>>,
    pub upload_mode: UploadMode,
    pub environment: Environment,

    pub challenge_filename: String,
    pub challenge_hash_filename: String,
    pub response_filename: String,
    pub response_hash_filename: String,

    pub max_in_download_lane: usize,
    pub max_in_process_lane: usize,
    pub max_in_upload_lane: usize,
    pub disable_pipelining: bool,

    pub current_task: Option<LockResponse>,
}

impl Contribute {
    pub fn new(
        opts: &ContributeOptions,
        environment: &Environment,
        private_key: &[u8],
        seed: Arc<SecretVec<u8>>,
    ) -> Result<Self> {
        let private_key = PrivateKey::from_str(std::str::from_utf8(&private_key)?)?;

        // TODO (raychu86): Pass in pipelining options from the CLI.

        let contribute = Self {
            server_url: opts.api_url.clone(),
            participant_id: Address::from(&private_key)?.to_string(),
            private_key: private_key.to_string(),
            seed,
            upload_mode: opts.upload_mode.clone(),
            environment: environment.clone(),

            challenge_filename: CHALLENGE_FILENAME.to_string(),
            challenge_hash_filename: CHALLENGE_HASH_FILENAME.to_string(),
            response_filename: RESPONSE_FILENAME.to_string(),
            response_hash_filename: RESPONSE_HASH_FILENAME.to_string(),

            max_in_download_lane: 1,
            max_in_process_lane: 1,
            max_in_upload_lane: 1,
            disable_pipelining: false,

            current_task: None,
        };

        Ok(contribute)
    }

    pub fn clone_with_new_filenames(&self, index: usize) -> Self {
        let mut cloned = self.clone();
        cloned.challenge_filename = format!("{}_{}", self.challenge_filename, index);
        cloned.challenge_hash_filename = format!("{}_{}", self.challenge_hash_filename, index);
        cloned.response_filename = format!("{}_{}", self.response_filename, index);
        cloned.response_hash_filename = format!("{}_{}", self.response_hash_filename, index);
        cloned
    }

    async fn run_and_catch_errors<E: PairingEngine>(&self) -> Result<()> {
        let delay_poll_ceremony_duration = Duration::seconds(DELAY_POLL_CEREMONY_SECS).to_std()?;
        let heartbeat_poll_duration = Duration::seconds(HEARTBEAT_POLL_SECS).to_std()?;

        let progress_bar = ProgressBar::new(0);
        let progress_style =
            ProgressStyle::default_bar().template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}");
        progress_bar.enable_steady_tick(1000);
        progress_bar.set_style(progress_style);
        progress_bar.set_message("Getting initial data from the server...");

        // Attempt to join the queue.
        match self.join_queue(&mut rand::thread_rng()).await {
            Ok(joined) => info!("Attempting to join the queue with response: {}", joined),
            Err(err) => warn!("Failed to join the queue (error {})", err),
        }

        // Get total number of tokio tasks to generate.
        let n_concurrent_tasks = match self.disable_pipelining {
            true => 1,
            false => self.max_in_download_lane + self.max_in_process_lane + self.max_in_upload_lane,
        };

        // Run status bar updater.
        let updater = self.clone();
        tokio::task::spawn(async move {
            loop {
                match updater.status_updater(progress_bar.clone()).await {
                    Ok(_) => {}
                    Err(e) => {
                        warn!("Got error from updater: {}", e);
                        progress_bar.set_message(&format!("Could not update status: {}", e.to_string().trim()));
                    }
                }
                sleep(delay_poll_ceremony_duration).await;
            }
        });

        let mut futures = vec![];

        for i in 0..n_concurrent_tasks {
            let delay_duration = Duration::seconds(DELAY_AFTER_ERROR_DURATION_SECS).to_std()?;
            let mut cloned_contribute = self.clone_with_new_filenames(i);

            let join_handle = tokio::task::spawn(async move {
                // Run contributor loop.
                loop {
                    let result = cloned_contribute.run::<E>().await;
                    match result {
                        Ok(_) => {
                            info!(
                                "Successfully contributed, thank you for participation! Waiting to see if you're still needed... Don't turn this off!"
                            );
                        }
                        Err(err) => {
                            println!("Got error from run: {}, retrying...", err);
                            tracing::error!("Error from contribution run: {}", err);

                            if let Some(lock_response) = cloned_contribute.current_task.as_ref() {
                                tracing::warn!("Retrying task {:?}", lock_response);
                                cloned_contribute
                                    .remove_task_from_lane_if_exists(&PipelineLane::Download, &lock_response)
                                    .expect("Should have removed task from download lane");

                                cloned_contribute
                                    .remove_task_from_lane_if_exists(&PipelineLane::Upload, &lock_response)
                                    .expect("Should have removed task from upload lane");

                                cloned_contribute
                                    .remove_task_from_lane_if_exists(&PipelineLane::Process, &lock_response)
                                    .expect("Should have removed task from process lane");

                                cloned_contribute.add_task_to_queue(lock_response.clone());
                            }
                        }
                    }

                    sleep(delay_duration).await;
                }
            });
            futures.push(join_handle);
            sleep(Duration::seconds(DELAY_WAIT_FOR_PIPELINE_SECS).to_std()?).await;
        }

        let cloned = self.clone();
        // NOTE: Attempted to use a task, however this did not work as
        // epected. It seems likely there is some blocking code in one
        // of the other tasks.
        std::thread::spawn(move || {
            let auth_rng = &mut rand::rngs::OsRng;
            let runtime = tokio::runtime::Runtime::new().unwrap();
            loop {
                tracing::info!("Performing heartbeat.");
                if let Err(error) = runtime.block_on(cloned.heartbeat(auth_rng)) {
                    tracing::error!("Error performing heartbeat: {}", error);
                }
                std::thread::sleep(heartbeat_poll_duration);
            }
        });

        futures::future::try_join_all(futures).await?;

        Ok(())
    }

    ///
    /// The function attempts to fetch a task from the queue. If there are
    /// no tasks in the queue or the task is already complete, then return `None`
    ///
    #[inline]
    pub async fn get_task_from_queue(&self, ceremony: &Round) -> Result<Option<LockResponse>> {
        // Acquire the tasks lock.
        let mut tasks = TASKS.write().expect("Should have opened queue for writing");

        let task = tasks.next_task();

        // Return `None` if the participant doesn't hold the lock on the chunk.
        if let Some(task) = &task {
            if !self.is_pending_task(&task, ceremony)? {
                return Ok(None);
            }
        }

        Ok(task)
    }

    ///
    /// Add a task to the queue.
    ///
    #[inline]
    pub fn add_task_to_queue(&self, task: LockResponse) {
        // Acquire the tasks lock.
        let mut tasks = TASKS.write().expect("Should have opened queue for writing");

        tasks.add_task(task);
    }

    ///
    /// Remove a task from the queue.
    ///
    #[inline]
    pub async fn remove_task_from_queue(&self, task: &LockResponse) -> Result<()> {
        // Acquire the tasks lock.
        let mut tasks = TASKS.write().expect("Should have opened queue for writing");

        // Remove the given task from `tasks`.
        tasks.remove_task(task);

        Ok(())
    }

    async fn wait_for_available_spot_in_lane(&self, lane: &PipelineLane) -> Result<()> {
        let max_in_lane = match *lane {
            PipelineLane::Download => self.max_in_download_lane,
            PipelineLane::Process => self.max_in_process_lane,
            PipelineLane::Upload => self.max_in_upload_lane,
        };
        loop {
            {
                let pipeline = PIPELINE.read().expect("Should have opened pipeline for reading");
                if pipeline
                    .get(lane)
                    .ok_or(ContributeError::LaneWasNullError(lane.to_string()))?
                    .len()
                    < max_in_lane
                {
                    return Ok(());
                }
            }
            sleep(Duration::seconds(DELAY_WAIT_FOR_PIPELINE_SECS).to_std()?).await;
        }
    }

    fn move_task_from_lane_to_lane(&self, from: &PipelineLane, to: &PipelineLane, task: &LockResponse) -> Result<bool> {
        let max_in_lane = match *to {
            PipelineLane::Download => self.max_in_download_lane,
            PipelineLane::Process => self.max_in_process_lane,
            PipelineLane::Upload => self.max_in_upload_lane,
        };
        {
            let mut pipeline = PIPELINE.write().expect("Should have opened pipeline for writing");

            // Check that the `to` pipeline has enough space.
            {
                let to_list = pipeline
                    .get_mut(to)
                    .ok_or(ContributeError::LaneWasNullError(to.to_string()))?;

                if to_list.len() >= max_in_lane {
                    return Ok(false);
                }
            }

            // Check that the tasks exists in the `from` pipeline.
            {
                let from_list = pipeline
                    .get_mut(from)
                    .ok_or(ContributeError::LaneWasNullError(from.to_string()))?;
                if !from_list.contains(&task) {
                    return Err(ContributeError::LaneDidNotContainChunkWithIDError(
                        from.to_string(),
                        task.chunk_id.to_string(),
                    )
                    .into());
                }
                from_list.retain(|c| c != task);
            }

            // Add the task to the `to` pipeline.
            {
                let to_list = pipeline
                    .get_mut(to)
                    .ok_or(ContributeError::LaneWasNullError(to.to_string()))?;

                if to_list.contains(&task) {
                    return Err(ContributeError::LaneAlreadyContainsChunkWithIDError(
                        to.to_string(),
                        task.chunk_id.to_string(),
                    )
                    .into());
                }
                to_list.push_back(task.clone());
            }
            debug!(
                "Chunk ID {} moved successfully from lane {} to lane {}. Current pipeline is: {:#?}\n",
                task.chunk_id,
                from,
                to,
                pipeline.deref()
            );
            Ok(true)
        }
    }

    async fn wait_and_move_task_from_lane_to_lane(
        &self,
        from: &PipelineLane,
        to: &PipelineLane,
        task: &LockResponse,
    ) -> Result<()> {
        loop {
            match self.move_task_from_lane_to_lane(from, to, task)? {
                true => return Ok(()),
                false => sleep(Duration::seconds(DELAY_WAIT_FOR_PIPELINE_SECS).to_std()?).await,
            }
        }
    }

    async fn wait_and_add_task_to_download_lane(&self, task: &LockResponse) -> Result<()> {
        loop {
            match self.add_task_to_download_lane(task)? {
                true => return Ok(()),
                false => sleep(Duration::seconds(DELAY_WAIT_FOR_PIPELINE_SECS).to_std()?).await,
            }
        }
    }

    fn add_task_to_download_lane(&self, task: &LockResponse) -> Result<bool> {
        let lane = &PipelineLane::Download;
        let mut pipeline = PIPELINE.write().expect("Should have opened pipeline for writing");

        let lane_list = pipeline
            .get_mut(lane)
            .ok_or(ContributeError::LaneWasNullError(lane.to_string()))?;

        if lane_list.contains(&task) || lane_list.len() >= self.max_in_download_lane {
            return Ok(false);
        }
        lane_list.push_back(task.clone());
        debug!(
            "Chunk ID {} added successfully to lane {}. Current pipeline is: {:#?}",
            task.chunk_id,
            lane,
            pipeline.deref()
        );
        Ok(true)
    }

    fn remove_task_from_lane_if_exists(&self, lane: &PipelineLane, task: &LockResponse) -> Result<bool> {
        let mut pipeline = PIPELINE.write().expect("Should have opened pipeline for writing");

        let lane_list = pipeline
            .get_mut(lane)
            .ok_or(ContributeError::LaneWasNullError(lane.to_string()))?;
        if !lane_list.contains(&task) {
            return Ok(false);
        }
        lane_list.retain(|c| c != task);
        debug!(
            "Chunk ID {} removed successfully from lane {}... Current pipeline is: {:#?}\n",
            task.chunk_id,
            lane,
            pipeline.deref()
        );
        Ok(true)
    }

    async fn run<E: PairingEngine>(&mut self) -> Result<()> {
        loop {
            self.wait_for_available_spot_in_lane(&PipelineLane::Download).await?;
            let auth_rng = &mut rand::rngs::OsRng;

            let ceremony = self.get_ceremony().await?;
            let non_contributed_chunks = self.get_non_contributed_chunks(&ceremony);
            let incomplete_chunks = self.get_non_contributed_and_available_chunks(&ceremony);

            // Check if the contributor is finished or needs to wait for an available lock
            if incomplete_chunks.len() == 0 {
                if non_contributed_chunks.len() == 0 {
                    println!("You have completed your contribution! Thank you!");
                    remove_file_if_exists(&self.challenge_filename)?;
                    remove_file_if_exists(&self.challenge_hash_filename)?;
                    remove_file_if_exists(&self.response_filename)?;
                    remove_file_if_exists(&self.response_hash_filename)?;
                    return Ok(());
                } else {
                    tokio::time::sleep(Duration::seconds(DELAY_POLL_CEREMONY_SECS).to_std()?).await;
                    continue;
                }
            }

            // Attempt to fetch a task from the queue or lock a chunk from the coordinator.
            let lock_response = match self.get_task_from_queue(&ceremony).await? {
                Some(lock_response) => lock_response,
                None => self.lock_chunk(auth_rng).await?,
            };

            // Add the lock response to the download lane
            self.wait_and_add_task_to_download_lane(&lock_response).await?;

            self.current_task = Some(lock_response.clone());
            let chunk_id = lock_response.chunk_id;

            remove_file_if_exists(&self.challenge_filename)?;
            remove_file_if_exists(&self.challenge_hash_filename)?;
            let download_url = &lock_response.challenge_locator;
            self.download_challenge(&download_url, &self.challenge_filename, auth_rng)
                .await?;

            // Wait for the process pipeline to open up
            self.wait_and_move_task_from_lane_to_lane(&PipelineLane::Download, &PipelineLane::Process, &lock_response)
                .await?;

            let exposed_seed = self.seed.expose_secret();
            let seeded_rng = derive_rng_from_seed(&exposed_seed[..]);
            let start = Instant::now();
            remove_file_if_exists(&self.response_filename)?;
            remove_file_if_exists(&self.response_hash_filename)?;

            // Fetch parameters required for contribution.
            let parameters = create_parameters_for_chunk::<E>(&self.environment, chunk_id as usize)?;
            let compressed_input = self.environment.compressed_inputs();
            let compressed_output = self.environment.compressed_outputs();
            let check_input_correctness = self.environment.check_input_for_correctness();

            let challenge_filename = self.challenge_filename.to_string();
            let response_filename = self.response_filename.to_string();

            // Run the contribution.
            let h = spawn_quiet(move || {
                contribute(
                    compressed_input,
                    &challenge_filename,
                    compressed_output,
                    &response_filename,
                    check_input_correctness,
                    &parameters,
                    seeded_rng,
                );
            });
            let result = h.join();
            if !result.is_ok() {
                if let Some(panic_value) = result.panic_value_as_str() {
                    error!("Contribute failed: {}", panic_value);
                }
                return Err(ContributeError::FailedRunningContributeError.into());
            }
            let duration = start.elapsed();

            info!("Completed chunk {} in {} seconds", chunk_id, duration.as_secs());

            // Read the challenge and response files.
            let challenge_file = read_from_file(&self.challenge_filename)?;
            let response_file = read_from_file(&self.response_filename)?;

            // Hash the challenge and response files.
            let challenge_hash = calculate_hash(&challenge_file).to_vec();
            let response_hash = calculate_hash(&response_file).to_vec();

            // Sign the contribution state.
            let view_key = ViewKey::from(&PrivateKey::from_str(&self.private_key)?)?;
            let signed_contribution_state =
                sign_contribution_state(&view_key.to_string(), &challenge_hash, &response_hash, None, auth_rng)?;

            // Construct the serialized response
            let mut file = File::open(&self.response_filename)?;
            let mut response_file = Vec::new();
            file.read_to_end(&mut response_file)?;

            // Concatenate the signed contribution data and next challenge file.
            let verifier_flag = vec![0];
            let signature_bytes = hex::decode(signed_contribution_state.get_signature())?;

            let signature_and_response_file_bytes = [
                verifier_flag,
                signature_bytes,
                challenge_hash,
                response_hash,
                response_file,
            ]
            .concat();

            // Wait for the Upload pipeline to open up
            self.wait_and_move_task_from_lane_to_lane(&PipelineLane::Process, &PipelineLane::Upload, &lock_response)
                .await?;

            let upload_url = &lock_response.response_locator;

            // Upload the response and contribution file signature to the coordinator.
            match self.upload_mode {
                UploadMode::Auto => {
                    if upload_url.contains("blob.core.windows.net") {
                        #[cfg(feature = "azure")]
                        upload_file_to_azure_async(&self.response_filename, &upload_url).await?;
                        #[cfg(not(feature = "azure"))]
                        self.upload_response(&upload_url, signature_and_response_file_bytes, auth_rng)
                            .await?;
                    } else {
                        self.upload_response(&upload_url, signature_and_response_file_bytes, auth_rng)
                            .await?;
                    }
                }
                #[cfg(feature = "azure")]
                UploadMode::Azure => upload_file_to_azure_async(&self.response_filename, &upload_url).await?,
                UploadMode::Direct => {
                    self.upload_response(&upload_url, signature_and_response_file_bytes, auth_rng)
                        .await?
                }
            }

            // Attempt to perform the contribution with the uploaded response file at the `upload_url`.
            self.notify_contribution(chunk_id, serde_json::json!({}), auth_rng)
                .await?;

            // Remove the task from the upload pipeline.
            self.remove_task_from_lane_if_exists(&PipelineLane::Upload, &lock_response)?;

            // Remove the task from the queue
            self.remove_task_from_queue(&lock_response).await?;
        }
    }

    async fn status_updater(&self, progress_bar: ProgressBar) -> Result<()> {
        let ceremony = self.get_ceremony().await?;
        let number_of_chunks = ceremony.chunks().len();

        progress_bar.set_length(number_of_chunks as u64);
        let non_contributed_chunks = self.get_non_contributed_chunks(&ceremony);

        let participant_locked_chunks = self.get_participant_locked_chunks_display(&ceremony)?;
        if participant_locked_chunks.len() > 0 {
            progress_bar.set_message(&format!(
                "Contributing to {} {}...",
                if participant_locked_chunks.len() > 1 {
                    "chunks"
                } else {
                    "chunk"
                },
                participant_locked_chunks.join(", "),
            ));
            progress_bar.set_position((number_of_chunks - non_contributed_chunks.len()) as u64);
        } else if non_contributed_chunks.len() == 0 {
            let completed_message = "Successfully contributed, thank you for participation! Waiting to see if you're still needed... Don't turn this off!";

            info!(completed_message);

            progress_bar.set_position(number_of_chunks as u64);
            progress_bar.set_message(completed_message);
        } else {
            progress_bar.set_position((number_of_chunks - non_contributed_chunks.len()) as u64);
            progress_bar.set_message(&format!("Waiting for an available chunk...",));
        }

        Ok(())
    }

    fn get_participant_locked_chunks_display(&self, ceremony: &Round) -> Result<Vec<String>> {
        let mut chunk_ids = vec![];

        for chunk in ceremony.chunks().iter() {
            let chunk_id = chunk.chunk_id();
            let chunk_lock_holder = chunk.lock_holder();

            if chunk_lock_holder.is_some()
                && chunk_lock_holder
                    .as_ref()
                    .map(|c| c.to_string().split('.').collect::<Vec<_>>()[0].to_string())
                    == Some(self.participant_id.clone())
            {
                chunk_ids.push(format!("{}", chunk_id));
            }
        }

        Ok(chunk_ids)
    }

    /// Get references to the chunks which have been completely
    /// verified, and do not yet contain a contribution from this
    /// contributor.
    fn get_non_contributed_chunks<'r>(&self, ceremony: &'r Round) -> Vec<&'r Chunk> {
        ceremony
            .chunks()
            .iter()
            .filter_map(|chunk| {
                if !chunk_all_verified(chunk) {
                    return None;
                }

                if !contributor_ids_in_chunk(chunk).contains(&self.participant_id) {
                    Some(chunk)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns `true` if the participant currently holds the lock on the chunk.
    fn is_pending_task(&self, task: &LockResponse, ceremony: &Round) -> Result<bool> {
        if let Some(chunk) = ceremony.chunks().get(task.chunk_id as usize) {
            if chunk.is_locked_by(&Participant::Contributor(self.participant_id.to_string())) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Get references to the unlocked chunks which have been
    /// completely verified, and do not yet contain a contribution
    /// from this contributor.
    fn get_non_contributed_and_available_chunks<'r>(&self, ceremony: &'r Round) -> Vec<&'r Chunk> {
        self.get_non_contributed_chunks(ceremony)
            .into_iter()
            .filter(|chunk| chunk.lock_holder().is_none())
            .collect()
    }

    async fn join_queue<R: Rng + CryptoRng>(&self, auth_rng: &mut R) -> Result<bool> {
        let join_queue_path = "/v1/queue/contributor/join";
        let join_queue_path_url = self.server_url.join(&join_queue_path)?;
        let client = reqwest::Client::new();
        let authorization = get_authorization_value(&self.private_key, "POST", &join_queue_path, auth_rng)?;
        let response = client
            .post(join_queue_path_url.as_str())
            .header(http::header::AUTHORIZATION, authorization)
            .header(http::header::CONTENT_LENGTH, 0)
            .send()
            .await?
            .error_for_status()?;

        let data = response.bytes().await?;
        let joined = serde_json::from_slice::<bool>(&*data)?;

        Ok(joined)
    }

    async fn get_ceremony(&self) -> Result<Round> {
        let ceremony_url = self.server_url.join("/v1/round/current")?;
        let response = reqwest::get(ceremony_url.as_str()).await?.error_for_status()?;

        let data = response.bytes().await?;
        let ceremony: Round = serde_json::from_slice(&*data)?;

        Ok(ceremony)
    }

    async fn lock_chunk<R: Rng + CryptoRng>(&self, auth_rng: &mut R) -> Result<LockResponse> {
        let lock_path = "/v1/contributor/try_lock";
        let lock_chunk_url = self.server_url.join(&lock_path)?;
        let client = reqwest::Client::new();
        let authorization = get_authorization_value(&self.private_key, "POST", &lock_path, auth_rng)?;
        let response = client
            .post(lock_chunk_url.as_str())
            .header(http::header::AUTHORIZATION, authorization)
            .header(http::header::CONTENT_LENGTH, 0)
            .send()
            .await?
            .error_for_status()?;

        let data = response.bytes().await?;
        let lock_response = serde_json::from_slice::<LockResponse>(&*data)?;

        Ok(lock_response)
    }

    async fn heartbeat<R: Rng + CryptoRng>(&self, auth_rng: &mut R) -> Result<()> {
        let heartbeat_path = "/v1/contributor/heartbeat";
        let url = self.server_url.join(&heartbeat_path)?;
        let client = reqwest::Client::new();
        let authorization = get_authorization_value(&self.private_key, "POST", &heartbeat_path, auth_rng)?;
        let response = client
            .post(url.as_str())
            .header(http::header::AUTHORIZATION, authorization)
            .header(http::header::CONTENT_LENGTH, 0)
            .send()
            .await?
            .error_for_status()?;

        response.error_for_status()?;

        Ok(())
    }

    async fn download_challenge<R: Rng + CryptoRng>(
        &self,
        challenge_locator: &str,
        file_path: &str,
        auth_rng: &mut R,
    ) -> Result<()> {
        let sanitized_challenge_locator = challenge_locator.replace("./", "");

        let download_path = format!("/v1/download/challenge/{}", sanitized_challenge_locator);
        let download_path_url = self.server_url.join(&download_path)?;
        let client = reqwest::Client::new();
        let authorization = get_authorization_value(&self.private_key, "GET", &download_path, auth_rng)?;
        let mut response = client
            .get(download_path_url.as_str())
            .header(http::header::AUTHORIZATION, authorization)
            .send()
            .await?
            .error_for_status()?;

        remove_file_if_exists(file_path)?;
        let mut out = File::create(file_path)?;
        while let Some(chunk) = response.chunk().await? {
            out.write_all(&chunk)?;
        }

        Ok(())
    }

    async fn upload_response<R: Rng + CryptoRng>(
        &self,
        response_locator: &str,
        contents: Vec<u8>,
        auth_rng: &mut R,
    ) -> Result<()> {
        let sanitized_response_locator = response_locator.replace("./", "");

        let upload_path = format!("/v1/upload/response/{}", sanitized_response_locator);
        let upload_path_url = self.server_url.join(&upload_path)?;
        let client = reqwest::Client::new();
        let authorization = get_authorization_value(&self.private_key, "POST", &upload_path, auth_rng)?;
        client
            .post(upload_path_url.as_str())
            .header(http::header::AUTHORIZATION, authorization)
            .header(http::header::CONTENT_TYPE, "application/octet-stream")
            .header(http::header::CONTENT_LENGTH, contents.len())
            .body(contents)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    async fn notify_contribution<R: Rng + CryptoRng>(
        &self,
        chunk_id: u64,
        body: serde_json::Value,
        auth_rng: &mut R,
    ) -> Result<()> {
        let contribute_path = format!("/v1/contributor/try_contribute/{}", chunk_id);
        let contribute_chunk_url = self.server_url.join(&contribute_path)?;
        let client = reqwest::Client::new();
        let authorization = get_authorization_value(&self.private_key, "POST", &contribute_path, auth_rng)?;
        let bytes = serde_json::to_vec(&body)?;
        client
            .post(contribute_chunk_url.as_str())
            .header(http::header::AUTHORIZATION, authorization)
            .header(http::header::CONTENT_LENGTH, bytes.len())
            .body(bytes)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

fn decrypt(passphrase: &SecretString, encrypted: &str) -> Result<Vec<u8>> {
    let decoded = SecretVec::new(hex::decode(encrypted)?);
    let decryptor = age::Decryptor::new(decoded.expose_secret().as_slice())?;
    let mut output = vec![];
    if let age::Decryptor::Passphrase(decryptor) = decryptor {
        let mut reader = decryptor
            .decrypt(passphrase, None)
            .map_err(|decrypt_error: DecryptError| match decrypt_error {
                DecryptError::ExcessiveWork { .. } => anyhow::Error::from(decrypt_error)
                    .context("Perhaps you have forgotten to compile in release mode, or your hardware is too slow?"),
                _ => anyhow::Error::from(decrypt_error),
            })
            .context("Unable to create decrypt reader")?;

        reader.read_to_end(&mut output)?;
    } else {
        return Err(ContributeError::UnsupportedDecryptorError.into());
    }

    Ok(output)
}

/// Decrypts and reads the private key from the specified `keys_path`,
/// decrypting using the specified `passphrase`. If `passphrase` is
/// `None`, will request passphrase via pinentry or tty.
fn read_keys<P: AsRef<Path>>(keys_path: P, passphrase: Option<SecretString>) -> Result<(SecretVec<u8>, SecretVec<u8>)> {
    let mut contents = String::new();
    std::fs::File::open(keys_path)?.read_to_string(&mut contents)?;
    let keys: AleoSetupKeys = serde_json::from_str(&contents)?;
    let passphrase = if let Some(passphrase) = passphrase {
        passphrase
    } else {
        age::cli_common::read_secret("Enter your Aleo setup passphrase", "Passphrase", None)
            .map_err(|_| ContributeError::CouldNotReadPassphraseError)?
    };

    let aleo_seed = SecretVec::new(decrypt(&passphrase, &keys.encrypted_seed)?);
    let aleo_private_key = SecretVec::new(decrypt(&passphrase, &keys.encrypted_private_key)?);

    Ok((aleo_seed, aleo_private_key))
}

async fn request_coordinator_public_settings(coordinator_url: &Url) -> anyhow::Result<PublicSettings> {
    let settings_endpoint_url = coordinator_url.join("/v1/coordinator/settings")?;
    let client = reqwest::Client::new();
    let bytes = client
        .post(settings_endpoint_url)
        .header(http::header::CONTENT_LENGTH, 0)
        .send()
        .await?
        .bytes()
        .await?;
    PublicSettings::decode(&bytes.to_vec())
        .map_err(|e| anyhow::anyhow!("Error decoding coordinator PublicSettings: {}", e))
}

pub async fn contribute_subcommand(opts: &ContributeOptions) -> anyhow::Result<()> {
    let public_settings = request_coordinator_public_settings(&opts.api_url)
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch the coordinator public settings");
            e
        })
        .with_context(|| format!("Failed to fetch the coordinator public settings"))?;

    start_contributor(opts, &public_settings).await
}

async fn start_contributor(opts: &ContributeOptions, public_settings: &PublicSettings) -> Result<()> {
    let environment = crate::utils::environment_by_setup_kind(&public_settings.setup);

    // Initialize tracing logger. Stored to `aleo-setup.log`.
    let appender = tracing_appender::rolling::never(".", "aleo-setup.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(appender);
    tracing_subscriber::fmt().with_writer(non_blocking).init();

    // Read the stored contribution seed and Aleo private key.
    let (seed, private_key) =
        read_keys(&opts.keys_path, opts.passphrase.clone()).expect("Unable to load Aleo setup keys");

    let curve_kind = environment.parameters().curve();

    // Initialize the contributor.
    let contribute = Contribute::new(opts, &environment, private_key.expose_secret(), Arc::new(seed))
        .expect("Unable to initialize a contributor");

    if public_settings.check_reliability {
        tracing::info!("Checking reliability score before joining the queue");
        crate::reliability::check(&opts.api_url, &contribute.private_key).await?;
        tracing::info!("Reliability checks completed successfully");
    }

    // Run the contributor.
    let contribution = match curve_kind {
        CurveKind::Bls12_377 => contribute.run_and_catch_errors::<Bls12_377>().await,
        CurveKind::BW6 => contribute.run_and_catch_errors::<BW6_761>().await,
    };

    match contribution {
        Err(e) => info!("Error occurred during contribution: {}", e.to_string()),
        _ => {}
    }

    Ok(())
}

/// Check that every contribution in the chunk has been verified.
fn chunk_all_verified(chunk: &Chunk) -> bool {
    chunk.get_contributions().iter().all(|(_, c)| c.is_verified())
}

/// Obtain a set with the id of every contributor in the chunk who's
/// contribution has been verified.
fn contributor_ids_in_chunk(chunk: &Chunk) -> HashSet<String> {
    chunk
        .get_contributions()
        .iter()
        .filter(|(_, c)| c.is_verified())
        .filter_map(|(_, c)| {
            c.get_contributor()
                .as_ref()
                .map(|c| c.to_string().split('.').collect::<Vec<_>>()[0].to_string())
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::{chunk_all_verified, contributor_ids_in_chunk};
    use phase1_coordinator::objects::{Chunk, Participant};

    #[test]
    fn test_participant_ids_in_chunk() {
        let verifier = Participant::Verifier(
            "aleo1yphn5z63acdpelyk2c3xmf6fuzpxymusp3c260ne6q0rrhrtdufqenlwqg.verifier".to_string(),
        );
        let contributor1 = Participant::Contributor(
            "aleo1fa6q44gpw0vkpx7xsfhgadz48swtg3wqf98w0xkrydwtvs62q5zsqyv5d7.contributor".to_string(),
        );
        let contributor2 = Participant::Contributor(
            "aleo1h7pwa3dh2egahqj7yvq7f7e533lr0ueysaxde2ktmtu2pxdjvqfqsj607a.contributor".to_string(),
        );

        let mut chunk = Chunk::new(0, verifier.clone(), String::new().into(), String::new().into()).unwrap();

        chunk.acquire_lock(contributor1.clone(), 3).unwrap();
        chunk
            .add_contribution(1, &contributor1, String::new().into(), String::new().into())
            .unwrap();
        assert!(!chunk_all_verified(&chunk));
        chunk.acquire_lock(verifier.clone(), 3).unwrap();
        chunk
            .verify_contribution(1, verifier.clone(), String::new().into(), String::new().into())
            .unwrap();
        assert!(chunk_all_verified(&chunk));

        chunk.acquire_lock(contributor2.clone(), 3).unwrap();
        chunk
            .add_contribution(2, &contributor2, String::new().into(), String::new().into())
            .unwrap();
        assert!(!chunk_all_verified(&chunk));
        chunk.acquire_lock(verifier.clone(), 3).unwrap();
        chunk
            .verify_contribution(2, verifier.clone(), String::new().into(), String::new().into())
            .unwrap();
        assert!(chunk_all_verified(&chunk));

        let ids = contributor_ids_in_chunk(&chunk);
        assert_eq!(2, ids.len());
        assert!(ids.contains(&contributor1.to_string().replace(".contributor", "")));
        assert!(ids.contains(&contributor2.to_string().replace(".contributor", "")));
    }
}
