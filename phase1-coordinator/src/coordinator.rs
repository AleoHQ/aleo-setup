//! This module contains the central piece of this crate, the
//! [Coordinator]. The coordinator's state is stored in a
//! [CoordinatorState] object.

use crate::{
    authentication::Signature,
    commands::{Aggregation, Initialization},
    coordinator_state::{CoordinatorState, DropParticipant, ParticipantInfo, RoundMetrics},
    environment::{Deployment, Environment},
    objects::{participant::*, task::TaskInitializationError, ContributionFileSignature, LockedLocators, Round, Task},
    storage::{ContributionLocator, ContributionSignatureLocator, Locator, LocatorPath, Object, Storage, StorageLock},
};
use setup_utils::calculate_hash;

use chrono::{DateTime, Utc};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::{
    fmt,
    sync::{Arc, RwLock},
};
use tracing::*;

#[derive(Debug)]
pub enum CoordinatorError {
    AggregateContributionFileSizeMismatch,
    ChallengeHashSizeInvalid,
    ChunkAlreadyComplete,
    ChunkAlreadyVerified,
    ChunkIdAlreadyAdded,
    ChunkIdInvalid,
    ChunkIdMismatch,
    ChunkIdMissing,
    ChunkLockAlreadyAcquired,
    ChunkLockLimitReached,
    ChunkMissing,
    ChunkMissingVerification,
    ChunkCannotLockZeroContributions { chunk_id: u64 },
    ChunkNotLockedOrByWrongParticipant,
    ComputationFailed,
    CompressedContributionHashingUnsupported,
    ContributorPendingTasksCannotBeEmpty(Participant),
    ContributionAlreadyAssignedVerifiedLocator,
    ContributionAlreadyAssignedVerifier,
    ContributionAlreadyVerified,
    ContributionFailed,
    ContributionFileSignatureLocatorAlreadyExists,
    ContributionFileSizeMismatch,
    ContributionHashMismatch,
    ContributionIdIsNonzero,
    ContributionIdMismatch,
    ContributionIdMustBeNonzero,
    ContributionLocatorAlreadyExists,
    ContributionLocatorIncorrect,
    ContributionLocatorMissing,
    ContributionMissing,
    ContributionMissingVerification,
    ContributionMissingVerifiedLocator,
    ContributionMissingVerifier,
    ContributionShouldNotExist,
    ContributionSignatureFileSizeMismatch,
    ContributionSignatureSizeMismatch,
    ContributionsComplete,
    ContributorAlreadyContributed,
    ContributorSignatureInvalid,
    ContributorsMissing,
    CoordinatorContributorMissing,
    CoordinatorStateNotInitialized,
    CurrentRoundAggregating,
    CurrentRoundAggregated,
    CurrentRoundFinished,
    CurrentRoundNotAggregated,
    CurrentRoundNotFinished,
    DropParticipantFailed,
    ExpectedContributor,
    ExpectedVerifier,
    Error(anyhow::Error),
    InitializationFailed,
    InitializationTranscriptsDiffer,
    Integer(std::num::ParseIntError),
    IOError(std::io::Error),
    Hex(hex::FromHexError),
    JsonError(serde_json::Error),
    JustificationInvalid,
    LocatorDeserializationFailed,
    LocatorFileAlreadyExists,
    LocatorFileAlreadyExistsAndOpen,
    LocatorFileAlreadyOpen,
    LocatorFileMissing,
    LocatorFileNotOpen,
    LocatorFileShouldBeOpen,
    LocatorSerializationFailed,
    NextChallengeHashAlreadyExists,
    NextChallengeHashSizeInvalid,
    NextChallengeHashMissing,
    NextRoundAlreadyInPrecommit,
    NextRoundShouldBeEmpty,
    NumberOfChunksInvalid,
    NumberOfContributionsDiffer,
    ParticipantAlreadyAdded,
    ParticipantAlreadyAddedChunk,
    ParticipantAlreadyBanned,
    ParticipantAlreadyDropped,
    ParticipantAlreadyFinished,
    ParticipantAlreadyFinishedChunk { chunk_id: u64 },
    ParticipantAlreadyFinishedTask(Task),
    ParticipantAlreadyHasLockedChunk,
    ParticipantAlreadyHasLockedChunks,
    ParticipantAlreadyPrecommitted,
    ParticipantAlreadyStarted,
    ParticipantAlreadyWorkingOnChunk { chunk_id: u64 },
    ParticipantBanned,
    ParticipantDidNotDoWork,
    ParticipantDidntLockChunkId,
    ParticipantHasAssignedTasks,
    ParticipantHasLockedMaximumChunks,
    ParticipantHasNotStarted,
    ParticipantHasNoRemainingTasks,
    ParticipantHasRemainingTasks,
    ParticipantInCurrentRoundCannotJoinQueue,
    ParticipantLockedChunkWithManyContributions,
    ParticipantMissing,
    ParticipantMissingDisposingTask,
    ParticipantMissingPendingTask { pending_task: Task },
    ParticipantNotFound(Participant),
    ParticipantNotReady,
    ParticipantRoundHeightInvalid,
    ParticipantRoundHeightMissing,
    ParticipantShouldHavePendingTasks,
    ParticipantShouldNotBeFinished,
    ParticipantStillHasLock,
    ParticipantStillHasLocks,
    ParticipantStillHasTaskAsAssigned,
    ParticipantStillHasTaskAsPending,
    ParticipantUnauthorized,
    ParticipantUnauthorizedForChunkId { chunk_id: u64 },
    ParticipantWasDropped,
    PendingTasksMustContainResponseTask { response_task: Task },
    Phase1Setup(setup_utils::Error),
    QueueIsEmpty,
    QueueWaitTimeIncomplete,
    ResponseHashSizeInvalid,
    RoundAggregationFailed,
    RoundAlreadyInitialized,
    RoundAlreadyAggregated,
    RoundCommitFailedOrCorrupted,
    RoundContributorMissing,
    RoundContributorsMissing,
    RoundContributorsNotUnique,
    RoundDirectoryMissing,
    RoundDoesNotExist,
    RoundFileMissing,
    RoundFileSizeMismatch,
    RoundHeightIsZero,
    RoundHeightMismatch,
    RoundHeightNotSet,
    RoundLocatorAlreadyExists,
    RoundLocatorMissing,
    RoundNotAggregated,
    RoundNotComplete,
    RoundNotReady,
    RoundNumberOfContributorsUnauthorized,
    RoundNumberOfVerifiersUnauthorized,
    RoundShouldNotExist,
    RoundStateMissing,
    RoundUpdateCorruptedStateOfContributors,
    RoundUpdateCorruptedStateOfVerifiers,
    RoundVerifiersMissing,
    RoundVerifiersNotUnique,
    SignatureSchemeIsInsecure,
    StateLockFailed,
    StorageCopyFailed,
    StorageFailed,
    StorageInitializationFailed,
    StorageLocatorAlreadyExists,
    StorageLocatorAlreadyExistsAndOpen,
    StorageLocatorFormatIncorrect,
    StorageLocatorMissing,
    StorageLocatorNotOpen,
    StorageLockFailed,
    StorageReaderFailed,
    StorageSizeLookupFailed,
    StorageUpdateFailed,
    TaskInitializationFailed(TaskInitializationError),
    PreviousContributionMissing { current_task: Task },
    TryFromSliceError(std::array::TryFromSliceError),
    UnauthorizedChunkContributor,
    UnauthorizedChunkVerifier,
    VerificationFailed,
    VerificationOnContributionIdZero,
    VerifierMissing,
    VerifierSignatureInvalid,
    VerifiersMissing,
}

impl From<TaskInitializationError> for CoordinatorError {
    fn from(error: TaskInitializationError) -> Self {
        Self::TaskInitializationFailed(error)
    }
}

impl From<anyhow::Error> for CoordinatorError {
    fn from(error: anyhow::Error) -> Self {
        CoordinatorError::Error(error)
    }
}

impl From<hex::FromHexError> for CoordinatorError {
    fn from(error: hex::FromHexError) -> Self {
        CoordinatorError::Hex(error)
    }
}

impl From<serde_json::Error> for CoordinatorError {
    fn from(error: serde_json::Error) -> Self {
        CoordinatorError::JsonError(error)
    }
}

impl From<setup_utils::Error> for CoordinatorError {
    fn from(error: setup_utils::Error) -> Self {
        CoordinatorError::Phase1Setup(error)
    }
}

impl From<std::array::TryFromSliceError> for CoordinatorError {
    fn from(error: std::array::TryFromSliceError) -> Self {
        CoordinatorError::TryFromSliceError(error)
    }
}

impl From<std::io::Error> for CoordinatorError {
    fn from(error: std::io::Error) -> Self {
        CoordinatorError::IOError(error)
    }
}

impl From<std::num::ParseIntError> for CoordinatorError {
    fn from(error: std::num::ParseIntError) -> Self {
        CoordinatorError::Integer(error)
    }
}

impl fmt::Display for CoordinatorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        error!("{}", self);
        write!(f, "{:?}", self)
    }
}

impl From<CoordinatorError> for anyhow::Error {
    fn from(error: CoordinatorError) -> Self {
        error!("{}", error);
        match error {
            CoordinatorError::Error(anyhow_error) => anyhow_error,
            _ => Self::msg(error.to_string()),
        }
    }
}

/// A trait for providing a source of time to the coordinator, used
/// for mocking system time during testing.
pub trait TimeSource: Send + Sync {
    /// Provide the current time now in the UTC timezone
    fn utc_now(&self) -> DateTime<Utc>;
}

// Private tuple field to force use of constructor.
/// A [TimeSource] implementation that fetches the current system time
/// using [Utc].
pub(crate) struct SystemTimeSource(());

impl SystemTimeSource {
    pub fn new() -> Self {
        Self(())
    }
}

impl TimeSource for SystemTimeSource {
    fn utc_now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A time source to use for testing, allows the current time to be
/// set manually.
pub struct MockTimeSource {
    time: RwLock<DateTime<Utc>>,
}

impl MockTimeSource {
    pub fn new(time: DateTime<Utc>) -> Self {
        Self {
            time: RwLock::new(time),
        }
    }

    pub fn set_time(&self, time: DateTime<Utc>) {
        *self.time.write().expect("Unable to lock to write time") = time;
    }

    pub fn time(&self) -> DateTime<Utc> {
        *self.time.read().expect("Unable to obtain lock to read time")
    }

    pub fn update<F: Fn(DateTime<Utc>) -> DateTime<Utc>>(&self, f: F) {
        self.set_time(f(self.time()))
    }
}

impl TimeSource for MockTimeSource {
    fn utc_now(&self) -> DateTime<Utc> {
        *self.time.read().expect("Unable to obtain lock to read time")
    }
}

/// A core structure for operating the Phase 1 ceremony. This struct
/// is designed to be [Send] + [Sync]. The state of the ceremony is
/// stored in a [CoordinatorState] object.
#[derive(Clone)]
pub struct Coordinator {
    /// The parameters and settings of this coordinator.
    environment: Environment,
    /// The signature scheme for contributors & verifiers with this coordinator.
    signature: Arc<Box<dyn Signature>>,
    /// The storage of contributions and rounds for this coordinator.
    storage: Arc<RwLock<Box<dyn Storage>>>,
    /// The current round and participant state.
    state: Arc<RwLock<CoordinatorState>>,
    /// The source of time, allows mocking system time for testing.
    time: Arc<dyn TimeSource>,
    /// Callback to call after aggregation is done
    aggregation_callback: Arc<dyn Fn(Vec<Participant>) -> () + Send + Sync>,
}

impl Coordinator {
    ///
    /// Creates a new instance of the `Coordinator`, for a given environment.
    ///
    /// The coordinator loads and instantiates an internal instance of storage.
    /// All subsequent interactions with the coordinator are directly from storage.
    ///
    /// The coordinator is forbidden from caching state about any round.
    ///
    #[inline]
    pub fn new(environment: Environment, signature: Box<dyn Signature>) -> Result<Self, CoordinatorError> {
        Self::new_with_time(environment, signature, Arc::new(SystemTimeSource::new()))
    }

    /// Constructor that allows mocking time for testing.
    pub fn new_with_time(
        environment: Environment,
        signature: Box<dyn Signature>,
        time: Arc<dyn TimeSource>,
    ) -> Result<Self, CoordinatorError> {
        // Load an instance of storage.
        let storage = environment.storage()?;
        // Load an instance of coordinator state.
        let state = match storage.get(&Locator::CoordinatorState)? {
            Object::CoordinatorState(state) => state,
            _ => return Err(CoordinatorError::StorageFailed),
        };

        Ok(Self {
            environment: environment.clone(),
            signature: Arc::new(signature),
            storage: Arc::new(RwLock::new(storage)),
            state: Arc::new(RwLock::new(state)),
            aggregation_callback: Arc::new(|_| ()),
            time,
        })
    }

    ///
    /// Set a callback which will be called after the round is aggregated.
    /// Current round finished contributors will be passed to a callback
    /// as an argument
    ///
    pub fn set_aggregation_callback(&mut self, callback: Arc<dyn Fn(Vec<Participant>) -> () + Send + Sync>) {
        self.aggregation_callback = callback;
    }

    ///
    /// Runs a set of operations to initialize state and start the coordinator.
    ///
    #[inline]
    pub fn initialize(&self) -> Result<(), CoordinatorError> {
        // Check if the deployment is in production, that the signature scheme is secure.
        if *self.environment.deployment() == Deployment::Production && !self.signature.is_secure() {
            return Err(CoordinatorError::SignatureSchemeIsInsecure);
        }

        info!("Coordinator is booting up");
        info!("{:#?}", self.environment.parameters());

        // Ensure the ceremony is initialized, if it has not started yet.
        {
            // Acquire the storage write lock.
            let mut storage = StorageLock::Write(self.storage.write().unwrap());

            // Check if the ceremony has been initialized yet.
            if Self::load_current_round_height(&storage).is_err() {
                info!("Initializing ceremony");
                let round_height = self.run_initialization(&mut storage, self.time.utc_now())?;
                info!("Initialized ceremony");

                // Acquire the state write lock.
                let mut state = self.state.write().unwrap();

                // Initialize the coordinator state to round 0.
                state.initialize(round_height);
                state.save(&mut storage)?;
            }
        }

        // Fetch the current round height from storage. As a sanity check,
        // this call will fail if the ceremony was not initialized.
        let current_round_height = self.current_round_height()?;

        info!("Current round height is {}", current_round_height);
        info!("{}", serde_json::to_string_pretty(&self.current_round()?)?);
        info!("Coordinator has booted up");

        Ok(())
    }

    ///
    /// Runs a set of operations to update the coordinator state to reflect
    /// newly finished, dropped, or banned participants.
    ///
    #[inline]
    pub fn update(&self) -> Result<(), CoordinatorError> {
        // Process ceremony updates for the current round and queue.
        let (is_current_round_finished, is_current_round_aggregated) = {
            // Acquire the storage write lock.
            let mut storage = StorageLock::Write(self.storage.write().unwrap());

            // Acquire the state write lock.
            let mut state = self.state.write().unwrap();

            info!("\n{}", state.status_report(self.time.as_ref()));

            // Update the metrics for the current round and participants.
            state.update_round_metrics();
            state.save(&mut storage)?;

            // Update the state of the queue.
            state.update_queue()?;
            state.save(&mut storage)?;

            // Update the state of current round contributors.
            state.update_current_contributors(self.time.as_ref())?;
            state.save(&mut storage)?;

            // Update the state of current round verifiers.
            state.update_current_verifiers(self.time.as_ref())?;
            state.save(&mut storage)?;

            // Drop disconnected participants from the current round.
            for drop in state.update_dropped_participants(self.time.as_ref())? {
                // Update the round to reflect the coordinator state changes.
                self.drop_participant_from_storage(&mut storage, &drop)?;
            }
            state.save(&mut storage)?;

            // Ban any participants who meet the coordinator criteria.
            state.update_banned_participants()?;
            state.save(&mut storage)?;

            // Check if the current round is finished and if the current round is aggregated.
            (state.is_current_round_finished(), state.is_current_round_aggregated())
        };

        // Try aggregating the current round if the current round is finished,
        // and has not yet been aggregated.
        let (is_current_round_aggregated, is_precommit_next_round_ready) = {
            // Check if the coordinator should aggregate, and attempt aggregation.
            if is_current_round_finished && !is_current_round_aggregated {
                // Aggregate the current round.
                self.try_aggregate()?;

                // Acquire the storage write lock.
                let mut storage = StorageLock::Write(self.storage.write().unwrap());

                // Acquire the state write lock.
                let mut state = self.state.write().unwrap();

                // Update the metrics for the current round and participants.
                state.update_round_metrics();
                state.save(&mut storage)?;

                match state.current_round_finished_contributors() {
                    Ok(contributors) => {
                        (self.aggregation_callback)(contributors);
                    }
                    Err(e) => {
                        tracing::error!("Failed to get current round finished contributors: {}", e);
                    }
                }
            }

            // Acquire the state read lock.
            let state = self.state.read().unwrap();

            // Check if the current round is aggregated, and if the precommit for
            // the next round is ready.
            (
                state.is_current_round_aggregated(),
                state.is_precommit_next_round_ready(self.time.as_ref()),
            )
        };

        // Check if the manual lock for transitioning to the next round is enabled.
        {
            // Acquire the state read lock.
            let state = self.state.read().unwrap();

            // Check if the manual lock is enabled.
            if state.is_manual_lock_enabled() {
                info!("Manual lock is enabled");
                return Ok(());
            }
        }

        // Try advancing to the next round if the current round is finished,
        // the current round has been aggregated, and the precommit for
        // the next round is now ready.
        if is_current_round_finished && is_current_round_aggregated && is_precommit_next_round_ready {
            // Backup a copy of the current coordinator.

            // Fetch the current time.
            let started_at = self.time.utc_now();

            // Attempt to advance to the next round.
            let next_round_height = self.try_advance(started_at)?;

            info!("Advanced ceremony to round {}", next_round_height);
        }

        Ok(())
    }

    ///
    /// Initializes a listener to handle the shutdown signal.
    ///
    #[inline]
    pub fn shutdown_listener(self) -> anyhow::Result<()> {
        ctrlc::set_handler(move || {
            warn!("\n\nATTENTION - Coordinator is shutting down...\n");

            // Acquire the storage lock.
            let mut storage = StorageLock::Write(self.storage.write().unwrap());
            trace!("Coordinator has acquired the storage lock");

            // Acquire the coordinator state lock.
            let state = self.state.write().unwrap();
            trace!("Coordinator has acquired the state lock");

            // Save the coordinator state to storage.
            state.save(&mut storage).unwrap();
            debug!("Coordinator has safely shutdown storage");

            // Print the final coordinator state.
            let final_state = serde_json::to_string_pretty(&*state).unwrap();
            info!("\n\nCoordinator State at Shutdown\n\n{}\n", final_state);

            info!("\n\nCoordinator has safely shutdown.\n\nGoodbye.\n");
            std::process::exit(0);
        })?;

        Ok(())
    }

    ///
    /// Returns `true` if the given participant is a contributor in the queue.
    ///
    #[inline]
    pub fn is_queue_contributor(&self, participant: &Participant) -> bool {
        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Fetch the state of the queue contributor.
        state.is_queue_contributor(&participant)
    }

    ///
    /// Returns `true` if the given participant is a verifier in the queue.
    ///
    #[inline]
    pub fn is_queue_verifier(&self, participant: &Participant) -> bool {
        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Fetch the state of the queue verifier.
        state.is_queue_verifier(&participant)
    }

    ///
    /// Returns the total number of contributors currently in the queue.
    ///  
    #[inline]
    pub fn number_of_queue_contributors(&self) -> usize {
        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Fetch the number of queued contributors.
        state.number_of_queue_contributors()
    }

    ///
    /// Returns the total number of verifiers currently in the queue.
    ///
    #[inline]
    pub fn number_of_queue_verifiers(&self) -> usize {
        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Fetch the number of queued verifiers.
        state.number_of_queue_verifiers()
    }

    ///
    /// Returns a list of the contributors currently in the queue.
    ///
    #[inline]
    pub fn queue_contributors(&self) -> Vec<(Participant, (u8, Option<u64>))> {
        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Fetch the queue contributors.
        state.queue_contributors()
    }

    ///
    /// Returns a list of the verifiers currently in the queue.
    ///
    #[inline]
    pub fn queue_verifiers(&self) -> Vec<(Participant, (u8, Option<u64>))> {
        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Fetch the queue verifiers.
        state.queue_verifiers()
    }

    ///
    /// Returns a list of the contributors currently in the round.
    ///
    #[inline]
    pub fn current_contributors(&self) -> Vec<(Participant, ParticipantInfo)> {
        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Fetch the current contributors.
        state.current_contributors()
    }

    ///
    /// Returns a list of the verifiers currently in the round.
    ///
    #[inline]
    pub fn current_verifiers(&self) -> Vec<(Participant, ParticipantInfo)> {
        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Fetch the current verifiers.
        state.current_verifiers()
    }

    ///
    /// Returns a list of participants that were dropped from the current round.
    ///
    #[inline]
    pub fn dropped_participants(&self) -> Vec<ParticipantInfo> {
        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        state.dropped_participants()
    }

    ///
    /// Returns the metrics for the current round and current round participants.
    ///
    #[inline]
    pub fn current_round_metrics(&self) -> Option<RoundMetrics> {
        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Fetch the current round metrics.
        state.current_round_metrics()
    }

    ///
    /// Adds the given participant to the queue if they are permitted to participate.
    ///
    #[inline]
    pub fn add_to_queue(&self, participant: Participant, reliability_score: u8) -> Result<(), CoordinatorError> {
        // Acquire the storage write lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Acquire the state write lock.
        let mut state = self.state.write().unwrap();

        // Attempt to add the participant to the next round.
        state.add_to_queue(participant, reliability_score)?;

        // Save the coordinator state in storage.
        state.save(&mut storage)?;

        Ok(())
    }

    ///
    /// Removes the given participant from the queue if they are in the queue.
    ///
    #[inline]
    pub fn remove_from_queue(&self, participant: &Participant) -> Result<(), CoordinatorError> {
        // Acquire the storage write lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Acquire the state write lock.
        let mut state = self.state.write().unwrap();

        // Attempt to remove the participant from the next round.
        state.remove_from_queue(participant)?;

        // Save the coordinator state in storage.
        state.save(&mut storage)?;

        Ok(())
    }

    ///
    /// Drops the given participant from the ceremony.
    ///
    #[tracing::instrument(
        skip(self, participant),
        fields(participant = %participant)
    )]
    pub fn drop_participant(&self, participant: &Participant) -> Result<Vec<LocatorPath>, CoordinatorError> {
        // Acquire the storage write lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Acquire a state write lock.
        let mut state = self.state.write().unwrap();

        // Drop the participant from the ceremony.
        let drop = state.drop_participant(participant, self.time.as_ref())?;

        // Update the round to reflect the coordinator state change.
        let locations = self.drop_participant_from_storage(&mut storage, &drop)?;

        // Save the coordinator state in storage.
        state.save(&mut storage)?;

        Ok(locations)
    }

    ///
    /// Bans the given participant from the ceremony.
    ///
    #[inline]
    pub fn ban_participant(&self, participant: &Participant) -> Result<Vec<LocatorPath>, CoordinatorError> {
        // Acquire the storage write lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Acquire a state write lock.
        let mut state = self.state.write().unwrap();

        // Ban the participant from the ceremony.
        let drop = state.ban_participant(participant, self.time.as_ref())?;

        // Update the round on disk to reflect the coordinator state change.
        let locations = self.drop_participant_from_storage(&mut storage, &drop)?;

        // Save the coordinator state in storage.
        state.save(&mut storage)?;

        Ok(locations)
    }

    ///
    /// Unbans the given participant from joining the queue.
    ///
    #[inline]
    pub fn unban_participant(&self, participant: &Participant) -> Result<(), CoordinatorError> {
        // Acquire the storage write lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Acquire a state write lock.
        let mut state = self.state.write().unwrap();

        // Unban the participant from the ceremony.
        state.unban_participant(participant);

        // Save the coordinator state in storage.
        state.save(&mut storage)?;

        Ok(())
    }

    ///
    /// Returns `true` if the manual lock for transitioning to the next round is enabled.
    ///
    #[inline]
    pub fn is_manual_lock_enabled(&self) -> bool {
        // Acquire the state read lock.
        let state = self.state.read().unwrap();
        // Fetch the state of the manual lock.
        state.is_manual_lock_enabled()
    }

    ///
    /// Sets the manual lock for transitioning to the next round to `true`.
    ///
    #[inline]
    pub fn enable_manual_lock(&mut self) -> Result<(), CoordinatorError> {
        // Acquire the storage write lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Acquire the state write lock.
        let mut state = self.state.write().unwrap();

        // Sets the manual lock to `true`.
        state.enable_manual_lock();

        // Save the coordinator state in storage.
        state.save(&mut storage)?;

        Ok(())
    }

    ///
    /// Sets the manual lock for transitioning to the next round to `false`.
    ///
    #[inline]
    pub fn disable_manual_lock(&mut self) -> Result<(), CoordinatorError> {
        // Acquire the storage write lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Acquire the state write lock.
        let mut state = self.state.write().unwrap();

        // Sets the manual lock to `false`.
        state.disable_manual_lock();

        // Save the coordinator state in storage.
        state.save(&mut storage)?;

        Ok(())
    }

    ///
    /// Returns `true` if the given participant is authorized as a
    /// contributor and listed in the contributor IDs for this round.
    ///
    /// If the participant is not a contributor, or if there are
    /// no prior rounds, returns `false`.
    ///
    #[inline]
    pub fn is_current_contributor(&self, participant: &Participant) -> bool {
        // Acquire a storage read lock.
        let storage = StorageLock::Read(self.storage.read().unwrap());

        // Fetch the current round from storage.
        let round = match Self::load_current_round(&storage) {
            // Case 1 - This is a typical round of the ceremony.
            Ok(round) => round,
            // Case 2 - The ceremony has not started or storage has failed.
            _ => return false,
        };

        // Check that the participant is a contributor for the given round height.
        if !round.is_contributor(participant) {
            return false;
        }

        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Check that the participant is a current contributor.
        state.is_current_contributor(participant)
    }

    ///
    /// Returns `true` if the given participant is authorized as a
    /// verifier and listed in the verifier IDs for this round.
    ///
    /// If the participant is not a verifier, or if there are
    /// no prior rounds, returns `false`.
    ///
    #[inline]
    pub fn is_current_verifier(&self, participant: &Participant) -> bool {
        // Acquire a storage read lock.
        let storage = StorageLock::Read(self.storage.read().unwrap());

        // Fetch the current round from storage.
        let round = match Self::load_current_round(&storage) {
            // Case 1 - This is a typical round of the ceremony.
            Ok(round) => round,
            // Case 2 - The ceremony has not started or storage has failed.
            _ => return false,
        };

        // Release the storage read lock.
        drop(storage);

        // Check that the participant is a verifier for the given round height.
        if !round.is_verifier(participant) {
            return false;
        }

        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Check that the participant is a current verifier.
        state.is_current_verifier(participant)
    }

    ///
    /// Returns `true` if the given participant has finished contributing
    /// in the current round.
    ///
    #[inline]
    pub fn is_finished_contributor(&self, participant: &Participant) -> bool {
        // Check that the participant is a current contributor.
        if !self.is_current_contributor(participant) {
            return false;
        }

        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Fetch the state of the current contributor.
        state.is_finished_contributor(&participant)
    }

    ///
    /// Returns `true` if the given participant has finished verifying
    /// in the current round.
    ///
    #[inline]
    pub fn is_finished_verifier(&self, participant: &Participant) -> bool {
        // Check that the participant is a current verifier.
        if !self.is_current_verifier(participant) {
            return false;
        }

        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Fetch the state of the current verifier.
        state.is_finished_verifier(&participant)
    }

    ///
    /// Returns `true` if the given participant is a contributor managed
    /// by the coordinator.
    ///
    #[inline]
    pub fn is_coordinator_contributor(&self, participant: &Participant) -> bool {
        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Check that the participant is a coordinator contributor.
        state.is_coordinator_contributor(&participant)
    }

    ///
    /// Returns `true` if the given participant is a verifier managed
    /// by the coordinator.
    ///
    #[inline]
    pub fn is_coordinator_verifier(&self, participant: &Participant) -> bool {
        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Check that the participant is a coordinator verifier.
        state.is_coordinator_verifier(&participant)
    }

    ///
    /// Returns the current round height of the ceremony from storage,
    /// irrespective of the stage of its completion.
    ///
    /// When loading the current round height from storage, this function
    /// checks that the corresponding round is in storage. Note that it
    /// only checks for the existence of a round value and does not
    /// check for its correctness.
    ///
    #[inline]
    pub fn current_round_height(&self) -> Result<u64, CoordinatorError> {
        trace!("Fetching the current round height from storage");

        // Acquire the storage lock.
        let storage = StorageLock::Read(self.storage.read().unwrap());

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;

        // Fetch the current round from storage.
        match storage.exists(&Locator::RoundState {
            round_height: current_round_height,
        }) {
            // Case 1 - This is a typical round of the ceremony.
            true => Ok(current_round_height),
            // Case 2 - Storage failed to locate the current round.
            false => Err(CoordinatorError::StorageFailed),
        }
    }

    ///
    /// Returns the current round state of the ceremony from storage,
    /// irrespective of the stage of its completion.
    ///
    /// If there are no prior rounds in storage, returns `CoordinatorError`.
    ///
    /// When loading the current round from storage, this function
    /// checks that the current round height matches the height
    /// set in the returned `Round` instance.
    ///
    #[inline]
    pub fn current_round(&self) -> Result<Round, CoordinatorError> {
        trace!("Fetching the current round from storage");

        // Acquire a storage read lock.
        let storage = StorageLock::Read(self.storage.read().unwrap());

        // Fetch the current round from storage.
        Self::load_current_round(&storage)
    }

    ///
    /// Returns the round state corresponding to the given height from storage.
    ///
    /// If there are no prior rounds, returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn get_round(&self, round_height: u64) -> Result<Round, CoordinatorError> {
        // Acquire the storage lock.
        let storage = StorageLock::Read(self.storage.read().unwrap());

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;

        // Check that the given round height is valid.
        match round_height <= current_round_height {
            // Fetch the round corresponding to the given round height from storage.
            true => Ok(serde_json::from_slice(
                &*storage.reader(&Locator::RoundState { round_height })?.as_ref(),
            )?),
            // The given round height does not exist.
            false => Err(CoordinatorError::RoundDoesNotExist),
        }
    }

    /// Lets the coordinator know that the participant is still alive
    /// and participating (or waiting to participate) in the ceremony.
    pub fn heartbeat(&self, participant: &Participant) -> Result<(), CoordinatorError> {
        let mut state = self.state.write().expect("unable to obtain write lock on state");
        state.heartbeat(participant, self.time.as_ref())
    }

    ///
    /// Attempts to acquire the lock to a chunk for the given participant.
    ///
    /// On failure, this function returns a `CoordinatorError`.
    ///
    #[tracing::instrument(skip(self, participant), err)]
    pub fn try_lock(&self, participant: &Participant) -> Result<(u64, LockedLocators), CoordinatorError> {
        // Acquire the state write lock.
        let mut state = self.state.write().unwrap();

        // Check that the participant is in the current round, and has not been dropped or finished.
        if !state.is_current_contributor(participant) && !state.is_current_verifier(participant) {
            return Err(CoordinatorError::ParticipantUnauthorized);
        }

        // Check that the current round is not yet finished.
        if state.is_current_round_finished() {
            return Err(CoordinatorError::CurrentRoundFinished);
        }

        // Check that the current round is not yet aggregating.
        if state.is_current_round_aggregating() {
            return Err(CoordinatorError::CurrentRoundAggregating);
        }

        // Check that the current round is not yet aggregated.
        if state.is_current_round_aggregated() {
            return Err(CoordinatorError::CurrentRoundAggregated);
        }

        // Acquire the storage write lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Attempt to fetch the next chunk ID and contribution ID for the given participant.
        let current_task = state.fetch_task(participant, self.time.as_ref())?;
        trace!("Fetched task {} for {}", current_task, participant);

        let round = Self::load_current_round(&storage)?;
        let chunk = round.chunk(current_task.chunk_id())?;
        if current_task.contribution_id() > (chunk.current_contribution_id() + 1) {
            state.rollback_pending_task(participant, current_task, &*self.time)?;
            return Err(CoordinatorError::PreviousContributionMissing { current_task });
        }

        debug!("Locking chunk {} for {}", current_task.chunk_id(), participant);
        match self.try_lock_chunk(&mut storage, current_task.chunk_id(), participant) {
            // Case 1 - Participant acquired lock, return the locator.
            Ok(locked_locators) => {
                trace!("Incrementing the number of locks held by {}", participant);
                state.acquired_lock(participant, current_task.chunk_id(), self.time.as_ref())?;

                // Save the coordinator state in storage.
                state.save(&mut storage)?;

                info!("Acquired lock on chunk {} for {}", current_task.chunk_id(), participant);
                Ok((current_task.chunk_id(), locked_locators))
            }
            // Case 2 - Participant failed to acquire the lock, put the chunk ID back.
            Err(error) => {
                info!("Failed to acquire lock for {}", participant);

                trace!("Adding task {} back to assigned tasks", current_task);
                state.rollback_pending_task(participant, current_task, self.time.as_ref())?;

                // Save the coordinator state in storage.
                state.save(&mut storage)?;

                error!("{}", error);
                return Err(error);
            }
        }
    }

    ///
    /// Attempts to add a contribution for the given chunk ID from the given participant.
    ///
    /// This function constructs the expected response locator for the given participant
    /// and chunk ID, and checks that the participant has uploaded the response file and
    /// contribution file signature prior to adding the unverified contribution to the
    /// round state.
    ///
    /// On success, this function releases the lock from the contributor and returns
    /// the response file locator.
    ///
    /// On failure, it returns a `CoordinatorError`.
    ///
    #[tracing::instrument(
        level = "error",
        skip(self, participant, chunk_id),
        fields(chunk = chunk_id)
    )]
    pub fn try_contribute(
        &self,
        participant: &Participant,
        chunk_id: u64,
    ) -> Result<ContributionLocator, CoordinatorError> {
        // Check that the participant is a contributor.
        if !participant.is_contributor() {
            return Err(CoordinatorError::ExpectedContributor);
        }

        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Acquire the state write lock.
        let mut state = self.state.write().unwrap();

        // Check that the participant is in the current round, and has not been dropped or finished.
        if !state.is_current_contributor(participant) {
            return Err(CoordinatorError::ParticipantUnauthorized);
        }

        // Check that the current round is not yet finished.
        if state.is_current_round_finished() {
            return Err(CoordinatorError::CurrentRoundFinished);
        }

        // Check that the current round is not yet aggregating.
        if state.is_current_round_aggregating() {
            return Err(CoordinatorError::CurrentRoundAggregating);
        }

        // Check that the current round is not yet aggregated.
        if state.is_current_round_aggregated() {
            return Err(CoordinatorError::CurrentRoundAggregated);
        }

        // Acquire the storage write lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Fetch the current round height from storage.
        let round_height = Self::load_current_round_height(&storage)?;
        trace!("Current round height in storage is {}", round_height);

        // Check if the participant should dispose the response being contributed.
        if let Some(task) = state.lookup_disposing_task(participant, chunk_id)?.cloned() {
            let contribution_id = task.contribution_id();

            // Move the task to the disposed tasks of the contributor.
            state.disposed_task(participant, &task, self.time.as_ref())?;

            // Remove the response file from storage.
            let response = ContributionLocator::new(round_height, chunk_id, contribution_id, false);
            storage.remove(&Locator::ContributionFile(response.clone()))?;

            // Save the coordinator state in storage.
            state.save(&mut storage)?;

            debug!("Removing lock for disposed task {} {}", chunk_id, contribution_id);

            // Fetch the current round from storage.
            let mut round = Self::load_current_round(&storage)?;

            // TODO (raychu86): Move this unsafe call out of `try_contribute`.
            // Release the lock on this chunk from the contributor.
            round.chunk_mut(chunk_id)?.set_lock_holder_unsafe(None);

            // Save the updated round to storage.
            storage.update(
                &Locator::RoundState {
                    round_height: round.round_height(),
                },
                Object::RoundState(round),
            )?;

            return Ok(response);
        }

        // Check if the participant has this chunk ID in a pending task.
        if let Some(task) = state.lookup_pending_task(participant, chunk_id)? {
            debug!("Adding contribution for chunk");

            match self.add_contribution(&mut storage, chunk_id, participant) {
                // Case 1 - Participant added contribution, return the response file locator.
                Ok((locator, contribution_id)) => {
                    trace!("Release the lock on chunk");
                    let completed_task = Task::new(chunk_id, contribution_id);
                    state.completed_task(participant, completed_task, self.time.as_ref())?;

                    // Save the coordinator state in storage.
                    state.save(&mut storage)?;

                    info!("Added contribution");
                    return Ok(locator);
                }
                // Case 2 - Participant failed to add their contribution, remove the contribution file.
                Err(error) => {
                    info!("Failed to add a contribution and removing the contribution file");
                    // Remove the invalid response file from storage.
                    let response = Locator::ContributionFile(ContributionLocator::new(
                        round_height,
                        chunk_id,
                        task.contribution_id(),
                        false,
                    ));
                    storage.remove(&response)?;

                    error!("{}", error);
                    return Err(error);
                }
            }
        }

        Err(CoordinatorError::ContributionFailed)
    }

    ///
    /// Attempts to add a verification for the given chunk ID from the given participant.
    ///
    /// This function constructs the expected next challenge locator for the given participant
    /// and chunk ID, and checks that the participant has uploaded the next challenge file and
    /// contribution file signature prior to adding the verified contribution to the round state.
    ///
    /// On success, this function releases the lock from the verifier.
    ///
    /// On failure, it returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn try_verify(&self, participant: &Participant, chunk_id: u64) -> Result<(), CoordinatorError> {
        // Check that the participant is a verifier.
        if !participant.is_verifier() {
            return Err(CoordinatorError::ExpectedVerifier);
        }

        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Acquire the state write lock.
        let mut state = self.state.write().unwrap();

        // Check that the participant is in the current round, and has not been dropped or finished.
        if !state.is_current_verifier(participant) {
            return Err(CoordinatorError::ParticipantUnauthorized);
        }

        // Check that the current round is not yet finished.
        if state.is_current_round_finished() {
            return Err(CoordinatorError::CurrentRoundFinished);
        }

        // Check that the current round is not yet aggregating.
        if state.is_current_round_aggregating() {
            return Err(CoordinatorError::CurrentRoundAggregating);
        }

        // Check that the current round is not yet aggregated.
        if state.is_current_round_aggregated() {
            return Err(CoordinatorError::CurrentRoundAggregated);
        }

        // Acquire the storage write lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Check if the participant should dispose the response being contributed.
        if let Some(task) = state.lookup_disposing_task(participant, chunk_id)?.cloned() {
            let contribution_id = task.contribution_id();

            // Move the task to the disposed tasks of the verifier.
            state.disposed_task(participant, &task, self.time.as_ref())?;

            // Save the coordinator state in storage.
            state.save(&mut storage)?;

            debug!(
                "Removing lock for verifier {} disposed task {} {}",
                participant, chunk_id, contribution_id
            );

            // Fetch the current round from storage.
            let mut round = Self::load_current_round(&storage)?;

            // TODO (raychu86): Move this unsafe call out of `try_verify`.
            // Release the lock on this chunk from the contributor.
            round.chunk_mut(chunk_id)?.set_lock_holder_unsafe(None);

            // Fetch the next challenge locator.
            let is_final_contribution = contribution_id == round.expected_number_of_contributions() - 1;
            let next_challenge = match is_final_contribution {
                true => {
                    Locator::ContributionFile(ContributionLocator::new(round.round_height() + 1, chunk_id, 0, true))
                }
                false => Locator::ContributionFile(ContributionLocator::new(
                    round.round_height(),
                    chunk_id,
                    contribution_id,
                    true,
                )),
            };

            // Save the updated round to storage.
            storage.update(
                &Locator::RoundState {
                    round_height: round.round_height(),
                },
                Object::RoundState(round),
            )?;

            // Remove the next challenge file from storage.
            storage.remove(&next_challenge)?;

            return Ok(());
        }

        // Check if the participant has this chunk ID in a pending task.
        if let Some(task) = state.lookup_pending_task(participant, chunk_id)? {
            let contribution_id = task.contribution_id();

            debug!(
                "Adding verification from {} for chunk {} contribution {}",
                participant, chunk_id, contribution_id
            );

            match self.verify_contribution(&mut storage, chunk_id, participant) {
                // Case 1 - Participant verified contribution, return the response file locator.
                Ok(contribution_id) => {
                    trace!("Release the lock on chunk {} from {}", chunk_id, participant);
                    let completed_task = Task::new(chunk_id, contribution_id);
                    state.completed_task(participant, completed_task, self.time.as_ref())?;

                    // Save the coordinator state in storage.
                    state.save(&mut storage)?;

                    info!("Added verification from {} for chunk {}", participant, chunk_id);
                    return Ok(());
                }
                // Case 2 - Participant failed to add their contribution, remove the contribution file.
                Err(error) => {
                    info!("Failed to add a verification and removing the contribution file");

                    // Fetch the current round from storage.
                    let round = Self::load_current_round(&storage)?;

                    // Fetch the next challenge locator.
                    let is_final_contribution = contribution_id == round.expected_number_of_contributions() - 1;
                    let next_challenge = match is_final_contribution {
                        true => Locator::ContributionFile(ContributionLocator::new(
                            round.round_height() + 1,
                            chunk_id,
                            0,
                            true,
                        )),
                        false => Locator::ContributionFile(ContributionLocator::new(
                            round.round_height(),
                            chunk_id,
                            contribution_id,
                            true,
                        )),
                    };

                    // Remove the invalid next challenge file from storage.
                    storage.remove(&next_challenge)?;

                    error!("{}", error);
                    return Err(error);
                }
            }
        }

        Err(CoordinatorError::VerificationFailed)
    }

    ///
    /// Attempts to aggregate the contributions of the current round of the ceremony.
    ///
    #[inline]
    pub fn try_aggregate(&self) -> Result<(), CoordinatorError> {
        // Acquire the storage write lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Acquire the state write lock.
        let mut state = self.state.write().unwrap();

        // Check that the current round height matches in storage and state.
        let current_round_height = {
            // Fetch the current round height from storage.
            let current_round_height_in_storage = Self::load_current_round_height(&storage)?;
            trace!("Current round height in storage is {}", current_round_height_in_storage);

            // Fetch the current round height from coordinator state.
            let current_round_height = state.current_round_height();
            trace!("Current round height in coordinator state is {}", current_round_height);

            // Check that the current round height matches in storage and state.
            match current_round_height_in_storage == current_round_height {
                true => current_round_height,
                false => return Err(CoordinatorError::RoundHeightMismatch),
            }
        };

        // Check that the current round is finished by contributors and verifiers.
        if !state.is_current_round_finished() {
            error!("Coordinator state shows round {} is not finished", current_round_height);
            return Err(CoordinatorError::RoundNotReady);
        }

        // Check that the current round has not been aggregated.
        if state.is_current_round_aggregated() {
            error!("Coordinator state shows round {} was aggregated", current_round_height);
            return Err(CoordinatorError::RoundAlreadyAggregated);
        }

        // Update the coordinator state to set the start of aggregation for the current round.
        state.aggregating_current_round(self.time.as_ref())?;

        // Check if this is round 0, as coordinator may safely skip aggregation.
        if current_round_height == 0 {
            // Set the current round as aggregated in coordinator state.
            state.aggregated_current_round(self.time.as_ref())?;

            debug!("Coordinator is safely skipping aggregation for round 0");
            return Ok(());
        }

        // Attempt to aggregate the current round.
        trace!("Trying to aggregate round {}", current_round_height);
        match self.aggregate_contributions(&mut storage) {
            // Case 1a - Coordinator aggregated the current round.
            Ok(()) => {
                info!("Coordinator has aggregated round {}", current_round_height);

                // Set the current round as aggregated in coordinator state.
                state.aggregated_current_round(self.time.as_ref())?;

                // Check that the current round has now been aggregated.
                if !state.is_current_round_aggregated() {
                    error!("Coordinator state says round {} isn't aggregated", current_round_height);
                    return Err(CoordinatorError::RoundAggregationFailed);
                }

                Ok(())
            }
            // Case 1b - Coordinator failed to aggregate the current round.
            Err(error) => {
                error!("Coordinator failed to aggregate the current round\n{}", error);

                // Rollback the current round aggregation.
                state.rollback_aggregating_current_round()?;

                Err(error)
            }
        }
    }

    ///
    /// Attempts to advance the ceremony to the next round.
    ///
    #[inline]
    pub fn try_advance(&self, started_at: DateTime<Utc>) -> Result<u64, CoordinatorError> {
        // Acquire the storage write lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Acquire the state write lock.
        let mut state = self.state.write().unwrap();

        // Check that the current round height matches in storage and state.
        let current_round_height = {
            // Fetch the current round height from storage.
            let current_round_height_in_storage = Self::load_current_round_height(&storage)?;
            trace!("Current round height in storage is {}", current_round_height_in_storage);

            // Fetch the current round height from coordinator state.
            let current_round_height = state.current_round_height();
            trace!("Current round height in coordinator state is {}", current_round_height);

            // Check that the current round height matches in storage and state.
            match current_round_height_in_storage == current_round_height {
                true => current_round_height,
                false => return Err(CoordinatorError::RoundHeightMismatch),
            }
        };

        // Check that the current round is finished.
        if !state.is_current_round_finished() {
            return Err(CoordinatorError::CurrentRoundNotFinished);
        }

        // Check that the current round is aggregated, if this is not round 0.
        if current_round_height > 0 && !state.is_current_round_aggregated() {
            return Err(CoordinatorError::CurrentRoundNotAggregated);
        }

        // Attempt to advance the round.
        trace!("Running precommit for the next round");
        let result = match state.precommit_next_round(current_round_height + 1, self.time.as_ref()) {
            // Case 1 - Precommit succeed, attempt to advance the round.
            Ok((contributors, verifiers)) => {
                trace!("Trying to add advance to the next round");
                match self.next_round(&mut storage, started_at, contributors, verifiers) {
                    // Case 1a - Coordinator advanced the round.
                    Ok(next_round_height) => {
                        // If success, update coordinator state to next round.
                        info!("Coordinator has advanced to round {}", next_round_height);
                        state.commit_next_round();
                        Ok(next_round_height)
                    }
                    // Case 1b - Coordinator failed to advance the round.
                    Err(error) => {
                        // If failed, rollback coordinator state to the current round.
                        error!("Coordinator failed to advance the next round, performing state rollback");
                        state.rollback_next_round();
                        error!("{}", error);
                        Err(error)
                    }
                }
            }
            // Case 2 - Precommit failed, roll back precommit.
            Err(error) => {
                // If failed, rollback coordinator state to the current round.
                error!("Precommit for next round has failed, performing state rollback");
                state.rollback_next_round();
                error!("{}", error);
                Err(error)
            }
        };

        // Save the coordinator state in storage.
        state.save(&mut storage)?;

        result
    }

    ///
    /// Returns the chunk ID from the given contribution file locator path.
    ///
    /// If the given locator path is invalid, returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn contribution_locator_to_chunk_id(&self, locator_path: &LocatorPath) -> Result<u64, CoordinatorError> {
        // Acquire the storage lock.
        let storage = StorageLock::Read(self.storage.read().unwrap());

        // Fetch the chunk ID corresponding to the given locator path.
        let locator = storage.to_locator(&locator_path)?;
        match &locator {
            Locator::ContributionFile(contribution_locator) => match storage.exists(&locator) {
                true => Ok(contribution_locator.chunk_id()),
                false => Err(CoordinatorError::ContributionLocatorMissing),
            },
            _ => Err(CoordinatorError::ContributionLocatorIncorrect),
        }
    }

    /// Convert a locator to a path string using the coordinator's
    /// storage layer.
    pub fn locator_to_path(&self, locator: Locator) -> Result<LocatorPath, CoordinatorError> {
        self.storage.read().unwrap().to_path(&locator)
    }

    ///
    /// Attempts to acquire the lock for a given chunk ID and
    /// participant.
    ///
    /// **Important**: The returned next contribution locator does not
    /// always match the current task being performed. If it is the
    /// final verification for a chunk, the a [Participant::Verifier]
    /// will receive a locator to the contribution 0 of the same chunk
    /// in the next round.
    ///
    /// On failure, this function returns a `CoordinatorError`.
    ///
    #[inline]
    pub(crate) fn try_lock_chunk(
        &self,
        mut storage: &mut StorageLock,
        chunk_id: u64,
        participant: &Participant,
    ) -> Result<LockedLocators, CoordinatorError> {
        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;
        trace!("Current round height from storage is {}", current_round_height);

        // Fetch the current round from storage.
        let mut round = Self::load_current_round(&storage)?;

        // Attempt to acquire the chunk lock for participant.
        trace!("Preparing to lock chunk {}", chunk_id);
        let locked_locators = round.try_lock_chunk(&self.environment, &mut storage, chunk_id, &participant)?;
        trace!("Participant {} locked chunk {}", participant, chunk_id);

        // Add the updated round to storage.
        match storage.update(
            &Locator::RoundState {
                round_height: current_round_height,
            },
            Object::RoundState(round),
        ) {
            Ok(_) => {
                debug!("{} acquired lock on chunk {}", participant, chunk_id);
                Ok(locked_locators)
            }
            _ => Err(CoordinatorError::StorageUpdateFailed),
        }
    }

    ///
    /// Attempts to add a contribution for a given chunk ID from a given participant.
    ///
    /// This function checks that the participant is a contributor and has uploaded
    /// a valid response file to the coordinator. The coordinator sanity checks
    /// (however, does not verify) the contribution before accepting the response file.
    ///
    /// On success, this function releases the chunk lock from the contributor and
    /// returns the response file locator and contribution ID of the response file.
    ///
    /// On failure, it returns a `CoordinatorError`.
    ///
    #[inline]
    pub(crate) fn add_contribution(
        &self,
        storage: &mut StorageLock,
        chunk_id: u64,
        participant: &Participant,
    ) -> Result<(ContributionLocator, u64), CoordinatorError> {
        debug!("Adding contribution from {} to chunk {}", participant, chunk_id);

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;
        trace!("Current round height from storage is {}", current_round_height);

        // Fetch the current round from storage.
        let mut round = Self::load_current_round(&storage)?;
        {
            // Check that the participant is an authorized contributor to the current round.
            if !round.is_contributor(participant) {
                error!("{} is unauthorized to contribute to chunk {})", participant, chunk_id);
                return Err(CoordinatorError::UnauthorizedChunkContributor);
            }

            // Check that the chunk lock is currently held by this contributor.
            if !round.is_chunk_locked_by(chunk_id, participant) {
                error!("{} should have lock on chunk {} but does not", participant, chunk_id);
                return Err(CoordinatorError::ChunkNotLockedOrByWrongParticipant);
            }
        }

        // Fetch the expected number of contributions for the current round.
        let expected_num_contributions = round.expected_number_of_contributions();
        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = round.chunk(chunk_id)?;
        // Fetch the next contribution ID of the chunk.
        let contribution_id = chunk.next_contribution_id(expected_num_contributions)?;

        // Check that the next contribution ID is one above the current contribution ID.
        if !chunk.is_next_contribution_id(contribution_id, expected_num_contributions) {
            return Err(CoordinatorError::ContributionIdMismatch);
        }

        // Fetch the challenge, response, and contribution file signature locators.
        let challenge_file_locator = Locator::ContributionFile(ContributionLocator::new(
            current_round_height,
            chunk_id,
            chunk.current_contribution_id(),
            true,
        ));
        let response_file_locator = ContributionLocator::new(current_round_height, chunk_id, contribution_id, false);
        let contribution_file_signature_locator = Locator::ContributionFileSignature(
            ContributionSignatureLocator::new(current_round_height, chunk_id, contribution_id, false),
        );

        // Check the challenge-response hash chain.
        let (challenge_hash, response_hash) = {
            // Compute the challenge hash using the challenge file.
            let challenge_reader = storage.reader(&challenge_file_locator)?;
            let challenge_hash = calculate_hash(challenge_reader.as_ref());
            trace!("Challenge is located in {}", storage.to_path(&challenge_file_locator)?);
            debug!("Challenge hash is {}", pretty_hash!(&challenge_hash.as_slice()));

            // Compute the response hash.
            let response_reader = storage.reader(&Locator::ContributionFile(response_file_locator))?;
            let response_hash = calculate_hash(response_reader.as_ref());
            trace!(
                "Response is located in {}",
                storage.to_path(&Locator::ContributionFile(response_file_locator))?
            );
            debug!("Response hash is {}", pretty_hash!(&response_hash.as_slice()));

            // Fetch the challenge hash from the response file.
            let challenge_hash_in_response = &response_reader
                .get(0..64)
                .ok_or(CoordinatorError::StorageReaderFailed)?[..];
            let pretty_hash = pretty_hash!(&challenge_hash_in_response);

            // Check the starting hash in the response file is based on the challenge.
            info!("The challenge hash is {}", pretty_hash!(&challenge_hash.as_slice()));
            info!("The challenge hash in response file is {}", pretty_hash);
            if challenge_hash_in_response != challenge_hash.as_slice() {
                error!("Challenge hash in response file does not match the expected challenge hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }

            (challenge_hash, response_hash)
        };

        // Check the challenge-response contribution signature.
        {
            // Fetch the stored contribution file signature.
            let contribution_file_signature: ContributionFileSignature =
                serde_json::from_slice(&*storage.reader(&contribution_file_signature_locator)?)?;

            // Check that the contribution file signature is valid.
            if !self.signature.verify(
                &participant.to_string(),
                &serde_json::to_string(&contribution_file_signature.get_state())?,
                contribution_file_signature.get_signature(),
            ) {
                error!("Contribution file signature failed to verify for {}", participant);
                return Err(CoordinatorError::ContributorSignatureInvalid);
            }

            // Check that the contribution file signature challenge hash is correct.
            if hex::decode(contribution_file_signature.get_challenge_hash())? != challenge_hash.as_slice() {
                error!("The signed challenge hash does not match the expected challenge hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }

            // Check that the contribution file signature response hash is correct.
            if hex::decode(contribution_file_signature.get_response_hash())? != response_hash.as_slice() {
                error!("The signed response hash does not match the expected response hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }

            // Check that the contribution file signature next challenge hash does not exist.
            if contribution_file_signature.get_next_challenge_hash().is_some() {
                error!("The signed next challenge hash should not exist");
                return Err(CoordinatorError::NextChallengeHashAlreadyExists);
            }
        }

        // Add the contribution response to the current chunk.
        round.chunk_mut(chunk_id)?.add_contribution(
            contribution_id,
            participant,
            storage.to_path(&Locator::ContributionFile(response_file_locator))?,
            storage.to_path(&contribution_file_signature_locator)?,
        )?;

        // Add the updated round to storage.
        match storage.update(
            &Locator::RoundState {
                round_height: current_round_height,
            },
            Object::RoundState(round),
        ) {
            Ok(_) => {
                debug!("Updated round {} in storage", current_round_height);
                debug!("{} added a contribution to chunk {}", participant, chunk_id);
                Ok((response_file_locator, contribution_id))
            }
            _ => Err(CoordinatorError::StorageUpdateFailed),
        }
    }

    ///
    /// Attempts to run verification in the current round for a given
    /// chunk ID and participant.
    ///
    /// This function checks that the participant is a verifier and
    /// has uploaded a valid next challenge file to the coordinator.
    /// The coordinator sanity checks that the next challenge file
    /// contains the hash of the corresponding response file.
    ///
    /// This function stores the next challenge locator into the round
    /// transcript and releases the chunk lock from the verifier.
    ///
    /// On success, this function returns the contribution ID of the
    /// unverified response file.
    ///
    /// **Important**: if the contribution is the final contribution
    /// for the chunk for the round, its verification will be stored
    /// in the next round's directory as contribution 0.
    ///
    #[tracing::instrument(
        skip(self, storage, chunk_id, participant),
        fields(chunk = chunk_id, participant = %participant)
    )]
    pub(crate) fn verify_contribution(
        &self,
        storage: &mut StorageLock,
        chunk_id: u64,
        participant: &Participant,
    ) -> Result<u64, CoordinatorError> {
        debug!("Attempting to verify a contribution for chunk {}", chunk_id);

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;
        trace!("Current round height from storage is {}", current_round_height);

        // Fetch the current round from storage.
        let mut round = Self::load_current_round(&storage)?;
        {
            // Check that the participant is an authorized verifier to the current round.
            if !round.is_verifier(participant) {
                error!("{} is unauthorized to verify chunk {})", participant, chunk_id);
                return Err(CoordinatorError::UnauthorizedChunkContributor);
            }

            // Check that the chunk lock is currently held by this verifier.
            if !round.is_chunk_locked_by(chunk_id, participant) {
                error!("{} should have lock on chunk {} but does not", participant, chunk_id);
                return Err(CoordinatorError::ChunkNotLockedOrByWrongParticipant);
            }
        }

        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = round.chunk(chunk_id)?;
        // Fetch the current contribution ID.
        let contribution_id = chunk.current_contribution_id();

        // Fetch the challenge, response, next challenge, and contribution file signature locators.
        let challenge_file_locator = Locator::ContributionFile(ContributionLocator::new(
            current_round_height,
            chunk_id,
            contribution_id - 1,
            true,
        ));
        let response_file_locator = Locator::ContributionFile(ContributionLocator::new(
            current_round_height,
            chunk_id,
            contribution_id,
            false,
        ));
        let (next_challenge_locator, contribution_file_signature_locator) = {
            // Fetch whether this is the final contribution of the specified chunk.
            let is_final_contribution = chunk.only_contributions_complete(round.expected_number_of_contributions());
            match is_final_contribution {
                true => (
                    Locator::ContributionFile(ContributionLocator::new(current_round_height + 1, chunk_id, 0, true)),
                    Locator::ContributionFileSignature(ContributionSignatureLocator::new(
                        current_round_height + 1,
                        chunk_id,
                        0,
                        true,
                    )),
                ),
                false => (
                    Locator::ContributionFile(ContributionLocator::new(
                        current_round_height,
                        chunk_id,
                        contribution_id,
                        true,
                    )),
                    Locator::ContributionFileSignature(ContributionSignatureLocator::new(
                        current_round_height,
                        chunk_id,
                        contribution_id,
                        true,
                    )),
                ),
            }
        };

        // Check the challenge-response hash chain.
        let (challenge_hash, response_hash) = {
            // Compute the challenge hash using the challenge file.
            let challenge_reader = storage.reader(&challenge_file_locator)?;
            let challenge_hash = calculate_hash(challenge_reader.as_ref());
            trace!("Challenge is located in {}", storage.to_path(&challenge_file_locator)?);
            debug!("Challenge hash is {}", pretty_hash!(&challenge_hash.as_slice()));

            // Compute the response hash.
            let response_reader = storage.reader(&response_file_locator)?;
            let response_hash = calculate_hash(response_reader.as_ref());
            trace!("Response is located in {}", storage.to_path(&response_file_locator)?);
            debug!("Response hash is {}", pretty_hash!(&response_hash.as_slice()));

            // Fetch the challenge hash from the response file.
            let challenge_hash_in_response = &response_reader
                .get(0..64)
                .ok_or(CoordinatorError::StorageReaderFailed)?[..];
            let pretty_hash = pretty_hash!(&challenge_hash_in_response);

            // Check the starting hash in the response file is based on the challenge.
            info!("The challenge hash is {}", pretty_hash!(&challenge_hash.as_slice()));
            info!("The challenge hash in response file is {}", pretty_hash);
            if challenge_hash_in_response != challenge_hash.as_slice() {
                error!("Challenge hash in response file does not match the expected challenge hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }

            (challenge_hash, response_hash)
        };

        // Check the response-next_challenge hash chain.
        let next_challenge_hash = {
            // Compute the next challenge hash.
            let next_challenge_reader = storage.reader(&next_challenge_locator)?;
            let next_challenge_hash = calculate_hash(next_challenge_reader.as_ref());
            trace!(
                "Next challenge is located in {}",
                storage.to_path(&next_challenge_locator)?
            );
            debug!(
                "Next challenge hash is {}",
                pretty_hash!(&next_challenge_hash.as_slice())
            );

            // Fetch the saved response hash in the next challenge file.
            let saved_response_hash = next_challenge_reader.as_ref().chunks(64).next().unwrap().to_vec();
            let pretty_hash = pretty_hash!(&saved_response_hash);

            // Check that the response hash matches the next challenge hash.
            info!("The response hash is {}", pretty_hash!(&response_hash));
            info!("The response hash in next challenge file is {}", pretty_hash);
            if response_hash.as_slice() != saved_response_hash {
                error!("Response hash does not match the saved response hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }

            next_challenge_hash
        };

        // Check the response-next_challenge contribution signature.
        {
            // Fetch the stored contribution file signature.
            let contribution_file_signature: ContributionFileSignature =
                serde_json::from_slice(&*storage.reader(&contribution_file_signature_locator)?)?;

            // Check that the contribution file signature is valid.
            if !self.signature.verify(
                &participant.to_string(),
                &serde_json::to_string(&contribution_file_signature.get_state())?,
                contribution_file_signature.get_signature(),
            ) {
                error!("Contribution file signature failed to verify for {}", participant);
                return Err(CoordinatorError::VerifierSignatureInvalid);
            }

            // Check that the contribution file signature challenge hash is correct.
            if hex::decode(contribution_file_signature.get_challenge_hash())? != challenge_hash.as_slice() {
                error!("The signed challenge hash does not match the expected challenge hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }

            // Check that the contribution file signature response hash is correct.
            if hex::decode(contribution_file_signature.get_response_hash())? != response_hash.as_slice() {
                error!("The signed response hash does not match the expected response hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }

            // Check that the contribution file signature next challenge hash exists.
            if contribution_file_signature.get_next_challenge_hash().is_none() {
                error!("The signed next challenge hash is missing");
                return Err(CoordinatorError::NextChallengeHashMissing);
            }

            // Check that the contribution file signature next challenge hash is correct.
            let signed_next_challenge_hash = contribution_file_signature
                .get_next_challenge_hash()
                .as_ref()
                .ok_or(CoordinatorError::NextChallengeHashMissing)?;
            if hex::decode(signed_next_challenge_hash)? != next_challenge_hash.as_slice() {
                error!("The signed next challenge hash does not match the expected next challenge hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }
        }

        // Sets the current contribution as verified in the current round.
        round.verify_contribution(
            chunk_id,
            contribution_id,
            participant.clone(),
            storage.to_path(&next_challenge_locator)?,
            storage.to_path(&contribution_file_signature_locator)?,
        )?;

        // Add the updated round to storage.
        match storage.update(
            &Locator::RoundState {
                round_height: current_round_height,
            },
            Object::RoundState(round),
        ) {
            Ok(_) => {
                debug!("Updated round {} in storage", current_round_height);
                debug!(
                    "{} verified chunk {} contribution {}",
                    participant, chunk_id, contribution_id
                );
                Ok(contribution_id)
            }
            _ => Err(CoordinatorError::StorageUpdateFailed),
        }
    }

    ///
    /// Aggregates the contributions for the current round of the ceremony.
    ///
    /// This function loads the current round from storage and checks that
    /// it is fully verified before proceeding to aggregate the round, and
    /// initialize the next round, saving it to storage for the coordinator.
    ///
    /// On success, the function returns the new round height.
    /// Otherwise, it returns a `CoordinatorError`.
    ///
    #[inline]
    pub(crate) fn aggregate_contributions(&self, mut storage: &mut StorageLock) -> Result<(), CoordinatorError> {
        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;

        trace!("Current round height from storage is {}", current_round_height);

        // This function executes aggregation of the current round in preparation
        // for transitioning to the next round. If this is the initial round,
        // there should be nothing to aggregate and we return a `CoordinatorError`.
        if current_round_height == 0 {
            error!("Unauthorized to aggregate round 0");
            return Err(CoordinatorError::RoundDoesNotExist);
        }

        // Check that the current round state exists in storage.
        if !storage.exists(&Locator::RoundState {
            round_height: current_round_height,
        }) {
            return Err(CoordinatorError::RoundStateMissing);
        }

        // Check that the next round state does not exist in storage.
        if storage.exists(&Locator::RoundState {
            round_height: current_round_height + 1,
        }) {
            return Err(CoordinatorError::RoundShouldNotExist);
        }

        // Check that the current round file does not exist.
        let round_file = Locator::RoundFile {
            round_height: current_round_height,
        };
        if storage.exists(&round_file) {
            error!("Round file locator already exists ({})", storage.to_path(&round_file)?);
            return Err(CoordinatorError::RoundLocatorAlreadyExists);
        }

        // Fetch the current round from storage.
        let round = Self::load_current_round(&storage)?;

        // Check that the final unverified and verified contribution locators exist.
        let contribution_id = round.expected_number_of_contributions() - 1;
        for chunk_id in 0..self.environment.number_of_chunks() {
            // Check that the final unverified contribution locator exists.
            let locator = Locator::ContributionFile(ContributionLocator::new(
                current_round_height,
                chunk_id,
                contribution_id,
                false,
            ));
            if !storage.exists(&locator) {
                error!("Unverified contribution is missing ({})", storage.to_path(&locator)?);
                return Err(CoordinatorError::ContributionMissing);
            }
            // Check that the final verified contribution locator exists.
            let locator =
                Locator::ContributionFile(ContributionLocator::new(current_round_height + 1, chunk_id, 0, true));
            if !storage.exists(&locator) {
                error!("Verified contribution is missing ({})", storage.to_path(&locator)?);
                return Err(CoordinatorError::ContributionMissing);
            }
        }

        // Check that all chunks in the current round are verified.
        if !round.is_complete() {
            error!("Round {} is not complete", current_round_height);
            trace!("{:#?}", &round);
            return Err(CoordinatorError::RoundNotComplete);
        }

        // Execute round aggregation and aggregate verification for the current round.
        {
            debug!("Coordinator is starting aggregation and aggregate verification");
            Aggregation::run(&self.environment, &mut storage, &round)?;
            debug!("Coordinator completed aggregation and aggregate verification");
        }

        // Check that the round file for the current round now exists.
        if !storage.exists(&round_file) {
            error!("Round file locator is missing ({})", storage.to_path(&round_file)?);
            return Err(CoordinatorError::RoundFileMissing);
        }

        Ok(())
    }

    ///
    /// Initiates the next round of the ceremony.
    ///
    /// If there are no prior rounds in storage, this initializes a new ceremony
    /// by invoking `Initialization`, and saves it to storage.
    ///
    /// Otherwise, this loads the current round from storage and checks that
    /// it is fully verified before proceeding to aggregate the round, and
    /// initialize the next round, saving it to storage for the coordinator.
    ///
    /// On success, the function returns the new round height.
    /// Otherwise, it returns a `CoordinatorError`.
    ///
    #[inline]
    pub(crate) fn next_round(
        &self,
        storage: &mut StorageLock,
        started_at: DateTime<Utc>,
        contributors: Vec<Participant>,
        verifiers: Vec<Participant>,
    ) -> Result<u64, CoordinatorError> {
        // Check that the next round has at least one authorized contributor.
        if contributors.is_empty() {
            return Err(CoordinatorError::ContributorsMissing);
        }
        // Check that the next round has at least one authorized verifier.
        if verifiers.is_empty() {
            return Err(CoordinatorError::VerifierMissing);
        }

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;

        trace!("Current round height from storage is {}", current_round_height);

        // Ensure the current round has been aggregated if this is not the initial round.
        if current_round_height != 0 {
            // Check that the round file for the current round exists.
            let round_file = Locator::RoundFile {
                round_height: current_round_height,
            };
            if !storage.exists(&round_file) {
                error!("Round file locator is missing ({})", storage.to_path(&round_file)?);
                warn!("Coordinator may be missing a call to `try_aggregate` for the current round");
                return Err(CoordinatorError::RoundFileMissing);
            }
            // self.aggregate_contributions(&mut storage)?;
        }

        // Create the new round height.
        let new_height = current_round_height + 1;
        info!("Transitioning from round {} to {}", current_round_height, new_height);

        // Check that the new round does not exist in storage.
        // If it exists, this means the round was already initialized.
        let locator = Locator::RoundState {
            round_height: new_height,
        };
        if storage.exists(&locator) {
            error!("Round {} already exists ({})", new_height, storage.to_path(&locator)?);
            return Err(CoordinatorError::RoundAlreadyInitialized);
        }

        // Check that each contribution for the next round exists.
        for chunk_id in 0..self.environment.number_of_chunks() {
            debug!("Locating round {} chunk {} contribution 0", new_height, chunk_id);
            let locator = Locator::ContributionFile(ContributionLocator::new(new_height, chunk_id, 0, true));
            if !storage.exists(&locator) {
                error!("Contribution locator is missing ({})", storage.to_path(&locator)?);
                return Err(CoordinatorError::ContributionLocatorMissing);
            }
        }

        // Instantiate the new round and height.
        let new_round = Round::new(
            &self.environment,
            &storage,
            new_height,
            started_at,
            contributors,
            verifiers,
        )?;

        #[cfg(test)]
        trace!("{:#?}", &new_round);

        // Insert the new round into storage.
        storage.insert(
            Locator::RoundState {
                round_height: new_height,
            },
            Object::RoundState(new_round),
        )?;

        // Next, update the round height to reflect the new round.
        storage.update(&Locator::RoundHeight, Object::RoundHeight(new_height))?;

        debug!("Added round {} to storage", current_round_height);
        info!("Transitioned from round {} to {}", current_round_height, new_height);
        Ok(new_height)
    }

    ///
    /// Attempts to run initialization for the ceremony.
    ///
    /// If this is the initial round, ensure the round does not exist yet.
    ///
    /// If there is no round in storage, proceed to create a new round instance,
    /// and run `Initialization` to start the ceremony.
    ///
    #[inline]
    pub(super) fn run_initialization(
        &self,
        mut storage: &mut StorageLock,
        started_at: DateTime<Utc>,
    ) -> Result<u64, CoordinatorError> {
        // Check that the ceremony has not begun yet.
        if Self::load_current_round_height(&storage).is_ok() {
            return Err(CoordinatorError::RoundAlreadyInitialized);
        }

        // Establish the round height convention for initialization.
        let round_height = 0;

        // Check that the current round does not exist in storage.
        if storage.exists(&Locator::RoundState { round_height }) {
            return Err(CoordinatorError::RoundShouldNotExist);
        }

        // Check that the next round does not exist in storage.
        if storage.exists(&Locator::RoundState {
            round_height: round_height + 1,
        }) {
            return Err(CoordinatorError::RoundShouldNotExist);
        }

        // Create an instantiation of `Round` for round 0.
        let mut round = {
            // Initialize the contributors as an empty list as this is for initialization.
            let contributors = vec![];

            // Initialize the verifiers as a list comprising only one coordinator verifier,
            // as this is for initialization.
            let verifiers = vec![
                self.environment
                    .coordinator_verifiers()
                    .first()
                    .ok_or(CoordinatorError::VerifierMissing)?
                    .clone(),
            ];

            // Create a new round instance.
            Round::new(
                &self.environment,
                &storage,
                round_height,
                started_at,
                contributors,
                verifiers,
            )?
        };

        info!("Starting initialization of round {}", round_height);

        // Execute initialization of contribution 0 for all chunks
        // in the new round and check that the new locators exist.
        for chunk_id in 0..self.environment.number_of_chunks() {
            // 1 - Check that the contribution locator corresponding to this round's chunk does not exist.
            let locator = Locator::ContributionFile(ContributionLocator::new(round_height, chunk_id, 0, true));
            if storage.exists(&locator) {
                error!("Contribution locator already exists ({})", storage.to_path(&locator)?);
                return Err(CoordinatorError::ContributionLocatorAlreadyExists);
            }

            // 2 - Check that the contribution locator corresponding to the next round's chunk does not exists.
            let locator = Locator::ContributionFile(ContributionLocator::new(round_height + 1, chunk_id, 0, true));
            if storage.exists(&locator) {
                error!("Contribution locator already exists ({})", storage.to_path(&locator)?);
                return Err(CoordinatorError::ContributionLocatorAlreadyExists);
            }

            info!("Coordinator is starting initialization on chunk {}", chunk_id);
            let _contribution_hash = Initialization::run(&self.environment, &mut storage, round_height, chunk_id)?;
            info!("Coordinator completed initialization on chunk {}", chunk_id);

            // 1 - Check that the contribution locator corresponding to this round's chunk now exists.
            let locator = Locator::ContributionFile(ContributionLocator::new(round_height, chunk_id, 0, true));
            if !storage.exists(&locator) {
                error!("Contribution locator is missing ({})", storage.to_path(&locator)?);
                return Err(CoordinatorError::ContributionLocatorMissing);
            }

            // 2 - Check that the contribution locator corresponding to the next round's chunk now exists.
            let locator = Locator::ContributionFile(ContributionLocator::new(round_height + 1, chunk_id, 0, true));
            if !storage.exists(&locator) {
                error!("Contribution locator is missing ({})", storage.to_path(&locator)?);
                return Err(CoordinatorError::ContributionLocatorMissing);
            }
        }

        // Set the finished time for round 0.
        round.try_finish(self.time.utc_now());

        // Add the new round to storage.
        storage.insert(Locator::RoundState { round_height }, Object::RoundState(round))?;

        // Next, add the round height to storage.
        storage.insert(Locator::RoundHeight, Object::RoundHeight(round_height))?;

        info!("Completed initialization of round {}", round_height);

        Ok(round_height)
    }

    /// Update the round on disk after a drop has occured.
    #[inline]
    fn drop_participant_from_storage(
        &self,
        mut storage: &mut StorageLock,
        drop: &DropParticipant,
    ) -> Result<Vec<LocatorPath>, CoordinatorError> {
        debug!(
            "Dropping participant from storage with the following information: {:#?}",
            drop
        );
        // Fetch the current round from storage.
        let mut round = match Self::load_current_round(&storage) {
            // Case 1 - This is a typical round of the ceremony.
            Ok(round) => round,
            // Case 2 - The ceremony has not started or storage has failed.
            _ => return Err(CoordinatorError::RoundDoesNotExist),
        };

        // Check the justification and extract the tasks.
        let (tasks, replacement) = match drop {
            DropParticipant::BanCurrent(data) => (&data.tasks, &data.replacement),
            DropParticipant::DropCurrent(data) => (&data.tasks, &data.replacement),
            DropParticipant::Inactive(_) => {
                // Participant is not part of the round, therefore
                // there is nothing to do.
                return Ok(vec![]);
            }
        };

        warn!("Removing locked chunks and all impacted contributions");

        // Remove the lock from the specified chunks.
        round.remove_locks_unsafe(&mut storage, &drop)?;
        warn!("Removed locked chunks");

        // Remove the contributions from the specified chunks.
        round.remove_chunk_contributions_unsafe(&mut storage, &drop)?;
        warn!("Removed impacted contributions");

        // Assign a replacement contributor to the dropped tasks for the current round.
        if let Some(replacement_contributor) = replacement {
            round.add_replacement_contributor_unsafe(replacement_contributor.clone())?;
            warn!("Added a replacement contributor {}", replacement_contributor);
        }

        // Convert the tasks into contribution file locators.
        let locators = tasks
            .par_iter()
            .map(|task| {
                storage
                    .to_path(&Locator::ContributionFile(ContributionLocator::new(
                        round.round_height(),
                        task.chunk_id(),
                        task.contribution_id(),
                        true,
                    )))
                    .unwrap()
            })
            .collect();

        // Save the updated round to storage.
        storage.update(
            &Locator::RoundState {
                round_height: round.round_height(),
            },
            Object::RoundState(round),
        )?;

        Ok(locators)
    }

    #[inline]
    fn load_current_round_height(storage: &StorageLock) -> Result<u64, CoordinatorError> {
        // Fetch the current round height from storage.
        match storage.get(&Locator::RoundHeight)? {
            // Case 1 - This is a typical round of the ceremony.
            Object::RoundHeight(current_round_height) => Ok(current_round_height),
            // Case 2 - Storage failed to fetch the round height.
            _ => Err(CoordinatorError::StorageFailed),
        }
    }

    #[inline]
    fn load_current_round(storage: &StorageLock) -> Result<Round, CoordinatorError> {
        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(storage)?;

        // Fetch the current round from storage.
        Self::load_round(storage, current_round_height)
    }

    #[inline]
    fn load_round(storage: &StorageLock, round_height: u64) -> Result<Round, CoordinatorError> {
        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;

        // Fetch the specified round from storage.
        match round_height <= current_round_height {
            // Load the corresponding round data from storage.
            true => match storage.get(&Locator::RoundState { round_height })? {
                // Case 1 - The ceremony is running and the round state was fetched.
                Object::RoundState(round) => Ok(round),
                // Case 2 - Storage failed to fetch the round height.
                _ => Err(CoordinatorError::StorageFailed),
            },
            // Case 3 - There are no prior rounds of the ceremony.
            false => Err(CoordinatorError::RoundDoesNotExist),
        }
    }

    ///
    /// Returns a reference to the instantiation of `Storage` that this
    /// coordinator is using.
    ///
    #[cfg(test)]
    #[inline]
    pub(super) fn storage(&self) -> Arc<RwLock<Box<dyn Storage>>> {
        self.storage.clone()
    }

    ///
    /// Returns a reference to the instantiation of `Signature` that this
    /// coordinator is using.
    ///
    #[cfg(test)]
    #[inline]
    pub(super) fn signature(&self) -> Arc<Box<dyn Signature>> {
        self.signature.clone()
    }
}

#[cfg(any(test, feature = "operator"))]
use crate::commands::{Computation, Seed, SigningKey, Verification};

#[cfg(any(test, feature = "operator"))]
impl Coordinator {
    #[tracing::instrument(
        skip(self, contributor, contributor_signing_key, contributor_seed),
        fields(contributor = %contributor),
    )]
    pub fn contribute(
        &self,
        contributor: &Participant,
        contributor_signing_key: &SigningKey,
        contributor_seed: &Seed,
    ) -> Result<(), CoordinatorError> {
        let (_chunk_id, locked_locators) = self.try_lock(contributor)?;
        let response_locator = locked_locators.next_contribution();
        let round_height = response_locator.round_height();
        let chunk_id = response_locator.chunk_id();
        let contribution_id = response_locator.contribution_id();

        // check that the response matches a pending task
        {
            let response_task = Task::new(chunk_id, contribution_id);
            let info = self
                .state
                .read()
                .unwrap()
                .current_participant_info(contributor)
                .unwrap()
                .clone();

            if info.pending_tasks().is_empty() {
                return Err(CoordinatorError::ContributorPendingTasksCannotBeEmpty(
                    contributor.clone(),
                ));
            }

            if !info
                .pending_tasks()
                .iter()
                .find(|pending_task| pending_task == &&response_task)
                .is_some()
            {
                return Err(CoordinatorError::PendingTasksMustContainResponseTask { response_task });
            }
        }

        debug!("Computing contributions for round {} chunk {}", round_height, chunk_id);
        self.run_computation(
            round_height,
            chunk_id,
            contribution_id,
            contributor,
            contributor_signing_key,
            contributor_seed,
        )?;
        let _response = self.try_contribute(contributor, chunk_id)?;
        debug!(
            "Successfully computed contribution for round {} chunk {}",
            round_height, chunk_id
        );
        Ok(())
    }

    #[tracing::instrument(
        skip(self, verifier, verifier_signing_key),
        fields(verifier = %verifier),
    )]
    pub fn verify(&self, verifier: &Participant, verifier_signing_key: &SigningKey) -> anyhow::Result<()> {
        let (_chunk_id, locked_locators) = self.try_lock(&verifier)?;
        let response_locator = &locked_locators.current_contribution();
        let round_height = response_locator.round_height();
        let chunk_id = response_locator.chunk_id();
        let contribution_id = response_locator.contribution_id();

        debug!("Running verification for round {} chunk {}", round_height, chunk_id);
        let _next_challenge =
            self.run_verification(round_height, chunk_id, contribution_id, &verifier, verifier_signing_key)?;
        self.try_verify(&verifier, chunk_id)?;
        debug!("Successful verification for round {} chunk {}", round_height, chunk_id);
        Ok(())
    }

    ///
    /// Attempts to run computation for a given round height, given chunk ID, and contribution ID.
    ///
    /// This function is used for testing purposes and can also be used for completing contributions
    /// of participants who may have dropped off and handed over control of their session.
    ///
    #[inline]
    pub fn run_computation(
        &self,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
        participant: &Participant,
        participant_signing_key: &SigningKey,
        participant_seed: &Seed,
    ) -> Result<(), CoordinatorError> {
        info!(
            "Running computation for round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );

        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Check that the contribution ID is valid.
        if contribution_id == 0 {
            return Err(CoordinatorError::ContributionIdMustBeNonzero);
        }

        // Check that the participant is a contributor.
        if !participant.is_contributor() {
            return Err(CoordinatorError::ExpectedContributor);
        }

        // Acquire the storage write lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Fetch the specified round from storage.
        let round = Self::load_round(&storage, round_height)?;

        // Check that the chunk lock is currently held by this contributor.
        if !round.is_chunk_locked_by(chunk_id, &participant) {
            error!("{} should have lock on chunk {} but does not", &participant, chunk_id);
            return Err(CoordinatorError::ChunkNotLockedOrByWrongParticipant);
        }

        // Check that the given contribution ID does not exist yet.
        if round.chunk(chunk_id)?.get_contribution(contribution_id).is_ok() {
            return Err(CoordinatorError::ContributionShouldNotExist);
        }

        // Fetch the challenge locator.
        let challenge_locator = &Locator::ContributionFile(ContributionLocator::new(
            round_height,
            chunk_id,
            contribution_id - 1,
            true,
        ));
        // Fetch the response locator.
        let response_locator =
            &Locator::ContributionFile(ContributionLocator::new(round_height, chunk_id, contribution_id, false));
        // Fetch the contribution file signature locator.
        let contribution_file_signature_locator = &Locator::ContributionFileSignature(
            ContributionSignatureLocator::new(round_height, chunk_id, contribution_id, false),
        );

        info!(
            "Starting computation on round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );
        Computation::run(
            &self.environment,
            &mut storage,
            self.signature.clone(),
            participant_signing_key,
            challenge_locator,
            response_locator,
            contribution_file_signature_locator,
            participant_seed,
        )?;
        info!(
            "Completed computation on round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );

        Ok(())
    }

    ///
    /// Attempts to run verification for a given round height, given chunk ID, and contribution ID.
    ///
    /// On success, this function returns the verified contribution locator.
    ///
    /// This function copies the unverified contribution into the verified contribution locator,
    /// which is the next contribution ID within a round, or the next round height if this round
    /// is complete.
    ///
    #[inline]
    pub fn run_verification(
        &self,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
        participant: &Participant,
        participant_signing_key: &SigningKey,
    ) -> Result<LocatorPath, CoordinatorError> {
        info!(
            "Running verification for round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );

        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Check that the contribution ID is valid.
        if contribution_id == 0 {
            return Err(CoordinatorError::ContributionIdMustBeNonzero);
        }

        // Check that the participant is a verifier.
        if !participant.is_verifier() {
            return Err(CoordinatorError::ExpectedContributor);
        }

        // Acquire the storage write lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Fetch the specified round from storage.
        let round = Self::load_round(&storage, round_height)?;

        // Check that the chunk lock is currently held by this contributor.
        if !round.is_chunk_locked_by(chunk_id, &participant) {
            error!("{} should have lock on chunk {} but does not", &participant, chunk_id);
            return Err(CoordinatorError::ChunkNotLockedOrByWrongParticipant);
        }

        // Check that the contribution locator corresponding to the response file exists.
        let response_locator =
            Locator::ContributionFile(ContributionLocator::new(round_height, chunk_id, contribution_id, false));
        if !storage.exists(&response_locator) {
            error!("Response file at {} is missing", storage.to_path(&response_locator)?);
            return Err(CoordinatorError::ContributionLocatorMissing);
        }

        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = round.chunk(chunk_id)?;

        // Chat that the specified contribution ID has NOT been verified yet.
        if chunk.get_contribution(contribution_id)?.is_verified() {
            return Err(CoordinatorError::ContributionAlreadyVerified);
        }

        // Fetch whether this is the final contribution of the specified chunk.
        let is_final_contribution = chunk.only_contributions_complete(round.expected_number_of_contributions());

        // Fetch the verified response locator and the contribution file signature locator.
        let verified_locator = match is_final_contribution {
            true => Locator::ContributionFile(ContributionLocator::new(round_height + 1, chunk_id, 0, true)),
            false => Locator::ContributionFile(ContributionLocator::new(round_height, chunk_id, contribution_id, true)),
        };

        info!(
            "Starting verification on round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );
        Verification::run(
            &self.environment,
            &mut storage,
            self.signature.clone(),
            participant_signing_key,
            round_height,
            chunk_id,
            contribution_id,
            is_final_contribution,
        )?;
        info!(
            "Completed verification on round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );

        // Check that the verified contribution locator exists.
        if !storage.exists(&verified_locator) {
            let verified_response = storage.to_path(&verified_locator)?;
            error!("Verified response file at {} is missing", verified_response);
            return Err(CoordinatorError::ContributionLocatorMissing);
        }

        Ok(storage.to_path(&verified_locator)?)
    }

    ///
    /// Returns a reference to the instantiation of `CoordinatorState` that this
    /// coordinator is using.
    ///
    #[inline]
    pub fn state(&self) -> CoordinatorState {
        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Clone the state struct.
        state.clone()
    }

    ///
    /// Returns a reference to the instantiation of `Environment` that this
    /// coordinator is using.
    ///
    #[inline]
    pub fn environment(&self) -> &Environment {
        &self.environment
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        authentication::Dummy,
        commands::{Seed, SigningKey, SEED_LENGTH},
        environment::*,
        objects::Participant,
        storage::StorageLock,
        testing::prelude::*,
        Coordinator,
    };

    use chrono::Utc;
    use once_cell::sync::Lazy;
    use rand::RngCore;
    use std::collections::HashMap;

    fn initialize_to_round_1(
        coordinator: &Coordinator,
        contributors: &[Participant],
        verifiers: &[Participant],
    ) -> anyhow::Result<()> {
        let storage = coordinator.storage();

        // Initialize the ceremony and add the contributors and verifiers to the queue.
        {
            // Acquire the storage write lock.
            let mut storage = StorageLock::Write(storage.write().unwrap());

            // Acquire the state write lock.
            let mut state = coordinator.state.write().unwrap();

            // Run initialization.
            info!("Initializing ceremony");
            let round_height = coordinator.run_initialization(&mut storage, *TEST_STARTED_AT)?;
            info!("Initialized ceremony");

            // Check current round height is now 0.
            assert_eq!(0, round_height);

            // Initialize the coordinator state to round 0.
            state.initialize(round_height);
            state.save(&mut storage)?;

            // Add the contributor and verifier of the coordinator to execute round 1.
            for contributor in contributors {
                state.add_to_queue(contributor.clone(), 10)?;
            }
            for verifier in verifiers {
                state.add_to_queue(verifier.clone(), 10)?;
            }

            // Update the queue.
            state.update_queue()?;
            state.save(&mut storage)?;
        }

        info!("Advancing ceremony to round 1");
        coordinator.try_advance(*TEST_STARTED_AT)?;
        info!("Advanced ceremony to round 1");

        // Check current round height is now 1.
        assert_eq!(1, coordinator.current_round_height()?);

        // info!("Add contributions and verifications for round 1");
        // for _ in 0..coordinator.environment.number_of_chunks() {
        //     for contributor in contributors {
        //         coordinator.contribute(contributor)?;
        //     }
        //     for verifier in verifiers {
        //         coordinator.verify(verifier)?;
        //     }
        // }
        // info!("Added contributions and verifications for round 1");

        Ok(())
    }

    fn initialize_coordinator(coordinator: &Coordinator) -> anyhow::Result<()> {
        // Load the contributors and verifiers.
        let contributors = vec![
            Lazy::force(&TEST_CONTRIBUTOR_ID).clone(),
            Lazy::force(&TEST_CONTRIBUTOR_ID_2).clone(),
        ];
        let verifiers = vec![Lazy::force(&TEST_VERIFIER_ID).clone()];

        initialize_to_round_1(coordinator, &contributors, &verifiers)
    }

    fn initialize_coordinator_single_contributor(coordinator: &Coordinator) -> anyhow::Result<()> {
        // Load the contributors and verifiers.
        let contributors = vec![Lazy::force(&TEST_CONTRIBUTOR_ID).clone()];
        let verifiers = vec![Lazy::force(&TEST_VERIFIER_ID).clone()];

        initialize_to_round_1(coordinator, &contributors, &verifiers)
    }

    #[test]
    #[serial]
    fn coordinator_initialization_matches_json() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT);

        let coordinator = Coordinator::new(TEST_ENVIRONMENT.clone(), Box::new(Dummy))?;
        initialize_coordinator(&coordinator)?;

        // Check that round 0 matches the round 0 JSON specification.
        {
            let now = Utc::now();

            // Fetch round 0 from coordinator.
            let mut expected = test_round_0_json()?;
            let mut candidate = coordinator.get_round(0)?;

            // Set the finish time to match.
            println!("{} {}", expected.is_complete(), candidate.is_complete());
            expected.try_finish_testing_only_unsafe(now.clone());
            candidate.try_finish_testing_only_unsafe(now);

            print_diff(&expected, &candidate);
            assert_eq!(expected, candidate);
        }

        Ok(())
    }

    #[test]
    #[serial]
    fn coordinator_initialization() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT);

        let coordinator = Coordinator::new(TEST_ENVIRONMENT.clone(), Box::new(Dummy))?;

        {
            // Acquire the storage write lock.
            let storage = coordinator.storage();
            let mut storage = StorageLock::Write(storage.write().unwrap());

            // Run initialization.
            info!("Initializing ceremony");
            let round_height = coordinator.run_initialization(&mut storage, *TEST_STARTED_AT).unwrap();
            info!("Initialized ceremony");

            // Check current round height is now 0.
            assert_eq!(0, round_height);

            // Load the contributors and verifiers.
            let contributors = vec![
                Lazy::force(&TEST_CONTRIBUTOR_ID).clone(),
                Lazy::force(&TEST_CONTRIBUTOR_ID_2).clone(),
            ];
            let verifiers = vec![Lazy::force(&TEST_VERIFIER_ID).clone()];

            // Start round 1.
            coordinator
                .next_round(&mut storage, *TEST_STARTED_AT, contributors, verifiers)
                .unwrap();
        }

        {
            // Check round 0 is complete.
            assert!(coordinator.get_round(0)?.is_complete());

            // Check current round height is now 1.
            assert_eq!(1, coordinator.current_round_height()?);

            // Check that the coordinator participants are established correctly.
            assert!(coordinator.is_coordinator_contributor(&TEST_CONTRIBUTOR_ID));
            assert!(coordinator.is_coordinator_verifier(&TEST_VERIFIER_ID));

            // Check the current round has a matching height.
            let current_round = coordinator.current_round()?;
            assert_eq!(coordinator.current_round_height()?, current_round.round_height());

            // Check round 1 contributors.
            assert_eq!(2, current_round.number_of_contributors());
            assert!(current_round.is_contributor(&TEST_CONTRIBUTOR_ID));
            assert!(current_round.is_contributor(&TEST_CONTRIBUTOR_ID_2));
            assert!(!current_round.is_contributor(&TEST_CONTRIBUTOR_ID_3));
            assert!(!current_round.is_contributor(&TEST_VERIFIER_ID));

            // Check round 1 verifiers.
            assert_eq!(1, current_round.number_of_verifiers());
            assert!(current_round.is_verifier(&TEST_VERIFIER_ID));
            assert!(!current_round.is_verifier(&TEST_VERIFIER_ID_2));
            assert!(!current_round.is_verifier(&TEST_CONTRIBUTOR_ID));

            // Check round 1 is NOT complete.
            assert!(!current_round.is_complete());
        }

        Ok(())
    }

    #[test]
    #[serial]
    fn coordinator_contributor_try_lock_chunk() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT);

        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID);
        let contributor_2 = Lazy::force(&TEST_CONTRIBUTOR_ID_2);

        let coordinator = Coordinator::new(TEST_ENVIRONMENT.clone(), Box::new(Dummy))?;
        let storage = coordinator.storage();
        initialize_coordinator(&coordinator)?;

        {
            // Acquire the storage write lock.
            let mut storage = StorageLock::Write(storage.write().unwrap());

            // Acquire the lock for chunk 0 as contributor 1.
            assert!(coordinator.try_lock_chunk(&mut storage, 0, &contributor).is_ok());

            // Attempt to acquire the lock for chunk 0 as contributor 1 again.
            assert!(coordinator.try_lock_chunk(&mut storage, 0, &contributor).is_err());

            // Acquire the lock for chunk 1 as contributor 1.
            assert!(coordinator.try_lock_chunk(&mut storage, 1, &contributor).is_ok());

            // Attempt to acquire the lock for chunk 0 as contributor 2.
            assert!(coordinator.try_lock_chunk(&mut storage, 0, &contributor_2).is_err());

            // Attempt to acquire the lock for chunk 1 as contributor 2.
            assert!(coordinator.try_lock_chunk(&mut storage, 1, &contributor_2).is_err());

            // Acquire the lock for chunk 1 as contributor 2.
            assert!(coordinator.try_lock_chunk(&mut storage, 2, &contributor_2).is_ok());
        }

        {
            // Check that chunk 0 is locked.
            let round = coordinator.current_round()?;
            let chunk = round.chunk(0)?;
            assert!(chunk.is_locked());
            assert!(!chunk.is_unlocked());

            // Check that chunk 0 is locked by contributor 1.
            assert!(chunk.is_locked_by(contributor));
            assert!(!chunk.is_locked_by(contributor_2));

            // Check that chunk 1 is locked.
            let chunk = round.chunk(1)?;
            assert!(chunk.is_locked());
            assert!(!chunk.is_unlocked());

            // Check that chunk 1 is locked by contributor 1.
            assert!(chunk.is_locked_by(contributor));
            assert!(!chunk.is_locked_by(contributor_2));

            // Check that chunk 2 is locked.
            let chunk = round.chunk(2)?;
            assert!(chunk.is_locked());
            assert!(!chunk.is_unlocked());

            // Check that chunk 2 is locked by contributor 2.
            assert!(chunk.is_locked_by(contributor_2));
            assert!(!chunk.is_locked_by(contributor));
        }

        Ok(())
    }

    #[test]
    #[serial]
    fn coordinator_contributor_add_contribution() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT_3);

        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID).clone();
        let contributor_signing_key: SigningKey = "secret_key".to_string();

        let coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone(), Box::new(Dummy))?;
        let storage = coordinator.storage();
        initialize_coordinator(&coordinator)?;

        {
            // Acquire the storage write lock.
            let mut storage = StorageLock::Write(storage.write().unwrap());

            // Acquire the lock for chunk 0 as contributor 1.
            coordinator.try_lock_chunk(&mut storage, 0, &contributor)?;
        }

        // Run computation on round 1 chunk 0 contribution 1.
        {
            // Check current round is 1.
            let round = coordinator.current_round()?;
            let round_height = round.round_height();
            assert_eq!(1, round_height);

            // Check chunk 0 is not verified.
            let chunk_id = 0;
            let chunk = round.chunk(chunk_id)?;
            assert!(!chunk.is_complete(round.expected_number_of_contributions()));

            // Check next contribution is 1.
            let contribution_id = 1;
            assert!(chunk.is_next_contribution_id(contribution_id, round.expected_number_of_contributions()));

            // Run the computation
            let mut seed: Seed = [0; SEED_LENGTH];
            rand::thread_rng().fill_bytes(&mut seed[..]);
            assert!(
                coordinator
                    .run_computation(
                        round_height,
                        chunk_id,
                        contribution_id,
                        &contributor,
                        &contributor_signing_key,
                        &seed
                    )
                    .is_ok()
            );
        }

        // Add contribution for round 1 chunk 0 contribution 1.
        let chunk_id = 0;
        {
            // Acquire the storage write lock.
            let mut storage = StorageLock::Write(storage.write().unwrap());

            // Add round 1 chunk 0 contribution 1.
            assert!(
                coordinator
                    .add_contribution(&mut storage, chunk_id, &contributor)
                    .is_ok()
            );
        }
        {
            // Check chunk 0 lock is released.
            let round = coordinator.current_round()?;
            let chunk = round.chunk(chunk_id)?;
            assert!(chunk.is_unlocked());
            assert!(!chunk.is_locked());
        }

        Ok(())
    }

    #[test]
    #[serial]
    fn coordinator_verifier_verify_contribution() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT_3);

        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID);
        let contributor_signing_key: SigningKey = "secret_key".to_string();

        let coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone(), Box::new(Dummy))?;
        let storage = coordinator.storage();
        initialize_coordinator(&coordinator)?;

        // Check current round height is now 1.
        let round_height = coordinator.current_round_height()?;
        assert_eq!(1, round_height);

        // Acquire the lock for chunk 0 as contributor 1.
        let chunk_id = 0;
        let contribution_id = 1;
        {
            // Acquire the storage write lock.
            let mut storage = StorageLock::Write(storage.write().unwrap());

            assert!(coordinator.try_lock_chunk(&mut storage, chunk_id, &contributor).is_ok());
        }
        {
            // Run computation on round 1 chunk 0 contribution 1.
            let mut seed: Seed = [0; SEED_LENGTH];
            rand::thread_rng().fill_bytes(&mut seed[..]);
            assert!(
                coordinator
                    .run_computation(
                        round_height,
                        chunk_id,
                        contribution_id,
                        contributor,
                        &contributor_signing_key,
                        &seed
                    )
                    .is_ok()
            );

            // Acquire the storage write lock.
            let mut storage = StorageLock::Write(storage.write().unwrap());

            // Add round 1 chunk 0 contribution 1.
            assert!(
                coordinator
                    .add_contribution(&mut storage, chunk_id, &contributor)
                    .is_ok()
            );
        }

        // Acquire lock for round 1 chunk 0 contribution 1.
        {
            // Acquire the lock on chunk 0 for the verifier.
            let verifier = Lazy::force(&TEST_VERIFIER_ID).clone();
            {
                // Acquire the storage write lock.
                let mut storage = StorageLock::Write(storage.write().unwrap());

                assert!(coordinator.try_lock_chunk(&mut storage, chunk_id, &verifier).is_ok());
            }

            // Check that chunk 0 is locked.
            let round = coordinator.current_round()?;
            let chunk = round.chunk(chunk_id)?;
            assert!(chunk.is_locked());
            assert!(!chunk.is_unlocked());

            // Check that chunk 0 is locked by the verifier.
            debug!("{:#?}", round);
            assert!(chunk.is_locked_by(&verifier));
        }

        // Verify round 1 chunk 0 contribution 1.
        {
            let verifier = Lazy::force(&TEST_VERIFIER_ID).clone();
            let verifier_signing_key: SigningKey = "secret_key".to_string();

            // Run verification.
            let verify = coordinator.run_verification(
                round_height,
                chunk_id,
                contribution_id,
                &verifier,
                &verifier_signing_key,
            );
            assert!(verify.is_ok());

            // Acquire the storage write lock.
            let mut storage = StorageLock::Write(storage.write().unwrap());

            // Verify contribution 1.
            coordinator.verify_contribution(&mut storage, chunk_id, &verifier)?;
        }

        Ok(())
    }

    #[test]
    #[serial]
    // This test runs a round with a single coordinator and single verifier
    // The verifier instances are run on a separate thread to simulate an environment where
    // verification and contribution happen concurrently.
    fn coordinator_concurrent_contribution_verification() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT_3);

        let coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone(), Box::new(Dummy))?;
        let storage = coordinator.storage();
        initialize_coordinator_single_contributor(&coordinator)?;

        // Check current round height is now 1.
        let round_height = coordinator.current_round_height()?;
        assert_eq!(1, round_height);

        let coordinator = std::sync::Arc::new(coordinator);

        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID).clone();
        let contributor_signing_key: SigningKey = "secret_key".to_string();

        let verifier = Lazy::force(&TEST_VERIFIER_ID);

        let mut verifier_threads = vec![];

        let contribution_id = 1;

        let mut seed: Seed = [0; SEED_LENGTH];
        rand::thread_rng().fill_bytes(&mut seed[..]);

        for chunk_id in 0..TEST_ENVIRONMENT_3.number_of_chunks() {
            {
                // Acquire the storage write lock.
                let mut storage = StorageLock::Write(storage.write().unwrap());

                // Acquire the lock as contributor.
                let try_lock = coordinator.try_lock_chunk(&mut storage, chunk_id, &contributor);
                if try_lock.is_err() {
                    println!(
                        "Failed to acquire lock for chunk {} as contributor {:?}\n{}",
                        chunk_id,
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.current_round()?)?
                    );
                    try_lock?;
                }
            }
            {
                // Run computation as contributor.
                let contribute = coordinator.run_computation(
                    round_height,
                    chunk_id,
                    contribution_id,
                    &contributor,
                    &contributor_signing_key,
                    &seed,
                );
                if contribute.is_err() {
                    println!(
                        "Failed to run computation for chunk {} as contributor {:?}\n{}",
                        chunk_id,
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.current_round()?)?
                    );
                    contribute?;
                }

                // Acquire the storage write lock.
                let mut storage = StorageLock::Write(storage.write().unwrap());

                // Add the contribution as the contributor.
                let contribute = coordinator.add_contribution(&mut storage, chunk_id, &contributor);
                if contribute.is_err() {
                    println!(
                        "Failed to add contribution for chunk {} as contributor {:?}\n{}",
                        chunk_id,
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.current_round()?)?
                    );
                    contribute?;
                }
            }

            // Spawn a thread to concurrently verify the contributions.
            let coordinator_clone = coordinator.clone();
            let storage_clone = storage.clone();
            let verifier_thread = std::thread::spawn(move || {
                let verifier = verifier.clone();
                let verifier_signing_key: SigningKey = "secret_key".to_string();

                {
                    // Acquire the storage write lock.
                    let mut storage = StorageLock::Write(storage_clone.write().unwrap());

                    // Acquire the lock as the verifier.
                    let try_lock = coordinator_clone.try_lock_chunk(&mut storage, chunk_id, &verifier);
                    if try_lock.is_err() {
                        println!(
                            "Failed to acquire lock as verifier {:?}\n{}",
                            verifier.clone(),
                            serde_json::to_string_pretty(&coordinator_clone.current_round().unwrap()).unwrap()
                        );
                        panic!(format!("{:?}", try_lock.unwrap()))
                    }
                }
                {
                    // Run verification as verifier.
                    let verify = coordinator_clone.run_verification(
                        round_height,
                        chunk_id,
                        contribution_id,
                        &verifier,
                        &verifier_signing_key,
                    );
                    if verify.is_err() {
                        error!(
                            "Failed to run verification as verifier {:?}\n{}",
                            &verifier,
                            serde_json::to_string_pretty(&coordinator_clone.current_round().unwrap()).unwrap()
                        );
                        verify.unwrap();
                    }

                    // Acquire the storage write lock.
                    let mut storage = StorageLock::Write(storage_clone.write().unwrap());

                    // Add the verification as the verifier.
                    let verify = coordinator_clone.verify_contribution(&mut storage, chunk_id, &verifier);
                    if verify.is_err() {
                        println!(
                            "Failed to run verification as verifier {:?}\n{}",
                            verifier.clone(),
                            serde_json::to_string_pretty(&coordinator_clone.current_round().unwrap()).unwrap()
                        );
                        panic!(format!("{:?}", verify.unwrap()))
                    }
                }
            });
            verifier_threads.push(verifier_thread);
        }

        for verifier_thread in verifier_threads {
            verifier_thread.join().expect("Couldn't join on the verifier thread");
        }

        Ok(())
    }

    #[test]
    #[serial]
    fn coordinator_aggregation() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT_3);

        let coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone(), Box::new(Dummy))?;
        let storage = coordinator.storage();
        initialize_coordinator(&coordinator)?;

        // Check current round height is now 1.
        let round_height = coordinator.current_round_height()?;
        assert_eq!(1, round_height);

        // Run computation and verification on each contribution in each chunk.
        let contributors = vec![
            Lazy::force(&TEST_CONTRIBUTOR_ID).clone(),
            Lazy::force(&TEST_CONTRIBUTOR_ID_2).clone(),
        ];

        let verifier = Lazy::force(&TEST_VERIFIER_ID).clone();
        let verifier_signing_key: SigningKey = "secret_key".to_string();

        let mut seeds = HashMap::new();

        // Iterate over all chunk IDs.
        for chunk_id in 0..TEST_ENVIRONMENT_3.number_of_chunks() {
            // As contribution ID 0 is initialized by the coordinator, iterate from
            // contribution ID 1 up to the expected number of contributions.
            for contribution_id in 1..coordinator.current_round()?.expected_number_of_contributions() {
                let contributor = &contributors[contribution_id as usize - 1].clone();
                let contributor_signing_key: SigningKey = "secret_key".to_string();

                trace!("{} is processing contribution {}", contributor, contribution_id);
                {
                    // Acquire the storage write lock.
                    let mut storage = StorageLock::Write(storage.write().unwrap());

                    // Acquire the lock as contributor.
                    let try_lock = coordinator.try_lock_chunk(&mut storage, chunk_id, &contributor);
                    if try_lock.is_err() {
                        println!(
                            "Failed to acquire lock for chunk {} as contributor {:?}\n{}",
                            chunk_id,
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        try_lock?;
                    }
                }
                {
                    let seed = if seeds.contains_key(&contribution_id) {
                        seeds[&contribution_id]
                    } else {
                        let mut seed: Seed = [0; SEED_LENGTH];
                        rand::thread_rng().fill_bytes(&mut seed[..]);
                        seeds.insert(contribution_id.clone(), seed);
                        seed
                    };

                    // Run computation as contributor.
                    let contribute = coordinator.run_computation(
                        round_height,
                        chunk_id,
                        contribution_id,
                        &contributor,
                        &contributor_signing_key,
                        &seed,
                    );
                    if contribute.is_err() {
                        error!(
                            "Failed to run computation as contributor {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        contribute?;
                    }

                    // Acquire the storage write lock.
                    let mut storage = StorageLock::Write(storage.write().unwrap());

                    // Add the contribution as the contributor.
                    let contribute = coordinator.add_contribution(&mut storage, chunk_id, &contributor);
                    if contribute.is_err() {
                        error!(
                            "Failed to add contribution as contributor {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        contribute?;
                    }
                }
                {
                    // Acquire the storage write lock.
                    let mut storage = StorageLock::Write(storage.write().unwrap());

                    // Acquire the lock as the verifier.
                    let try_lock = coordinator.try_lock_chunk(&mut storage, chunk_id, &verifier);
                    if try_lock.is_err() {
                        error!(
                            "Failed to acquire lock as verifier {:?}\n{}",
                            &verifier,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        try_lock?;
                    }
                }
                {
                    // Run verification as verifier.
                    let verify = coordinator.run_verification(
                        round_height,
                        chunk_id,
                        contribution_id,
                        &verifier,
                        &verifier_signing_key,
                    );
                    if verify.is_err() {
                        error!(
                            "Failed to run verification as verifier {:?}\n{}",
                            &verifier,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        verify?;
                    }

                    // Acquire the storage write lock.
                    let mut storage = StorageLock::Write(storage.write().unwrap());

                    // Add the verification as the verifier.
                    let verify = coordinator.verify_contribution(&mut storage, chunk_id, &verifier);
                    if verify.is_err() {
                        error!(
                            "Failed to run verification as verifier {:?}\n{}",
                            &verifier,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        verify?;
                    }
                }
            }
        }

        println!(
            "Starting aggregation with this transcript {}",
            serde_json::to_string_pretty(&coordinator.current_round()?)?
        );

        {
            // Acquire the storage write lock.
            let mut storage = StorageLock::Write(storage.write().unwrap());

            // Run aggregation for round 1.
            coordinator.aggregate_contributions(&mut storage)?;
        }

        // Check that the round is still round 1, as try_advance has not been called.
        assert_eq!(1, coordinator.current_round_height()?);

        println!(
            "Finished aggregation with this transcript {}",
            serde_json::to_string_pretty(&coordinator.current_round()?)?
        );

        Ok(())
    }

    #[test]
    #[serial]
    fn coordinator_next_round() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT_3);

        let coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone(), Box::new(Dummy))?;
        let storage = coordinator.storage();
        initialize_coordinator_single_contributor(&coordinator)?;

        let round_height = 1;
        assert_eq!(round_height, coordinator.current_round_height()?);

        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID).clone();
        let contributor_signing_key: SigningKey = "secret_key".to_string();

        let verifier = Lazy::force(&TEST_VERIFIER_ID);
        let verifier_signing_key: SigningKey = "secret_key".to_string();

        // Run computation and verification on each contribution in each chunk.
        let mut seeds = HashMap::new();
        for chunk_id in 0..TEST_ENVIRONMENT_3.number_of_chunks() {
            // Ensure contribution ID 0 is already verified by the coordinator.
            assert!(
                coordinator
                    .current_round()?
                    .chunk(chunk_id)?
                    .get_contribution(0)?
                    .is_verified()
            );

            // As contribution ID 0 is initialized by the coordinator, iterate from
            // contribution ID 1 up to the expected number of contributions.
            for contribution_id in 1..coordinator.current_round()?.expected_number_of_contributions() {
                {
                    // Acquire the storage write lock.
                    let mut storage = StorageLock::Write(storage.write().unwrap());

                    // Acquire the lock as contributor.
                    let try_lock = coordinator.try_lock_chunk(&mut storage, chunk_id, &contributor);
                    if try_lock.is_err() {
                        error!(
                            "Failed to acquire lock as contributor {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        try_lock?;
                    }
                }
                {
                    // Run computation as contributor.
                    let seed = if seeds.contains_key(&contribution_id) {
                        seeds[&contribution_id]
                    } else {
                        let mut seed: Seed = [0; SEED_LENGTH];
                        rand::thread_rng().fill_bytes(&mut seed[..]);
                        seeds.insert(contribution_id.clone(), seed);
                        seed
                    };

                    let contribute = coordinator.run_computation(
                        round_height,
                        chunk_id,
                        contribution_id,
                        &contributor,
                        &contributor_signing_key,
                        &seed,
                    );
                    if contribute.is_err() {
                        error!(
                            "Failed to run computation as contributor {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        contribute?;
                    }

                    // Acquire the storage write lock.
                    let mut storage = StorageLock::Write(storage.write().unwrap());

                    // Add the contribution as the contributor.
                    let contribute = coordinator.add_contribution(&mut storage, chunk_id, &contributor);
                    if contribute.is_err() {
                        error!(
                            "Failed to add contribution as contributor {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        contribute?;
                    }
                }
                {
                    // Acquire the storage write lock.
                    let mut storage = StorageLock::Write(storage.write().unwrap());

                    // Acquire the lock as the verifier.
                    let try_lock = coordinator.try_lock_chunk(&mut storage, chunk_id, &verifier);
                    if try_lock.is_err() {
                        error!(
                            "Failed to acquire lock as verifier {:?}\n{}",
                            &verifier,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        try_lock?;
                    }
                }
                {
                    // Run verification as verifier.
                    let verify = coordinator.run_verification(
                        round_height,
                        chunk_id,
                        contribution_id,
                        &verifier,
                        &verifier_signing_key,
                    );
                    if verify.is_err() {
                        error!(
                            "Failed to run verification as verifier {:?}\n{}",
                            &verifier,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        verify?;
                    }

                    // Acquire the storage write lock.
                    let mut storage = StorageLock::Write(storage.write().unwrap());

                    // Add the verification as the verifier.
                    let verify = coordinator.verify_contribution(&mut storage, chunk_id, &verifier);
                    if verify.is_err() {
                        error!(
                            "Failed to run verification as verifier {:?}\n{}",
                            &verifier,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        verify?;
                    }
                }
            }
        }

        info!(
            "Starting aggregation with this transcript {}",
            serde_json::to_string_pretty(&coordinator.current_round()?)?
        );

        {
            // Acquire the storage write lock.
            let mut storage = StorageLock::Write(storage.write().unwrap());

            // Run aggregation for round 1.
            coordinator.aggregate_contributions(&mut storage)?;

            // Run aggregation and transition from round 1 to round 2.
            coordinator.next_round(&mut storage, Utc::now(), vec![contributor.clone()], vec![
                verifier.clone(),
            ])?;
        }

        // Check that the ceremony has advanced to round 2.
        assert_eq!(2, coordinator.current_round_height()?);

        info!(
            "Finished aggregation with this transcript {}",
            serde_json::to_string_pretty(&coordinator.current_round()?)?
        );

        Ok(())
    }

    #[test]
    #[serial]
    #[ignore]
    fn coordinator_number_of_chunks() {
        let environment = &*Testing::from(Parameters::TestChunks { number_of_chunks: 4096 });
        initialize_test_environment(environment);

        let coordinator = Coordinator::new(environment.clone(), Box::new(Dummy)).unwrap();
        initialize_coordinator(&coordinator).unwrap();

        assert_eq!(
            environment.number_of_chunks(),
            coordinator.get_round(0).unwrap().chunks().len() as u64
        );
    }
}
