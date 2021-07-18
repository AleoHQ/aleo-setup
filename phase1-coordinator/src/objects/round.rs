use crate::{
    environment::Environment,
    objects::{participant::*, Chunk},
    storage::{
        ContributionLocator,
        ContributionSignatureLocator,
        Locator,
        LocatorPath,
        Object,
        RemoveAction,
        Storage,
        StorageAction,
        UpdateAction,
    },
    CoordinatorError,
};

use chrono::{DateTime, Utc};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_aux::prelude::*;
use serde_diff::SerdeDiff;
use std::{collections::HashSet, hash::Hash};
use tracing::{debug, error, trace, warn};

use super::Task;

/// A helper function used to check that each list of participants is unique.
fn has_unique_elements<T>(iter: T) -> bool
where
    T: IntoIterator,
    T::Item: Eq + Hash,
{
    let mut uniq = HashSet::new();
    iter.into_iter().all(move |x| uniq.insert(x))
}

/// Locators for files that are locked by [Round::try_lock_chunk()]
#[derive(Debug, Clone)]
pub struct LockedLocators {
    previous_contribution: ContributionLocator,
    current_contribution: ContributionLocator,
    next_contribution: ContributionLocator,
    next_contribution_file_signature: ContributionSignatureLocator,
}

impl LockedLocators {
    /// Get a reference previous contribution's locator.
    pub fn previous_contribution(&self) -> ContributionLocator {
        self.previous_contribution
    }

    /// Get a reference current contribution's locator.
    pub fn current_contribution(&self) -> ContributionLocator {
        self.current_contribution
    }

    /// Get a reference next contribution's locator.
    pub fn next_contribution(&self) -> ContributionLocator {
        self.next_contribution
    }

    /// Get a reference next contribution's signtature locator.
    pub fn next_contribution_file_signature(&self) -> ContributionSignatureLocator {
        self.next_contribution_file_signature
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, SerdeDiff)]
#[serde(rename_all = "camelCase")]
pub struct Round {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    version: u64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    height: u64,
    #[serde_diff(opaque)]
    started_at: Option<DateTime<Utc>>,
    #[serde_diff(opaque)]
    finished_at: Option<DateTime<Utc>>,
    contributor_ids: Vec<Participant>,
    verifier_ids: Vec<Participant>,
    chunks: Vec<Chunk>,
}

impl Round {
    /// Creates a new instance of `Round`.
    #[inline]
    pub(crate) fn new(
        environment: &Environment,
        storage: &mut impl Storage,
        round_height: u64,
        started_at: DateTime<Utc>,
        contributor_ids: Vec<Participant>,
        verifier_ids: Vec<Participant>,
    ) -> Result<Self, CoordinatorError> {
        debug!("Starting to create round {}", round_height);

        // Check that the number of chunks is nonzero.
        if environment.number_of_chunks() == 0 {
            return Err(CoordinatorError::NumberOfChunksInvalid);
        }

        // Fetch the initial verifier.
        let verifier = verifier_ids.first().ok_or(CoordinatorError::VerifierMissing)?;

        // Check that all contributor IDs are valid.
        {
            // Check that each contributor ID is unique.
            if !has_unique_elements(&contributor_ids) {
                return Err(CoordinatorError::RoundContributorsNotUnique);
            }
            // Check that each contributor ID is a contributor participant type.
            let num_contributors = contributor_ids
                .par_iter()
                .filter(|id| Participant::is_contributor(id))
                .count();
            if num_contributors != contributor_ids.len() {
                error!("{} IDs are not contributors", contributor_ids.len() - num_contributors);
                return Err(CoordinatorError::ExpectedContributor);
            }
            // Check that the list of contributor IDs is not empty.
            // This check is only enforced if the round height is not 0.
            if round_height != 0 && num_contributors == 0 {
                return Err(CoordinatorError::RoundContributorsMissing);
            }
        }

        // Check that all verifier IDs are valid.
        {
            // Check that each verifier ID is unique.
            if !has_unique_elements(&verifier_ids) {
                return Err(CoordinatorError::RoundVerifiersNotUnique);
            }
            // Check that each verifier ID is a verifier participant type.
            let num_verifiers = verifier_ids
                .par_iter()
                .filter(|id| Participant::is_verifier(id))
                .count();
            if num_verifiers != verifier_ids.len() {
                error!("{} IDs are not verifiers", verifier_ids.len() - num_verifiers);
                return Err(CoordinatorError::ExpectedVerifier);
            }
            // Check that the list of verifier IDs is not empty.
            if num_verifiers == 0 {
                return Err(CoordinatorError::RoundVerifiersMissing);
            }
        }

        // Construct the chunks for this round.
        //
        // Initialize the chunk verifiers as a list comprising only
        // the coordinator verifier, as this is for initialization.
        let chunks: Vec<Chunk> = (0..environment.number_of_chunks() as usize)
            .into_par_iter()
            .map(|chunk_id| {
                Chunk::new(
                    chunk_id as u64,
                    verifier.clone(),
                    storage.to_path(&Locator::ContributionFile(ContributionLocator::new(
                        round_height,
                        chunk_id as u64,
                        0,
                        true,
                    )))?,
                    storage.to_path(&Locator::ContributionFileSignature(ContributionSignatureLocator::new(
                        round_height,
                        chunk_id as u64,
                        0,
                        true,
                    )))?,
                )
            })
            .collect::<Result<Vec<Chunk>, CoordinatorError>>()?;

        debug!("Completed creating round {}", round_height);

        Ok(Self {
            version: environment.software_version(),
            height: round_height,
            started_at: Some(started_at),
            finished_at: None,
            contributor_ids,
            verifier_ids,
            chunks,
        })
    }

    /// Returns the version number set in the round.
    #[inline]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Returns the height of the round.
    #[inline]
    pub fn round_height(&self) -> u64 {
        self.height
    }

    /// Returns the number of contributors authorized for this round.
    #[inline]
    pub fn number_of_contributors(&self) -> u64 {
        self.contributor_ids.len() as u64
    }

    /// Returns the number of verifiers authorized for this round.
    #[inline]
    pub fn number_of_verifiers(&self) -> u64 {
        self.verifier_ids.len() as u64
    }

    /// Returns a reference to a list of contributors.
    #[inline]
    pub fn contributors(&self) -> &Vec<Participant> {
        &self.contributor_ids
    }

    /// Returns a reference to a list of verifiers.
    #[inline]
    pub fn verifiers(&self) -> &Vec<Participant> {
        &self.verifier_ids
    }

    ///
    /// Returns `true` if the given participant is authorized as a
    /// contributor and listed in the contributor IDs for this round.
    ///
    /// If the participant is not a contributor, or if there are
    /// no prior rounds, returns `false`.
    ///
    #[inline]
    pub fn is_contributor(&self, participant: &Participant) -> bool {
        // Check that the participant is a contributor.
        match participant {
            // Check that the participant is a contributor for the given round height.
            Participant::Contributor(_) => self.contributor_ids.contains(participant),
            Participant::Verifier(_) => false,
        }
    }

    ///
    /// Returns `true` if the given participant is authorized as a
    /// verifier and listed in the verifier IDs for this round.
    ///
    /// If the participant is not a verifier, or if there are
    /// no prior rounds, returns `false`.
    ///
    #[inline]
    pub fn is_verifier(&self, participant: &Participant) -> bool {
        // Check that the participant is a verifier.
        match participant {
            Participant::Contributor(_) => false,
            // Check that the participant is a verifier for the given round height.
            Participant::Verifier(_) => self.verifier_ids.contains(participant),
        }
    }

    ///
    /// Returns a reference to the chunk, if it exists.
    /// Otherwise returns `None`.
    ///
    #[inline]
    pub fn chunk(&self, chunk_id: u64) -> Result<&Chunk, CoordinatorError> {
        // Fetch the chunk with the given chunk ID.
        let chunk = match self.chunks.get(chunk_id as usize) {
            Some(chunk) => chunk,
            _ => return Err(CoordinatorError::ChunkMissing),
        };

        // Check the ID in the chunk matches the given chunk ID.
        match chunk.chunk_id() == chunk_id {
            true => Ok(chunk),
            false => Err(CoordinatorError::ChunkIdMismatch),
        }
    }

    ///
    /// Returns a reference to a list of the chunks.
    ///
    #[inline]
    pub fn chunks(&self) -> &Vec<Chunk> {
        &self.chunks
    }

    ///
    /// Returns the expected number of contributions.
    ///
    #[inline]
    pub fn expected_number_of_contributions(&self) -> u64 {
        // The expected number of contributions is one more than
        // the total number of authorized contributions to account
        // for the initialization contribution in each round.
        self.number_of_contributors() + 1
    }

    ///
    /// Returns `true` if the chunk corresponding to the given chunk ID is
    /// locked by the given participant. Otherwise, returns `false`.
    ///
    #[inline]
    pub fn is_chunk_locked_by(&self, chunk_id: u64, participant: &Participant) -> bool {
        match self.chunk(chunk_id) {
            Ok(chunk) => chunk.is_locked_by(participant),
            _ => false,
        }
    }

    ///
    /// Returns the number of locks held by the given participant in this round.
    ///
    #[inline]
    pub fn number_of_locks_held(&self, participant: &Participant) -> Result<u64, CoordinatorError> {
        debug!("Checking the lock count for {}", participant);

        // Check that the participant is authorized for the current round.
        match participant {
            Participant::Contributor(_) => {
                // Check that the participant is an authorized contributor
                // for the current round.
                if !self.is_contributor(participant) {
                    error!("{} is not an authorized contributor", participant);
                    trace!("{:?} {:?}", self.contributor_ids, participant);
                    return Err(CoordinatorError::UnauthorizedChunkContributor);
                }
            }
            Participant::Verifier(_) => {
                // Check that the participant is an authorized verifier
                // for the current round.
                if !self.is_verifier(participant) {
                    error!("{} is not an authorized verifier", participant);
                    return Err(CoordinatorError::UnauthorizedChunkVerifier);
                }
            }
        };

        // Fetch the number of locks held by the participant.
        let number_of_locks_held = self
            .chunks
            .par_iter()
            .filter(|chunk| chunk.is_locked_by(participant))
            .count() as u64;

        debug!("{} is holding {} locks", participant, number_of_locks_held);
        Ok(number_of_locks_held)
    }

    ///
    /// Returns `true` if all chunks are unlocked and all contributions in all chunks
    /// have been verified. Otherwise, returns `false`.
    ///
    #[inline]
    pub fn is_complete(&self) -> bool {
        // Check that all chunks are unlocked.
        let number_of_locks_held = self.chunks.par_iter().filter(|chunk| chunk.is_locked()).count();
        if number_of_locks_held > 0 {
            trace!("{} chunks are locked in round {}", &number_of_locks_held, self.height);
            return false;
        }

        // Check that all contributions in all chunks have been verified.
        self.chunks
            .par_iter()
            .filter(|chunk| !chunk.is_complete(self.expected_number_of_contributions()))
            .collect::<Vec<_>>()
            .is_empty()
    }

    ///
    /// Returns the current contribution locator for a given chunk ID.
    ///
    /// If the current contribution is NOT contributed yet,
    /// this function will return a `CoordinatorError`.
    ///
    /// If the current contribution is already verified,
    /// this function will return a `CoordinatorError`.
    ///
    #[tracing::instrument(
        level = "error",
        skip(self, storage, chunk_id),
        fields(round = self.round_height(), chunk = chunk_id, verified = verified),
        err
    )]
    pub(crate) fn current_contribution_locator(
        &self,
        storage: &impl Storage,
        chunk_id: u64,
        verified: bool,
    ) -> Result<ContributionLocator, CoordinatorError> {
        // Fetch the current round height.
        let current_round_height = self.round_height();
        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = self.chunk(chunk_id)?;
        // Fetch the current contribution ID.
        let current_contribution_id = chunk.current_contribution_id();

        // Fetch the current contribution locator.
        let current_contribution_locator =
            ContributionLocator::new(current_round_height, chunk_id, current_contribution_id, verified);

        // Check that the contribution locator corresponding to the current contribution ID
        // exists for the current round and given chunk ID.
        if !storage.exists(&Locator::ContributionFile(current_contribution_locator.clone())) {
            error!(
                "{} is missing",
                storage.to_path(&Locator::ContributionFile(current_contribution_locator.clone()))?
            );
            return Err(CoordinatorError::ContributionLocatorMissing);
        }

        // Check that the current contribution ID is NOT verified yet.
        if chunk.get_contribution(current_contribution_id)?.is_verified() {
            error!(
                "{} is already verified",
                storage.to_path(&Locator::ContributionFile(current_contribution_locator.clone()))?
            );
            return Err(CoordinatorError::ContributionAlreadyVerified);
        }

        Ok(current_contribution_locator)
    }

    ///
    /// Returns the next contribution locator for a given chunk ID.
    ///
    /// If the current contribution is NOT contributed yet,
    /// this function will return a `CoordinatorError`.
    ///
    /// If the current contribution is NOT verified yet,
    /// this function will return a `CoordinatorError`.
    ///
    /// If the next contribution locator already exists,
    /// this function will return a `CoordinatorError`.
    ///
    /// If the chunk corresponding to the given chunk ID
    /// is already completed for the current round,
    /// this function will return a `CoordinatorError`.
    ///
    #[tracing::instrument(
        level = "error",
        skip(self, storage, chunk_id),
        fields(round = self.round_height(), chunk = chunk_id),
        err
    )]
    pub(crate) fn next_contribution_locator(
        &self,
        storage: &impl Storage,
        chunk_id: u64,
    ) -> Result<ContributionLocator, CoordinatorError> {
        // Fetch the current round height.
        let current_round_height = self.round_height();
        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = self.chunk(chunk_id)?;
        // Fetch the expected number of contributions for the current round.
        let expected_num_contributions = self.expected_number_of_contributions();
        // Fetch the next contribution ID.
        let next_contribution_id = chunk.next_contribution_id(expected_num_contributions)?;

        // Check that the current contribution has been verified.
        if !chunk.current_contribution()?.is_verified() {
            return Err(CoordinatorError::ContributionMissingVerification);
        }

        // Fetch the next contribution locator.
        let next_contribution_locator =
            ContributionLocator::new(current_round_height, chunk_id, next_contribution_id, false);

        // Check that the contribution locator corresponding to the next contribution ID
        // does NOT exist for the current round and given chunk ID.
        if storage.exists(&Locator::ContributionFile(next_contribution_locator.clone())) {
            tracing::error!("Contribution locator already exists: {:?}", next_contribution_locator);
            return Err(CoordinatorError::ContributionLocatorAlreadyExists);
        }

        Ok(next_contribution_locator)
    }

    ///
    /// Returns the next contribution file signature locator for a given chunk ID.
    ///
    /// If the current contribution is NOT contributed yet,
    /// this function will return a `CoordinatorError`.
    ///
    /// If the current contribution is NOT verified yet,
    /// this function will return a `CoordinatorError`.
    ///
    /// If the next contribution locator already exists,
    /// this function will return a `CoordinatorError`.
    ///
    /// If the chunk corresponding to the given chunk ID
    /// is already completed for the current round,
    /// this function will return a `CoordinatorError`.
    ///
    #[tracing::instrument(
        level = "error",
        skip(self, storage, chunk_id),
        fields(round = self.round_height(), chunk = chunk_id),
        err
    )]
    #[inline]
    pub(crate) fn next_contribution_file_signature_locator(
        &self,
        storage: &impl Storage,
        chunk_id: u64,
    ) -> Result<ContributionSignatureLocator, CoordinatorError> {
        // Fetch the current round height.
        let current_round_height = self.round_height();
        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = self.chunk(chunk_id)?;
        // Fetch the expected number of contributions for the current round.
        let expected_num_contributions = self.expected_number_of_contributions();
        // Fetch the next contribution ID.
        let next_contribution_id = chunk.next_contribution_id(expected_num_contributions)?;

        // Check that the current contribution has been verified.
        if !chunk.current_contribution()?.is_verified() {
            return Err(CoordinatorError::ContributionMissingVerification);
        }

        // Fetch the contribution file signature locator.
        let contribution_file_signature_locator =
            ContributionSignatureLocator::new(current_round_height, chunk_id, next_contribution_id, false);

        // Check that the contribution file signature locator corresponding to the next contribution ID
        // does NOT exist for the current round and given chunk ID.
        if storage.exists(&Locator::ContributionFileSignature(
            contribution_file_signature_locator.clone(),
        )) {
            return Err(CoordinatorError::ContributionFileSignatureLocatorAlreadyExists);
        }

        Ok(contribution_file_signature_locator)
    }

    ///
    /// Attempts to acquire the lock of a given chunk ID from storage
    /// for a given participant.
    ///
    /// **Important**: The returned next contribution locator does not
    /// always match the current task being performed. If it is the
    /// final verification for a chunk, the a [Participant::Verifier]
    /// will receive a locator to the contribution 0 of the same chunk
    /// in the next round.
    ///
    #[tracing::instrument(
        level = "error",
        skip(self, environment, storage, chunk_id, participant),
        fields(chunk = chunk_id),
        err
    )]
    pub(crate) fn try_lock_chunk(
        &mut self,
        environment: &Environment,
        storage: &mut impl Storage,
        chunk_id: u64,
        participant: &Participant,
    ) -> Result<LockedLocators, CoordinatorError> {
        debug!("{} is attempting to lock chunk {}", participant, chunk_id);

        // Check that the participant is holding less than the chunk lock limit.
        let number_of_locks_held = self.number_of_locks_held(&participant)? as usize;
        match participant {
            Participant::Contributor(_) => {
                if number_of_locks_held >= environment.contributor_lock_chunk_limit() {
                    trace!("{} chunks are locked by {}", &number_of_locks_held, participant);
                    return Err(CoordinatorError::ChunkLockLimitReached);
                }
            }
            Participant::Verifier(_) => {
                if number_of_locks_held >= environment.verifier_lock_chunk_limit() {
                    trace!("{} chunks are locked by {}", &number_of_locks_held, participant);
                    return Err(CoordinatorError::ChunkLockLimitReached);
                }
            }
        };

        // Check that the participant is authorized to acquire the lock
        // associated with the given chunk ID for the current round,
        // and fetch the appropriate contribution locator.
        let locked_locators = match participant {
            Participant::Contributor(_) => {
                // Check that the participant is an authorized contributor
                // for the current round.
                if !self.is_contributor(participant) {
                    error!("{} is not an authorized contributor", participant);
                    return Err(CoordinatorError::UnauthorizedChunkContributor);
                }

                // Fetch the current round height.
                let current_round_height = self.round_height();
                // Fetch the current contribution ID.
                let current_contribution_id = self.chunk(chunk_id)?.current_contribution_id();
                // Fetch if this is the first round.
                let is_initial_round = current_round_height == 1;
                // Fetch if this is the initial contribution.
                let is_initial_contribution = current_contribution_id == 0;
                // Fetch the final contribution ID from the previous round.
                let previous_final_id = self.expected_number_of_contributions() - 1;
                // Fetch the previous contribution locator.
                let previous_contribution = match (is_initial_round, is_initial_contribution) {
                    // This is the initial contribution in the initial round, return the verified response from the previous round.
                    (true, true) => ContributionLocator::new(0, chunk_id, 0, true),
                    // This is the initial contribution in the chunk, return the final response from the previous round.
                    (false, true) => {
                        ContributionLocator::new(current_round_height - 1, chunk_id, previous_final_id, false)
                    }
                    // This is a typical contribution in the chunk, return the previous response from this round.
                    (true, false) | (false, false) => {
                        ContributionLocator::new(current_round_height, chunk_id, current_contribution_id - 1, false)
                    }
                };

                // Fetch the current contribution locator.
                let current_contribution =
                    ContributionLocator::new(current_round_height, chunk_id, current_contribution_id, true);

                // This call enforces a strict check that the
                // next contribution locator does NOT exist and
                // that the current contribution locator exists
                // and has already been verified.
                let next_contribution = self.next_contribution_locator(storage, chunk_id)?;

                // Fetch the contribution file signature locator.
                let next_contribution_file_signature =
                    self.next_contribution_file_signature_locator(storage, chunk_id)?;

                LockedLocators {
                    previous_contribution,
                    current_contribution,
                    next_contribution,
                    next_contribution_file_signature,
                }
            }
            Participant::Verifier(_) => {
                // Check that the participant is an authorized verifier
                // for the current round.
                if !self.is_verifier(participant) {
                    error!("{} is not an authorized verifier", participant);
                    return Err(CoordinatorError::UnauthorizedChunkVerifier);
                }

                // Fetch the current round height.
                let current_round_height = self.round_height();
                // Fetch the chunk corresponding to the given chunk ID.
                let chunk = self.chunk(chunk_id)?;
                // Fetch the current contribution ID.
                let current_contribution_id = chunk.current_contribution_id();

                if current_contribution_id == 0 {
                    return Err(CoordinatorError::ChunkCannotLockZeroContributions { chunk_id });
                }

                // Fetch the previous contribution locator.
                let previous_contribution =
                    ContributionLocator::new(current_round_height, chunk_id, current_contribution_id - 1, true);

                // This call enforces a strict check that the
                // current contribution locator exist and
                // has not been verified yet.
                let current_contribution = self.current_contribution_locator(storage, chunk_id, false)?;

                tracing::debug!("Obtained response locator {:?}", current_contribution);

                // Fetch whether this is the final contribution of the specified chunk.
                let is_final_contribution = chunk.only_contributions_complete(self.expected_number_of_contributions());
                // Fetch the next contribution locator and its contribution file signature locator.
                let (next_contribution, next_contribution_file_signature) = match is_final_contribution {
                    // This is the final contribution in the chunk.
                    true => (
                        ContributionLocator::new(current_round_height + 1, chunk_id, 0, true),
                        ContributionSignatureLocator::new(current_round_height + 1, chunk_id, 0, true),
                    ),
                    // This is a typical contribution in the chunk.
                    false => (
                        ContributionLocator::new(current_round_height, chunk_id, current_contribution_id, true),
                        ContributionSignatureLocator::new(
                            current_round_height,
                            chunk_id,
                            current_contribution_id,
                            true,
                        ),
                    ),
                };

                LockedLocators {
                    previous_contribution,
                    current_contribution,
                    next_contribution,
                    next_contribution_file_signature,
                }
            }
        };

        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = self.chunk(chunk_id)?;
        // Fetch the next contribution ID.
        let current_contribution = chunk.current_contribution()?;

        // As a corollary, if the current contribution locator exists
        // and the current contribution has not been verified yet,
        // check that the given participant is not a contributor.
        if !current_contribution.is_verified() && self.current_contribution_locator(storage, chunk_id, false).is_ok() {
            // Check that the given participant is not a contributor.
            if participant.is_contributor() {
                return Err(CoordinatorError::UnauthorizedChunkContributor);
            }
        }

        // Attempt to acquire the lock for the given participant ID.
        let expected_num_contributions = self.expected_number_of_contributions();
        self.chunk_mut(chunk_id)?
            .acquire_lock(participant.clone(), expected_num_contributions)?;

        // Initialize the next contribution locator.
        match participant {
            Participant::Contributor(_) => {
                // Initialize the unverified response file.
                storage.initialize(
                    Locator::ContributionFile(locked_locators.next_contribution.clone()),
                    Object::contribution_file_size(environment, chunk_id, false),
                )?;

                // Initialize the contribution file signature.
                storage.initialize(
                    Locator::ContributionFileSignature(locked_locators.next_contribution_file_signature.clone()),
                    Object::contribution_file_signature_size(false),
                )?;
            }
            Participant::Verifier(_) => {
                // Initialize the next challenge file.
                storage.initialize(
                    Locator::ContributionFile(locked_locators.next_contribution.clone()),
                    Object::contribution_file_size(environment, chunk_id, true),
                )?;

                // Initialize the contribution file signature.
                storage.initialize(
                    Locator::ContributionFileSignature(locked_locators.next_contribution_file_signature.clone()),
                    Object::contribution_file_signature_size(true),
                )?;
            }
        };

        debug!("{} locked chunk {}", participant, chunk_id);
        Ok(locked_locators)
    }

    ///
    /// Updates the contribution corresponding to a given chunk ID and
    /// contribution ID as verified.
    ///
    /// This function assumes the current contribution already has
    /// a verifier assigned to it.
    ///
    #[inline]
    pub(crate) fn verify_contribution(
        &mut self,
        chunk_id: u64,
        contribution_id: u64,
        participant: Participant,
        verified_locator: LocatorPath,
        verified_signature_locator: LocatorPath,
    ) -> Result<(), CoordinatorError> {
        // Set the current contribution as verified for the given chunk ID.
        self.chunk_mut(chunk_id)?.verify_contribution(
            contribution_id,
            participant,
            verified_locator,
            verified_signature_locator,
        )?;

        // If all chunks are complete and the finished at timestamp has not been set yet,
        // then set it with the current UTC timestamp.
        self.try_finish(Utc::now());

        Ok(())
    }

    ///
    /// Returns a mutable reference to the chunk, if it exists.
    /// Otherwise returns `None`.
    ///
    #[inline]
    pub(crate) fn chunk_mut(&mut self, chunk_id: u64) -> Result<&mut Chunk, CoordinatorError> {
        // Fetch the chunk with the given chunk ID.
        let chunk = match self.chunks.get_mut(chunk_id as usize) {
            Some(chunk) => chunk,
            _ => return Err(CoordinatorError::ChunkMissing),
        };

        // Check the ID in the chunk matches the given chunk ID.
        match chunk.chunk_id() == chunk_id {
            true => Ok(chunk),
            false => Err(CoordinatorError::ChunkIdMismatch),
        }
    }

    /// Remove a contributor from the round.
    pub(crate) fn remove_contributor_unsafe(
        &mut self,
        storage: &mut impl Storage,
        contributor: &Participant,
        locked_chunks: &[u64],
        tasks: &[Task],
    ) -> Result<(), CoordinatorError> {
        warn!("Removing locked chunks and all impacted contributions");

        // Remove the lock from the specified chunks.
        self.remove_locks_unsafe(storage, contributor, locked_chunks)?;
        warn!("Removed locked chunks");

        // Remove the contributions from the specified chunks.
        self.remove_chunk_contributions_unsafe(storage, contributor, tasks)?;
        warn!("Removed impacted contributions");

        self.contributor_ids = self
            .contributor_ids
            .clone()
            .into_iter()
            .filter(|participant| participant != contributor)
            .collect();

        Ok(())
    }

    ///
    /// Removes the locks for the current round from the given chunk IDs.
    ///
    /// If the given chunk IDs are not currently locked,
    /// this function will return a `CoordinatorError`.
    ///
    /// If the given participant is not the current lock holder of the given chunk IDs,
    /// this function will return a `CoordinatorError`.
    ///
    #[inline]
    pub(crate) fn remove_locks_unsafe(
        &mut self,
        storage: &mut impl Storage,
        participant: &Participant,
        locked_chunks: &[u64],
    ) -> Result<(), CoordinatorError> {
        // Sanity check that the participant holds the lock for each specified chunk.
        let locked_chunks: Vec<_> = locked_chunks
            .par_iter()
            .filter(|chunk_id| self.is_chunk_locked_by(**chunk_id, participant))
            .collect();

        trace!("Removing locks for chunks {:?} from {}", locked_chunks, participant);

        // Fetch the current round height.
        let current_round_height = self.round_height();
        // Fetch the expected number of contributions for the current round.
        let expected_number_of_contributions = self.expected_number_of_contributions();

        // Remove the response locator for a contributor, and remove the next challenge locator
        // for both a contributor and verifier.
        for chunk_id in locked_chunks.into_iter() {
            match participant {
                Participant::Contributor(_) => {
                    // Check that the participant is an *authorized* contributor
                    // for the current round.
                    if !self.is_contributor(participant) {
                        error!("{} is not an authorized contributor", participant);
                        return Err(CoordinatorError::UnauthorizedChunkContributor);
                    }
                }
                Participant::Verifier(_) => {
                    // Check that the participant is an *authorized* verifier
                    // for the current round.
                    if !self.is_verifier(participant) {
                        error!("{} is not an authorized verifier", participant);
                        return Err(CoordinatorError::UnauthorizedChunkVerifier);
                    }
                }
            };

            // Fetch the chunk corresponding to the given chunk ID.
            let chunk = self.chunk_mut(*chunk_id)?;

            if participant.is_contributor() {
                // Fetch the next contribution ID and remove the response locator.
                let next_contribution_id = chunk.next_contribution_id(expected_number_of_contributions)?;
                let response_locator = Locator::ContributionFile(ContributionLocator::new(
                    current_round_height,
                    *chunk_id,
                    next_contribution_id,
                    false,
                ));
                if storage.exists(&response_locator) {
                    storage.remove(&response_locator)?;
                }

                // Removing contribution file signature for pending task
                let response_signature_locator = Locator::ContributionFileSignature(ContributionSignatureLocator::new(
                    current_round_height,
                    *chunk_id,
                    next_contribution_id,
                    false,
                ));
                if storage.exists(&response_signature_locator) {
                    storage.remove(&response_signature_locator)?;
                }

                // Removing contribution file signature for verified task
                let response_signature_locator = Locator::ContributionFileSignature(ContributionSignatureLocator::new(
                    current_round_height,
                    *chunk_id,
                    next_contribution_id,
                    true,
                ));
                if storage.exists(&response_signature_locator) {
                    storage.remove(&response_signature_locator)?;
                }

                // TODO: revisit the logic of removing challenges
                //       https://github.com/AleoHQ/aleo-setup/issues/250
                // Remove the next challenge locator if the current response has been verified.
                //
                // TODO: check why is this calculating the locators again when
                // we already have them in strings in Contribution?
                if chunk.current_contribution()?.is_verified() {
                    // Fetch whether this is the final contribution of the specified chunk.
                    let is_final_contribution = chunk.only_contributions_complete(expected_number_of_contributions);
                    // Remove the next challenge locator.
                    let contribution_file = if is_final_contribution {
                        // This is the final contribution in the chunk.
                        Locator::ContributionFile(ContributionLocator::new(
                            current_round_height + 1,
                            *chunk_id,
                            0,
                            true,
                        ))
                    } else {
                        // This is a typical contribution in the chunk.
                        Locator::ContributionFile(ContributionLocator::new(
                            current_round_height,
                            *chunk_id,
                            chunk.current_contribution_id(),
                            true,
                        ))
                    };
                    // Don't remove initial challenge
                    if storage.exists(&contribution_file) && chunk.current_contribution()?.get_contributor().is_some() {
                        storage.remove(&contribution_file)?;
                    }
                }
            }

            warn!("Removing the lock for chunk {} from {}", chunk_id, participant);

            // Remove the lock for each given chunk ID.
            chunk.set_lock_holder_unsafe(None);
        }

        Ok(())
    }

    ///
    /// Removes the contributions from the current round from the
    /// given (chunk ID, contribution ID) tasks.
    ///
    ///
    /// If the given (chunk ID, contribution ID) tasks are not
    /// currently locked, this function will return a
    /// `CoordinatorError`.
    ///
    /// If the given participant is not the current lock holder of the
    /// given chunk IDs, this function will return a
    /// `CoordinatorError`.
    ///
    #[tracing::instrument(
        level = "error",
        skip(self, storage, tasks),
        fields(round = self.round_height())
    )]
    pub(crate) fn remove_chunk_contributions_unsafe(
        &mut self,
        storage: &mut impl Storage,
        participant: &Participant,
        tasks: &[Task],
    ) -> Result<(), CoordinatorError> {
        // Check if the participant is a verifier. As verifications are not dependent
        // on each other, no further update is necessary in the round state.
        if participant.is_verifier() {
            warn!("Skipping removal of contributions as {} is a verifier", participant);
            return Ok(());
        }

        // Check that the participant is in the current contributors ID.
        if self.contributor_ids.par_iter().filter(|p| **p == *participant).count() != 1 {
            error!("Missing contributor (to drop) in current contributors of coordinator state");
            return Err(CoordinatorError::RoundContributorMissing);
        }

        // Remove the given contribution from each chunk in the current round.
        for task in tasks {
            let chunk = self.chunk_mut(task.chunk_id())?;
            if let Ok(contribution) = chunk.get_contribution(task.contribution_id()) {
                warn!("Removing task {:?}", task.to_tuple());

                // Remove the unverified contribution file, if it exists.
                if let Some(locator) = contribution.get_contributed_location() {
                    let path = storage.to_locator(&locator)?;
                    if storage.exists(&path) {
                        storage.remove(&path)?;
                    }
                }

                // Remove the contribution signature file, if it exists.
                if let Some(locator) = contribution.get_contributed_signature_location() {
                    let path = storage.to_locator(&locator)?;
                    if storage.exists(&path) {
                        storage.remove(&path)?;
                    }
                }

                // Remove the verified contribution file, if it exists.
                if let Some(locator) = contribution.get_verified_location() {
                    let path = storage.to_locator(&locator)?;
                    if storage.exists(&path) {
                        storage.remove(&path)?;
                    }
                }

                // Remove the verified contribution file signature, if it exists.
                if let Some(locator) = contribution.get_verified_signature_location() {
                    let path = storage.to_locator(&locator)?;
                    if storage.exists(&path) {
                        storage.remove(&path)?;
                    }
                }

                // Remove the given contribution and all subsequent contributions.
                for contribution_id in task.contribution_id()..(chunk.get_contributions().len() as u64) {
                    chunk.remove_contribution_unsafe(contribution_id);
                }
            } else {
                warn!(
                    "Skipping removal of chunk {} contribution {} because it does not exist in the chunk {:#?}",
                    task.chunk_id(),
                    task.contribution_id(),
                    chunk,
                );
            }
        }

        Ok(())
    }

    ///
    /// Adds a replacement contributor from the given environment into the round contributor IDs.
    ///
    #[inline]
    pub(crate) fn add_replacement_contributor_unsafe(
        &mut self,
        participant: Participant,
    ) -> Result<(), CoordinatorError> {
        // Check that the participant is a contributor.
        if !participant.is_contributor() {
            error!("Failed to add {} as a replacement contributor", participant);
            return Err(CoordinatorError::ExpectedContributor);
        }

        // Add in a replacement contributor to the set of contributor IDs, if one is available.
        self.contributor_ids.push(participant.clone());
        warn!("Added replacement contributor {} to round {}", participant, self.height);

        Ok(())
    }

    ///
    /// If all chunks are complete and the finished at timestamp has not been set yet,
    /// then set it with the current UTC timestamp.
    ///
    #[inline]
    pub(crate) fn try_finish(&mut self, timestamp: DateTime<Utc>) {
        if self.is_complete() && self.finished_at.is_none() {
            self.finished_at = Some(timestamp);
        }
    }

    ///
    /// If all chunks are complete, then set it with the current UTC timestamp.
    ///
    #[cfg(test)]
    #[inline]
    pub(crate) fn try_finish_testing_only_unsafe(&mut self, timestamp: DateTime<Utc>) {
        if self.is_complete() {
            warn!("Modifying finished_at timestamp for testing only");
            self.finished_at = Some(timestamp);
            warn!("Modified finished_at timestamp for testing only");
        }
    }

    /// Reset this round back to its initial state before
    /// contributions started, and remove any
    /// contributions/verifications that have been made since the
    /// start of the round. Returns a vector of actions to perform on
    /// the [crate::storage::Storage] to reflect the changes to the
    /// round state. `remove_participants` is a list of participants
    /// to remove from the round.
    pub(crate) fn reset(&mut self, remove_participants: &[Participant]) -> Vec<StorageAction> {
        let mut actions: Vec<StorageAction> = self
            .chunks
            .iter_mut()
            .flat_map(|chunk| {
                let contributions_remove: Vec<(u64, Vec<RemoveAction>)> = chunk.get_contributions()
                .iter()
                .filter(|(id, _)| **id != 0) // don't remove initial challenge
                .map(|(id, contribution)| {
                    let actions: Vec<RemoveAction> = contribution.get_locators()
                        .into_iter()
                        .map(|path| RemoveAction::new(path))
                        .collect();
                    (*id, actions)
                })
                .collect();

                chunk.set_lock_holder_unsafe(None);

                let actions: Vec<StorageAction> = contributions_remove
                    .into_iter()
                    .flat_map(|(contribution_id, actions)| {
                        chunk.remove_contribution_unsafe(contribution_id);
                        actions.into_iter()
                    })
                    .map(StorageAction::Remove)
                    .collect();

                actions.into_iter()
            })
            .collect();

        // Remove the requested participants from the set of contributor IDs.
        self.contributor_ids = self
            .contributor_ids
            .par_iter()
            .cloned()
            .filter(|c| remove_participants.iter().find(|p| p == &c).is_none())
            .collect();

        // Remove the requested participants from the set of verifier IDs.
        self.verifier_ids = self
            .verifier_ids
            .par_iter()
            .cloned()
            .filter(|v| remove_participants.iter().find(|p| p == &v).is_none())
            .collect();

        actions.push(StorageAction::Update(UpdateAction {
            locator: Locator::RoundState {
                round_height: self.height,
            },
            object: Object::RoundState(self.clone()), // PERFORMANCE: clone here is not great for performance
        }));

        actions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::prelude::*;

    #[test]
    #[serial]
    fn test_round_0_matches() {
        initialize_test_environment(&TEST_ENVIRONMENT);

        // Define test storage.
        let mut storage = test_storage(&TEST_ENVIRONMENT);

        let expected = test_round_0().unwrap();
        let candidate = Round::new(
            &TEST_ENVIRONMENT,
            &mut storage,
            0, /* height */
            *TEST_STARTED_AT,
            vec![],
            TEST_VERIFIER_IDS.to_vec(),
        )
        .unwrap();

        if candidate != expected {
            print_diff(&expected, &candidate);
        }
        assert_eq!(candidate, expected);
    }

    #[test]
    #[serial]
    fn test_round_height() {
        initialize_test_environment(&TEST_ENVIRONMENT);

        let round_0 = test_round_0_json().unwrap();
        assert_eq!(0, round_0.round_height());

        let round_0 = test_round_0().unwrap();
        assert_eq!(0, round_0.round_height());

        let round_1 = test_round_1_initial_json().unwrap();
        assert_eq!(1, round_1.round_height());
    }

    #[test]
    #[serial]
    fn test_reset_partial() {
        initialize_test_environment(&TEST_ENVIRONMENT);

        let mut round_1: Round = test_round_1_partial_json().unwrap();
        assert!(round_1.is_contributor(&TEST_CONTRIBUTOR_ID_2));
        assert!(round_1.is_contributor(&TEST_CONTRIBUTOR_ID_3));
        assert!(round_1.is_verifier(&TEST_VERIFIER_ID_2));
        assert!(round_1.is_verifier(&TEST_VERIFIER_ID_3));

        let n_contributions = 89;
        let n_verifications = 30;
        let n_files = 2 * n_contributions + 2 * n_verifications;
        let n_actions = n_files + 1; // include action to update round

        let actions = round_1.reset(&[TEST_CONTRIBUTOR_ID_2.clone()]);
        assert_eq!(n_actions, actions.len());

        assert_eq!(64, round_1.chunks().len());

        for chunk in round_1.chunks() {
            assert!(!chunk.is_locked());
            assert_eq!(1, chunk.get_contributions().len());
            let contribution = chunk.get_contribution(0).unwrap();
            assert!(contribution.is_verified());
        }

        assert!(!round_1.is_contributor(&*TEST_CONTRIBUTOR_ID_2));
        assert!(round_1.is_contributor(&*TEST_CONTRIBUTOR_ID_3));
        assert!(round_1.is_verifier(&*TEST_VERIFIER_ID_2));
        assert!(round_1.is_verifier(&*TEST_VERIFIER_ID_3));
    }

    #[test]
    #[serial]
    fn test_is_authorized_contributor() {
        initialize_test_environment(&TEST_ENVIRONMENT);

        let round_1 = test_round_1_initial_json().unwrap();
        assert!(round_1.is_contributor(&TEST_CONTRIBUTOR_ID));
    }

    #[test]
    #[serial]
    fn test_is_authorized_verifier() {
        initialize_test_environment(&TEST_ENVIRONMENT);

        let round_0 = test_round_0().unwrap();
        assert!(round_0.is_verifier(&TEST_VERIFIER_ID));

        let round_1 = test_round_1_initial_json().unwrap();
        assert!(round_1.is_contributor(&TEST_CONTRIBUTOR_ID));
    }

    #[test]
    #[serial]
    fn test_get_chunk() {
        initialize_test_environment(&TEST_ENVIRONMENT);

        let expected = test_round_0_json().unwrap().chunks.get(0).unwrap().clone();
        let candidate = test_round_0().unwrap().chunk(0).unwrap().clone();
        print_diff(&expected, &candidate);
        assert_eq!(expected, candidate);
    }

    #[test]
    #[serial]
    fn test_get_chunk_mut_basic() {
        initialize_test_environment(&TEST_ENVIRONMENT);

        let expected = test_round_0_json().unwrap().chunks.get(0).unwrap().clone();
        let candidate = test_round_0().unwrap().chunk_mut(0).unwrap().clone();
        print_diff(&expected, &candidate);
        assert_eq!(expected, candidate);
    }

    #[test]
    #[serial]
    fn test_get_verifiers() {
        initialize_test_environment(&TEST_ENVIRONMENT);

        let candidates = test_round_0().unwrap().verifiers().clone();
        assert_eq!(TEST_VERIFIER_IDS.len(), candidates.len());
        for id in TEST_VERIFIER_IDS.iter() {
            assert!(candidates.contains(id));
        }
    }

    #[test]
    #[serial]
    fn test_is_complete() {
        initialize_test_environment(&TEST_ENVIRONMENT);

        // TODO (howardwu): Add tests for a full completeness check.
        let round_0 = test_round_0_json().unwrap();
        assert!(round_0.is_complete());

        let round_0 = test_round_0().unwrap();
        assert!(round_0.is_complete());

        let round_1 = test_round_1_initial_json().unwrap();
        assert!(!round_1.is_complete());
    }
}
