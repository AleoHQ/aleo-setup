use crate::{
    environment::Environment,
    objects::{
        participant::*,
        task::{initialize_tasks, Task},
    },
    storage::{Locator, Object, StorageLock},
    CoordinatorError,
    TimeSource,
};
use phase1::ProvingSystem;

use chrono::{DateTime, Duration, Utc};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, LinkedList},
    iter::FromIterator,
};
use tracing::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) enum CoordinatorStatus {
    Initializing,
    Initialized,
    Precommit,
    Commit,
    Rollback,
}

/// Represents a participant's exclusive lock on a chunk with the
/// specified `chunk_id`, which was obtained at the specified
/// `lock_time`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkLock {
    /// The id of the chunk which is locked.
    chunk_id: u64,
    /// The time that the chunk was locked.
    lock_time: DateTime<Utc>,
}

impl ChunkLock {
    /// Create a new chunk lock for the specified `chunk_id`, and
    /// recording the `lock_time` using the specified `time` source.
    pub fn new(chunk_id: u64, time: &dyn TimeSource) -> Self {
        Self {
            chunk_id,
            lock_time: time.utc_now(),
        }
    }

    /// The id of the chunk which is locked.
    pub fn chunk_id(&self) -> u64 {
        self.chunk_id
    }

    /// The time that the chunk was locked.
    pub fn lock_time(&self) -> &DateTime<Utc> {
        &self.lock_time
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParticipantInfo {
    /// The ID of the participant.
    id: Participant,
    /// The round height that this participant is contributing to.
    round_height: u64,
    /// The reliability of the participant from an initial calibration.
    reliability: u8,
    /// The bucket ID that this participant starts contributing from.
    bucket_id: u64,
    /// The timestamp of the first seen instance of this participant.
    first_seen: DateTime<Utc>,
    /// The timestamp of the last seen instance of this participant.
    last_seen: DateTime<Utc>,
    /// The timestamp when this participant started the round.
    started_at: Option<DateTime<Utc>>,
    /// The timestamp when this participant finished the round.
    finished_at: Option<DateTime<Utc>>,
    /// The timestamp when this participant was dropped from the round.
    dropped_at: Option<DateTime<Utc>>,
    /// A map of chunk IDs to locks on chunks that this participant currently holds.
    locked_chunks: HashMap<u64, ChunkLock>,
    /// The list of (chunk ID, contribution ID) tasks that this participant is assigned to compute.
    assigned_tasks: LinkedList<Task>,
    /// The list of (chunk ID, contribution ID) tasks that this participant is currently computing.
    pending_tasks: LinkedList<Task>,
    /// The list of (chunk ID, contribution ID) tasks that this participant finished computing.
    completed_tasks: LinkedList<Task>,
    /// The list of (chunk ID, contribution ID) tasks that are pending disposal while computing.
    disposing_tasks: LinkedList<Task>,
    /// The list of (chunk ID, contribution ID) tasks that are disposed of while computing.
    disposed_tasks: LinkedList<Task>,
}

impl ParticipantInfo {
    #[inline]
    fn new(
        participant: Participant,
        round_height: u64,
        reliability: u8,
        bucket_id: u64,
        time: &dyn TimeSource,
    ) -> Self {
        // Fetch the current time.
        let now = time.utc_now();
        Self {
            id: participant,
            round_height,
            reliability,
            bucket_id,
            first_seen: now,
            last_seen: now,
            started_at: None,
            finished_at: None,
            dropped_at: None,
            locked_chunks: HashMap::new(),
            assigned_tasks: LinkedList::new(),
            pending_tasks: LinkedList::new(),
            completed_tasks: LinkedList::new(),
            disposing_tasks: LinkedList::new(),
            disposed_tasks: LinkedList::new(),
        }
    }

    ///
    /// Returns the ID of this participant.
    ///
    pub fn id(&self) -> &Participant {
        &self.id
    }

    ///
    /// Returns the set of chunk IDs that this participant is computing.
    ///
    pub fn locked_chunks(&self) -> &HashMap<u64, ChunkLock> {
        &self.locked_chunks
    }

    ///
    /// Returns the list of (chunk ID, contribution ID) tasks that this participant is assigned to compute.
    ///
    pub fn assigned_tasks(&self) -> &LinkedList<Task> {
        &self.assigned_tasks
    }

    ///
    /// Returns the list of (chunk ID, contribution ID) tasks that this participant is currently computing.
    ///
    pub fn pending_tasks(&self) -> &LinkedList<Task> {
        &self.pending_tasks
    }

    ///
    /// Returns the list of (chunk ID, contribution ID) tasks that this participant finished computing.
    ///
    pub fn completed_tasks(&self) -> &LinkedList<Task> {
        &self.completed_tasks
    }

    ///
    /// Returns the list of (chunk ID, contribution ID) tasks that are pending disposal while computing.
    ///
    pub fn disposing_tasks(&self) -> &LinkedList<Task> {
        &self.disposing_tasks
    }

    ///
    /// Returns the list of (chunk ID, contribution ID) tasks that are disposed of while computing.
    ///
    pub fn disposed_tasks(&self) -> &LinkedList<Task> {
        &self.disposed_tasks
    }

    ///
    /// Returns `true` if the participant is dropped from the current round.
    ///
    #[inline]
    fn is_dropped(&self) -> bool {
        // Check that the participant has not already finished the round.
        if self.is_finished() {
            return false;
        }

        // Check if the participant was dropped from the round.
        self.dropped_at.is_some()
    }

    ///
    /// Returns `true` if the participant is finished with the current round.
    ///
    #[inline]
    fn is_finished(&self) -> bool {
        // Check that the participant already started in the round.
        if self.started_at.is_none() {
            return false;
        }

        // Check that the participant was not dropped from the round.
        if self.dropped_at.is_some() {
            return false;
        }

        // Check that the participant has no more locked chunks.
        if !self.locked_chunks.is_empty() {
            return false;
        }

        // Check that the participant has no more assigned tasks.
        if !self.assigned_tasks.is_empty() {
            return false;
        }

        // Check that the participant has no more pending tasks.
        if !self.pending_tasks.is_empty() {
            return false;
        }

        // Check that the participant is not disposing tasks.
        if !self.disposing_tasks.is_empty() {
            return false;
        }

        // Check that if the participant is a contributor, that they completed tasks.
        if self.id.is_contributor() && self.completed_tasks.is_empty() {
            return false;
        }

        // Check if the participant has finished the round.
        self.finished_at.is_some()
    }

    ///
    /// Assigns the participant to the given chunks for the current round,
    /// and sets the start time as the current time.
    ///
    #[inline]
    fn start(&mut self, tasks: LinkedList<Task>, time: &dyn TimeSource) -> Result<(), CoordinatorError> {
        trace!("Starting {}", self.id);

        // Check that the participant has a valid round height set.
        if self.round_height == 0 {
            return Err(CoordinatorError::ParticipantRoundHeightInvalid);
        }

        // Check that the participant has not already started in the round.
        if self.started_at.is_some() || self.dropped_at.is_some() || self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyStarted);
        }

        // Check that the participant has no locked chunks.
        if !self.locked_chunks.is_empty() {
            return Err(CoordinatorError::ParticipantAlreadyHasLockedChunks);
        }

        // Check that the participant has no assigned tasks.
        if !self.assigned_tasks.is_empty() {
            return Err(CoordinatorError::ParticipantHasAssignedTasks);
        }

        // Check that the participant has no pending tasks.
        if !self.pending_tasks.is_empty() {
            return Err(CoordinatorError::ParticipantHasRemainingTasks);
        }

        // Check that the participant has not completed any tasks yet.
        if !self.completed_tasks.is_empty() {
            return Err(CoordinatorError::ParticipantAlreadyStarted);
        }

        // Check that the participant is not disposing tasks.
        if !self.disposing_tasks.is_empty() {
            return Err(CoordinatorError::ParticipantAlreadyStarted);
        }

        // Check that the participant has not discarded any tasks yet.
        if !self.disposed_tasks.is_empty() {
            return Err(CoordinatorError::ParticipantAlreadyStarted);
        }

        // Fetch the current time.
        let now = time.utc_now();

        // Update the last seen time.
        self.last_seen = now;

        // Set the start time to reflect the current time.
        self.started_at = Some(now);

        // Set the assigned tasks to the given tasks.
        self.assigned_tasks = tasks;

        Ok(())
    }

    ///
    /// Adds the given (chunk ID, contribution ID) task in FIFO order for the participant to process.
    ///
    #[inline]
    fn push_back_task(&mut self, task: Task, time: &dyn TimeSource) -> Result<(), CoordinatorError> {
        trace!("Pushing back task for {}", self.id);

        // Check that the participant has started in the round.
        if self.started_at.is_none() {
            return Err(CoordinatorError::ParticipantHasNotStarted);
        }

        // Check that the participant was not dropped from the round.
        if self.dropped_at.is_some() {
            return Err(CoordinatorError::ParticipantWasDropped);
        }

        // Check that the participant has not finished the round.
        if self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyFinished);
        }

        // Check that if the participant is a contributor, this chunk is not currently locked.
        if self.id.is_contributor() && self.locked_chunks.contains_key(&task.chunk_id()) {
            return Err(CoordinatorError::ParticipantAlreadyWorkingOnChunk {
                chunk_id: task.chunk_id(),
            });
        }

        // Check that the task was not already given the assigned task.
        if self.assigned_tasks.contains(&task) {
            return Err(CoordinatorError::ParticipantAlreadyAddedChunk);
        }

        // Check that the task was not already in progress.
        if self.pending_tasks.contains(&task) {
            return Err(CoordinatorError::ParticipantAlreadyWorkingOnChunk {
                chunk_id: task.chunk_id(),
            });
        }

        // Check that the participant has not already completed the task.
        if self.completed_tasks.contains(&task) {
            return Err(CoordinatorError::ParticipantAlreadyFinishedChunk {
                chunk_id: task.chunk_id(),
            });
        }

        // Update the last seen time.
        self.last_seen = time.utc_now();

        // Add the task to the back of the pending tasks.
        self.assigned_tasks.push_back(task);

        Ok(())
    }

    ///
    /// Adds the given (chunk ID, contribution ID) task in LIFO order for the participant to process.
    ///
    #[inline]
    fn push_front_task(&mut self, task: Task, time: &dyn TimeSource) -> Result<(), CoordinatorError> {
        trace!("Pushing front task for {}", self.id);

        // Check that the participant has started in the round.
        if self.started_at.is_none() {
            return Err(CoordinatorError::ParticipantHasNotStarted);
        }

        // Check that the participant was not dropped from the round.
        if self.dropped_at.is_some() {
            return Err(CoordinatorError::ParticipantWasDropped);
        }

        // Check that the participant has not finished the round.
        if self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyFinished);
        }

        // Check that if the participant is a contributor, this chunk is not currently locked.
        if self.id.is_contributor() && self.locked_chunks.contains_key(&task.chunk_id()) {
            return Err(CoordinatorError::ParticipantAlreadyWorkingOnChunk {
                chunk_id: task.chunk_id(),
            });
        }

        // Check that the task was not already given the assigned task.
        if self.assigned_tasks.contains(&task) {
            return Err(CoordinatorError::ParticipantAlreadyAddedChunk);
        }

        // Check that the task was not already added to the pending tasks.
        if self.pending_tasks.contains(&task) {
            return Err(CoordinatorError::ParticipantAlreadyWorkingOnChunk {
                chunk_id: task.chunk_id(),
            });
        }

        // Check that the participant has not already completed the task.
        if self.completed_tasks.contains(&task) {
            return Err(CoordinatorError::ParticipantAlreadyFinishedChunk {
                chunk_id: task.chunk_id(),
            });
        }

        // Update the last seen time.
        self.last_seen = time.utc_now();

        // Add the task to the front of the pending tasks.
        self.assigned_tasks.push_front(task);

        Ok(())
    }

    ///
    /// Pops the next (chunk ID, contribution ID) task the participant should process,
    /// in FIFO order when added to the linked list.
    ///
    #[inline]
    fn pop_task(&mut self, time: &dyn TimeSource) -> Result<Task, CoordinatorError> {
        trace!("Popping task for {}", self.id);

        // Check that the participant has started in the round.
        if self.started_at.is_none() {
            return Err(CoordinatorError::ParticipantHasNotStarted);
        }

        // Check that the participant was not dropped from the round.
        if self.dropped_at.is_some() {
            return Err(CoordinatorError::ParticipantWasDropped);
        }

        // Check that the participant has not finished the round.
        if self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyFinished);
        }

        // Check that the participant has assigned tasks.
        if self.assigned_tasks.is_empty() {
            return Err(CoordinatorError::ParticipantHasNoRemainingTasks);
        }

        // Update the last seen time.
        self.last_seen = time.utc_now();

        // Fetch the next task in order as stored.
        match self.assigned_tasks.pop_front() {
            Some(task) => {
                // Add the task to the front of the pending tasks.
                self.pending_tasks.push_back(task);

                Ok(task)
            }
            None => Err(CoordinatorError::ParticipantHasNoRemainingTasks),
        }
    }

    ///
    /// Adds the given chunk ID to the locked chunks held by this participant.
    ///
    #[inline]
    fn acquired_lock(&mut self, chunk_id: u64, time: &dyn TimeSource) -> Result<(), CoordinatorError> {
        trace!("Acquiring lock on chunk {} for {}", chunk_id, self.id);

        // Check that the participant has started in the round.
        if self.started_at.is_none() {
            return Err(CoordinatorError::ParticipantHasNotStarted);
        }

        // Check that the participant was not dropped from the round.
        if self.dropped_at.is_some() {
            return Err(CoordinatorError::ParticipantWasDropped);
        }

        // Check that the participant has not finished the round.
        if self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyFinished);
        }

        // Check that this chunk is not currently locked by the participant.
        if self.locked_chunks.contains_key(&chunk_id) {
            return Err(CoordinatorError::ParticipantAlreadyHasLockedChunk);
        }

        // Check that if the participant is a contributor, this chunk was popped and already pending.
        if self.id.is_contributor()
            && self
                .pending_tasks
                .par_iter()
                .filter(|task| task.contains(chunk_id))
                .count()
                == 0
        {
            return Err(CoordinatorError::ParticipantUnauthorizedForChunkId { chunk_id });
        }

        // Check that if the participant is a contributor, this chunk was not already completed.
        if self.id.is_contributor()
            && self
                .completed_tasks
                .par_iter()
                .filter(|task| task.contains(chunk_id))
                .count()
                > 0
        {
            return Err(CoordinatorError::ParticipantAlreadyFinishedChunk { chunk_id });
        }

        // Update the last seen time.
        self.last_seen = time.utc_now();

        let chunk_lock = ChunkLock::new(chunk_id, time);

        self.locked_chunks.insert(chunk_id, chunk_lock);

        Ok(())
    }

    ///
    /// Reverts the given (chunk ID, contribution ID) task to the list of assigned tasks
    /// from the list of pending tasks.
    ///
    /// This function is used to move a pending task back as an assigned task when the
    /// participant fails to acquire the lock for the chunk ID corresponding to the task.
    ///
    #[inline]
    fn rollback_pending_task(&mut self, task: Task, time: &dyn TimeSource) -> Result<(), CoordinatorError> {
        trace!("Rolling back pending task on chunk {} for {}", task.chunk_id(), self.id);

        // Check that the participant has started in the round.
        if self.started_at.is_none() {
            return Err(CoordinatorError::ParticipantHasNotStarted);
        }

        // Check that the participant was not dropped from the round.
        if self.dropped_at.is_some() {
            return Err(CoordinatorError::ParticipantWasDropped);
        }

        // Check that the participant has not finished the round.
        if self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyFinished);
        }

        // Check that this chunk is not currently locked by the participant.
        if self.locked_chunks.contains_key(&task.chunk_id()) {
            return Err(CoordinatorError::ParticipantAlreadyHasLockedChunk);
        }

        // Check that if the participant is a contributor, this chunk was popped and already pending.
        if self.id.is_contributor()
            && self
                .pending_tasks
                .par_iter()
                .filter(|t| t.contains(task.chunk_id()))
                .count()
                == 0
        {
            return Err(CoordinatorError::ParticipantUnauthorizedForChunkId {
                chunk_id: task.chunk_id(),
            });
        }

        // Check that if the participant is a contributor, this chunk was not already completed.
        if self.id.is_contributor()
            && self
                .completed_tasks
                .par_iter()
                .filter(|t| t.contains(task.chunk_id()))
                .count()
                > 0
        {
            return Err(CoordinatorError::ParticipantAlreadyFinishedChunk {
                chunk_id: task.chunk_id(),
            });
        }

        // Update the last seen time.
        self.last_seen = time.utc_now();

        // Remove the task from the pending tasks.
        self.pending_tasks = self
            .pending_tasks
            .clone()
            .into_par_iter()
            .filter(|t| *t != task)
            .collect();

        // Add the task to the front of the assigned tasks.
        self.push_front_task(task, time)?;

        Ok(())
    }

    ///
    /// Adds the given [Task] to the list of completed tasks and
    /// removes the given chunk ID from the locked chunks held by this
    /// participant.
    ///
    fn completed_task(&mut self, task: Task, time: &dyn TimeSource) -> Result<(), CoordinatorError> {
        trace!("Completing task for {}", self.id);

        // Check that the participant has started in the round.
        if self.started_at.is_none() {
            return Err(CoordinatorError::ParticipantHasNotStarted);
        }

        // Check that the participant was not dropped from the round.
        if self.dropped_at.is_some() {
            return Err(CoordinatorError::ParticipantWasDropped);
        }

        // Check that the participant has not finished the round.
        if self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyFinished);
        }

        // Check that the participant had locked this chunk.
        if !self.locked_chunks.contains_key(&task.chunk_id()) {
            return Err(CoordinatorError::ParticipantDidntLockChunkId);
        }

        // Check that the participant does not have a assigned task remaining for this.
        if self.assigned_tasks.contains(&task) {
            return Err(CoordinatorError::ParticipantStillHasTaskAsAssigned);
        }

        // Check that the participant has a pending task for this.
        if !self.pending_tasks.contains(&task) {
            return Err(CoordinatorError::ParticipantMissingPendingTask { pending_task: task });
        }

        // Check that the participant has not already completed the task.
        if self.completed_tasks.contains(&task) {
            return Err(CoordinatorError::ParticipantAlreadyFinishedTask(task));
        }

        // Check that if the participant is a contributor, this chunk was not already completed.
        if self.id.is_contributor()
            && self
                .completed_tasks
                .par_iter()
                .filter(|t| t.contains(task.chunk_id()))
                .count()
                > 0
        {
            return Err(CoordinatorError::ParticipantAlreadyFinishedChunk {
                chunk_id: task.chunk_id(),
            });
        }

        // Update the last seen time.
        self.last_seen = time.utc_now();

        // Remove the given chunk ID from the locked chunks.
        self.locked_chunks.remove(&task.chunk_id());

        // Remove the task from the pending tasks.
        self.pending_tasks = self
            .pending_tasks
            .clone()
            .into_par_iter()
            .filter(|t| *t != task)
            .collect();

        // Add the task to the completed tasks.
        self.completed_tasks.push_back(task);

        Ok(())
    }

    ///
    /// Completes the disposal of a given chunk (chunk ID, contribution ID) task present in the `disposing_tasks` list to the list of disposed tasks
    /// and removes the given chunk ID from the locked chunks held by this participant.
    ///
    #[inline]
    fn dispose_task(
        &mut self,
        chunk_id: u64,
        contribution_id: u64,
        time: &dyn TimeSource,
    ) -> Result<(), CoordinatorError> {
        trace!("Disposed task for {}", self.id);

        // Set the task as the given chunk ID and contribution ID.
        let task = Task::new(chunk_id, contribution_id);

        // Check that the participant has started in the round.
        if self.started_at.is_none() {
            return Err(CoordinatorError::ParticipantHasNotStarted);
        }

        // Check that the participant was not dropped from the round.
        if self.dropped_at.is_some() {
            return Err(CoordinatorError::ParticipantWasDropped);
        }

        // Check that the participant has not finished the round.
        if self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyFinished);
        }

        // Check that the participant had locked this chunk.
        if !self.locked_chunks.contains_key(&chunk_id) {
            return Err(CoordinatorError::ParticipantDidntLockChunkId);
        }

        // TODO (raychu86): Reevaluate this check. When a participant is dropped, all tasks
        //  are reassigned so the tasks will always be present.
        // Check that the participant does not have a assigned task remaining for this.
        // if self.assigned_tasks.contains(&task) {
        //     return Err(CoordinatorError::ParticipantStillHasTaskAsAssigned);
        // }

        // Check that the participant has a disposing task for this.
        if !self.disposing_tasks.contains(&task) {
            return Err(CoordinatorError::ParticipantMissingDisposingTask);
        }

        // Update the last seen time.
        self.last_seen = time.utc_now();

        // Remove the given chunk ID from the locked chunks.
        self.locked_chunks.remove(&chunk_id);

        // Remove the task from the disposing tasks.
        self.disposing_tasks = self
            .disposing_tasks
            .clone()
            .into_par_iter()
            .filter(|t| *t != task)
            .collect();

        // Add the task to the completed tasks.
        self.disposed_tasks.push_back(task);

        Ok(())
    }

    ///
    /// Sets the participant to dropped and saves the current time as the dropped time.
    ///
    #[inline]
    fn drop(&mut self, time: &dyn TimeSource) -> Result<(), CoordinatorError> {
        trace!("Dropping {}", self.id);

        // Check that the participant was not already dropped from the round.
        if self.dropped_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyDropped);
        }

        // Check that the participant has not already finished the round.
        if self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyFinished);
        }

        // Fetch the current time.
        let now = time.utc_now();

        // Set the participant info to reflect them dropping now.
        self.dropped_at = Some(now);

        Ok(())
    }

    ///
    /// Sets the participant to finished and saves the current time as the completed time.
    ///
    #[inline]
    fn finish(&mut self, time: &dyn TimeSource) -> Result<(), CoordinatorError> {
        trace!("Finishing {}", self.id);

        // Check that the participant already started in the round.
        if self.started_at.is_none() {
            return Err(CoordinatorError::ParticipantHasNotStarted);
        }

        // Check that the participant was not dropped from the round.
        if self.dropped_at.is_some() {
            return Err(CoordinatorError::ParticipantWasDropped);
        }

        // Check that the participant has not already finished the round.
        if self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyFinished);
        }

        // Check that the participant has no more locked chunks.
        if !self.locked_chunks.is_empty() {
            return Err(CoordinatorError::ParticipantStillHasLocks);
        }

        // Check that the participant has no more assigned tasks.
        if !self.assigned_tasks.is_empty() {
            return Err(CoordinatorError::ParticipantHasRemainingTasks);
        }

        // Check that the participant has no more pending tasks.
        if !self.pending_tasks.is_empty() {
            return Err(CoordinatorError::ParticipantHasRemainingTasks);
        }

        // Check that if the participant is a contributor, that they completed tasks.
        if self.id.is_contributor() && self.completed_tasks.is_empty() {
            return Err(CoordinatorError::ParticipantDidNotDoWork);
        }

        // Check that the participant is not disposing tasks.
        if !self.disposing_tasks.is_empty() {
            return Err(CoordinatorError::ParticipantHasRemainingTasks);
        }

        // Fetch the current time.
        let now = time.utc_now();

        // Update the last seen time.
        self.last_seen = now;

        // Set the finish time to reflect the current time.
        self.finished_at = Some(now);

        Ok(())
    }

    ///
    /// Resets the participant information.
    ///
    #[deprecated]
    #[allow(dead_code)]
    #[inline]
    fn reset(&mut self, time: &dyn TimeSource) {
        warn!("Resetting the state of participant {}", self.id);
        *self = Self::new(self.id.clone(), self.round_height, self.reliability, 0, time);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoundMetrics {
    /// The number of contributors participating in the current round.
    number_of_contributors: u64,
    /// The number of verifiers participating in the current round.
    number_of_verifiers: u64,
    /// The boolean for denoting if the current round has been aggregated by the coordinator.
    is_round_aggregated: bool,
    /// The map of participants to their tasks and corresponding start and end timers.
    task_timer: HashMap<Participant, HashMap<Task, (i64, Option<i64>)>>,
    /// The map of participants to their average seconds per task.
    seconds_per_task: HashMap<Participant, u64>,
    /// The average seconds per task calculated from all current contributors.
    contributor_average_per_task: Option<u64>,
    /// The average seconds per task calculated from all current verifiers.
    verifier_average_per_task: Option<u64>,
    /// The timestamp when the coordinator started aggregation of the current round.
    started_aggregation_at: Option<DateTime<Utc>>,
    /// The timestamp when the coordinator finished aggregation of the current round.
    finished_aggregation_at: Option<DateTime<Utc>>,
    /// The estimated number of seconds remaining for the current round to finish.
    estimated_finish_time: Option<u64>,
    /// The estimated number of seconds remaining for the current round to aggregate.
    estimated_aggregation_time: Option<u64>,
    /// The estimated number of seconds remaining until the queue is closed for the next round.
    estimated_wait_time: Option<u64>,
    /// The timestamp of the earliest start time for the next round.
    next_round_after: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorState {
    /// The parameters and settings of this coordinator.
    environment: Environment,
    /// The current status of the coordinator.
    status: CoordinatorStatus,
    /// The map of queue participants with a reliability score and an assigned future round.
    queue: HashMap<Participant, (u8, Option<u64>)>,
    /// The map of unique participants for the next round.
    next: HashMap<Participant, ParticipantInfo>,
    /// The metrics for the current round of the ceremony.
    current_metrics: Option<RoundMetrics>,
    /// The height for the current round of the ceremony.
    current_round_height: Option<u64>,
    /// The map of unique contributors for the current round.
    current_contributors: HashMap<Participant, ParticipantInfo>,
    /// The map of unique verifiers for the current round.
    current_verifiers: HashMap<Participant, ParticipantInfo>,
    /// The map of tasks pending verification in the current round.
    pending_verification: HashMap<Task, Participant>,
    /// The map of each round height to the corresponding contributors from that round.
    finished_contributors: HashMap<u64, HashMap<Participant, ParticipantInfo>>,
    /// The map of each round height to the corresponding verifiers from that round.
    finished_verifiers: HashMap<u64, HashMap<Participant, ParticipantInfo>>,
    /// The list of information about participants that dropped in current and past rounds.
    dropped: Vec<ParticipantInfo>,
    /// The list of participants that are banned from all current and future rounds.
    banned: HashSet<Participant>,
    /// The manual lock to hold the coordinator from transitioning to the next round.
    manual_lock: bool,
}

impl CoordinatorState {
    ///
    /// Creates a new instance of `CoordinatorState`.
    ///
    #[inline]
    pub(super) fn new(environment: Environment) -> Self {
        Self {
            environment,
            status: CoordinatorStatus::Initializing,
            queue: HashMap::default(),
            next: HashMap::default(),
            current_metrics: None,
            current_round_height: None,
            current_contributors: HashMap::default(),
            current_verifiers: HashMap::default(),
            pending_verification: HashMap::default(),
            finished_contributors: HashMap::default(),
            finished_verifiers: HashMap::default(),
            dropped: Vec::new(),
            banned: HashSet::new(),
            manual_lock: false,
        }
    }

    ///
    /// Initializes the coordinator state by setting the round height & metrics, and instantiating
    /// the finished contributors and verifiers map for the given round in the coordinator state.
    ///
    #[inline]
    pub(super) fn initialize(&mut self, current_round_height: u64) {
        // Set the current round height to the given round height.
        if self.current_round_height.is_none() {
            self.current_round_height = Some(current_round_height);
        }

        // Initialize the metrics for this round.
        if self.current_metrics.is_none() {
            self.current_metrics = Some(RoundMetrics {
                number_of_contributors: 0,
                number_of_verifiers: 0,
                is_round_aggregated: false,
                task_timer: HashMap::new(),
                seconds_per_task: HashMap::new(),
                contributor_average_per_task: None,
                verifier_average_per_task: None,
                started_aggregation_at: None,
                finished_aggregation_at: None,
                estimated_finish_time: None,
                estimated_aggregation_time: None,
                estimated_wait_time: None,
                next_round_after: None,
            });
        }

        // Initialize the finished contributors map for the current round, if it does not exist.
        if !self.finished_contributors.contains_key(&current_round_height) {
            self.finished_contributors.insert(current_round_height, HashMap::new());
        }

        // Initialize the finished verifiers map for the current round, if it does not exist.
        if !self.finished_verifiers.contains_key(&current_round_height) {
            self.finished_verifiers.insert(current_round_height, HashMap::new());
        }

        // Set the status to initialized.
        self.status = CoordinatorStatus::Initialized;
    }

    ///
    /// Returns `true` if the given participant is a contributor in the queue.
    ///
    #[inline]
    pub fn is_queue_contributor(&self, participant: &Participant) -> bool {
        participant.is_contributor() && self.queue.contains_key(participant)
    }

    ///
    /// Returns `true` if the given participant is a verifier in the queue.
    ///
    #[inline]
    pub fn is_queue_verifier(&self, participant: &Participant) -> bool {
        participant.is_verifier() && self.queue.contains_key(participant)
    }

    ///
    /// Returns `true` if the given participant is an authorized contributor in the ceremony.
    ///
    #[inline]
    pub fn is_authorized_contributor(&self, participant: &Participant) -> bool {
        participant.is_contributor() && !self.banned.contains(participant)
    }

    ///
    /// Returns `true` if the given participant is an authorized verifier in the ceremony.
    ///
    #[inline]
    pub fn is_authorized_verifier(&self, participant: &Participant) -> bool {
        participant.is_verifier() && !self.banned.contains(participant)
    }

    ///
    /// Returns `true` if the given participant is actively contributing
    /// in the current round.
    ///
    #[inline]
    pub fn is_current_contributor(&self, participant: &Participant) -> bool {
        self.is_authorized_contributor(participant) && self.current_contributors.contains_key(participant)
    }

    ///
    /// Returns `true` if the given participant is actively verifying
    /// in the current round.
    ///
    #[inline]
    pub fn is_current_verifier(&self, participant: &Participant) -> bool {
        self.is_authorized_verifier(participant) && self.current_verifiers.contains_key(participant)
    }

    ///
    /// Returns `true` if the given participant has finished contributing
    /// in the current round.
    ///
    #[inline]
    pub fn is_finished_contributor(&self, participant: &Participant) -> bool {
        let current_round_height = self.current_round_height.unwrap_or_default();
        participant.is_contributor()
            && self
                .finished_contributors
                .get(&current_round_height)
                .get_or_insert(&HashMap::new())
                .contains_key(participant)
    }

    ///
    /// Returns `true` if the given participant has finished verifying
    /// in the current round.
    ///
    #[inline]
    pub fn is_finished_verifier(&self, participant: &Participant) -> bool {
        let current_round_height = self.current_round_height.unwrap_or_default();
        participant.is_verifier()
            && self
                .finished_verifiers
                .get(&current_round_height)
                .get_or_insert(&HashMap::new())
                .contains_key(participant)
    }

    pub fn current_round_finished_contributors(&self) -> anyhow::Result<Vec<Participant>> {
        let current_round_height = self
            .current_round_height
            .ok_or_else(|| anyhow::anyhow!("Current round height is None"))?;
        let contributors = self
            .finished_contributors
            .get(&current_round_height)
            .ok_or_else(|| anyhow::anyhow!("There are no finished contributors for round {}", current_round_height))?
            .keys()
            .cloned()
            .collect();
        Ok(contributors)
    }

    ///
    /// Returns `true` if the given participant is a contributor managed
    /// by the coordinator.
    ///
    #[inline]
    pub fn is_coordinator_contributor(&self, participant: &Participant) -> bool {
        participant.is_contributor() && self.environment.coordinator_contributors().contains(participant)
    }

    ///
    /// Returns `true` if the given participant is a verifier managed
    /// by the coordinator.
    ///
    #[inline]
    pub fn is_coordinator_verifier(&self, participant: &Participant) -> bool {
        participant.is_verifier() && self.environment.coordinator_verifiers().contains(participant)
    }

    ///
    /// Returns the total number of contributors currently in the queue.
    ///  
    #[inline]
    pub fn number_of_queue_contributors(&self) -> usize {
        self.queue.par_iter().filter(|(p, _)| p.is_contributor()).count()
    }

    ///
    /// Returns the total number of verifiers currently in the queue.
    ///
    #[inline]
    pub fn number_of_queue_verifiers(&self) -> usize {
        self.queue.par_iter().filter(|(p, _)| p.is_verifier()).count()
    }

    ///
    /// Returns a list of the contributors currently in the queue.
    ///
    #[inline]
    pub fn queue_contributors(&self) -> Vec<(Participant, (u8, Option<u64>))> {
        self.queue
            .clone()
            .into_par_iter()
            .filter(|(p, _)| p.is_contributor())
            .collect()
    }

    ///
    /// Returns a list of the verifiers currently in the queue.
    ///
    #[inline]
    pub fn queue_verifiers(&self) -> Vec<(Participant, (u8, Option<u64>))> {
        self.queue
            .clone()
            .into_par_iter()
            .filter(|(p, _)| p.is_verifier())
            .collect()
    }

    ///
    /// Returns a list of the contributors currently in the round.
    ///
    #[inline]
    pub fn current_contributors(&self) -> Vec<(Participant, ParticipantInfo)> {
        self.current_contributors.clone().into_iter().collect()
    }

    /// Gets reference to the [ParticipantInfo] for a participant
    /// currently in the round.
    pub fn current_participant_info(&self, participant: &Participant) -> Option<&ParticipantInfo> {
        match participant {
            Participant::Contributor(_) => self.current_contributors.get(participant),
            Participant::Verifier(_) => self.current_verifiers.get(participant),
        }
    }

    /// Gets mutable reference to the [ParticipantInfo] for a
    /// participant currently in the round.
    pub fn current_participant_info_mut(&mut self, participant: &Participant) -> Option<&mut ParticipantInfo> {
        match participant {
            Participant::Contributor(_) => self.current_contributors.get_mut(participant),
            Participant::Verifier(_) => self.current_verifiers.get_mut(participant),
        }
    }

    ///
    /// Returns a list of the verifiers currently in the round.
    ///
    #[inline]
    pub fn current_verifiers(&self) -> Vec<(Participant, ParticipantInfo)> {
        self.current_verifiers.clone().into_iter().collect()
    }

    ///
    /// Returns a list of participants that were dropped from the current round.
    ///
    #[inline]
    pub fn dropped_participants(&self) -> Vec<ParticipantInfo> {
        self.dropped.clone()
    }

    ///
    /// Returns the current round height stored in the coordinator state.
    ///
    /// This function returns `0` if the current round height has not been set.
    ///
    #[inline]
    pub fn current_round_height(&self) -> u64 {
        self.current_round_height.unwrap_or_default()
    }

    ///
    /// Returns the metrics for the current round and current round participants.
    ///
    #[inline]
    pub(super) fn current_round_metrics(&self) -> Option<RoundMetrics> {
        self.current_metrics.clone()
    }

    ///
    /// Returns `true` if all participants in the current round have no more pending chunks.
    ///
    #[inline]
    pub fn is_current_round_finished(&self) -> bool {
        // Check that all contributions have undergone verification.
        self.pending_verification.is_empty()
            // Check that all current contributors are finished.
            && self.current_contributors.is_empty()
            // Check that all current verifiers are finished.
            && self.current_verifiers.is_empty()
    }

    ///
    /// Returns `true` if the current round is currently being aggregated.
    ///
    #[inline]
    pub fn is_current_round_aggregating(&self) -> bool {
        match &self.current_metrics {
            Some(metrics) => {
                !metrics.is_round_aggregated
                    && metrics.started_aggregation_at.is_some()
                    && metrics.finished_aggregation_at.is_none()
            }
            None => false,
        }
    }

    ///
    /// Returns `true` if the current round has been aggregated.
    ///
    #[inline]
    pub fn is_current_round_aggregated(&self) -> bool {
        match &self.current_metrics {
            Some(metrics) => {
                metrics.is_round_aggregated
                    && metrics.started_aggregation_at.is_some()
                    && metrics.finished_aggregation_at.is_some()
            }
            None => false,
        }
    }

    ///
    /// Returns `true` if the precommit for the next round is ready.
    ///
    /// This function checks that the requisite number of contributors and verifiers are
    /// assigned for the next round.
    ///
    /// Note that this function does not check for banned participants, which is checked
    /// during the precommit phase for the next round.
    ///
    #[inline]
    pub(super) fn is_precommit_next_round_ready(&self, time: &dyn TimeSource) -> bool {
        // Check that the coordinator is initialized and is not already in a precommit stage.
        if self.status == CoordinatorStatus::Initializing || self.status == CoordinatorStatus::Precommit {
            return false;
        }

        // Check that the queue contains participants.
        if self.queue.is_empty() {
            trace!("Queue is currently empty");
            return false;
        }

        // Check that the current round height is set.
        if self.current_round_height.is_none() {
            warn!("Current round height is not set in the coordinator state");
            return false;
        }

        // Check that the current round has been aggregated.
        if self.current_round_height() > 0 && !self.is_current_round_aggregated() {
            trace!("Current round has not been aggregated");
            return false;
        }

        // Check that the time to trigger the next round has been reached.
        if let Some(metrics) = &self.current_metrics {
            if let Some(next_round_after) = metrics.next_round_after {
                if time.utc_now() < next_round_after {
                    trace!("Required queue wait time has not been reached yet");
                    return false;
                }
            } else {
                trace!("Required queue wait time has not been set yet");
                return false;
            }
        }

        // Fetch the next round height.
        let next_round_height = self.current_round_height.unwrap_or_default() + 1;

        // Fetch the state of assigned contributors for the next round in the queue.
        let minimum_contributors = self.environment.minimum_contributors_per_round();
        let maximum_contributors = self.environment.maximum_contributors_per_round();
        let number_of_assigned_contributors = self
            .queue
            .clone()
            .into_par_iter()
            .filter(|(p, (_, rh))| p.is_contributor() && rh.unwrap_or_default() == next_round_height)
            .count();

        // Fetch the state of assigned verifiers for the next round in the queue.
        let minimum_verifiers = self.environment.minimum_verifiers_per_round();
        let maximum_verifiers = self.environment.maximum_verifiers_per_round();
        let number_of_assigned_verifiers = self
            .queue
            .clone()
            .into_par_iter()
            .filter(|(p, (_, rh))| p.is_verifier() && rh.unwrap_or_default() == next_round_height)
            .count();

        trace!(
            "Prepare precommit status - {} contributors assigned ({}-{} required), {} verifiers assigned ({}-{} required)",
            number_of_assigned_contributors,
            minimum_contributors,
            maximum_contributors,
            number_of_assigned_verifiers,
            minimum_verifiers,
            maximum_verifiers
        );

        // Check that the next round contains a permitted number of contributors.
        if number_of_assigned_contributors < minimum_contributors
            || number_of_assigned_contributors > maximum_contributors
        {
            trace!("Insufficient or unauthorized number of contributors");
            return false;
        }

        // Check that the next round contains a permitted number of verifiers.
        if number_of_assigned_verifiers < minimum_verifiers || number_of_assigned_verifiers > maximum_verifiers {
            trace!("Insufficient or unauthorized number of verifiers");
            return false;
        }

        true
    }

    ///
    /// Adds the given participant to the queue if they are permitted to participate.
    ///
    #[inline]
    pub(super) fn add_to_queue(
        &mut self,
        participant: Participant,
        reliability_score: u8,
    ) -> Result<(), CoordinatorError> {
        // Check that the participant is not banned from participating.
        if self.banned.contains(&participant) {
            return Err(CoordinatorError::ParticipantBanned);
        }

        // Check that the participant is not already added to the queue.
        if self.queue.contains_key(&participant) {
            return Err(CoordinatorError::ParticipantAlreadyAdded);
        }

        // Check that the participant is not in precommit for the next round.
        if self.next.contains_key(&participant) {
            return Err(CoordinatorError::ParticipantAlreadyAdded);
        }

        match &participant {
            Participant::Contributor(_) => {
                // Check if the contributor is authorized.
                if !self.is_authorized_contributor(&participant) {
                    return Err(CoordinatorError::ParticipantUnauthorized);
                }

                // Check that the contributor is not in the current round.
                if !self.environment.allow_current_contributors_in_queue()
                    && self.current_contributors.contains_key(&participant)
                {
                    return Err(CoordinatorError::ParticipantInCurrentRoundCannotJoinQueue);
                }
            }
            Participant::Verifier(_) => {
                // Check if the verifier is authorized.
                if !self.is_authorized_verifier(&participant) {
                    return Err(CoordinatorError::ParticipantUnauthorized);
                }

                // Check that the verifier is not in the current round.
                if !self.environment.allow_current_verifiers_in_queue()
                    && self.current_verifiers.contains_key(&participant)
                {
                    return Err(CoordinatorError::ParticipantInCurrentRoundCannotJoinQueue);
                }
            }
        }

        // Add the participant to the queue.
        self.queue.insert(participant, (reliability_score, None));

        Ok(())
    }

    ///
    /// Removes the given participant from the queue.
    ///
    #[inline]
    pub(super) fn remove_from_queue(&mut self, participant: &Participant) -> Result<(), CoordinatorError> {
        // Check that the participant is not already in precommit for the next round.
        if self.next.contains_key(participant) {
            return Err(CoordinatorError::ParticipantAlreadyPrecommitted);
        }

        // Check that the participant is exists in the queue.
        if !self.queue.contains_key(participant) {
            return Err(CoordinatorError::ParticipantMissing);
        }

        // Remove the participant from the queue.
        self.queue.remove(participant);

        Ok(())
    }

    ///
    /// Pops the next (chunk ID, contribution ID) task that the participant should process.
    ///
    pub(super) fn fetch_task(
        &mut self,
        participant: &Participant,
        time: &dyn TimeSource,
    ) -> Result<Task, CoordinatorError> {
        // Fetch the contributor and verifier chunk lock limit.
        let contributor_limit = self.environment.contributor_lock_chunk_limit();
        let verifier_limit = self.environment.verifier_lock_chunk_limit();

        // Remove the next chunk ID from the pending chunks of the given participant.
        match participant {
            Participant::Contributor(_) => match self.current_contributors.get_mut(participant) {
                // Check that the participant is holding less than the chunk lock limit.
                Some(participant_info) => match participant_info.locked_chunks.len() < contributor_limit {
                    true => {
                        let task = participant_info.pop_task(time)?;
                        self.start_task_timer(participant, &task, time);
                        Ok(task)
                    }
                    false => Err(CoordinatorError::ParticipantHasLockedMaximumChunks),
                },
                None => Err(CoordinatorError::ParticipantNotFound(participant.clone())),
            },
            Participant::Verifier(_) => match self.current_verifiers.get_mut(participant) {
                // Check that the participant is holding less than the chunk lock limit.
                Some(participant_info) => match participant_info.locked_chunks.len() < verifier_limit {
                    true => {
                        let task = participant_info.pop_task(time)?;
                        self.start_task_timer(participant, &task, time);
                        Ok(task)
                    }
                    false => Err(CoordinatorError::ParticipantHasLockedMaximumChunks),
                },
                None => Err(CoordinatorError::ParticipantNotFound(participant.clone())),
            },
        }
    }

    ///
    /// Adds the given chunk ID to the locks held by the given participant.
    ///
    #[inline]
    pub(super) fn acquired_lock(
        &mut self,
        participant: &Participant,
        chunk_id: u64,
        time: &dyn TimeSource,
    ) -> Result<(), CoordinatorError> {
        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        match participant {
            Participant::Contributor(_) => match self.current_contributors.get_mut(participant) {
                // Acquire the chunk lock for the contributor.
                Some(participant) => Ok(participant.acquired_lock(chunk_id, time)?),
                None => Err(CoordinatorError::ParticipantNotFound(participant.clone())),
            },
            Participant::Verifier(_) => match self.current_verifiers.get_mut(participant) {
                // Acquire the chunk lock for the verifier.
                Some(participant) => Ok(participant.acquired_lock(chunk_id, time)?),
                None => Err(CoordinatorError::ParticipantNotFound(participant.clone())),
            },
        }
    }

    ///
    /// Reverts the given (chunk ID, contribution ID) task to the list of assigned tasks
    /// from the list of pending tasks.
    ///
    #[inline]
    pub(super) fn rollback_pending_task(
        &mut self,
        participant: &Participant,
        task: Task,
        time: &dyn TimeSource,
    ) -> Result<(), CoordinatorError> {
        // Check that the chunk ID is valid.
        if task.chunk_id() > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        match self.current_participant_info_mut(participant) {
            Some(participant) => Ok(participant.rollback_pending_task(task, time)?),
            None => Err(CoordinatorError::ParticipantNotFound(participant.clone())),
        }
    }

    ///
    /// Returns the (chunk ID, contribution ID) task if the given participant has the
    /// given chunk ID in a pending task.
    ///
    pub(super) fn lookup_pending_task(
        &self,
        participant: &Participant,
        chunk_id: u64,
    ) -> Result<Option<&Task>, CoordinatorError> {
        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Fetch the participant info for the given participant.
        let participant_info = match participant {
            Participant::Contributor(_) => match self.current_contributors.get(participant) {
                Some(participant_info) => participant_info,
                None => return Err(CoordinatorError::ParticipantNotFound(participant.clone())),
            },
            Participant::Verifier(_) => match self.current_verifiers.get(participant) {
                Some(participant_info) => participant_info,
                None => return Err(CoordinatorError::ParticipantNotFound(participant.clone())),
            },
        };

        // Check that the given chunk ID is locked by the participant,
        // and filter the pending tasks for the given chunk ID.
        let output: Vec<&Task> = match participant_info.locked_chunks.contains_key(&chunk_id) {
            true => participant_info
                .pending_tasks
                .par_iter()
                .filter(|t| t.contains(chunk_id))
                .collect(),
            false => return Err(CoordinatorError::ParticipantDidntLockChunkId),
        };

        match output.len() {
            0 => Ok(None),
            1 => Ok(Some(output[0])),
            _ => return Err(CoordinatorError::ParticipantLockedChunkWithManyContributions),
        }
    }

    ///
    /// Returns the (chunk ID, contribution ID) task if the given participant is disposing a task
    /// for the given chunk ID.
    ///
    pub(super) fn lookup_disposing_task(
        &self,
        participant: &Participant,
        chunk_id: u64,
    ) -> Result<Option<&Task>, CoordinatorError> {
        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Fetch the participant info for the given participant.
        let participant_info = match participant {
            Participant::Contributor(_) => match self.current_contributors.get(participant) {
                Some(participant_info) => participant_info,
                None => return Err(CoordinatorError::ParticipantNotFound(participant.clone())),
            },
            Participant::Verifier(_) => match self.current_verifiers.get(participant) {
                Some(participant_info) => participant_info,
                None => return Err(CoordinatorError::ParticipantNotFound(participant.clone())),
            },
        };

        // Check that the given chunk ID is locked by the participant,
        // and filter the disposing tasks for the given chunk ID.
        let output: Vec<&Task> = match participant_info.locked_chunks.contains_key(&chunk_id) {
            true => participant_info
                .disposing_tasks
                .par_iter()
                .filter(|t| t.contains(chunk_id))
                .collect(),
            false => return Err(CoordinatorError::ParticipantDidntLockChunkId),
        };

        match output.len() {
            0 => Ok(None),
            1 => Ok(Some(output[0])),
            _ => return Err(CoordinatorError::ParticipantLockedChunkWithManyContributions),
        }
    }

    ///
    /// Completes the disposal of the given (chunk ID, contribution
    /// ID) task for a participant. Called when the participant
    /// confirms that it has disposed of the task.
    ///
    #[inline]
    pub(super) fn disposed_task(
        &mut self,
        participant: &Participant,
        task: &Task,
        time: &dyn TimeSource,
    ) -> Result<(), CoordinatorError> {
        let chunk_id = task.chunk_id();
        let contribution_id = task.contribution_id();

        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        warn!(
            "Disposing chunk {} contribution {} from {}",
            chunk_id, contribution_id, participant
        );

        match participant {
            Participant::Contributor(_) => match self.current_contributors.get_mut(participant) {
                // Move the disposing task to the list of disposed tasks for the contributor.
                Some(participant) => participant.dispose_task(chunk_id, contribution_id, time),
                None => Err(CoordinatorError::ParticipantNotFound(participant.clone())),
            },
            Participant::Verifier(_) => match self.current_verifiers.get_mut(participant) {
                // Move the disposing task to the list of disposed tasks for the verifier.
                Some(participant) => participant.dispose_task(chunk_id, contribution_id, time),
                None => Err(CoordinatorError::ParticipantNotFound(participant.clone())),
            },
        }
    }

    ///
    /// Adds the given (chunk ID, contribution ID) task to the pending verification set.
    /// The verification task is then assigned to the verifier with the least number of tasks in its queue.
    ///
    /// On success, this function returns the verifier that was assigned to the verification task.
    ///
    #[inline]
    pub(super) fn add_pending_verification(
        &mut self,
        task: Task,
        time: &dyn TimeSource,
    ) -> Result<Participant, CoordinatorError> {
        // Check that the chunk ID is valid.
        if task.chunk_id() > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Check that the pending verification set does not already contain the chunk ID.
        if self.pending_verification.contains_key(&task) {
            return Err(CoordinatorError::ChunkIdAlreadyAdded);
        }

        let verifier = match self
            .current_verifiers
            .par_iter()
            .min_by_key(|(_, v)| v.assigned_tasks.len() + v.pending_tasks.len() + v.locked_chunks.len())
        {
            Some((verifier, _verifier_info)) => verifier.clone(),
            None => return Err(CoordinatorError::VerifierMissing),
        };

        info!(
            "Assigning (chunk {}, contribution {}) to {} for verification",
            task.chunk_id(),
            task.contribution_id(),
            verifier
        );

        match self.current_verifiers.get_mut(&verifier) {
            Some(verifier_info) => verifier_info.push_back_task(task, time)?,
            None => return Err(CoordinatorError::VerifierMissing),
        };

        self.pending_verification.insert(task, verifier.clone());

        Ok(verifier)
    }

    ///
    /// Remove the given (chunk ID, contribution ID) task from the map of chunks that are pending verification.
    ///
    /// On success, this function returns the verifier that completed the verification task.
    ///
    #[inline]
    pub(super) fn remove_pending_verification(&mut self, task: Task) -> Result<Participant, CoordinatorError> {
        // Check that the chunk ID is valid.
        if task.chunk_id() > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Check that the set pending verification does not already contain the chunk ID.
        if !self.pending_verification.contains_key(&task) {
            return Err(CoordinatorError::ChunkIdMissing);
        }

        debug!(
            "Removing (chunk {}, contribution {}) from the pending verifications",
            task.chunk_id(),
            task.contribution_id()
        );

        // Remove the task from the pending verification.
        let verifier = self
            .pending_verification
            .remove(&task)
            .ok_or(CoordinatorError::VerifierMissing)?;

        Ok(verifier)
    }

    ///
    /// Adds the given (chunk ID, contribution ID) task to the completed tasks of the given participant,
    /// and removes the chunk ID from the locks held by the given participant.
    ///
    /// On success, this function returns the verifier assigned to the verification task.
    ///
    #[tracing::instrument(
        level = "error",
        skip(self, time, participant),
        fields(task = %task),
        err
    )]
    pub(super) fn completed_task(
        &mut self,
        participant: &Participant,
        task: Task,
        time: &dyn TimeSource,
    ) -> Result<Participant, CoordinatorError> {
        // Check that the chunk ID is valid.
        if task.chunk_id() > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        match participant {
            Participant::Contributor(_) => match self.current_contributors.get_mut(participant) {
                // Adds the task to the list of completed tasks for the contributor,
                // and add the task to the pending verification set.
                Some(participant_info) => {
                    participant_info.completed_task(task, time)?;
                    self.stop_task_timer(participant, &task, time);
                    Ok(self.add_pending_verification(task, time)?)
                }
                None => Err(CoordinatorError::ParticipantNotFound(participant.clone())),
            },
            Participant::Verifier(_) => match self.current_verifiers.get_mut(participant) {
                // Adds the task to the list of completed tasks for the verifier,
                // and remove the task from the pending verification set.
                Some(participant_info) => {
                    participant_info.completed_task(task, time)?;
                    self.stop_task_timer(participant, &task, time);
                    Ok(self.remove_pending_verification(task)?)
                }
                None => Err(CoordinatorError::ParticipantNotFound(participant.clone())),
            },
        }
    }

    ///
    /// Starts the timer for a given participant and task,
    /// in order to track the runtime of a given task.
    ///
    /// This function is a best effort tracker and should
    /// not be used for mission-critical logic. It is
    /// provided only for convenience to produce metrics.
    ///
    #[inline]
    pub(super) fn start_task_timer(&mut self, participant: &Participant, task: &Task, time: &dyn TimeSource) {
        // Fetch the current metrics for this round.
        if let Some(metrics) = &mut self.current_metrics {
            // Fetch the tasks for the given participant.
            let mut updated_tasks = match metrics.task_timer.get(participant) {
                Some(tasks) => tasks.clone(),
                None => HashMap::new(),
            };

            // Add the given task with a new start timer.
            updated_tasks.insert(*task, (time.utc_now().timestamp(), None));

            // Set the current task timer for the given participant to the updated task timer.
            metrics.task_timer.insert(participant.clone(), updated_tasks);
        }
    }

    ///
    /// Stops the timer for a given participant and task,
    /// in order to track the runtime of a given task.
    ///
    /// This function is a best effort tracker and should
    /// not be used for mission-critical logic. It is
    /// provided only for convenience to produce metrics.
    ///
    #[inline]
    pub(super) fn stop_task_timer(&mut self, participant: &Participant, task: &Task, time: &dyn TimeSource) {
        // Fetch the current metrics for this round.
        if let Some(metrics) = &mut self.current_metrics {
            // Fetch the tasks for the given participant.
            let mut updated_tasks = match metrics.task_timer.get(participant) {
                Some(tasks) => tasks.clone(),
                None => {
                    warn!("Task timer metrics for {} are missing", participant);
                    return;
                }
            };

            // Set the end timer for the given task.
            match updated_tasks.get_mut(task) {
                Some((_, end)) => {
                    if end.is_none() {
                        *end = Some(time.utc_now().timestamp());
                    }
                }
                None => {
                    warn!("Task timer metrics for {} on {:?} are missing", participant, task);
                    return;
                }
            };

            // Set the current task timer for the given participant to the updated task timer.
            metrics.task_timer.insert(participant.clone(), updated_tasks);
        };
    }

    ///
    /// Sets the current round as aggregating in round metrics, indicating that the
    /// current round is now being aggregated.
    ///
    #[inline]
    pub(super) fn aggregating_current_round(&mut self, time: &dyn TimeSource) -> Result<(), CoordinatorError> {
        let metrics = match &mut self.current_metrics {
            Some(metrics) => metrics,
            None => return Err(CoordinatorError::CoordinatorStateNotInitialized),
        };

        // Check that the start aggregation timestamp was not yet set.
        if metrics.started_aggregation_at.is_some() {
            error!("Round metrics shows starting aggregation timestamp was already set");
            return Err(CoordinatorError::RoundAggregationFailed);
        }

        // Check that the round aggregation is not yet set.
        if metrics.is_round_aggregated || metrics.finished_aggregation_at.is_some() {
            error!("Round metrics shows current round is already aggregated");
            return Err(CoordinatorError::RoundAlreadyAggregated);
        }

        // Set the start aggregation timestamp to now.
        metrics.started_aggregation_at = Some(time.utc_now());

        Ok(())
    }

    ///
    /// Sets the current round as aggregated in round metrics, indicating that the
    /// current round has been aggregated.
    ///
    #[inline]
    pub(super) fn aggregated_current_round(&mut self, time: &dyn TimeSource) -> Result<(), CoordinatorError> {
        let metrics = match &mut self.current_metrics {
            Some(metrics) => metrics,
            None => return Err(CoordinatorError::CoordinatorStateNotInitialized),
        };

        // Check that the start aggregation timestamp was set.
        if metrics.started_aggregation_at.is_none() {
            error!("Round metrics shows starting aggregation timestamp was not set");
            return Err(CoordinatorError::RoundAggregationFailed);
        }

        // Check that the round aggregation is not yet set.
        if metrics.is_round_aggregated || metrics.finished_aggregation_at.is_some() {
            error!("Round metrics shows current round is already aggregated");
            return Err(CoordinatorError::RoundAlreadyAggregated);
        }

        // Set the round aggregation boolean to true.
        metrics.is_round_aggregated = true;

        // Set the finish aggregation timestamp to now.
        metrics.finished_aggregation_at = Some(time.utc_now());

        // Update the time to trigger the next round.
        if metrics.next_round_after.is_none() {
            metrics.next_round_after =
                Some(time.utc_now() + Duration::seconds(self.environment.queue_wait_time() as i64));
        }

        Ok(())
    }

    ///
    /// Rolls back the current round from aggregating in round metrics.
    ///
    #[inline]
    pub(super) fn rollback_aggregating_current_round(&mut self) -> Result<(), CoordinatorError> {
        warn!("Rolling back aggregating indicator from coordinator state");

        let metrics = match &mut self.current_metrics {
            Some(metrics) => metrics,
            None => return Err(CoordinatorError::CoordinatorStateNotInitialized),
        };

        // Check that the round aggregation is not yet set.
        if metrics.is_round_aggregated || metrics.finished_aggregation_at.is_some() {
            error!("Round metrics shows current round is already aggregated");
            return Err(CoordinatorError::RoundAlreadyAggregated);
        }

        // Set the start aggregation timestamp to None.
        metrics.started_aggregation_at = None;

        Ok(())
    }

    ///
    /// Drops the given participant from the queue, precommit, and current round.
    ///
    /// The dropped participant information preserves the state of locked chunks,
    /// pending tasks, and completed tasks, as reference in case this state is
    /// necessary in the future.
    ///
    /// On success, this function returns a `Justification` for the coordinator to use.
    ///
    #[tracing::instrument(
        skip(self, participant, time),
        fields(participant = %participant)
    )]
    pub(super) fn drop_participant(
        &mut self,
        participant: &Participant,
        time: &dyn TimeSource,
    ) -> Result<DropParticipant, CoordinatorError> {
        // Check that the coordinator state is initialized.
        if self.status == CoordinatorStatus::Initializing {
            return Err(CoordinatorError::CoordinatorStateNotInitialized);
        }

        warn!("Dropping {} from the ceremony", participant);

        // Remove the participant from the queue and precommit, if present.
        if self.queue.contains_key(participant) || self.next.contains_key(participant) {
            // Remove the participant from the queue.
            if self.queue.contains_key(participant) {
                trace!("Removing {} from the queue", participant);
                self.queue.remove(participant);
            }

            // Remove the participant from the precommit for the next round.
            if self.next.contains_key(participant) {
                trace!("Removing {} from the precommit for the next round", participant);
                self.next.remove(participant);
                // Trigger a rollback as the precommit has changed.
                self.rollback_next_round();
            }

            return Ok(DropParticipant::Inactive(DropInactiveParticipantData {
                participant: participant.clone(),
            }));
        }

        // Fetch the current participant information.
        let participant_info = match participant {
            Participant::Contributor(_) => self
                .current_contributors
                .get(participant)
                .ok_or_else(|| CoordinatorError::ParticipantNotFound(participant.clone()))?
                .clone(),
            Participant::Verifier(_) => self
                .current_verifiers
                .get(participant)
                .ok_or_else(|| CoordinatorError::ParticipantNotFound(participant.clone()))?
                .clone(),
        };
        {
            // Check that the participant is not already dropped.
            if participant_info.is_dropped() {
                return Err(CoordinatorError::ParticipantAlreadyDropped);
            }

            // Check that the participant is not already finished.
            if participant_info.is_finished() {
                return Err(CoordinatorError::ParticipantAlreadyFinished);
            }
        }

        // Fetch the bucket ID, locked chunks, and tasks.
        let bucket_id = participant_info.bucket_id;
        let locked_chunks: Vec<u64> = participant_info.locked_chunks.keys().cloned().collect();
        let tasks: Vec<Task> = match participant {
            Participant::Contributor(_) => participant_info.completed_tasks.iter().cloned().collect(),
            Participant::Verifier(_) => {
                let mut tasks = participant_info.assigned_tasks.clone();
                tasks.extend(&mut participant_info.pending_tasks.iter());
                tasks.into_iter().collect()
            }
        };

        // Drop the contributor from the current round, and update participant info and coordinator state.
        match participant {
            Participant::Contributor(_id) => {
                // TODO (howardwu): Optimization only.
                //  -----------------------------------------------------------------------------------
                //  Update this implementation to minimize recomputation by not re-assigning
                //  tasks for affected contributors which are not affected by the dropped contributor.
                //  It sounds like a mess, but is easier than you think, once you've loaded state.
                //  In short, compute the minimum overlapping chunk ID between affected & dropped contributor,
                //  and reinitialize from there. If there is no overlap, you can skip reinitializing
                //  any tasks for the affected contributor.
                //  -----------------------------------------------------------------------------------

                // Set the participant as dropped.
                let mut dropped_info = participant_info.clone();
                dropped_info.drop(time)?;

                // Fetch the number of chunks and number of contributors.
                let number_of_chunks = self.environment.number_of_chunks() as u64;
                let number_of_contributors = self
                    .current_metrics
                    .clone()
                    .ok_or(CoordinatorError::CoordinatorStateNotInitialized)?
                    .number_of_contributors;

                // Initialize sets for disposed tasks.
                let mut all_disposed_tasks: HashSet<Task> = participant_info.completed_tasks.iter().cloned().collect();

                // A HashMap of tasks represented as (chunk ID, contribution ID) pairs.
                let tasks_by_chunk: HashMap<u64, u64> = tasks.iter().map(|task| task.to_tuple()).collect();

                // For every contributor we check if there are affected tasks. If the task
                // is affected, it will be dropped and reassigned
                for contributor_info in self.current_contributors.values_mut() {
                    // If the pending task is in the same chunk with the dropped task
                    // then it should be recomputed
                    let (disposing_tasks, pending_tasks) = contributor_info
                        .pending_tasks
                        .iter()
                        .cloned()
                        .partition(|task| tasks_by_chunk.get(&task.chunk_id()).is_some());

                    // TODO: revisit the handling of disposing_tasks
                    //       https://github.com/AleoHQ/aleo-setup/issues/249

                    contributor_info.disposing_tasks = disposing_tasks;
                    contributor_info.pending_tasks = pending_tasks;

                    // If completed task is based on the dropped task, it should also be dropped
                    let (disposed_tasks, completed_tasks) =
                        contributor_info.completed_tasks.iter().cloned().partition(|task| {
                            if let Some(contribution_id) = tasks_by_chunk.get(&task.chunk_id()) {
                                *contribution_id < task.contribution_id()
                            } else {
                                false
                            }
                        });

                    // TODO: revisit the handling of disposed_tasks
                    // https://github.com/AleoHQ/aleo-setup/issues/249
                    contributor_info.completed_tasks = completed_tasks;
                    contributor_info.disposed_tasks.extend(disposed_tasks);

                    all_disposed_tasks.extend(contributor_info.disposed_tasks.iter());

                    // Determine the excluded tasks, which are filtered out from the list of newly assigned tasks.
                    let mut excluded_tasks: HashSet<u64> =
                        HashSet::from_iter(contributor_info.completed_tasks.iter().map(|task| task.chunk_id()));
                    excluded_tasks.extend(contributor_info.pending_tasks.iter().map(|task| task.chunk_id()));

                    // Reassign tasks for the affected contributor.
                    contributor_info.assigned_tasks =
                        initialize_tasks(contributor_info.bucket_id, number_of_chunks, number_of_contributors)?
                            .into_iter()
                            .filter(|task| !excluded_tasks.contains(&task.chunk_id()))
                            .collect();
                }

                // All verifiers assigned to affected tasks must dispose their affected
                // pending and completed tasks.
                for verifier_info in self.current_verifiers.values_mut() {
                    // Filter the current verifier for pending tasks that have been disposed.
                    let (disposing_tasks, pending_tasks) = verifier_info
                        .pending_tasks
                        .iter()
                        .cloned()
                        .partition(|task| all_disposed_tasks.contains(&task));

                    // TODO: revisit the handling of disposing_tasks
                    //       https://github.com/AleoHQ/aleo-setup/issues/249
                    verifier_info.pending_tasks = pending_tasks;
                    verifier_info.disposing_tasks = disposing_tasks;

                    // Filter the current verifier for completed tasks that have been disposed.
                    let (disposed_tasks, completed_tasks) = verifier_info
                        .completed_tasks
                        .iter()
                        .cloned()
                        .partition(|task| all_disposed_tasks.contains(&task));

                    // TODO: revisit the handling of disposed_tasks
                    //       https://github.com/AleoHQ/aleo-setup/issues/249
                    verifier_info.completed_tasks = completed_tasks;
                    verifier_info.disposed_tasks.extend(disposed_tasks);
                }

                // Remove the current verifier from the coordinator state.
                self.current_contributors.remove(&participant);

                // Add the participant info to the dropped participants.
                self.dropped.push(dropped_info);

                // TODO (howardwu): Add a flag guard to this call, and return None, to support
                //  the 'drop round' feature in the coordinator.
                // Assign the replacement contributor to the dropped tasks.
                let replacement_contributor = self.add_replacement_contributor_unsafe(bucket_id, time)?;

                warn!("Dropped {} from the ceremony", participant);

                return Ok(DropParticipant::DropCurrent(DropCurrentParticpantData {
                    participant: participant.clone(),
                    bucket_id,
                    locked_chunks,
                    tasks,
                    replacement: Some(replacement_contributor),
                }));
            }
            Participant::Verifier(_id) => {
                // Add just the current pending tasks to a pending verifications list.
                let mut pending_verifications = vec![];
                for task in &tasks {
                    pending_verifications.push(task);
                }

                // Set the participant as dropped.
                let mut dropped_info = participant_info.clone();
                dropped_info.drop(time)?;

                // Remove the current verifier from the coordinator state.
                self.current_verifiers.remove(&participant);

                // TODO (howardwu): Make this operation atomic.
                for task in pending_verifications {
                    // Remove the task from the pending verifications.
                    self.remove_pending_verification(*task)?;

                    // Reassign the pending verification task to a new verifier.
                    self.add_pending_verification(*task, time)?;
                }

                // Add the participant info to the dropped participants.
                self.dropped.push(dropped_info);

                warn!("Dropped {} from the ceremony", participant);

                return Ok(DropParticipant::DropCurrent(DropCurrentParticpantData {
                    participant: participant.clone(),
                    bucket_id,
                    locked_chunks,
                    tasks,
                    replacement: None,
                }));
            }
        }
    }

    ///
    /// Bans the given participant from the queue, precommit, and current round.
    ///
    #[inline]
    pub(super) fn ban_participant(
        &mut self,
        participant: &Participant,
        time: &dyn TimeSource,
    ) -> Result<DropParticipant, CoordinatorError> {
        // Check that the participant is not already banned from participating.
        if self.banned.contains(&participant) {
            return Err(CoordinatorError::ParticipantAlreadyBanned);
        }

        // Drop the participant from the queue, precommit, and current round.
        match self.drop_participant(participant, time)? {
            DropParticipant::DropCurrent(drop_data) => {
                // Add the participant to the banned list.
                self.banned.insert(participant.clone());

                debug!("{} was banned from the ceremony", participant);

                Ok(DropParticipant::BanCurrent(drop_data))
            }
            _ => Err(CoordinatorError::JustificationInvalid),
        }
    }

    ///
    /// Unbans the given participant from joining the queue.
    ///
    #[inline]
    pub(super) fn unban_participant(&mut self, participant: &Participant) {
        // Remove the participant from the banned list.
        self.banned = self
            .banned
            .clone()
            .into_par_iter()
            .filter(|p| p != participant)
            .collect();
    }

    ///
    /// Adds a replacement contributor from the coordinator as a current contributor
    /// and assigns them tasks from the given starting bucket ID.
    ///
    #[inline]
    pub(crate) fn add_replacement_contributor_unsafe(
        &mut self,
        bucket_id: u64,
        time: &dyn TimeSource,
    ) -> Result<Participant, CoordinatorError> {
        // Fetch a coordinator contributor with the least load.
        let coordinator_contributor =
            self.environment
                .coordinator_contributors()
                .iter()
                .min_by_key(|c| match self.current_contributors.get(c) {
                    Some(participant_info) => {
                        participant_info.pending_tasks.len() + participant_info.assigned_tasks.len()
                    }
                    None => 0,
                });

        // Assign the replacement contributor to the dropped tasks.
        let contributor = coordinator_contributor.ok_or(CoordinatorError::CoordinatorContributorMissing)?;
        let number_of_contributors = self
            .current_metrics
            .clone()
            .ok_or(CoordinatorError::CoordinatorStateNotInitialized)?
            .number_of_contributors;

        // TODO (raychu86): Update the participant info (interleave the tasks by contribution id).
        // TODO (raychu86): Add tasks to the replacement contributor if it already has pending tasks.

        let tasks = initialize_tasks(bucket_id, self.environment.number_of_chunks(), number_of_contributors)?;
        let mut participant_info =
            ParticipantInfo::new(contributor.clone(), self.current_round_height(), 10, bucket_id, time);
        participant_info.start(tasks, time)?;
        trace!("{:?}", participant_info);
        self.current_contributors.insert(contributor.clone(), participant_info);

        Ok(contributor.clone())
    }

    ///
    /// Returns `true` if the manual lock for transitioning to the next round is enabled.
    ///
    #[inline]
    pub(super) fn is_manual_lock_enabled(&self) -> bool {
        self.manual_lock
    }

    ///
    /// Sets the manual lock for transitioning to the next round to `true`.
    ///
    #[inline]
    pub(super) fn enable_manual_lock(&mut self) {
        self.manual_lock = true;
    }

    ///
    /// Sets the manual lock for transitioning to the next round to `false`.
    ///
    #[inline]
    pub(super) fn disable_manual_lock(&mut self) {
        self.manual_lock = false;
    }

    ///
    /// Updates the state of the queue for all waiting participants.
    ///
    #[inline]
    pub(super) fn update_queue(&mut self) -> Result<(), CoordinatorError> {
        // Fetch the next round height.
        let next_round = match self.current_round_height {
            Some(round_height) => round_height + 1,
            _ => return Err(CoordinatorError::RoundHeightNotSet),
        };

        // Sort the participants in the queue by reliability.
        let mut queue: Vec<_> = self.queue.clone().into_par_iter().map(|(p, (r, _))| (p, r)).collect();
        queue.par_sort_by(|a, b| (b.1).cmp(&a.1));

        // Parse the queue participants into contributors and verifiers,
        // and check that they are not banned participants.
        let contributors: Vec<(_, _)> = queue
            .clone()
            .into_par_iter()
            .filter(|(p, _)| p.is_contributor() && !self.banned.contains(&p))
            .collect();
        let verifiers: Vec<(_, _)> = queue
            .into_par_iter()
            .filter(|(p, _)| p.is_verifier() && !self.banned.contains(&p))
            .collect();

        // Fetch the permitted number of contributors and verifiers.
        let maximum_contributors = self.environment.maximum_contributors_per_round();
        let maximum_verifiers = self.environment.maximum_verifiers_per_round();

        // Initialize the updated queue.
        let mut updated_queue = HashMap::with_capacity(contributors.len() + verifiers.len());

        // Update assigned round height for each contributor.
        for (index, round) in contributors.chunks(maximum_contributors).enumerate() {
            for (contributor, reliability) in round.into_iter() {
                let assigned_round = next_round + index as u64;
                trace!(
                    "Assigning contributor {} with reliability {} in queue to round {}",
                    contributor,
                    reliability,
                    assigned_round
                );
                updated_queue.insert(contributor.clone(), (*reliability, Some(assigned_round)));
            }
        }

        // Update assigned round height for each verifier.
        for (index, round) in verifiers.chunks(maximum_verifiers).enumerate() {
            for (verifier, reliability) in round.into_iter() {
                let assigned_round = next_round + index as u64;
                trace!(
                    "Assigning verifier {} with reliability {} in queue to round {}",
                    verifier,
                    reliability,
                    assigned_round
                );
                updated_queue.insert(verifier.clone(), (*reliability, Some(assigned_round)));
            }
        }

        // Set the queue to the updated queue.
        self.queue = updated_queue;

        Ok(())
    }

    ///
    /// Updates the state of contributors in the current round.
    ///
    #[inline]
    pub(super) fn update_current_contributors(&mut self, time: &dyn TimeSource) -> Result<(), CoordinatorError> {
        // Fetch the current round height.
        let current_round_height = self.current_round_height.ok_or(CoordinatorError::RoundHeightNotSet)?;

        // Fetch the current number of contributors.
        let number_of_current_contributors = self.current_contributors.len();

        // Initialize a map for newly finished contributors.
        let mut newly_finished: HashMap<Participant, ParticipantInfo> = HashMap::new();

        // Iterate through all of the current contributors and check if they have finished.
        self.current_contributors = self
            .current_contributors
            .clone()
            .into_iter()
            .filter(|(contributor, contributor_info)| {
                // Check if the contributor has finished.
                if contributor_info.is_finished() {
                    return false;
                }

                // Attempt to set the contributor as finished.
                let mut finished_info = contributor_info.clone();
                if let Err(_) = finished_info.finish(time) {
                    return true;
                }

                // Add the contributor to the set of finished contributors.
                newly_finished.insert(contributor.clone(), finished_info);

                debug!("{} has finished", contributor);
                false
            })
            .collect();

        // Check that the update preserves the same number of contributors.
        if number_of_current_contributors != self.current_contributors.len() + newly_finished.len() {
            return Err(CoordinatorError::RoundUpdateCorruptedStateOfContributors);
        }

        trace!("Marking {} current contributors as finished", newly_finished.len());

        // Update the map of finished contributors.
        match self.finished_contributors.get_mut(&current_round_height) {
            Some(contributors) => contributors.extend(newly_finished.into_iter()),
            None => return Err(CoordinatorError::RoundCommitFailedOrCorrupted),
        };

        Ok(())
    }

    ///
    /// Updates the state of verifiers in the current round.
    ///
    /// This function should never be run prior to calling `update_current_contributors`.
    ///
    #[inline]
    pub(super) fn update_current_verifiers(&mut self, time: &dyn TimeSource) -> Result<(), CoordinatorError> {
        // Check if the contributors are finished.
        let is_contributors_finished = self.current_contributors.is_empty();

        // If all contributors are finished, this means there are no new verification jobs
        // to be added to the pending verifications queue. So if a verifier is finished
        // with their verifications, then they are finished for this round.
        if is_contributors_finished {
            // Fetch the current round height.
            let current_round_height = self.current_round_height.ok_or(CoordinatorError::RoundHeightNotSet)?;

            // Fetch the current number of verifiers.
            let number_of_current_verifiers = self.current_verifiers.len();

            // Initialize a map for newly finished verifiers.
            let mut newly_finished: HashMap<Participant, ParticipantInfo> = HashMap::new();

            // Iterate through all of the current verifiers and check if they have finished.
            self.current_verifiers = self
                .current_verifiers
                .clone()
                .into_iter()
                .filter(|(verifier, verifier_info)| {
                    // Check if the verifier has finished.
                    if verifier_info.is_finished() {
                        return false;
                    }

                    // Attempt to set the verifier as finished.
                    let mut finished_info = verifier_info.clone();
                    if let Err(_) = finished_info.finish(time) {
                        return true;
                    }

                    // Add the verifier to the set of finished verifier.
                    newly_finished.insert(verifier.clone(), finished_info);

                    debug!("{} has finished", verifier);
                    false
                })
                .collect();

            // Check that the update preserves the same number of verifiers.
            if number_of_current_verifiers != self.current_verifiers.len() + newly_finished.len() {
                return Err(CoordinatorError::RoundUpdateCorruptedStateOfVerifiers);
            }

            trace!("Marking {} current verifiers as finished", newly_finished.len());

            // Update the map of finished verifiers.
            match self.finished_verifiers.get_mut(&current_round_height) {
                Some(contributors) => contributors.extend(newly_finished.into_iter()),
                None => return Err(CoordinatorError::RoundCommitFailedOrCorrupted),
            };
        }

        Ok(())
    }

    ///
    /// Updates the current round for dropped participants.
    ///
    /// On success, returns a list of justifications for the coordinator to take actions on.
    ///
    pub(super) fn update_dropped_participants(
        &mut self,
        time: &dyn TimeSource,
    ) -> Result<Vec<DropParticipant>, CoordinatorError> {
        Ok(self
            .update_contributor_seen_drops(time)?
            .into_iter()
            .chain(self.update_participant_lock_drops(time)?.into_iter())
            .collect())
    }

    /// This will drop a participant (verifier or contributor) if it
    /// has been holding a lock for longer than
    /// [crate::environment::Environment]'s
    /// `participant_lock_timeout`.
    fn update_participant_lock_drops(
        &mut self,
        time: &dyn TimeSource,
    ) -> Result<Vec<DropParticipant>, CoordinatorError> {
        // Fetch the timeout threshold for contributors.
        let participant_lock_timeout = self.environment.participant_lock_timeout();

        // Fetch the current time.
        let now = time.utc_now();

        self.current_contributors
            .clone()
            .iter()
            .chain(self.current_verifiers.clone().iter())
            .filter_map(|(participant, participant_info)| {
                if !self.is_coordinator_contributor(&participant)
                    && participant_info
                        .locked_chunks
                        .values()
                        .find(|lock| {
                            let elapsed = now - lock.lock_time;
                            elapsed > participant_lock_timeout
                        })
                        .is_some()
                {
                    Some(self.drop_participant(participant, time))
                } else {
                    None
                }
            })
            .collect()
    }

    /// This will drop a contributor if it hasn't been seen for more
    /// than [crate::environment::Environment]'s
    /// `contributor_seen_timeout`.
    fn update_contributor_seen_drops(
        &mut self,
        time: &dyn TimeSource,
    ) -> Result<Vec<DropParticipant>, CoordinatorError> {
        // Fetch the timeout threshold for contributors.
        let contributor_seen_timeout = self.environment.contributor_seen_timeout();

        // Fetch the current time.
        let now = time.utc_now();

        self.current_contributors
            .clone()
            .iter()
            .filter_map(|(participant, participant_info)| {
                // Fetch the elapsed time.
                let elapsed = now - participant_info.last_seen;

                // Check if the participant is still live and not a coordinator contributor.
                if elapsed > contributor_seen_timeout && !self.is_coordinator_contributor(&participant) {
                    tracing::info!(
                        "Dropping participant {} because it has exceeded the maximum ({} minutes) allowed time \
                        since it was last seen by the coordinator (last seen {} minutes ago).",
                        participant,
                        contributor_seen_timeout,
                        elapsed.num_minutes()
                    );
                    // Drop the participant.
                    Some(self.drop_participant(participant, time))
                } else {
                    None
                }
            })
            .collect()
    }

    ///
    /// Updates the list of dropped participants for participants who
    /// meet the ban criteria of the coordinator.
    ///
    /// Note that as this function only checks dropped participants who have already
    /// been processed, we do not need to call `CoordinatorState::ban_participant`.
    ///
    #[inline]
    pub(super) fn update_banned_participants(&mut self) -> Result<(), CoordinatorError> {
        for participant_info in self.dropped.clone() {
            if !self.banned.contains(&participant_info.id) {
                // Fetch the number of times this participant has been dropped.
                let count = self
                    .dropped
                    .par_iter()
                    .filter(|dropped| dropped.id == participant_info.id)
                    .count();

                // Check if the participant meets the ban threshold.
                if count > self.environment.participant_ban_threshold() as usize {
                    self.banned.insert(participant_info.id.clone());

                    debug!("{} is being banned", participant_info.id);
                }
            }
        }

        Ok(())
    }

    ///
    /// Updates the metrics for the current round and current round participants,
    /// if the current round is not yet finished.
    ///
    #[inline]
    pub(super) fn update_round_metrics(&mut self) {
        if !self.is_current_round_finished() {
            // Update the round metrics if the current round is not yet finished.
            if let Some(metrics) = &mut self.current_metrics {
                // Update the average time per task for each participant.
                let (contributor_average_per_task, verifier_average_per_task) = {
                    let mut cumulative_contributor_averages = 0;
                    let mut cumulative_verifier_averages = 0;
                    let mut number_of_contributor_averages = 0;
                    let mut number_of_verifier_averages = 0;

                    for (participant, tasks) in &metrics.task_timer {
                        // (task, (start, end))
                        let timed_tasks: Vec<u64> = tasks
                            .par_iter()
                            .filter_map(|(_, (s, e))| match e {
                                Some(e) => match e > s {
                                    true => Some((e - s) as u64),
                                    false => None,
                                },
                                _ => None,
                            })
                            .collect();
                        if timed_tasks.len() > 0 {
                            let average_in_seconds = timed_tasks.par_iter().sum::<u64>() / timed_tasks.len() as u64;
                            metrics.seconds_per_task.insert(participant.clone(), average_in_seconds);

                            match participant {
                                Participant::Contributor(_) => {
                                    cumulative_contributor_averages += average_in_seconds;
                                    number_of_contributor_averages += 1;
                                }
                                Participant::Verifier(_) => {
                                    cumulative_verifier_averages += average_in_seconds;
                                    number_of_verifier_averages += 1;
                                }
                            };
                        }
                    }

                    let contributor_average_per_task = match number_of_contributor_averages > 0 {
                        true => {
                            let contributor_average_per_task =
                                cumulative_contributor_averages / number_of_contributor_averages;
                            metrics.contributor_average_per_task = Some(contributor_average_per_task);
                            contributor_average_per_task
                        }
                        false => 0,
                    };

                    let verifier_average_per_task = match number_of_verifier_averages > 0 {
                        true => {
                            let verifier_average_per_task = cumulative_verifier_averages / number_of_verifier_averages;
                            metrics.verifier_average_per_task = Some(verifier_average_per_task);
                            verifier_average_per_task
                        }
                        false => 0,
                    };

                    (contributor_average_per_task, verifier_average_per_task)
                };

                // Estimate the time remaining for the current round.
                {
                    let number_of_contributors_left = self.current_contributors.len() as u64;
                    if number_of_contributors_left > 0 {
                        let cumulative_seconds = self
                            .current_contributors
                            .par_iter()
                            .map(|(participant, participant_info)| {
                                let seconds = match metrics.seconds_per_task.get(participant) {
                                    Some(seconds) => *seconds,
                                    None => contributor_average_per_task,
                                };

                                seconds
                                    * (participant_info.pending_tasks.len() + participant_info.assigned_tasks.len())
                                        as u64
                            })
                            .sum::<u64>();

                        let estimated_time_remaining = match self.environment.parameters().proving_system() {
                            ProvingSystem::Groth16 => (cumulative_seconds / number_of_contributors_left) / 2,
                            ProvingSystem::Marlin => cumulative_seconds / number_of_contributors_left,
                        };

                        let estimated_aggregation_time = (contributor_average_per_task + verifier_average_per_task)
                            * self.environment.number_of_chunks();

                        let estimated_queue_time = self.environment.queue_wait_time();

                        // Note that these are extremely rough estimates. These should be updated
                        // to be much more granular, if used in mission-critical logic.
                        metrics.estimated_finish_time = Some(estimated_time_remaining);
                        metrics.estimated_aggregation_time = Some(estimated_aggregation_time);
                        metrics.estimated_wait_time =
                            Some(estimated_time_remaining + estimated_aggregation_time + estimated_queue_time);
                    }
                }
            };
        }
    }

    ///
    /// Prepares transition of the coordinator state from the current round to the next round.
    /// On precommit success, returns the list of contributors and verifiers for the next round.
    ///
    #[inline]
    pub(super) fn precommit_next_round(
        &mut self,
        next_round_height: u64,
        time: &dyn TimeSource,
    ) -> Result<(Vec<Participant>, Vec<Participant>), CoordinatorError> {
        trace!("Attempting to run precommit for round {}", next_round_height);

        // Check that the coordinator state is initialized.
        if self.status == CoordinatorStatus::Initializing {
            return Err(CoordinatorError::CoordinatorStateNotInitialized);
        }

        // Check that the coordinator is not already in the precommit stage.
        if self.status == CoordinatorStatus::Precommit {
            return Err(CoordinatorError::NextRoundAlreadyInPrecommit);
        }

        // Check that the given round height is correct.
        // Fetch the next round height.
        let current_round_height = match self.current_round_height {
            Some(current_round_height) => {
                if next_round_height != current_round_height + 1 {
                    error!(
                        "Attempting to precommit to round {} when the next round should be {}",
                        next_round_height,
                        current_round_height + 1
                    );
                    return Err(CoordinatorError::RoundHeightMismatch);
                }
                current_round_height
            }
            _ => return Err(CoordinatorError::RoundHeightNotSet),
        };

        // Check that the queue contains participants.
        if self.queue.is_empty() {
            return Err(CoordinatorError::QueueIsEmpty);
        }

        // Check that the staging area for the next round is empty.
        if !self.next.is_empty() {
            return Err(CoordinatorError::NextRoundShouldBeEmpty);
        }

        // Check that the current round is complete.
        if !self.is_current_round_finished() {
            return Err(CoordinatorError::RoundNotComplete);
        }

        // Check that the current round is aggregated.
        if self.current_round_height() > 0 && !self.is_current_round_aggregated() {
            return Err(CoordinatorError::RoundNotAggregated);
        }

        // Check that the time to trigger the next round has been reached, if it is set.
        if let Some(metrics) = &self.current_metrics {
            if let Some(next_round_after) = metrics.next_round_after {
                if time.utc_now() < next_round_after {
                    return Err(CoordinatorError::QueueWaitTimeIncomplete);
                }
            }
        }

        // Parse the queued participants for the next round and split into contributors and verifiers.
        let mut contributors: Vec<(_, (_, _))> = self
            .queue
            .clone()
            .into_par_iter()
            .map(|(p, (r, rh))| (p, (r, rh.unwrap_or_default())))
            .filter(|(p, (_, rh))| p.is_contributor() && *rh == next_round_height)
            .collect();
        let verifiers: Vec<(_, (_, _))> = self
            .queue
            .clone()
            .into_par_iter()
            .map(|(p, (r, rh))| (p, (r, rh.unwrap_or_default())))
            .filter(|(p, (_, rh))| p.is_verifier() && *rh == next_round_height)
            .collect();

        // Check that each participant in the next round is authorized.
        if contributors
            .par_iter()
            .filter(|(participant, _)| self.banned.contains(participant))
            .count()
            > 0
        {
            return Err(CoordinatorError::ParticipantUnauthorized);
        }
        if verifiers
            .par_iter()
            .filter(|(participant, _)| self.banned.contains(participant))
            .count()
            > 0
        {
            return Err(CoordinatorError::ParticipantUnauthorized);
        }

        // Check that the next round contains a permitted number of contributors.
        let minimum_contributors = self.environment.minimum_contributors_per_round();
        let maximum_contributors = self.environment.maximum_contributors_per_round();
        let number_of_contributors = contributors.len();
        if number_of_contributors < minimum_contributors || number_of_contributors > maximum_contributors {
            warn!(
                "Precommit found {} contributors, but expected between {} and {} contributors",
                number_of_contributors, minimum_contributors, maximum_contributors
            );
            return Err(CoordinatorError::RoundNumberOfContributorsUnauthorized);
        }

        // Check that the next round contains a permitted number of verifiers.
        let minimum_verifiers = self.environment.minimum_verifiers_per_round();
        let maximum_verifiers = self.environment.maximum_verifiers_per_round();
        let number_of_verifiers = verifiers.len();
        if number_of_verifiers < minimum_verifiers || number_of_verifiers > maximum_verifiers {
            warn!(
                "Precommit found {} verifiers, but expected between {} and {} verifiers",
                number_of_verifiers, minimum_verifiers, maximum_verifiers
            );
            return Err(CoordinatorError::RoundNumberOfVerifiersUnauthorized);
        }

        // Initialize the precommit stage for the next round.
        let mut queue = self.queue.clone();
        let mut next = HashMap::default();
        let mut next_contributors = Vec::with_capacity(number_of_contributors);
        let mut next_verifiers = Vec::with_capacity(number_of_verifiers);

        // Create the initial chunk locking sequence for each contributor.
        {
            /* ***********************************************************************************
             *   The following is the approach for contributor task assignments.
             * ***********************************************************************************
             *
             *   N := NUMBER_OF_CONTRIBUTORS
             *   BUCKET_SIZE := NUMBER_OF_CHUNKS / NUMBER_OF_CONTRIBUTORS
             *
             * ***********************************************************************************
             *
             *   [    BUCKET 1    |    BUCKET 2    |    BUCKET 3    |  . . .  |    BUCKET N    ]
             *
             *   [  CONTRIBUTOR 1  --------------------------------------------------------->  ]
             *   [  ------------->  CONTRIBUTOR 2  ------------------------------------------  ]
             *   [  ------------------------------>  CONTRIBUTOR 3  -------------------------  ]
             *   [                                        .                                    ]
             *   [                                        .                                    ]
             *   [                                        .                                    ]
             *   [  --------------------------------------------------------->  CONTRIBUTOR N  ]
             *
             * ***********************************************************************************
             *
             *   1. Sort the round contributors from most reliable to least reliable.
             *
             *   2. Assign CONTRIBUTOR 1 to BUCKET 1, CONTRIBUTOR 2 to BUCKET 2,
             *      CONTRIBUTOR 3 to BUCKET 3, ..., CONTRIBUTOR N to BUCKET N,
             *      as the starting INDEX to contribute to in the round.
             *
             *   3. Construct the set of tasks for each contributor as follows:
             *
             *      for ID in 0..NUMBER_OF_CHUNKS:
             *          CHUNK_ID := (INDEX * BUCKET_SIZE + ID) % NUMBER_OF_CHUNKS
             *          CONTRIBUTION_ID := INDEX.
             *
             * ***********************************************************************************
             */

            // Sort the contributors by their reliability (in order of highest to lowest number).
            contributors.par_sort_by(|a, b| ((b.1).0).cmp(&(&a.1).0));

            // Fetch the number of chunks and bucket size.
            let number_of_chunks = self.environment.number_of_chunks() as u64;

            // Set the chunk ID ordering for each contributor.
            for (bucket_index, (participant, (reliability, next_round))) in contributors.into_iter().enumerate() {
                let bucket_id = bucket_index as u64;
                let tasks = initialize_tasks(bucket_id, number_of_chunks, number_of_contributors as u64)?;

                // Check that each participant is storing the correct round height.
                if next_round != next_round_height && next_round != current_round_height + 1 {
                    warn!("Contributor claims round is {}, not {}", next_round, next_round_height);
                    return Err(CoordinatorError::RoundHeightMismatch);
                }

                // Initialize the participant info for the contributor.
                let mut participant_info =
                    ParticipantInfo::new(participant.clone(), next_round_height, reliability, bucket_id, time);
                participant_info.start(tasks, time)?;

                // Check that the chunk IDs are set in the participant information.
                if participant_info.assigned_tasks.is_empty() {
                    return Err(CoordinatorError::ParticipantNotReady);
                }

                // Add the contributor to staging for the next round.
                next.insert(participant.clone(), participant_info);

                // Remove the contributor from the queue.
                queue.remove(&participant);

                // Add the next round contributors to the return output.
                next_contributors.push(participant);
            }
        }

        // Initialize the participant info for each verifier.
        for (participant, (reliability, next_round)) in verifiers {
            // Check that each participant is storing the correct round height.
            if next_round != next_round_height && next_round != current_round_height + 1 {
                warn!("Verifier claims round is {}, not {}", next_round, next_round_height);
                return Err(CoordinatorError::RoundHeightMismatch);
            }

            // Initialize the participant info for the verifier.
            let mut participant_info =
                ParticipantInfo::new(participant.clone(), next_round_height, reliability, 0, time);
            participant_info.start(LinkedList::new(), time)?;

            // Add the verifier to staging for the next round.
            next.insert(participant.clone(), participant_info);

            // Remove the verifier from the queue.
            queue.remove(&participant);

            // Add the next round contributors to the return output.
            next_verifiers.push(participant);
        }

        // Update the coordinator state to the updated queue and next map.
        self.queue = queue;
        self.next = next;

        // Set the coordinator status to precommit.
        self.status = CoordinatorStatus::Precommit;

        Ok((next_contributors, next_verifiers))
    }

    ///
    /// Executes transition of the coordinator state from the current round to the next round.
    ///
    /// This function always executes without failure or exists without modifying state
    /// if the commit was unauthorized.
    ///
    #[inline]
    pub(super) fn commit_next_round(&mut self) {
        // Check that the coordinator is authorized to advance to the next round.
        if self.status != CoordinatorStatus::Precommit {
            error!("Coordinator is not in the precommit stage and cannot advance the round");
            return;
        }

        // Increment the current round height.
        let next_round_height = match self.current_round_height {
            Some(current_round_height) => {
                trace!("Coordinator has advanced to round {}", current_round_height + 1);
                current_round_height + 1
            }
            None => {
                error!("Coordinator cannot commit to the next round without initializing the round height");
                return;
            }
        };
        self.current_round_height = Some(next_round_height);

        // Set the current status to the commit.
        self.status = CoordinatorStatus::Commit;

        // Add all participants from next to current.
        let mut number_of_contributors = 0;
        let mut number_of_verifiers = 0;
        for (participant, participant_info) in self.next.iter() {
            match participant {
                Participant::Contributor(_) => {
                    self.current_contributors
                        .insert(participant.clone(), participant_info.clone());
                    number_of_contributors += 1;
                }
                Participant::Verifier(_) => {
                    self.current_verifiers
                        .insert(participant.clone(), participant_info.clone());
                    number_of_verifiers += 1;
                }
            };
        }

        // Initialize the metrics for this round.
        self.current_metrics = Some(RoundMetrics {
            number_of_contributors,
            number_of_verifiers,
            is_round_aggregated: false,
            task_timer: HashMap::new(),
            seconds_per_task: HashMap::new(),
            contributor_average_per_task: None,
            verifier_average_per_task: None,
            started_aggregation_at: None,
            finished_aggregation_at: None,
            estimated_finish_time: None,
            estimated_aggregation_time: None,
            estimated_wait_time: None,
            next_round_after: None,
        });

        // Initialize the finished contributors map for the next round.
        self.finished_contributors.insert(next_round_height, HashMap::new());

        // Initialize the finished verifiers map for the next round.
        self.finished_verifiers.insert(next_round_height, HashMap::new());

        // Reset the next round map.
        self.next = HashMap::new();
    }

    ///
    /// Rolls back the precommit of the coordinator state for transitioning to the next round.
    ///
    /// This function always executes without failure or exists without modifying state
    /// if the rollback was unauthorized.
    ///
    #[inline]
    pub(super) fn rollback_next_round(&mut self) {
        // Check that the coordinator is authorized to rollback.
        if self.status != CoordinatorStatus::Precommit {
            error!("Coordinator is not in the precommit stage and cannot rollback");
            return;
        }

        // Set the current status to the commit.
        self.status = CoordinatorStatus::Rollback;

        // Add each participant back into the queue.
        for (participant, participant_info) in &self.next {
            self.queue.insert(
                participant.clone(),
                (participant_info.reliability, Some(participant_info.round_height)),
            );
        }

        // Reset the next round map.
        self.next = HashMap::new();

        trace!("Coordinator has rolled back");
    }

    ///
    /// Returns the status of the coordinator state.
    ///
    #[inline]
    pub(super) fn status_report(&self, time: &dyn TimeSource) -> String {
        let current_round_height = self.current_round_height.unwrap_or_default();
        let next_round_height = current_round_height + 1;

        let current_round_finished = match self.is_current_round_finished() {
            true => format!("Round {} is finished", current_round_height),
            false => format!("Round {} is in progress", current_round_height),
        };
        let current_round_aggregated = match (self.is_current_round_aggregated(), current_round_height) {
            (_, 0) => format!("Round {} can skip aggregation", current_round_height),
            (true, _) => format!("Round {} is aggregated", current_round_height),
            (false, _) => format!("Round {} is awaiting aggregation", current_round_height),
        };
        let precommit_next_round_ready = match self.is_precommit_next_round_ready(time) {
            true => format!("Round {} is ready to begin", next_round_height),
            false => format!("Round {} is awaiting participants", next_round_height),
        };

        let number_of_current_contributors = self.current_contributors.len();
        let number_of_current_verifiers = self.current_verifiers.len();
        let number_of_finished_contributors = self
            .finished_contributors
            .get(&current_round_height)
            .get_or_insert(&HashMap::new())
            .len();
        let number_of_finished_verifiers = self
            .finished_verifiers
            .get(&current_round_height)
            .get_or_insert(&HashMap::new())
            .len();
        let number_of_pending_verifications = self.pending_verification.len();

        // Parse the queue for assigned contributors and verifiers of the next round.
        let number_of_assigned_contributors = self
            .queue
            .clone()
            .into_par_iter()
            .filter(|(p, (_, rh))| p.is_contributor() && rh.unwrap_or_default() == next_round_height)
            .count();
        let number_of_assigned_verifiers = self
            .queue
            .clone()
            .into_par_iter()
            .filter(|(p, (_, rh))| p.is_verifier() && rh.unwrap_or_default() == next_round_height)
            .count();

        let number_of_queue_contributors = self.number_of_queue_contributors();
        let number_of_queue_verifiers = self.number_of_queue_verifiers();

        let number_of_dropped_participants = self.dropped.len();
        let number_of_banned_participants = self.banned.len();

        format!(
            r#"
    ----------------------------------------------------------------
    ||                        STATUS REPORT                       ||
    ----------------------------------------------------------------

    | {}
    | {}
    | {}

    | {} contributors and {} verifiers active in the current round
    | {} contributors and {} verifiers completed the current round
    | {} chunks are pending verification

    | {} contributors and {} verifiers assigned to the next round
    | {} contributors and {} verifiers in queue for the ceremony

    | {} participants dropped
    | {} participants banned

    "#,
            current_round_finished,
            current_round_aggregated,
            precommit_next_round_ready,
            number_of_current_contributors,
            number_of_current_verifiers,
            number_of_finished_contributors,
            number_of_finished_verifiers,
            number_of_pending_verifications,
            number_of_assigned_contributors,
            number_of_assigned_verifiers,
            number_of_queue_contributors,
            number_of_queue_verifiers,
            number_of_dropped_participants,
            number_of_banned_participants
        )
    }

    /// Updates the coordinator state with the knowledge that the
    /// participant is still alive and participating (or waiting to
    /// participate) in the ceremony.
    pub(crate) fn heartbeat(
        &mut self,
        participant: &Participant,
        time: &dyn TimeSource,
    ) -> Result<(), CoordinatorError> {
        if let Some(_) = self.queue.iter_mut().find(|(p, _score)| *p == participant) {
            // TODO: update reliability score if contributor is in the queue.
            return Ok(());
        }

        let info = self
            .current_contributors
            .iter_mut()
            .find(|(p, _info)| *p == participant)
            .map(|(_p, info)| info);

        let info = match info {
            Some(info) => Some(info),
            None => self
                .finished_contributors
                .iter_mut()
                .map(|(_round, finished_contributors)| {
                    finished_contributors
                        .iter_mut()
                        .find(|(p, _info)| *p == participant)
                        .map(|(_p, info)| info)
                })
                .next()
                .flatten(),
        };

        if let Some(info) = info {
            info.last_seen = time.utc_now();
            Ok(())
        } else {
            Err(CoordinatorError::ParticipantNotFound(participant.clone()))
        }
    }

    /// Save the coordinator state in storage.
    #[inline]
    pub(crate) fn save(&self, storage: &mut StorageLock) -> Result<(), CoordinatorError> {
        storage.update(&Locator::CoordinatorState, Object::CoordinatorState(self.clone()))
    }
}

/// Data required by the coordinator to drop a participant from the
/// ceremony.
#[derive(Debug)]
pub(crate) struct DropCurrentParticpantData {
    /// The participant being dropped.
    pub participant: Participant,
    /// Determines the starting chunk, and subsequent tasks selected
    /// for this participant. See [initialize_tasks] for more
    /// information about this parameter.
    pub bucket_id: u64,
    /// Chunks currently locked by the participant.
    pub locked_chunks: Vec<u64>,
    /// Tasks currently being performed by the participant.
    pub tasks: Vec<Task>,
    /// The participant which will replace the participant being
    /// dropped.
    pub replacement: Option<Participant>,
}

#[derive(Debug)]
pub(crate) struct DropInactiveParticipantData {
    /// The participant being dropped.
    pub participant: Participant,
}

/// The reason for dropping a participant and the and data needed to
/// perform the drop.
#[derive(Debug)]
pub(crate) enum DropParticipant {
    /// Coordinator has decided that a participant needs to be banned
    /// (for a variety of potential reasons).
    BanCurrent(DropCurrentParticpantData),
    /// Coordinator has decided that a participant needs to be dropped
    /// (for a variety of potential reasons).
    DropCurrent(DropCurrentParticpantData),
    /// Coordinator has decided that a participant in the queue is
    /// inactive and needs to be removed from the queue.
    Inactive(DropInactiveParticipantData),
}

#[cfg(test)]
mod tests {
    use crate::{coordinator_state::*, testing::prelude::*, CoordinatorState, SystemTimeSource};

    #[test]
    fn test_new() {
        // Initialize a new coordinator state.
        let state = CoordinatorState::new(TEST_ENVIRONMENT.clone());
        assert_eq!(0, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(None, state.current_round_height);
        assert_eq!(0, state.current_contributors.len());
        assert_eq!(0, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.len());
        assert_eq!(0, state.finished_verifiers.len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());
    }

    #[test]
    fn test_set_current_round_height() {
        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(TEST_ENVIRONMENT.clone());
        assert_eq!(None, state.current_round_height);

        // Set the current round height for coordinator state.
        let current_round_height = 5;
        state.initialize(current_round_height);
        assert_eq!(Some(current_round_height), state.current_round_height);
    }

    #[test]
    fn test_add_to_queue_contributor() {
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the contributor of the coordinator.
        let contributor = test_coordinator_contributor(&environment).unwrap();
        assert!(contributor.is_contributor());

        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(environment.clone());
        assert_eq!(0, state.queue.len());

        // Add the contributor of the coordinator.
        state.add_to_queue(contributor.clone(), 10).unwrap();
        assert_eq!(1, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(None, state.current_round_height);
        assert_eq!(0, state.current_contributors.len());
        assert_eq!(0, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.len());
        assert_eq!(0, state.finished_verifiers.len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());

        // Fetch the contributor from the queue.
        let participant = state.queue.get(&contributor);
        assert_eq!(Some(&(10, None)), participant);

        // Attempt to add the contributor again.
        for _ in 0..10 {
            let result = state.add_to_queue(contributor.clone(), 10);
            assert!(result.is_err());
            assert_eq!(1, state.queue.len());
        }
    }

    #[test]
    fn test_add_to_queue_verifier() {
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the verifier of the coordinator.
        let verifier = test_coordinator_verifier(&environment).unwrap();
        assert!(verifier.is_verifier());

        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(environment.clone());
        assert_eq!(0, state.queue.len());

        // Add the verifier of the coordinator.
        state.add_to_queue(verifier.clone(), 10).unwrap();
        assert_eq!(1, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(None, state.current_round_height);
        assert_eq!(0, state.current_contributors.len());
        assert_eq!(0, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.len());
        assert_eq!(0, state.finished_verifiers.len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());

        // Fetch the verifier from the queue.
        let participant = state.queue.get(&verifier);
        assert_eq!(Some(&(10, None)), participant);

        // Attempt to add the verifier again.
        for _ in 0..10 {
            let result = state.add_to_queue(verifier.clone(), 10);
            assert!(result.is_err());
            assert_eq!(1, state.queue.len());
        }
    }

    #[test]
    fn test_update_queue() {
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the contributor and verifier of the coordinator.
        let contributor = test_coordinator_contributor(&environment).unwrap();
        let verifier = test_coordinator_verifier(&environment).unwrap();

        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(environment.clone());
        assert_eq!(0, state.queue.len());
        assert_eq!(None, state.current_round_height);

        // Set the current round height for coordinator state.
        let current_round_height = 5;
        state.initialize(current_round_height);
        assert_eq!(0, state.queue.len());
        assert_eq!(Some(current_round_height), state.current_round_height);

        // Add the contributor and verifier of the coordinator.
        state.add_to_queue(contributor.clone(), 10).unwrap();
        state.add_to_queue(verifier.clone(), 10).unwrap();
        assert_eq!(2, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);

        // Fetch the contributor from the queue.
        let participant = state.queue.get(&contributor);
        assert_eq!(Some(&(10, None)), participant);

        // Fetch the verifier from the queue.
        let participant = state.queue.get(&verifier);
        assert_eq!(Some(&(10, None)), participant);

        // Update the state of the queue.
        state.update_queue().unwrap();
        assert_eq!(2, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
        assert_eq!(0, state.current_contributors.len());
        assert_eq!(0, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.get(&current_round_height).unwrap().len());
        assert_eq!(0, state.finished_verifiers.get(&current_round_height).unwrap().len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());

        // Fetch the contributor from the queue.
        let participant = state.queue.get(&contributor);
        assert_eq!(Some(&(10, Some(6))), participant);

        // Fetch the verifier from the queue.
        let participant = state.queue.get(&verifier);
        assert_eq!(Some(&(10, Some(6))), participant);

        // Attempt to add the contributor and verifier again.
        for _ in 0..10 {
            let contributor_result = state.add_to_queue(contributor.clone(), 10);
            let verifier_result = state.add_to_queue(verifier.clone(), 10);
            assert!(contributor_result.is_err());
            assert!(verifier_result.is_err());
            assert_eq!(2, state.queue.len());
        }
    }

    #[test]
    fn test_update_queue_assignment() {
        let environment = TEST_ENVIRONMENT.clone();

        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(environment.clone());
        assert_eq!(0, state.queue.len());
        assert_eq!(None, state.current_round_height);

        // Set the current round height for coordinator state.
        let current_round_height = 5;
        state.initialize(current_round_height);
        assert_eq!(0, state.queue.len());
        assert_eq!(Some(current_round_height), state.current_round_height);

        // Add (2 * maximum_contributors_per_round) to the queue.
        let maximum_contributors_per_round = environment.maximum_contributors_per_round();
        let number_of_contributors_in_queue = 2 * maximum_contributors_per_round;
        for id in 1..=number_of_contributors_in_queue {
            trace!("Adding contributor with ID {}", id);

            // Add a unique contributor.
            let contributor = Participant::Contributor(id.to_string());
            let reliability = 10 - id as u8;
            state.add_to_queue(contributor.clone(), reliability).unwrap();
            assert_eq!(id, state.queue.len());
            assert_eq!(0, state.next.len());
            assert_eq!(Some(current_round_height), state.current_round_height);

            // Fetch the contributor from the queue.
            let participant = state.queue.get(&contributor);
            assert_eq!(Some(&(reliability, None)), participant);

            // Update the state of the queue.
            state.update_queue().unwrap();
            assert_eq!(id, state.queue.len());
            assert_eq!(0, state.next.len());
            assert_eq!(Some(current_round_height), state.current_round_height);

            // Fetch the contributor from the queue.
            let participant = state.queue.get(&contributor);
            match id <= maximum_contributors_per_round {
                true => assert_eq!(Some(&(reliability, Some(6))), participant),
                false => assert_eq!(Some(&(reliability, Some(7))), participant),
            }
        }

        // Update the state of the queue.
        state.update_queue().unwrap();
        assert_eq!(number_of_contributors_in_queue, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);

        // Add (2 * maximum_verifiers_per_round) to the queue.
        let maximum_verifiers_per_round = environment.maximum_verifiers_per_round();
        let number_of_verifiers_in_queue = 2 * maximum_verifiers_per_round;
        for id in 1..=number_of_verifiers_in_queue {
            trace!("Adding verifier with ID {}", id);

            // Add a unique verifier.
            let verifier = Participant::Verifier(id.to_string());
            let reliability = 10 - id as u8;
            state.add_to_queue(verifier.clone(), reliability).unwrap();
            assert_eq!(number_of_contributors_in_queue + id, state.queue.len());
            assert_eq!(0, state.next.len());
            assert_eq!(Some(current_round_height), state.current_round_height);

            // Fetch the verifier from the queue.
            let participant = state.queue.get(&verifier);
            assert_eq!(Some(&(reliability, None)), participant);

            // Update the state of the queue.
            state.update_queue().unwrap();
            assert_eq!(number_of_contributors_in_queue + id, state.queue.len());
            assert_eq!(0, state.next.len());
            assert_eq!(Some(current_round_height), state.current_round_height);

            // Fetch the verifier from the queue.
            let participant = state.queue.get(&verifier);
            match id <= maximum_verifiers_per_round {
                true => assert_eq!(Some(&(reliability, Some(6))), participant),
                false => assert_eq!(Some(&(reliability, Some(7))), participant),
            }
        }

        // Update the state of the queue.
        state.update_queue().unwrap();
        assert_eq!(
            number_of_contributors_in_queue + number_of_verifiers_in_queue,
            state.queue.len()
        );
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
    }

    #[test]
    fn test_remove_from_queue_contributor() {
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the contributor of the coordinator.
        let contributor = test_coordinator_contributor(&environment).unwrap();

        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(environment.clone());
        assert_eq!(0, state.queue.len());

        // Add the contributor of the coordinator.
        state.add_to_queue(contributor.clone(), 10).unwrap();
        assert_eq!(1, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(None, state.current_round_height);

        // Fetch the contributor from the queue.
        let participant = state.queue.get(&contributor);
        assert_eq!(Some(&(10, None)), participant);

        // Remove the contributor from the queue.
        state.remove_from_queue(&contributor).unwrap();
        assert_eq!(0, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(None, state.current_round_height);

        // Attempt to remove the contributor again.
        for _ in 0..10 {
            let result = state.remove_from_queue(&contributor);
            assert!(result.is_err());
            assert_eq!(0, state.queue.len());
        }
    }

    #[test]
    fn test_remove_to_queue_verifier() {
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the verifier of the coordinator.
        let verifier = test_coordinator_verifier(&environment).unwrap();

        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(environment.clone());
        assert_eq!(0, state.queue.len());

        // Add the verifier of the coordinator.
        state.add_to_queue(verifier.clone(), 10).unwrap();
        assert_eq!(1, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(None, state.current_round_height);

        // Fetch the verifier from the queue.
        let participant = state.queue.get(&verifier);
        assert_eq!(Some(&(10, None)), participant);

        // Remove the verifier from the queue.
        state.remove_from_queue(&verifier).unwrap();
        assert_eq!(0, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(None, state.current_round_height);

        // Attempt to remove the verifier again.
        for _ in 0..10 {
            let result = state.remove_from_queue(&verifier);
            assert!(result.is_err());
            assert_eq!(0, state.queue.len());
        }
    }

    #[test]
    fn test_commit_next_round() {
        test_logger();
        let time = SystemTimeSource::new();
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the contributor and verifier of the coordinator.
        let contributor = test_coordinator_contributor(&environment).unwrap();
        let verifier = test_coordinator_verifier(&environment).unwrap();

        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(environment.clone());
        assert_eq!(0, state.queue.len());
        assert_eq!(None, state.current_round_height);

        // Set the current round height for coordinator state.
        let current_round_height = 5;
        state.initialize(current_round_height);
        assert_eq!(0, state.queue.len());
        assert_eq!(Some(current_round_height), state.current_round_height);

        // Add the contributor and verifier of the coordinator.
        state.add_to_queue(contributor.clone(), 10).unwrap();
        state.add_to_queue(verifier.clone(), 10).unwrap();
        assert_eq!(2, state.queue.len());

        // Update the state of the queue.
        state.update_queue().unwrap();
        assert_eq!(2, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
        assert_eq!(Some(&(10, Some(6))), state.queue.get(&contributor));
        assert_eq!(Some(&(10, Some(6))), state.queue.get(&verifier));

        // TODO (howardwu): Add individual tests and assertions after each of these operations.
        {
            // Update the current round to aggregated.
            state.aggregating_current_round(&time).unwrap();
            state.aggregated_current_round(&time).unwrap();

            // Update the current round metrics.
            state.update_round_metrics();

            // Update the state of current round contributors.
            state.update_current_contributors(&time).unwrap();

            // Update the state of current round verifiers.
            state.update_current_verifiers(&time).unwrap();

            // Drop disconnected participants from the current round.
            state.update_dropped_participants(&time).unwrap();

            // Ban any participants who meet the coordinator criteria.
            state.update_banned_participants().unwrap();
        }

        // Determine if current round is finished and precommit to next round is ready.
        assert_eq!(2, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
        assert!(state.is_current_round_finished());
        assert!(state.is_current_round_aggregated());
        assert!(state.is_precommit_next_round_ready(&time));

        // Attempt to advance the round.
        trace!("Running precommit for the next round");
        let next_round_height = current_round_height + 1;
        let _precommit = state.precommit_next_round(next_round_height, &time).unwrap();
        assert_eq!(0, state.queue.len());
        assert_eq!(2, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
        assert!(state.is_current_round_finished());
        assert!(state.is_current_round_aggregated());
        assert!(!state.is_precommit_next_round_ready(&time));

        // Advance the coordinator to the next round.
        state.commit_next_round();
        assert_eq!(0, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(next_round_height), state.current_round_height);
        assert_eq!(1, state.current_contributors.len());
        assert_eq!(1, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.get(&next_round_height).unwrap().len());
        assert_eq!(0, state.finished_verifiers.get(&next_round_height).unwrap().len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());
        assert!(!state.is_current_round_finished());
        assert!(!state.is_current_round_aggregated());
        assert!(!state.is_precommit_next_round_ready(&time));
    }

    #[test]
    fn test_rollback_next_round() {
        test_logger();

        let time = SystemTimeSource::new();
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the contributor and verifier of the coordinator.
        let contributor = test_coordinator_contributor(&environment).unwrap();
        let verifier = test_coordinator_verifier(&environment).unwrap();

        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(environment.clone());
        assert_eq!(0, state.queue.len());
        assert_eq!(None, state.current_round_height);

        // Set the current round height for coordinator state.
        let current_round_height = 5;
        state.initialize(current_round_height);
        assert_eq!(0, state.queue.len());
        assert_eq!(Some(current_round_height), state.current_round_height);

        // Add the contributor and verifier of the coordinator.
        state.add_to_queue(contributor.clone(), 10).unwrap();
        state.add_to_queue(verifier.clone(), 10).unwrap();
        assert_eq!(2, state.queue.len());

        // Update the state of the queue.
        state.update_queue().unwrap();
        assert_eq!(2, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
        assert_eq!(Some(&(10, Some(6))), state.queue.get(&contributor));
        assert_eq!(Some(&(10, Some(6))), state.queue.get(&verifier));

        // TODO (howardwu): Add individual tests and assertions after each of these operations.
        {
            // Update the current round to aggregated.
            state.aggregating_current_round(&time).unwrap();
            state.aggregated_current_round(&time).unwrap();

            // Update the current round metrics.
            state.update_round_metrics();

            // Update the state of current round contributors.
            state.update_current_contributors(&time).unwrap();

            // Update the state of current round verifiers.
            state.update_current_verifiers(&time).unwrap();

            // Drop disconnected participants from the current round.
            state.update_dropped_participants(&time).unwrap();

            // Ban any participants who meet the coordinator criteria.
            state.update_banned_participants().unwrap();
        }

        // Determine if current round is finished and precommit to next round is ready.
        assert_eq!(2, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
        assert!(state.is_current_round_finished());
        assert!(state.is_current_round_aggregated());
        assert!(state.is_precommit_next_round_ready(&time));

        // Attempt to advance the round.
        trace!("Running precommit for the next round");
        let _precommit = state.precommit_next_round(current_round_height + 1, &time).unwrap();
        assert_eq!(0, state.queue.len());
        assert_eq!(2, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
        assert!(state.is_current_round_finished());
        assert!(state.is_current_round_aggregated());
        assert!(!state.is_precommit_next_round_ready(&time));

        // Rollback the coordinator to the current round.
        state.rollback_next_round();
        assert_eq!(2, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
        assert_eq!(0, state.current_contributors.len());
        assert_eq!(0, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.get(&current_round_height).unwrap().len());
        assert_eq!(0, state.finished_verifiers.get(&current_round_height).unwrap().len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());
        assert!(state.is_current_round_finished());
        assert!(state.is_current_round_aggregated());
        assert!(state.is_precommit_next_round_ready(&time));
    }

    #[test]
    fn test_pop_and_complete_tasks_contributor() {
        let time = SystemTimeSource::new();
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the contributor and verifier of the coordinator.
        let contributor = test_coordinator_contributor(&environment).unwrap();
        let verifier = test_coordinator_verifier(&environment).unwrap();

        // Initialize a new coordinator state.
        let current_round_height = 5;
        let mut state = CoordinatorState::new(environment.clone());
        state.initialize(current_round_height);
        state.add_to_queue(contributor.clone(), 10).unwrap();
        state.add_to_queue(verifier.clone(), 10).unwrap();
        state.update_queue().unwrap();
        state.aggregating_current_round(&time).unwrap();
        state.aggregated_current_round(&time).unwrap();
        assert!(state.is_current_round_finished());
        assert!(state.is_current_round_aggregated());
        assert!(state.is_precommit_next_round_ready(&time));

        // Advance the coordinator to the next round.
        let next_round_height = current_round_height + 1;
        state.precommit_next_round(next_round_height, &time).unwrap();
        state.commit_next_round();
        assert_eq!(0, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(next_round_height), state.current_round_height);
        assert_eq!(1, state.current_contributors.len());
        assert_eq!(1, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.get(&next_round_height).unwrap().len());
        assert_eq!(0, state.finished_verifiers.get(&next_round_height).unwrap().len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());

        // Fetch the maximum number of tasks permitted for a contributor.
        let contributor_lock_chunk_limit = environment.contributor_lock_chunk_limit();
        for chunk_id in 0..contributor_lock_chunk_limit {
            // Fetch a pending task for the contributor.
            let task = state.fetch_task(&contributor, &time).unwrap();
            assert_eq!((chunk_id as u64, 1), (task.chunk_id(), task.contribution_id()));

            state.acquired_lock(&contributor, task.chunk_id(), &time).unwrap();
            assert_eq!(0, state.pending_verification.len());
        }

        assert_eq!(Some(next_round_height), state.current_round_height);
        assert_eq!(1, state.current_contributors.len());
        assert_eq!(1, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.get(&next_round_height).unwrap().len());
        assert_eq!(0, state.finished_verifiers.get(&next_round_height).unwrap().len());

        // Attempt to fetch past the permitted lock chunk limit.
        for _ in 0..10 {
            let try_task = state.fetch_task(&contributor, &time);
            assert!(try_task.is_err());
        }
    }

    #[test]
    fn test_pop_and_complete_tasks_verifier() {
        let time = SystemTimeSource::new();
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the contributor and verifier of the coordinator.
        let contributor = test_coordinator_contributor(&environment).unwrap();
        let verifier = test_coordinator_verifier(&environment).unwrap();

        // Initialize a new coordinator state.
        let current_round_height = 5;
        let mut state = CoordinatorState::new(environment.clone());
        state.initialize(current_round_height);
        state.add_to_queue(contributor.clone(), 10).unwrap();
        state.add_to_queue(verifier.clone(), 10).unwrap();
        state.update_queue().unwrap();
        state.aggregating_current_round(&time).unwrap();
        state.aggregated_current_round(&time).unwrap();
        assert!(state.is_current_round_finished());
        assert!(state.is_current_round_aggregated());
        assert!(state.is_precommit_next_round_ready(&time));

        // Advance the coordinator to the next round.
        let next_round_height = current_round_height + 1;
        state.precommit_next_round(next_round_height, &time).unwrap();
        state.commit_next_round();
        assert_eq!(Some(next_round_height), state.current_round_height);
        assert_eq!(1, state.current_contributors.len());
        assert_eq!(1, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.get(&next_round_height).unwrap().len());
        assert_eq!(0, state.finished_verifiers.get(&next_round_height).unwrap().len());

        // Ensure that the verifier cannot pop a task prior to a contributor completing a task.
        let try_task = state.fetch_task(&verifier, &time);
        assert!(try_task.is_err());

        // Fetch the maximum number of tasks permitted for a contributor.
        let contributor_lock_chunk_limit = environment.contributor_lock_chunk_limit();
        for i in 0..contributor_lock_chunk_limit {
            // Fetch a pending task for the contributor.
            let task = state.fetch_task(&contributor, &time).unwrap();
            let chunk_id = i as u64;
            assert_eq!((chunk_id, 1), (task.chunk_id(), task.contribution_id()));

            state.acquired_lock(&contributor, chunk_id, &time).unwrap();
            let completed_task = Task::new(chunk_id, task.contribution_id());
            state.completed_task(&contributor, completed_task, &time).unwrap();
            assert_eq!(i + 1, state.pending_verification.len());
        }
        assert_eq!(Some(next_round_height), state.current_round_height);
        assert_eq!(1, state.current_contributors.len());
        assert_eq!(1, state.current_verifiers.len());
        assert_eq!(contributor_lock_chunk_limit, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.get(&next_round_height).unwrap().len());
        assert_eq!(0, state.finished_verifiers.get(&next_round_height).unwrap().len());

        // Fetch the maximum number of tasks permitted for a verifier.
        for i in 0..environment.verifier_lock_chunk_limit() {
            // Fetch a pending task for the verifier.
            let task = state.fetch_task(&verifier, &time).unwrap();
            assert_eq!((i as u64, 1), (task.chunk_id(), task.contribution_id()));

            state.acquired_lock(&verifier, task.chunk_id(), &time).unwrap();
            state.completed_task(&verifier, task, &time).unwrap();
            assert_eq!(contributor_lock_chunk_limit - i - 1, state.pending_verification.len());
        }
        assert_eq!(Some(next_round_height), state.current_round_height);
        assert_eq!(1, state.current_contributors.len());
        assert_eq!(1, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.get(&next_round_height).unwrap().len());
        assert_eq!(0, state.finished_verifiers.get(&next_round_height).unwrap().len());

        // Attempt to fetch past the permitted lock chunk limit.
        for _ in 0..10 {
            let try_task = state.fetch_task(&verifier, &time);
            assert!(try_task.is_err());
        }
    }

    #[test]
    fn test_round_2x1() {
        test_logger();

        let time = SystemTimeSource::new();
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch two contributors and two verifiers.
        let contributor_1 = TEST_CONTRIBUTOR_ID.clone();
        let contributor_2 = TEST_CONTRIBUTOR_ID_2.clone();
        let verifier = TEST_VERIFIER_ID.clone();

        // Initialize a new coordinator state.
        let current_round_height = 5;
        let mut state = CoordinatorState::new(environment.clone());
        state.initialize(current_round_height);
        state.add_to_queue(contributor_1.clone(), 10).unwrap();
        state.add_to_queue(contributor_2.clone(), 9).unwrap();
        state.add_to_queue(verifier.clone(), 10).unwrap();
        state.update_queue().unwrap();
        state.aggregating_current_round(&time).unwrap();
        state.aggregated_current_round(&time).unwrap();
        assert!(state.is_current_round_finished());
        assert!(state.is_current_round_aggregated());
        assert!(state.is_precommit_next_round_ready(&time));

        // Advance the coordinator to the next round.
        let next_round_height = current_round_height + 1;
        assert_eq!(3, state.queue.len());
        assert_eq!(0, state.next.len());
        state.precommit_next_round(next_round_height, &time).unwrap();
        assert_eq!(0, state.queue.len());
        assert_eq!(3, state.next.len());
        state.commit_next_round();
        assert_eq!(0, state.queue.len());
        assert_eq!(0, state.next.len());

        // Process every chunk in the round as contributor 1 and contributor 2.
        let number_of_chunks = environment.number_of_chunks();
        let tasks1 = initialize_tasks(0, number_of_chunks, 2).unwrap();
        let mut tasks1 = tasks1.iter();
        let tasks2 = initialize_tasks(1, number_of_chunks, 2).unwrap();
        let mut tasks2 = tasks2.iter();
        for _ in 0..number_of_chunks {
            assert_eq!(Some(next_round_height), state.current_round_height);
            assert_eq!(2, state.current_contributors.len());
            assert_eq!(1, state.current_verifiers.len());
            assert_eq!(0, state.pending_verification.len());
            assert_eq!(0, state.finished_contributors.get(&next_round_height).unwrap().len());
            assert_eq!(0, state.finished_verifiers.get(&next_round_height).unwrap().len());
            assert_eq!(0, state.dropped.len());
            assert_eq!(0, state.banned.len());

            // Fetch a pending task for contributor 1.
            let task = state.fetch_task(&contributor_1, &time).unwrap();
            let expected_task1 = tasks1.next();
            assert_eq!(expected_task1, Some(&task));

            state.acquired_lock(&contributor_1, task.chunk_id(), &time).unwrap();
            let assigned_verifier_1 = state.completed_task(&contributor_1, task, &time).unwrap();
            assert_eq!(1, state.pending_verification.len());
            assert!(!state.is_current_round_finished());
            assert_eq!(verifier, assigned_verifier_1);

            // Fetch a pending task for contributor 2.
            let task = state.fetch_task(&contributor_2, &time).unwrap();
            let expected_task2 = tasks2.next();
            assert_eq!(expected_task2, Some(&task));

            state.acquired_lock(&contributor_2, task.chunk_id(), &time).unwrap();
            let assigned_verifier_2 = state.completed_task(&contributor_2, task, &time).unwrap();
            assert_eq!(2, state.pending_verification.len());
            assert!(!state.is_current_round_finished());
            assert_eq!(assigned_verifier_1, assigned_verifier_2);

            // Fetch a pending task for the verifier.
            let task = state.fetch_task(&verifier, &time).unwrap();
            assert_eq!(expected_task1, Some(&task));

            state.acquired_lock(&verifier, task.chunk_id(), &time).unwrap();
            state.completed_task(&verifier, task, &time).unwrap();
            assert_eq!(1, state.pending_verification.len());
            assert!(!state.is_current_round_finished());

            // Fetch a pending task for the verifier.
            let task = state.fetch_task(&verifier, &time).unwrap();
            assert_eq!(expected_task2, Some(&task));

            state.acquired_lock(&verifier, task.chunk_id(), &time).unwrap();
            state.completed_task(&verifier, task, &time).unwrap();
            assert_eq!(0, state.pending_verification.len());
            assert!(!state.is_current_round_finished());

            {
                // Update the current round metrics.
                state.update_round_metrics();

                // Update the state of current round contributors.
                state.update_current_contributors(&time).unwrap();

                // Update the state of current round verifiers.
                state.update_current_verifiers(&time).unwrap();

                // Drop disconnected participants from the current round.
                state.update_dropped_participants(&time).unwrap();

                // Ban any participants who meet the coordinator criteria.
                state.update_banned_participants().unwrap();
            }
        }

        assert_eq!(Some(next_round_height), state.current_round_height);
        assert_eq!(0, state.current_contributors.len());
        assert_eq!(0, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(2, state.finished_contributors.get(&next_round_height).unwrap().len());
        assert_eq!(1, state.finished_verifiers.get(&next_round_height).unwrap().len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());
    }

    #[test]
    fn test_round_2x2() {
        test_logger();

        let time = SystemTimeSource::new();
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch two contributors and two verifiers.
        let contributor_1 = TEST_CONTRIBUTOR_ID.clone();
        let contributor_2 = TEST_CONTRIBUTOR_ID_2.clone();
        let verifier_1 = TEST_VERIFIER_ID.clone();
        let verifier_2 = TEST_VERIFIER_ID_2.clone();

        // Initialize a new coordinator state.
        let current_round_height = 5;
        let mut state = CoordinatorState::new(environment.clone());
        state.initialize(current_round_height);
        state.add_to_queue(contributor_1.clone(), 10).unwrap();
        state.add_to_queue(contributor_2.clone(), 9).unwrap();
        state.add_to_queue(verifier_1.clone(), 10).unwrap();
        state.add_to_queue(verifier_2.clone(), 9).unwrap();
        state.update_queue().unwrap();
        state.aggregating_current_round(&time).unwrap();
        state.aggregated_current_round(&time).unwrap();
        assert!(state.is_current_round_finished());
        assert!(state.is_current_round_aggregated());
        assert!(state.is_precommit_next_round_ready(&time));

        // Advance the coordinator to the next round.
        let next_round_height = current_round_height + 1;
        assert_eq!(4, state.queue.len());
        assert_eq!(0, state.next.len());
        state.precommit_next_round(next_round_height, &time).unwrap();
        assert_eq!(0, state.queue.len());
        assert_eq!(4, state.next.len());
        state.commit_next_round();
        assert_eq!(0, state.queue.len());
        assert_eq!(0, state.next.len());

        // Process every chunk in the round as contributor 1.
        let number_of_chunks = environment.number_of_chunks();
        let tasks1 = initialize_tasks(0, number_of_chunks, 2).unwrap();
        let mut tasks1 = tasks1.iter();
        for _ in 0..number_of_chunks {
            assert_eq!(Some(next_round_height), state.current_round_height);
            assert_eq!(2, state.current_contributors.len());
            assert_eq!(2, state.current_verifiers.len());
            assert_eq!(0, state.pending_verification.len());
            assert_eq!(0, state.finished_contributors.get(&next_round_height).unwrap().len());
            assert_eq!(0, state.finished_verifiers.get(&next_round_height).unwrap().len());
            assert_eq!(0, state.dropped.len());
            assert_eq!(0, state.banned.len());

            // Fetch a pending task for the contributor.
            let task = state.fetch_task(&contributor_1, &time).unwrap();
            let expected_task1 = tasks1.next();
            assert_eq!(expected_task1, Some(&task));

            state.acquired_lock(&contributor_1, task.chunk_id(), &time).unwrap();
            let assigned_verifier = state.completed_task(&contributor_1, task, &time).unwrap();
            assert_eq!(1, state.pending_verification.len());
            assert!(!state.is_current_round_finished());

            // Fetch a pending task for the verifier.
            let task = state.fetch_task(&assigned_verifier, &time).unwrap();
            assert_eq!(expected_task1, Some(&task));

            state.acquired_lock(&assigned_verifier, task.chunk_id(), &time).unwrap();
            state.completed_task(&assigned_verifier, task, &time).unwrap();
            assert_eq!(0, state.pending_verification.len());
            assert!(!state.is_current_round_finished());

            {
                // Update the current round metrics.
                state.update_round_metrics();

                // Update the state of current round contributors.
                state.update_current_contributors(&time).unwrap();

                // Update the state of current round verifiers.
                state.update_current_verifiers(&time).unwrap();

                // Drop disconnected participants from the current round.
                state.update_dropped_participants(&time).unwrap();

                // Ban any participants who meet the coordinator criteria.
                state.update_banned_participants().unwrap();
            }
        }

        // Process every chunk in the round as contributor 2.
        let tasks2 = initialize_tasks(1, number_of_chunks, 2).unwrap();
        let mut tasks2 = tasks2.iter();
        for _ in 0..number_of_chunks {
            assert_eq!(Some(next_round_height), state.current_round_height);
            assert_eq!(1, state.current_contributors.len());
            assert_eq!(2, state.current_verifiers.len());
            assert_eq!(0, state.pending_verification.len());
            assert_eq!(1, state.finished_contributors.get(&next_round_height).unwrap().len());
            assert_eq!(0, state.finished_verifiers.get(&next_round_height).unwrap().len());
            assert_eq!(0, state.dropped.len());
            assert_eq!(0, state.banned.len());

            // Fetch a pending task for the contributor.
            let task = state.fetch_task(&contributor_2, &time).unwrap();
            let expected_task2 = tasks2.next();
            assert_eq!(expected_task2, Some(&task));

            state.acquired_lock(&contributor_2, task.chunk_id(), &time).unwrap();
            let assigned_verifier = state.completed_task(&contributor_2, task, &time).unwrap();
            assert_eq!(1, state.pending_verification.len());
            assert!(!state.is_current_round_finished());

            // Fetch a pending task for the verifier.
            let task = state.fetch_task(&assigned_verifier, &time).unwrap();
            assert_eq!(expected_task2, Some(&task));

            state.acquired_lock(&assigned_verifier, task.chunk_id(), &time).unwrap();
            state.completed_task(&assigned_verifier, task, &time).unwrap();
            assert_eq!(0, state.pending_verification.len());
            assert!(!state.is_current_round_finished());

            {
                // Update the current round metrics.
                state.update_round_metrics();

                // Update the state of current round contributors.
                state.update_current_contributors(&time).unwrap();

                // Update the state of current round verifiers.
                state.update_current_verifiers(&time).unwrap();

                // Drop disconnected participants from the current round.
                state.update_dropped_participants(&time).unwrap();

                // Ban any participants who meet the coordinator criteria.
                state.update_banned_participants().unwrap();
            }
        }

        assert_eq!(Some(next_round_height), state.current_round_height);
        assert_eq!(0, state.current_contributors.len());
        assert_eq!(0, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(2, state.finished_contributors.get(&next_round_height).unwrap().len());
        assert_eq!(2, state.finished_verifiers.get(&next_round_height).unwrap().len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());
    }
}
