use crate::environment::Environment;

pub trait Locator {
    /// Returns the round directory for a given round height from the coordinator.
    fn round_directory(environment: &Environment, round_height: u64) -> String
    where
        Self: Sized;

    /// Initializes the round directory for a given environment and round height.
    fn round_directory_init(environment: &Environment, round_height: u64)
    where
        Self: Sized;

    /// Returns `true` if the round directory for a given round height exists.
    /// Otherwise, returns `false`.
    fn round_directory_exists(environment: &Environment, round_height: u64) -> bool
    where
        Self: Sized;

    /// Resets the round directory for a given environment and round height.
    fn round_directory_reset(environment: &Environment, round_height: u64)
    where
        Self: Sized;

    /// Returns the chunk directory for a given round height and chunk ID from the coordinator.
    fn chunk_directory(environment: &Environment, round_height: u64, chunk_id: u64) -> String
    where
        Self: Sized;

    /// Initializes the chunk directory for a given environment, round height, and chunk ID.
    fn chunk_directory_init(environment: &Environment, round_height: u64, chunk_id: u64)
    where
        Self: Sized;

    /// Returns `true` if the chunk directory for a given round height and chunk ID exists.
    /// Otherwise, returns `false`.
    fn chunk_directory_exists(environment: &Environment, round_height: u64, chunk_id: u64) -> bool
    where
        Self: Sized;

    /// Returns the contribution locator for a given round, chunk ID, and
    /// contribution ID from the coordinator.
    fn contribution_locator(
        environment: &Environment,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
    ) -> String
    where
        Self: Sized;

    /// Initializes the contribution locator file for a given round, chunk ID, and
    /// contribution ID from the coordinator.
    fn contribution_locator_init(environment: &Environment, round_height: u64, chunk_id: u64, contribution_id: u64)
    where
        Self: Sized;

    /// Returns `true` if the contribution locator for a given round height, chunk ID,
    /// and contribution ID exists. Otherwise, returns `false`.
    fn contribution_locator_exists(
        environment: &Environment,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
    ) -> bool
    where
        Self: Sized;

    /// Returns the round locator for a given round from the coordinator.
    fn round_locator(environment: &Environment, round_height: u64) -> String
    where
        Self: Sized;

    /// Returns `true` if the round locator for a given round height exists.
    /// Otherwise, returns `false`.
    fn round_locator_exists(environment: &Environment, round_height: u64) -> bool
    where
        Self: Sized;
}
