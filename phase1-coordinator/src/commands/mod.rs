pub(crate) mod aggregation;
pub(crate) use aggregation::*;

#[cfg(any(test, feature = "operator"))]
pub(crate) mod computation;
#[cfg(any(test, feature = "operator"))]
pub(crate) use computation::*;

pub(crate) mod initialization;
pub(crate) use initialization::*;

#[cfg(any(test, feature = "operator"))]
pub(crate) mod verification;
#[cfg(any(test, feature = "operator"))]
pub(crate) use verification::*;

#[cfg(any(test, feature = "operator"))]
use crate::{
    authentication::Signature,
    objects::{ContributionFileSignature, ContributionState},
    storage::{Locator, Storage},
    CoordinatorError,
};

#[cfg(any(test, feature = "operator"))]
use setup_utils::calculate_hash;

#[cfg(any(test, feature = "operator"))]
use std::{io::Write, sync::Arc};

#[cfg(any(test, feature = "operator"))]
pub type SigningKey = String;

///
/// Writes the contribution file signature to a given `contribution_file_signature` locator.
///
/// This function constructs a contribution file signature with an empty signature and the
/// hashes of the following files:
/// 1. Challenge file
/// 2. Response file
/// 3. Next challenge file (for verifiers)
///
/// On success, this function writes a contribution file signature to disk.
///
/// On failure, this function returns a `CoordinatorError`.
///
#[cfg(any(test, feature = "operator"))]
#[inline]
pub(crate) fn write_contribution_file_signature(
    storage: &mut impl Storage,
    signature: Arc<dyn Signature>,
    signing_key: &SigningKey,
    challenge_locator: &Locator,
    response_locator: &Locator,
    next_challenge_locator: Option<&Locator>,
    contribution_file_signature_locator: &Locator,
) -> Result<(), CoordinatorError> {
    // Calculate the challenge hash.
    let challenge_reader = storage.reader(challenge_locator)?;
    let challenge_hash = calculate_hash(challenge_reader.as_ref()).to_vec();

    // Calculate the response hash.
    let response_reader = storage.reader(response_locator)?;
    let response_hash = calculate_hash(response_reader.as_ref()).to_vec();

    // Calculate the next challenge hash.
    let next_challenge_hash = match next_challenge_locator {
        Some(next_challenge_locator) => {
            let next_challenge_reader = storage.reader(next_challenge_locator)?;
            let next_challenge_hash = calculate_hash(next_challenge_reader.as_ref()).to_vec();

            Some(next_challenge_hash)
        }
        None => None,
    };

    // Construct the contribution state.
    let contribution_state = ContributionState::new(challenge_hash, response_hash, next_challenge_hash)?;

    // Generate the contribution signature.
    let contribution_signature = signature.sign(signing_key, &contribution_state.signature_message()?)?;

    // Construct the contribution file signature.
    let contribution_file_signature = ContributionFileSignature::new(contribution_signature, contribution_state)?;
    let contribution_file_signature_bytes = serde_json::to_vec_pretty(&contribution_file_signature)?;

    // Write the contribution file signature.
    let mut contribution_file_signature_writer = storage.writer(contribution_file_signature_locator)?;
    tracing::debug!(
        "Writing contribution file signature of size {} to {}",
        contribution_file_signature_bytes.len(),
        &storage.to_path(&contribution_file_signature_locator)?
    );

    contribution_file_signature_writer
        .as_mut()
        .write_all(&contribution_file_signature_bytes[..])?;
    contribution_file_signature_writer.flush()?;

    Ok(())
}
