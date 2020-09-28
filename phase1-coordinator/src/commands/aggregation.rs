use crate::{environment::Environment, objects::Round, CoordinatorError};
use phase1::{helpers::CurveKind, Phase1, Phase1Parameters};
use setup_utils::UseCompression;

use memmap::*;
use std::{fs::OpenOptions, panic, path::Path};
use tracing::{debug, error, trace};
use zexe_algebra::{Bls12_377, BW6_761};

pub struct Aggregation;

impl Aggregation {
    /// Runs aggregation for a given environment and round.
    pub fn run(environment: &Environment, round: &Round) -> anyhow::Result<()> {
        // Fetch the round height.
        let round_height = round.get_height();

        // Fetch the compressed output setting based on the round height.
        let compressed_output = match round_height != 0 && environment.compressed_outputs() {
            true => UseCompression::Yes,
            false => UseCompression::No,
        };

        // Load the contribution files.
        let readers = Self::readers(environment, round)?;
        let contribution_readers = readers
            .iter()
            .map(|r| (r.as_ref(), compressed_output))
            .collect::<Vec<_>>();

        // Fetch the compressed input setting for the final round file.
        let compressed_input = match round_height != 0 && environment.compressed_inputs() {
            true => UseCompression::Yes,
            false => UseCompression::No,
        };

        // Load the final round file.
        let round_writer = (&mut *Self::writer(environment, round)?, compressed_input);

        debug!("Starting aggregation on round {}", round_height);

        // Execute aggregation on given round.
        let chunk_id = 0usize;
        let settings = environment.to_settings();
        let (_, _, curve, _, _, _) = settings;
        let result = match curve {
            CurveKind::Bls12_377 => Phase1::aggregation(
                &contribution_readers,
                round_writer,
                &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
            ),
            CurveKind::BW6 => Phase1::aggregation(
                &contribution_readers,
                round_writer,
                &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
            ),
        };

        if let Err(error) = result {
            error!("Aggregation failed during execution ({})", error);
            Err(CoordinatorError::RoundAggregationFailed.into())
        } else {
            debug!("Completed aggregation on round {}", round_height);
            Ok(())
        }
    }

    /// Attempts to open every contribution for the given round and
    /// returns readers to each chunk contribution file.
    fn readers(environment: &Environment, round: &Round) -> anyhow::Result<Vec<Mmap>> {
        let mut readers = vec![];

        // Fetch the round height.
        let round_height = round.get_height();

        // Fetch the round height.
        let is_initial = round_height == 0;

        // Fetch the previous compressed output setting based on the round height.
        let compressed = match !is_initial && environment.compressed_outputs() {
            true => UseCompression::Yes,
            false => UseCompression::No,
        };

        // Create a variable to save the contribution ID of the prior iteration.
        let mut previous_chunk_contribution_id = 0;

        for chunk_id in 0..environment.number_of_chunks() {
            trace!("Loading contribution from chunk {}", chunk_id);

            // Fetch the contribution ID.
            let contribution_id = round.get_chunk(chunk_id)?.current_contribution_id();

            // Sanity check that each contribution ID is the same,
            // meaning all chunks have the same number of contributions
            // contributed to it.
            match chunk_id == 0 {
                true => previous_chunk_contribution_id = contribution_id,
                false => {
                    if previous_chunk_contribution_id != contribution_id {
                        return Err(CoordinatorError::NumberOfContributionsDiffer.into());
                    }
                }
            }

            // Fetch the reader with the contribution locator.
            let locator = environment.contribution_locator(round_height, chunk_id, contribution_id);
            let reader = OpenOptions::new()
                .read(true)
                .open(locator)
                .expect("unable to open contribution");

            // Derive the expected file size of the contribution.
            let settings = environment.to_settings();
            let (_, _, curve, _, _, _) = settings;
            let expected = match curve {
                CurveKind::Bls12_377 => contribution_filesize!(Bls12_377, settings, chunk_id, compressed, is_initial),
                CurveKind::BW6 => contribution_filesize!(BW6_761, settings, chunk_id, compressed, is_initial),
            };

            // Check that contribution filesize is correct.
            let metadata = reader.metadata().expect("unable to retrieve metadata");
            let found = metadata.len();
            debug!("Round {} contribution {} filesize is {}", round_height, chunk_id, found);
            if found != expected {
                error!("Contribution file size should be {} but found {}", expected, found);
                return Err(CoordinatorError::ContributionFileSizeMismatch.into());
            }

            unsafe {
                readers.push(MmapOptions::new().map(&reader).expect("should have mapped the reader"));
            }
        }

        Ok(readers)
    }

    /// Attempts to create the contribution file for the given round and
    /// returns a writer to it.
    fn writer(environment: &Environment, round: &Round) -> anyhow::Result<MmapMut> {
        // Fetch the round height.
        let round_height = round.get_height();

        // Fetch the round transcript locator for the given round.
        let round_locator = environment.round_locator(round_height);

        // Check that the round transcript locator does not already exist.
        if environment.round_locator_exists(round_height) {
            return Err(CoordinatorError::RoundLocatorAlreadyExists.into());
        }

        // Fetch the round height.
        let is_initial = round_height == 0;

        // Fetch the next compressed input setting based on the round height.
        let compressed = match !is_initial && environment.compressed_inputs() {
            true => UseCompression::Yes,
            false => UseCompression::No,
        };

        // Create the writer for the round transcript locator.
        let writer = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(round_locator)
            .expect("unable to create new file");

        // Check the round filesize will fit on the system.
        let settings = environment.to_settings();
        let (_, _, curve, _, _, _) = settings;
        let round_size = match curve {
            CurveKind::Bls12_377 => round_filesize!(Bls12_377, settings, chunk_id, compressed, is_initial),
            CurveKind::BW6 => round_filesize!(BW6_761, settings, chunk_id, compressed, is_initial),
        };
        debug!("Round {} filesize will be {}", round_height, round_size);
        writer.set_len(round_size).expect("round file must be large enough");

        unsafe {
            Ok(MmapOptions::new()
                .map_mut(&writer)
                .expect("unable to create a memory map for output"))
        }
    }
}
