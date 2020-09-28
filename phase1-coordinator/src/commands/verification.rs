use crate::{environment::Environment, CoordinatorError};
use phase1::{helpers::CurveKind, Phase1, Phase1Parameters};
use phase1_cli::transform_pok_and_correctness;
use setup_utils::{blank_hash, calculate_hash, BatchSerializer, CheckForCorrectness, GenericArray, UseCompression};

use memmap::*;
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    panic,
};
use tracing::{debug, error, info};
use typenum::consts::U64;
use zexe_algebra::{Bls12_377, BW6_761};

pub struct Verification;

impl Verification {
    ///
    /// Runs chunk verification for a given environment, round height, and chunk ID.
    ///
    /// Executes the round verification on a given chunk ID using phase1-cli logic.
    ///
    pub fn run(
        environment: &Environment,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
        previous_locator: String,
        current_locator: String,
        next_locator: String,
    ) -> anyhow::Result<()> {
        // Check that this is not the initial contribution.
        if (round_height == 0 || round_height == 1) && contribution_id == 0 {
            return Err(CoordinatorError::VerificationOnContributionIdZero.into());

            // info!("Sanity checking contribution 0 in round {}", round_height);
            //
            // // Open the transcript file.
            // let transcript = environment.contribution_locator(round_height, chunk_id, contribution_id);
            // let file = OpenOptions::new().read(true).open(&transcript)?;
            // let reader = unsafe { MmapOptions::new().map(&file)? };
            //
            // // Check that the contribution chunk was generated based on the blank hash.
            // let hash = blank_hash();
            // for (expected, candidate) in hash.iter().zip(reader.chunks(64).next().unwrap()) {
            //     if expected != candidate {
            //         return Err(CoordinatorError::ChunkVerificationFailed.into());
            //     }
            // }
            //
            // // Compute the contribution hash to ensure it works.
            // let contribution_hash = calculate_hash(&reader);
            //
            // if round_height == 0 {
            //     Self::copy_initial(environment, round_height, chunk_id, contribution_hash)?;
            // }
            //
            // info!("Completed sanity checking of contribution 0 in round {}", round_height);
            // return Ok(());
        }

        // Fetch the parameter settings.
        let settings = environment.to_settings();

        info!(
            "Starting verification of round {} chunk {} contribution {}",
            round_height, chunk_id, contribution_id
        );

        // Execute ceremony verification on chunk.
        let (_, _, curve, _, _, _) = settings.clone();
        let result = panic::catch_unwind(|| {
            match curve {
                CurveKind::Bls12_377 => transform_pok_and_correctness(
                    &previous_locator,
                    &current_locator,
                    &next_locator,
                    &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                ),
                CurveKind::BW6 => transform_pok_and_correctness(
                    &previous_locator,
                    &current_locator,
                    &next_locator,
                    &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
                ),
            };
        });

        info!(
            "Completed verification of round {} chunk {} contribution {}",
            round_height, chunk_id, contribution_id
        );

        match result.is_ok() {
            true => Ok(()),
            false => Err(CoordinatorError::VerificationFailed.into()),
        }
    }
}
