use super::*;

impl<'a, E: PairingEngine + Sync> Phase1<'a, E> {
    /// Verifies that the accumulator was transformed correctly
    /// given the `PublicKey` and the so-far hash of the accumulator.
    /// This verifies a single chunk and checks only that the points
    /// are not zero, that they're in the prime order subgroup.
    /// In the first chunk, it also checks the proofs of knowledge
    /// and that the elements were correctly multiplied.

    ///
    /// Phase 1 - Verification
    ///
    /// Verifies a transformation of the `Accumulator` with the `PublicKey`,
    /// given a 64-byte transcript `digest`.
    ///
    /// Verifies that the accumulator was transformed correctly
    /// given the `PublicKey` and the so-far hash of the accumulator.
    /// This verifies a single chunk and checks only that the points are not zero,
    /// that they're in the prime order subgroup. In the first chunk, it also checks
    /// the proofs of knowledge and that the elements were correctly multiplied.
    ///
    #[allow(clippy::too_many_arguments, clippy::cognitive_complexity)]
    pub fn verification(
        input: &[u8],
        output: &[u8],
        key: &PublicKey<E>,
        digest: &[u8],
        compressed_input: UseCompression,
        compressed_output: UseCompression,
        check_input_for_correctness: CheckForCorrectness,
        check_output_for_correctness: CheckForCorrectness,
        parameters: &'a Phase1Parameters<E>,
    ) -> Result<()> {
        let span = info_span!("phase1-verification");
        let _ = span.enter();

        info!("starting...");

        // Split the output buffer into its components.
        let (tau_g1, tau_g2, alpha_g1, beta_g1, beta_g2) = split(output, parameters, compressed_output);

        let (g1_check, g2_check, g1_alpha_check) = match parameters.contribution_mode == ContributionMode::Full
            || parameters.chunk_index == 0
        {
            // Run proof of knowledge checks if contribution mode is on full, or this is the first chunk index.
            true => {
                // Split the input buffer into its components.
                let (in_tau_g1, in_tau_g2, in_alpha_g1, in_beta_g1, in_beta_g2) =
                    split(input, parameters, compressed_input);

                let [tau_g2_s, alpha_g2_s, beta_g2_s] = compute_g2_s_key(&key, &digest)?;

                // Compose into tuple form for convenience.
                let tau_single_g1_check = &(key.tau_g1.0, key.tau_g1.1);
                let tau_single_g2_check = &(tau_g2_s, key.tau_g2);
                // let alpha_single_g1_check = &(key.alpha_g1.0, key.alpha_g1.1);
                let alpha_single_g2_check = &(alpha_g2_s, key.alpha_g2);
                let beta_single_g1_check = &(key.beta_g1.0, key.beta_g1.1);
                let beta_single_g2_check = &(beta_g2_s, key.beta_g2);

                // Ensure the key ratios are correctly produced
                {
                    // Check the proofs of knowledge for tau, alpha, and beta.
                    let check_ratios = &[
                        (&(key.tau_g1.0, key.tau_g1.1), &(tau_g2_s, key.tau_g2), "Tau G1<>G2"),
                        (
                            &(key.alpha_g1.0, key.alpha_g1.1),
                            &(alpha_g2_s, key.alpha_g2),
                            "Alpha G1<>G2",
                        ),
                        (
                            &(key.beta_g1.0, key.beta_g1.1),
                            &(beta_g2_s, key.beta_g2),
                            "Beta G1<>G2",
                        ),
                    ];

                    for (a, b, err) in check_ratios {
                        check_same_ratio::<E>(a, b, err)?;
                    }
                    debug!("key ratios were correctly produced");
                }

                // Ensure that the initial conditions are correctly formed (first 2 elements).
                // We allocate a G1 vector of length 2 and re-use it for our G1 elements.
                // We keep the values of the tau_g1 / tau_g2 elements for later use.

                // Check that tau^i was computed correctly in G1.
                let (mut before_g1, mut after_g1) = {
                    // Previous iteration of tau_g1[0].
                    let before_g1 =
                        read_initial_elements::<E::G1Affine>(in_tau_g1, compressed_input, check_input_for_correctness)?;
                    // Current iteration of tau_g1[0].
                    let after_g1 =
                        read_initial_elements::<E::G1Affine>(tau_g1, compressed_output, check_output_for_correctness)?;

                    // Check tau_g1[0] is the prime subgroup generator.
                    if after_g1[0] != E::G1Affine::prime_subgroup_generator() {
                        return Err(VerificationError::InvalidGenerator(ElementType::TauG1).into());
                    }

                    // Check that tau^1 was multiplied correctly.
                    check_same_ratio::<E>(
                        &(before_g1[1], after_g1[1]),
                        tau_single_g2_check,
                        "Before-After: tau_g1",
                    )?;

                    (before_g1, after_g1)
                };

                // Check that tau^i was computed correctly in G2.
                let (_before_g2, after_g2) = {
                    // Previous iteration of tau_g2[0].
                    let before_g2 =
                        read_initial_elements::<E::G2Affine>(in_tau_g2, compressed_input, check_input_for_correctness)?;
                    // Current iteration of tau_g2[0].
                    let after_g2 =
                        read_initial_elements::<E::G2Affine>(tau_g2, compressed_output, check_output_for_correctness)?;

                    // Check tau_g2[0] is the prime subgroup generator.
                    if after_g2[0] != E::G2Affine::prime_subgroup_generator() {
                        return Err(VerificationError::InvalidGenerator(ElementType::TauG2).into());
                    }

                    // Check that tau^1 was multiplied correctly.
                    check_same_ratio::<E>(
                        tau_single_g1_check,
                        &(before_g2[1], after_g2[1]),
                        "Before-After: tau_g2",
                    )?;

                    (before_g2, after_g2)
                };

                // Check that alpha_g1[0] and beta_g1[0] were computed correctly.
                {
                    // Determine the check based on the proof system's requirements.
                    let checks = match parameters.proving_system {
                        ProvingSystem::Groth16 => vec![
                            (in_alpha_g1, alpha_g1, alpha_single_g2_check),
                            (in_beta_g1, beta_g1, beta_single_g2_check),
                        ],
                        ProvingSystem::Marlin => vec![(in_alpha_g1, alpha_g1, alpha_single_g2_check)],
                    };

                    // Check that alpha_g1[0] and beta_g1[0] was multiplied correctly.
                    for (before, after, check) in &checks {
                        before.read_batch_preallocated(
                            &mut before_g1,
                            compressed_input,
                            check_input_for_correctness,
                        )?;
                        after.read_batch_preallocated(
                            &mut after_g1,
                            compressed_output,
                            check_output_for_correctness,
                        )?;
                        check_same_ratio::<E>(
                            &(before_g1[0], after_g1[0]),
                            check,
                            "Before-After: alpha_g1[0] / beta_g1[0]",
                        )?;
                    }
                }

                // Check that beta_g2[0] was computed correctly.
                {
                    if parameters.proving_system == ProvingSystem::Groth16 {
                        // Read in the first beta_g2 element from the previous iteration and current iteration.
                        let before_beta_g2 = (&*in_beta_g2)
                            .read_element::<E::G2Affine>(compressed_input, check_input_for_correctness)?;
                        let after_beta_g2 =
                            (&*beta_g2).read_element::<E::G2Affine>(compressed_output, check_output_for_correctness)?;

                        // Check that beta_g2[0] was multiplied correctly.
                        check_same_ratio::<E>(
                            beta_single_g1_check,
                            &(before_beta_g2, after_beta_g2),
                            "Before-After: beta_g2[0]",
                        )?;
                    }
                }

                // Fetch the iteration of alpha_g1[0]. This is done to unify this logic with Marlin mode.
                let after_alpha_g1 =
                    read_initial_elements::<E::G1Affine>(alpha_g1, compressed_output, check_output_for_correctness)?;

                let g1_check = (after_g1[0], after_g1[1]);
                let g2_check = (after_g2[0], after_g2[1]);
                let g1_alpha_check = (after_alpha_g1[0], after_alpha_g1[1]);

                (g1_check, g2_check, g1_alpha_check)
            }
            false => {
                // Ensure that the initial conditions are correctly formed (first 2 elements)
                // We allocate a G1 vector of length 2 and re-use it for our G1 elements.
                // We keep the values of the tau_g1 / tau_g2 elements for later use.

                // Current iteration of tau_g1[0].
                let after_g1 =
                    read_initial_elements::<E::G1Affine>(tau_g1, compressed_output, check_output_for_correctness)?;

                // Check tau_g1[0] is the prime subgroup generator.
                if after_g1[0] != E::G1Affine::prime_subgroup_generator() {
                    return Err(VerificationError::InvalidGenerator(ElementType::TauG1).into());
                }

                // Current iteration of tau_g2[0].
                let after_g2 =
                    read_initial_elements::<E::G2Affine>(tau_g2, compressed_output, check_output_for_correctness)?;

                // Check tau_g2[0] is the prime subgroup generator.
                if after_g2[0] != E::G2Affine::prime_subgroup_generator() {
                    return Err(VerificationError::InvalidGenerator(ElementType::TauG2).into());
                }

                // Fetch the iteration of alpha_g1[0]. This is done to unify this logic with Marlin mode.
                let after_alpha_g1 =
                    read_initial_elements::<E::G1Affine>(alpha_g1, compressed_output, check_output_for_correctness)?;

                let g1_check = (after_g1[0], after_g1[1]);
                let g2_check = (after_g2[0], after_g2[1]);
                let g1_alpha_check = (after_alpha_g1[0], after_alpha_g1[1]);

                (g1_check, g2_check, g1_alpha_check)
            }
        };

        debug!("initial elements were computed correctly");

        iter_chunk(&parameters, |start, end| {
            // Preallocate 2 vectors per batch.
            // Ensure that the pairs are created correctly (we do this in chunks!).
            // Load `batch_size` chunks on each iteration and perform the transformation.

            debug!("verifying chunk from {} to {}", start, end);

            let span = info_span!("batch", start, end);
            let _enter = span.enter();

            // Determine the chunk start and end indices based on the contribution mode.
            let (start_chunk, end_chunk) = match parameters.contribution_mode {
                ContributionMode::Chunked => (
                    start - parameters.chunk_index * parameters.chunk_size, // start index
                    end - parameters.chunk_index * parameters.chunk_size,   // end index
                ),
                ContributionMode::Full => (start, end),
            };

            match parameters.proving_system {
                ProvingSystem::Groth16 => {
                    rayon::scope(|t| {
                        let _enter = span.enter();

                        // Process tau_g1 elements.
                        t.spawn(|_| {
                            let _enter = span.enter();

                            let mut g1 = vec![E::G1Affine::zero(); parameters.batch_size];

                            match parameters.contribution_mode {
                                ContributionMode::Chunked => {
                                    check_elements_are_nonzero_and_in_prime_order_subgroup::<E::G1Affine>(
                                        (tau_g1, compressed_output),
                                        (start_chunk, end_chunk),
                                        &mut g1,
                                    )
                                    .expect("could not check ratios for tau_g1 elements");
                                }
                                ContributionMode::Full => {
                                    check_power_ratios::<E>(
                                        (tau_g1, compressed_output, check_output_for_correctness),
                                        (start_chunk, end_chunk),
                                        &mut g1,
                                        &g2_check,
                                    )
                                    .expect("could not check ratios for tau_g1 elements");
                                }
                            };

                            trace!("tau_g1 verification was successful");
                        });

                        if start < parameters.powers_length {
                            // If the `end` would be out of bounds, then just process until
                            // the end (this is necessary in case the last batch would try to
                            // process more elements than available).
                            let end = if start + parameters.batch_size > parameters.powers_length {
                                parameters.powers_length
                            } else {
                                end
                            };

                            // Determine the chunk start and end indices based on the contribution mode.
                            let (start_chunk, end_chunk) = match parameters.contribution_mode {
                                ContributionMode::Chunked => (
                                    start - parameters.chunk_index * parameters.chunk_size, // start index
                                    end - parameters.chunk_index * parameters.chunk_size,   // end index
                                ),
                                ContributionMode::Full => (start, end),
                            };

                            rayon::scope(|t| {
                                let _enter = span.enter();

                                // Process tau_g2 elements.
                                t.spawn(|_| {
                                    let _enter = span.enter();

                                    let mut g2 = vec![E::G2Affine::zero(); parameters.batch_size];

                                    match parameters.contribution_mode {
                                        ContributionMode::Chunked => {
                                            check_elements_are_nonzero_and_in_prime_order_subgroup::<E::G2Affine>(
                                                (tau_g2, compressed_output),
                                                (start_chunk, end_chunk),
                                                &mut g2,
                                            )
                                            .expect("could not check ratios for tau_g2 elements");
                                        }
                                        ContributionMode::Full => {
                                            check_power_ratios_g2::<E>(
                                                (tau_g2, compressed_output, check_output_for_correctness),
                                                (start_chunk, end_chunk),
                                                &mut g2,
                                                &g1_check,
                                            )
                                            .expect("could not check ratios for tau_g2 elements");
                                        }
                                    };

                                    trace!("tau_g2 verification was successful");
                                });

                                // Process alpha_g1 elements.
                                t.spawn(|_| {
                                    let _enter = span.enter();

                                    let mut g1 = vec![E::G1Affine::zero(); parameters.batch_size];

                                    match parameters.contribution_mode {
                                        ContributionMode::Chunked => {
                                            check_elements_are_nonzero_and_in_prime_order_subgroup::<E::G1Affine>(
                                                (alpha_g1, compressed_output),
                                                (start_chunk, end_chunk),
                                                &mut g1,
                                            )
                                            .expect("could not check ratios for alpha_g1 elements");
                                        }
                                        ContributionMode::Full => {
                                            check_power_ratios::<E>(
                                                (alpha_g1, compressed_output, check_output_for_correctness),
                                                (start_chunk, end_chunk),
                                                &mut g1,
                                                &g2_check,
                                            )
                                            .expect("could not check ratios for alpha_g1 elements");
                                        }
                                    };

                                    trace!("alpha_g1 verification was successful");
                                });

                                // Process beta_g1 elements.
                                t.spawn(|_| {
                                    let _enter = span.enter();

                                    let mut g1 = vec![E::G1Affine::zero(); parameters.batch_size];

                                    match parameters.contribution_mode {
                                        ContributionMode::Chunked => {
                                            check_elements_are_nonzero_and_in_prime_order_subgroup::<E::G1Affine>(
                                                (beta_g1, compressed_output),
                                                (start_chunk, end_chunk),
                                                &mut g1,
                                            )
                                            .expect("could not check ratios for beta_g1 elements");
                                        }
                                        ContributionMode::Full => {
                                            check_power_ratios::<E>(
                                                (beta_g1, compressed_output, check_output_for_correctness),
                                                (start_chunk, end_chunk),
                                                &mut g1,
                                                &g2_check,
                                            )
                                            .expect("could not check ratios for beta_g1 elements");
                                        }
                                    };

                                    trace!("beta_g1 verification was successful");
                                });
                            });
                        }
                    });
                }
                ProvingSystem::Marlin => {
                    rayon::scope(|t| {
                        let _ = span.enter();

                        // Process tau_g1 elements.
                        t.spawn(|_| {
                            let _ = span.enter();

                            let mut g1 = vec![E::G1Affine::zero(); parameters.batch_size];

                            match parameters.contribution_mode {
                                ContributionMode::Chunked => {
                                    check_elements_are_nonzero_and_in_prime_order_subgroup::<E::G1Affine>(
                                        (tau_g1, compressed_output),
                                        (start_chunk, end_chunk),
                                        &mut g1,
                                    )
                                    .expect("could not check ratios for tau_g1 elements");
                                }
                                ContributionMode::Full => {
                                    check_power_ratios::<E>(
                                        (tau_g1, compressed_output, check_output_for_correctness),
                                        (start_chunk, end_chunk),
                                        &mut g1,
                                        &g2_check,
                                    )
                                    .expect("could not check ratios for tau_g1 elements");
                                }
                            };

                            trace!("tau_g1 verification was successful");
                        });

                        // This is the first batch, check alpha_g1. batch size is guaranteed to be of size >= 3
                        // TODO (howardwu): Confirm this piece has been converted to chunked contribution mode.
                        if start_chunk == 0 {
                            let num_alpha_powers = 3;
                            let mut g1 = vec![E::G1Affine::zero(); num_alpha_powers];

                            check_power_ratios::<E>(
                                (alpha_g1, compressed_output, check_output_for_correctness),
                                (0, num_alpha_powers),
                                &mut g1,
                                &g2_check,
                            )
                            .expect("could not check ratios for alpha_g1");

                            trace!("alpha_g1 verification was successful");

                            let mut g2 = vec![E::G2Affine::zero(); 3];

                            check_power_ratios_g2::<E>(
                                (tau_g2, compressed_output, check_output_for_correctness),
                                (0, 2),
                                &mut g2,
                                &g1_check,
                            )
                            .expect("could not check ratios for tau_g2");

                            trace!("tau_g2 verification was successful");
                        }

                        // TODO (howardwu): Convert this piece to chunked contribution mode.
                        {
                            let powers_of_two_in_range = (0..parameters.size)
                                .map(|i| (i, parameters.powers_length as u64 - 1 - (1 << i) + 2))
                                .map(|(i, p)| (i, p as usize))
                                .filter(|(_, p)| start_chunk <= *p && *p < end_chunk)
                                .collect::<Vec<_>>();

                            for (i, p) in powers_of_two_in_range.into_iter() {
                                let g1_size = buffer_size::<E::G1Affine>(compressed_output);
                                let g2_size = buffer_size::<E::G2Affine>(compressed_output);

                                let g1 = (&tau_g1[p * g1_size..(p + 1) * g1_size])
                                    .read_element(compressed_output, check_output_for_correctness)
                                    .expect("should have read g1 element");
                                let g2 = (&tau_g2[(2 + i) * g2_size..(2 + i + 1) * g2_size])
                                    .read_element(compressed_output, check_output_for_correctness)
                                    .expect("should have read g2 element");
                                check_same_ratio::<E>(
                                    &(g1, E::G1Affine::prime_subgroup_generator()),
                                    &(E::G2Affine::prime_subgroup_generator(), g2),
                                    "G1<>G2",
                                )
                                .expect("should have checked same ratio");

                                let mut alpha_g1_elements = vec![E::G1Affine::zero(); 3];
                                (&alpha_g1[(3 + 3 * i) * g1_size..(3 + 3 * i + 3) * g1_size])
                                    .read_batch_preallocated(
                                        &mut alpha_g1_elements,
                                        compressed_output,
                                        check_output_for_correctness,
                                    )
                                    .expect("should have read alpha g1 elements");
                                check_same_ratio::<E>(
                                    &(alpha_g1_elements[0], alpha_g1_elements[1]),
                                    &g2_check,
                                    "alpha_g1 ratio 1",
                                )
                                .expect("should have checked same ratio");
                                check_same_ratio::<E>(
                                    &(alpha_g1_elements[1], alpha_g1_elements[2]),
                                    &g2_check,
                                    "alpha_g1 ratio 2",
                                )
                                .expect("should have checked same ratio");
                                check_same_ratio::<E>(
                                    &(alpha_g1_elements[0], g1_alpha_check.0),
                                    &(E::G2Affine::prime_subgroup_generator(), g2),
                                    "alpha consistent",
                                )
                                .expect("should have checked same ratio");
                            }
                        }
                    });
                }
            }

            debug!("batch verification successful");

            Ok(())
        })?;

        info!("phase1-verification complete");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::testing::{generate_input, generate_output};
    use setup_utils::calculate_hash;

    use zexe_algebra::{Bls12_377, BW6_761};

    fn curve_verification_test<E: PairingEngine>(
        powers: usize,
        batch: usize,
        compressed_input: UseCompression,
        compressed_output: UseCompression,
    ) {
        for proving_system in &[ProvingSystem::Marlin] {
            let parameters = Phase1Parameters::<E>::new(*proving_system, powers, batch);

            // allocate the input/output vectors
            let (input, _) = generate_input(&parameters, compressed_input, CheckForCorrectness::No);
            let mut output = generate_output(&parameters, compressed_output);

            // Construct our keypair
            let current_accumulator_hash = blank_hash();
            let mut rng = derive_rng_from_seed(b"test_verify_transformation 1");
            let (pubkey, privkey) = Phase1::key_generation(&mut rng, current_accumulator_hash.as_ref())
                .expect("could not generate keypair");

            // transform the accumulator
            Phase1::computation(
                &input,
                &mut output,
                compressed_input,
                compressed_output,
                CheckForCorrectness::No,
                &privkey,
                &parameters,
            )
            .unwrap();
            // ensure that the key is not available to the verifier
            drop(privkey);

            let res = Phase1::verification(
                &input,
                &output,
                &pubkey,
                &current_accumulator_hash,
                compressed_input,
                compressed_output,
                CheckForCorrectness::No,
                CheckForCorrectness::Full,
                &parameters,
            );
            assert!(res.is_ok());

            // subsequent participants must use the hash of the accumulator they received
            let current_accumulator_hash = calculate_hash(&output);
            let (pubkey, privkey) = Phase1::key_generation(&mut rng, current_accumulator_hash.as_ref())
                .expect("could not generate keypair");

            // generate a new output vector for the 2nd participant's contribution
            let mut output_2 = generate_output(&parameters, compressed_output);
            // we use the first output as input
            Phase1::computation(
                &output,
                &mut output_2,
                compressed_output,
                compressed_output,
                CheckForCorrectness::No,
                &privkey,
                &parameters,
            )
            .unwrap();
            // ensure that the key is not available to the verifier
            drop(privkey);

            let res = Phase1::verification(
                &output,
                &output_2,
                &pubkey,
                &current_accumulator_hash,
                compressed_output,
                compressed_output,
                CheckForCorrectness::No,
                CheckForCorrectness::Full,
                &parameters,
            );
            assert!(res.is_ok());

            // verification will fail if the old hash is used
            let res = Phase1::verification(
                &output,
                &output_2,
                &pubkey,
                &blank_hash(),
                compressed_output,
                compressed_output,
                CheckForCorrectness::No,
                CheckForCorrectness::Full,
                &parameters,
            );
            assert!(res.is_err());

            // verification will fail if even 1 byte is modified
            output_2[100] = 0;
            let res = Phase1::verification(
                &output,
                &output_2,
                &pubkey,
                &current_accumulator_hash,
                compressed_output,
                compressed_output,
                CheckForCorrectness::No,
                CheckForCorrectness::Full,
                &parameters,
            );
            assert!(res.is_err());
        }
    }

    fn chunk_verification_test<E: PairingEngine>(
        powers: usize,
        batch: usize,
        compressed_input: UseCompression,
        compressed_output: UseCompression,
    ) {
        let correctness = CheckForCorrectness::Full;

        let powers_length = 1 << powers;
        let powers_g1_length = (powers_length << 1) - 1;
        let num_chunks = (powers_g1_length + batch - 1) / batch;

        // TODO (howardwu): Uncomment after fixing Marlin mode.
        // for proving_system in &[ProvingSystem::Groth16, ProvingSystem::Marlin] {
        for proving_system in &[ProvingSystem::Groth16] {
            for chunk_index in 0..num_chunks {
                let parameters = Phase1Parameters::<E>::new_chunk(
                    ContributionMode::Chunked,
                    chunk_index,
                    batch,
                    *proving_system,
                    powers,
                    batch,
                );

                // Allocate the input/output vectors
                let (input, _) = generate_input(&parameters, compressed_input, correctness);
                let mut output = generate_output(&parameters, compressed_output);

                // Construct our keypair
                let current_accumulator_hash = blank_hash();
                let mut rng = derive_rng_from_seed(b"test_verify_transformation 1");
                let (pubkey, privkey) = Phase1::key_generation(&mut rng, current_accumulator_hash.as_ref())
                    .expect("could not generate keypair");

                // Transform the accumulator
                Phase1::computation(
                    &input,
                    &mut output,
                    compressed_input,
                    compressed_output,
                    correctness,
                    &privkey,
                    &parameters,
                )
                .unwrap();
                // Ensure that the key is not available to the verifier
                drop(privkey);

                let res = Phase1::verification(
                    &input,
                    &output,
                    &pubkey,
                    &current_accumulator_hash,
                    compressed_input,
                    compressed_output,
                    correctness,
                    correctness,
                    &parameters,
                );
                assert!(res.is_ok());

                // Subsequent participants must use the hash of the accumulator they received
                let current_accumulator_hash = calculate_hash(&output);

                let mut rng = derive_rng_from_seed(b"test_verify_transformation 2");
                let (pubkey, privkey) = Phase1::key_generation(&mut rng, current_accumulator_hash.as_ref())
                    .expect("could not generate keypair");

                // Generate a new output vector for the 2nd participant's contribution
                let mut output_2 = generate_output(&parameters, compressed_output);

                // We use the first output as input
                Phase1::computation(
                    &output,
                    &mut output_2,
                    compressed_output,
                    compressed_output,
                    correctness,
                    &privkey,
                    &parameters,
                )
                .unwrap();
                // Ensure that the key is not available to the verifier
                drop(privkey);

                let res = Phase1::verification(
                    &output,
                    &output_2,
                    &pubkey,
                    &current_accumulator_hash,
                    compressed_output,
                    compressed_output,
                    correctness,
                    correctness,
                    &parameters,
                );
                assert!(res.is_ok());

                if parameters.chunk_index == 0 {
                    // Verification will fail if the old hash is used
                    let res = Phase1::verification(
                        &output,
                        &output_2,
                        &pubkey,
                        &blank_hash(),
                        compressed_output,
                        compressed_output,
                        correctness,
                        correctness,
                        &parameters,
                    );
                    assert!(res.is_err());
                }

                /* TODO(kobi): bring back test
                // Verification will fail if even 1 byte is modified
                output_2[100] = 0;
                let res = BatchedAccumulator::verify_transformation(
                    &output,
                    &output_2,
                    &pubkey,
                    &current_accumulator_hash,
                    compressed_output,
                    compressed_output,
                    correctness,
                    correctness,
                    &parameters,
                );
                assert!(res.is_err());
                 */
            }
        }
    }

    // TODO (howardwu): Move to aggregation.rs
    fn full_verification_test<E: PairingEngine>(
        powers: usize,
        batch: usize,
        compressed_input: UseCompression,
        compressed_output: UseCompression,
        use_wrong_chunks: bool,
    ) {
        let correctness = CheckForCorrectness::Full;

        let powers_length = 1 << powers;
        let powers_g1_length = (powers_length << 1) - 1;
        let num_chunks = (powers_g1_length + batch - 1) / batch;

        let mut chunks_participant_2: Vec<Vec<u8>> = vec![];

        // TODO (howardwu): Uncomment after fixing Marlin mode.
        // for proving_system in &[ProvingSystem::Groth16, ProvingSystem::Marlin] {
        for proving_system in &[ProvingSystem::Groth16] {
            for chunk_index in 0..num_chunks {
                let parameters = Phase1Parameters::<E>::new_chunk(
                    ContributionMode::Chunked,
                    chunk_index,
                    batch,
                    *proving_system,
                    powers,
                    batch,
                );

                // Allocate the input/output vectors
                let (input, _) = generate_input(&parameters, compressed_input, correctness);
                let mut output = generate_output(&parameters, compressed_output);

                // Construct our keypair
                let current_accumulator_hash = blank_hash();
                let mut rng = derive_rng_from_seed(b"test_verify_transformation 1");
                let (pubkey, privkey) = Phase1::key_generation(&mut rng, current_accumulator_hash.as_ref())
                    .expect("could not generate keypair");

                // Transform the accumulator
                Phase1::computation(
                    &input,
                    &mut output,
                    compressed_input,
                    compressed_output,
                    correctness,
                    &privkey,
                    &parameters,
                )
                .unwrap();
                // ensure that the key is not available to the verifier
                drop(privkey);

                let res = Phase1::verification(
                    &input,
                    &output,
                    &pubkey,
                    &current_accumulator_hash,
                    compressed_input,
                    compressed_output,
                    correctness,
                    correctness,
                    &parameters,
                );
                assert!(res.is_ok());

                // subsequent participants must use the hash of the accumulator they received
                let current_accumulator_hash = calculate_hash(&output);

                let mut rng = derive_rng_from_seed(b"test_verify_transformation 2");
                let (pubkey, privkey) = Phase1::key_generation(&mut rng, current_accumulator_hash.as_ref())
                    .expect("could not generate keypair");

                // generate a new output vector for the 2nd participant's contribution
                let mut output_2 = generate_output(&parameters, compressed_output);
                // we use the first output as input
                Phase1::computation(
                    &output,
                    &mut output_2,
                    compressed_output,
                    compressed_output,
                    correctness,
                    &privkey,
                    &parameters,
                )
                .unwrap();
                // ensure that the key is not available to the verifier
                drop(privkey);
                if use_wrong_chunks {
                    if chunk_index == 1 {
                        let chunk_0_contribution: Vec<u8> = (*chunks_participant_2.iter().last().unwrap()).to_vec();
                        chunks_participant_2.push(chunk_0_contribution);
                    } else {
                        chunks_participant_2.push(output_2.clone());
                    }
                } else {
                    chunks_participant_2.push(output_2.clone());
                }

                let res = Phase1::verification(
                    &output,
                    &output_2,
                    &pubkey,
                    &current_accumulator_hash,
                    compressed_output,
                    compressed_output,
                    correctness,
                    correctness,
                    &parameters,
                );
                assert!(res.is_ok());

                if parameters.chunk_index == 0 {
                    // Verification will fail if the old hash is used
                    let res = Phase1::verification(
                        &output,
                        &output_2,
                        &pubkey,
                        &blank_hash(),
                        compressed_output,
                        compressed_output,
                        correctness,
                        correctness,
                        &parameters,
                    );
                    assert!(res.is_err());
                }
            }

            // TODO (howardwu): Fix this.

            // // Aggregate the right ones. Combining and verification should work.
            // let chunks_participant_2 = chunks_participant_2
            //     .iter()
            //     .map(|v| (v.as_slice(), compressed_output))
            //     .collect::<Vec<_>>();
            // let parameters = Phase1Parameters::<E>::new(*proving_system, powers, batch);
            // let mut output = generate_output(&parameters, compressed_output);
            //
            // let parameters =
            //     Phase1Parameters::<E>::new_chunk(ContributionMode::Chunked, 0, batch, *proving_system, powers, batch);
            // Phase1::aggregation(
            //     &chunks_participant_2,
            //     (&mut output, compressed_output),
            //     &parameters,
            // )
            //     .unwrap();
            //
            // let parameters = Phase1Parameters::<E>::new(*proving_system, powers, batch);
            // Phase1::verification(
            //     (&mut output, compressed_output, CheckForCorrectness::No),
            //     &parameters,
            // )
            //     .unwrap();
            //
            // let res = Phase1::verification(
            //     &output,
            //     &output,
            //     &pubkey,
            //     &current_accumulator_hash,
            //     compressed_output,
            //     compressed_output,
            //     correctness,
            //     correctness,
            //     &parameters,
            // );
            // assert!(res.is_ok());
        }
    }

    #[test]
    fn test_verification_bls12_377() {
        curve_verification_test::<Bls12_377>(4, 3, UseCompression::Yes, UseCompression::Yes);
        curve_verification_test::<Bls12_377>(4, 3, UseCompression::No, UseCompression::No);
        curve_verification_test::<Bls12_377>(4, 3, UseCompression::Yes, UseCompression::No);
        curve_verification_test::<Bls12_377>(4, 3, UseCompression::No, UseCompression::Yes);
    }

    #[test]
    fn test_verification_bw6_761() {
        curve_verification_test::<BW6_761>(4, 3, UseCompression::Yes, UseCompression::Yes);
        curve_verification_test::<BW6_761>(4, 3, UseCompression::No, UseCompression::No);
        curve_verification_test::<BW6_761>(4, 3, UseCompression::Yes, UseCompression::No);
        curve_verification_test::<BW6_761>(4, 3, UseCompression::No, UseCompression::Yes);
    }

    // #[test]
    // fn test_chunk_verification_bls12_377() {
    //     chunk_verification_test::<Bls12_377>(
    //         2,
    //         2,
    //         UseCompression::Yes,
    //         UseCompression::Yes,
    //     );
    //     chunk_verification_test::<Bls12_377>(2, 2, UseCompression::No, UseCompression::No);
    //     chunk_verification_test::<Bls12_377>(
    //         2,
    //         2,
    //         UseCompression::Yes,
    //         UseCompression::No,
    //     );
    // }
    //
    // #[test]
    // #[should_panic]
    // fn test_full_verification_bls12_377_wrong_chunks() {
    //     full_verification_test::<Bls12_377>(
    //         4,
    //         4,
    //         UseCompression::No,
    //         UseCompression::Yes,
    //         true,
    //     );
    // }
    //
    // #[test]
    // fn test_full_verification_bls12_377() {
    //     full_verification_test::<Bls12_377>(
    //         4,
    //         4,
    //         UseCompression::Yes,
    //         UseCompression::Yes,
    //         false,
    //     );
    //     full_verification_test::<Bls12_377>(
    //         4,
    //         4,
    //         UseCompression::Yes,
    //         UseCompression::Yes,
    //         false,
    //     );
    //     full_verification_test::<Bls12_377>(
    //         4,
    //         4,
    //         UseCompression::No,
    //         UseCompression::No,
    //         false,
    //     );
    //     full_verification_test::<Bls12_377>(
    //         4,
    //         4,
    //         UseCompression::Yes,
    //         UseCompression::No,
    //         false,
    //     );
    // }
}
