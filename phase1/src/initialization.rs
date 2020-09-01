use super::*;

impl<'a, E: PairingEngine + Sync> Phase1<'a, E> {
    ///
    /// Phase 1: Initialization
    ///
    /// Generates the initial accumulator.
    /// Populates the output buffer with an empty accumulator
    /// as dictated by parameters and compression.
    ///
    pub fn initialization(
        output: &mut [u8],
        compressed_output: UseCompression,
        parameters: &'a Phase1Parameters<E>,
    ) -> Result<()> {
        let span = info_span!("phase1-initialization");
        let _ = span.enter();

        let (tau_g1, tau_g2, alpha_g1, beta_g1, beta_g2) = split_mut(output, parameters, compressed_output);

        let one_g1 = &E::G1Affine::prime_subgroup_generator();
        let one_g2 = &E::G2Affine::prime_subgroup_generator();

        #[cfg(not(feature = "wasm"))]
        rayon::scope(|s| {
            s.spawn(|_| {
                tau_g1
                    .init_element(one_g1, compressed_output)
                    .expect("could not initialize tau_g1 elements")
            });
            s.spawn(|_| {
                tau_g2
                    .init_element(one_g2, compressed_output)
                    .expect("could not initialize tau_g2 elements")
            });
            s.spawn(|_| {
                alpha_g1
                    .init_element(one_g1, compressed_output)
                    .expect("could not initialize alpha_g1 elements")
            });
            s.spawn(|_| {
                beta_g1
                    .init_element(one_g1, compressed_output)
                    .expect("could not initialize beta_g1 elements")
            });
            s.spawn(|_| {
                beta_g2
                    .init_element(one_g2, compressed_output)
                    .expect("could not initialize beta_g2 element")
            });
        });

        #[cfg(feature = "wasm")]
        {
            tau_g1
                .init_element(one_g1, compressed_output)
                .expect("could not initialize tau_g1 elements");

            tau_g2
                .init_element(one_g2, compressed_output)
                .expect("could not initialize tau_g2 elements");

            alpha_g1
                .init_element(one_g1, compressed_output)
                .expect("could not initialize alpha_g1 elements");

            beta_g1
                .init_element(one_g1, compressed_output)
                .expect("could not initialize beta_g1 elements");

            beta_g2
                .init_element(one_g2, compressed_output)
                .expect("could not initialize beta_g2 element");
        }

        info!("phase1-initialization complete");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use zexe_algebra::{AffineCurve, Bls12_377, BW6_761};

    fn curve_initialization_test<E: PairingEngine>(powers: usize, batch: usize, compression: UseCompression) {
        let parameters = Phase1Parameters::<E>::new(powers, batch);
        let expected_challenge_length = match compression {
            UseCompression::Yes => parameters.contribution_size - parameters.public_key_size,
            UseCompression::No => parameters.accumulator_size,
        };

        let mut output = vec![0; expected_challenge_length];
        Phase1::initialization(&mut output, compression, &parameters).unwrap();

        let deserialized = Phase1::deserialize(&output, compression, CheckForCorrectness::Yes, &parameters).unwrap();

        let g1_zero = E::G1Affine::prime_subgroup_generator();
        let g2_zero = E::G2Affine::prime_subgroup_generator();

        assert_eq!(deserialized.tau_powers_g1, vec![g1_zero; parameters.powers_g1_length]);
        assert_eq!(deserialized.tau_powers_g2, vec![g2_zero; parameters.powers_length]);
        assert_eq!(deserialized.alpha_tau_powers_g1, vec![
            g1_zero;
            parameters.powers_length
        ]);
        assert_eq!(deserialized.beta_tau_powers_g1, vec![g1_zero; parameters.powers_length]);
        assert_eq!(deserialized.beta_g2, g2_zero);
    }

    #[test]
    fn test_initialization_bls12_377_compressed() {
        curve_initialization_test::<Bls12_377>(4, 4, UseCompression::Yes);
    }

    #[test]
    fn test_initialization_bls12_377_uncompressed() {
        curve_initialization_test::<Bls12_377>(4, 4, UseCompression::No);
    }

    #[test]
    fn test_initialization_bw6_761_compressed() {
        curve_initialization_test::<BW6_761>(4, 4, UseCompression::Yes);
    }

    #[test]
    fn test_initialization_bw6_761_uncompressed() {
        curve_initialization_test::<BW6_761>(4, 4, UseCompression::No);
    }
}
