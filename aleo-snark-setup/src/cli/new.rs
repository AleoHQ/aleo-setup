use gumdrop::Options;

use zexe_algebra::{Bls12_377, PairingEngine};

use phase2::parameters::{circuit_to_qap, MPCParameters};
use snark_utils::{log_2, Groth16Params, UseCompression};

use snarkos_dpc::base_dpc::{
    inner_circuit::InnerCircuit,
    instantiated::{CommitmentMerkleParameters, Components, InnerPairing, MerkleTreeCRH},
    parameters::CircuitParameters,
};
use snarkos_models::{
    algorithms::{MerkleParameters, CRH},
    curves::{Field, PairingEngine as AleoPairingengine},
    gadgets::r1cs::{ConstraintCounter, ConstraintSynthesizer},
    parameters::Parameters,
};
use snarkos_parameters::LedgerMerkleTreeParameters;
use snarkos_utilities::bytes::FromBytes;

use memmap::MmapOptions;
use std::fs::OpenOptions;

type AleoInner = InnerPairing;
type ZexeInner = Bls12_377;

const COMPRESSION: UseCompression = UseCompression::No;

#[derive(Debug, Clone)]
pub enum CurveKind {
    Bls12_377,
    BW6,
}

pub fn curve_from_str(src: &str) -> std::result::Result<CurveKind, String> {
    let curve = match src.to_lowercase().as_str() {
        "bls12_377" => CurveKind::Bls12_377,
        "bw6" => CurveKind::BW6,
        _ => return Err("unsupported curve.".to_string()),
    };
    Ok(curve)
}

#[derive(Debug, Options, Clone)]
pub struct NewOpts {
    help: bool,
    #[options(help = "the path to the phase1 parameters", default = "phase1")]
    pub phase1: String,
    #[options(
        help = "the total number of coefficients (in powers of 2) which were created after processing phase 1"
    )]
    pub phase1_size: u32,
    #[options(help = "the challenge file name to be created", default = "challenge")]
    pub output: String,

    #[options(
        help = "the elliptic curve to use",
        default = "bls12_377",
        parse(try_from_str = "curve_from_str")
    )]
    pub curve_type: CurveKind,

    #[options(help = "setup the inner or the outer circuit?")]
    pub is_inner: bool,
}

pub fn new(opt: &NewOpts) -> anyhow::Result<()> {
    let circuit_parameters = CircuitParameters::<Components>::load()?;

    // Load the inner circuit & merkle params
    let params_bytes = LedgerMerkleTreeParameters::load_bytes()?;
    let params = <MerkleTreeCRH as CRH>::Parameters::read(&params_bytes[..])?;
    let merkle_tree_hash_parameters =
        <CommitmentMerkleParameters as MerkleParameters>::H::from(params);
    let merkle_params = From::from(merkle_tree_hash_parameters);

    if opt.is_inner {
        let circuit = InnerCircuit::blank(&circuit_parameters, &merkle_params);
        generate_params::<AleoInner, ZexeInner, _>(opt, circuit)
    } else {
        todo!("How should we load the outer circuit's params?")
    }
}

/// Returns the number of powers required for the Phase 2 ceremony
/// = log2(aux + inputs + constraints)
fn ceremony_size<F: Field, C: Clone + ConstraintSynthesizer<F>>(circuit: &C) -> usize {
    let mut counter = ConstraintCounter::new();
    circuit
        .clone()
        .generate_constraints(&mut counter)
        .expect("could not calculate number of required constraints");
    let phase2_size = counter.num_aux + counter.num_inputs + counter.num_constraints;
    let power = log_2(phase2_size) as u32;

    // get the nearest power of 2
    if phase2_size < 2usize.pow(power) {
        2usize.pow(power + 1)
    } else {
        phase2_size
    }
}

pub fn generate_params<
    Aleo: AleoPairingengine,
    Zexe: PairingEngine,
    C: Clone + ConstraintSynthesizer<Aleo::Fr>,
>(
    opt: &NewOpts,
    circuit: C,
) -> anyhow::Result<()> {
    let phase1_transcript = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&opt.phase1)
        .expect("could not read phase 1 transcript file");
    let mut phase1_transcript = unsafe {
        MmapOptions::new()
            .map_mut(&phase1_transcript)
            .expect("unable to create a memory map for input")
    };
    let mut output = OpenOptions::new()
        .read(false)
        .write(true)
        .create_new(true)
        .open(&opt.output)
        .expect("could not open file for writing the MPC parameters ");

    let phase2_size = ceremony_size(&circuit);
    let keypair = circuit_to_qap::<Aleo, Zexe, _>(circuit)?;

    // Read `num_constraints` Lagrange coefficients from the Phase1 Powers of Tau which were
    // prepared for this step. This will fail if Phase 1 was too small.
    let phase1 = Groth16Params::<Zexe>::read(
        &mut phase1_transcript,
        COMPRESSION,
        2usize.pow(opt.phase1_size),
        phase2_size,
    )?;

    // Generate the initial transcript
    let mpc = MPCParameters::new(keypair, phase1)?;
    mpc.write(&mut output)?;

    Ok(())
}
