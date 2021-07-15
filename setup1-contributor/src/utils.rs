#[cfg(feature = "azure")]
use crate::blobstore::upload_sas;

use crate::errors::UtilsError;
use phase1::{ContributionMode, Phase1Parameters, ProvingSystem};
use phase1_coordinator::{
    environment::{Development, Environment, Parameters, Production},
    objects::{ContributionFileSignature, ContributionState},
};
use setup1_shared::structures::SetupKind;
use snarkos_toolkit::account::{Address, PrivateKey, ViewKey};
use zexe_algebra::PairingEngine;

use anyhow::Result;
use rand::{CryptoRng, Rng};
use reqwest::header::AUTHORIZATION;
use std::{
    fs::{copy, create_dir_all, remove_file, write, File},
    io::{Read, Write},
    path::Path,
    str::FromStr,
};
use tracing::error;

pub fn copy_file_if_exists(file_path: &str, dest_path: &str) -> Result<()> {
    if Path::new(file_path).exists() {
        copy(file_path, dest_path)?;
    }
    Ok(())
}

pub async fn download_file_async(url: &str, file_path: &str) -> Result<()> {
    remove_file_if_exists(file_path)?;
    let mut resp = reqwest::get(url).await?.error_for_status()?;
    let mut out = File::create(file_path)?;
    while let Some(chunk) = resp.chunk().await? {
        out.write_all(&chunk)?;
    }
    Ok(())
}

pub async fn upload_file_direct_async(authorization: &str, file_path: &str, url: &str) -> Result<()> {
    let mut file = File::open(file_path)?;
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)?;

    let client = reqwest::Client::new();
    client
        .post(url)
        .header(AUTHORIZATION, authorization)
        .header(http::header::CONTENT_LENGTH, contents.len())
        .body(contents)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

#[cfg(feature = "azure")]
pub async fn upload_file_to_azure_async(file_path: &str, url: &str) -> Result<()> {
    upload_sas(file_path, url).await?;
    Ok(())
}

pub fn remove_file_if_exists(file_path: &str) -> Result<()> {
    if Path::new(file_path).exists() {
        remove_file(file_path)?;
    }
    Ok(())
}

pub fn read_from_file(file_name: &str) -> Result<Vec<u8>> {
    let mut file = vec![];
    File::open(file_name)?.read_to_end(&mut file)?;
    Ok(file)
}

///
/// This function creates the `file_path`'s parent directories if it
/// does not already exists.
///
pub fn create_parent_directory(file_path: &str) {
    let file_path = Path::new(&file_path);
    if let Some(file_path_parent) = file_path.parent() {
        if let Err(err) = create_dir_all(file_path_parent) {
            error!(
                "Error initializing file path parent directory {:?} {}",
                &file_path_parent, err
            );
        }
    }
}

///
/// This function writes a `file_bytes` to the `file_path`.
///
/// If a parent directory doesn't already exists, this function will
/// automatically generate one.
///
pub fn write_to_file(file_path: &str, file_bytes: Vec<u8>) -> Result<()> {
    create_parent_directory(file_path);
    remove_file_if_exists(file_path)?;

    if let Err(err) = write(file_path, file_bytes) {
        error!("Error writing file to {} {}", &file_path, err);
    }

    Ok(())
}

pub fn proving_system_from_str(proving_system_str: &str) -> Result<ProvingSystem> {
    let proving_system = match proving_system_str {
        "groth16" => ProvingSystem::Groth16,
        "marlin" => ProvingSystem::Marlin,
        _ => {
            return Err(UtilsError::UnsupportedProvingSystemError(proving_system_str.to_string()).into());
        }
    };
    Ok(proving_system)
}

pub fn create_parameters_for_chunk<E: PairingEngine>(
    environment: &Environment,
    chunk_id: usize,
) -> Result<Phase1Parameters<E>> {
    let settings = environment.parameters();

    let parameters = Phase1Parameters::<E>::new_chunk(
        ContributionMode::Chunked,
        chunk_id,
        settings.chunk_size(),
        settings.proving_system(),
        settings.power(),
        settings.batch_size(),
    );
    Ok(parameters)
}

pub fn get_authorization_value<R: Rng + CryptoRng>(
    private_key: &PrivateKey,
    method: &str,
    path: &str,
    rng: &mut R,
) -> Result<String> {
    let view_key = ViewKey::from(&private_key)?;
    let address = Address::from(&private_key)?.to_string();
    let message = format!("{} {}", method.to_lowercase(), path.to_lowercase());
    let signature = view_key.sign(message.as_bytes(), rng)?.to_string();
    let authorization = format!("Aleo {}:{}", address, signature);
    Ok(authorization)
}

///
/// Signs and returns the contribution file signature.
///
pub fn sign_contribution_state<R: Rng + CryptoRng>(
    signing_key: &str,
    challenge_hash: &[u8],
    response_hash: &[u8],
    next_challenge_hash: Option<Vec<u8>>,
    rng: &mut R,
) -> Result<ContributionFileSignature> {
    let contribution_state =
        ContributionState::new(challenge_hash.to_vec(), response_hash.to_vec(), next_challenge_hash)?;
    let message = contribution_state.signature_message()?;

    let view_key = ViewKey::from_str(signing_key)?;
    let signature = view_key.sign(&message.into_bytes(), rng)?;

    let contribution_file_signature = ContributionFileSignature::new(signature.to_string(), contribution_state)?;

    Ok(contribution_file_signature)
}

#[inline]
fn development_environment() -> Environment {
    let environment = Development::from(Parameters::TestCustom {
        number_of_chunks: 64,
        power: 16,
        batch_size: 512,
    });
    environment.into()
}

#[inline]
fn inner_environment() -> Environment {
    Production::from(Parameters::AleoInner).into()
}

#[inline]
fn outer_environment() -> Environment {
    Production::from(Parameters::AleoOuter).into()
}

#[inline]
fn universal_environment() -> Environment {
    Production::from(Parameters::AleoUniversal).into()
}

/// Returns the [Environment] settings based on a setup kind
pub fn environment_by_setup_kind(kind: &SetupKind) -> Environment {
    match kind {
        SetupKind::Development => development_environment(),
        SetupKind::Inner => inner_environment(),
        SetupKind::Outer => outer_environment(),
        SetupKind::Universal => universal_environment(),
    }
}

#[derive(Debug, Clone)]
pub enum UploadMode {
    Auto,
    #[cfg(feature = "azure")]
    Azure,
    Direct,
}

impl UploadMode {
    pub fn variants() -> &'static [&'static str] {
        &[
            "auto",
            #[cfg(feature = "azure")]
            "azure",
            "direct",
        ]
    }
}

impl FromStr for UploadMode {
    type Err = UtilsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "auto" => Ok(Self::Auto),
            #[cfg(feature = "azure")]
            "azure" => Ok(Self::Azure),
            "direct" => Ok(Self::Direct),
            unexpected => Err(UtilsError::UnknownUploadModeError(unexpected.to_string())),
        }
    }
}
