use std::io::Write;

use age::{
    armor::{ArmoredWriter, Format},
    cli_common::Passphrase,
    EncryptError,
    Encryptor,
};
use anyhow::Result;
use rand::{rngs::OsRng, RngCore};
use secrecy::{ExposeSecret, SecretString, SecretVec};
use serde::{Deserialize, Serialize};
use snarkos_toolkit::account::{Address, PrivateKey};

use crate::errors::ContributeError;

pub mod confirmation_key;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AleoSetupKeys {
    pub encrypted_seed: String,
    pub encrypted_private_key: String,
    pub address: String,
}

struct UnencryptedKeys {
    seed: SecretVec<u8>,
    private_key: PrivateKey,
}

fn encrypt(passphrase: SecretString, secret: &[u8]) -> Result<String> {
    let encryptor = Encryptor::with_user_passphrase(passphrase);
    let mut encrypted_output = vec![];
    let mut writer = encryptor
        .wrap_output(ArmoredWriter::wrap_output(&mut encrypted_output, Format::Binary)?)
        .map_err(|e| match e {
            EncryptError::Io(e) => e,
        })?;
    writer.write_all(secret)?;
    writer.finish()?;
    let encrypted_secret = hex::encode(&encrypted_output);
    Ok(encrypted_secret)
}

fn encrypt_keys(unencrypted: &UnencryptedKeys, passphrase: SecretString) -> AleoSetupKeys {
    let address = Address::from(&unencrypted.private_key).expect("Should have derived an Aleo address");
    let encrypted_seed =
        encrypt(passphrase.clone(), unencrypted.seed.expose_secret()).expect("Should have encrypted Aleo seed");
    let encrypted_private_key =
        encrypt(passphrase, unencrypted.private_key.to_string().as_bytes()).expect("Should have encrypted private key");

    AleoSetupKeys {
        encrypted_seed,
        encrypted_private_key,
        address: address.to_string(),
    }
}

fn generate_unencrypted() -> UnencryptedKeys {
    let mut rng = OsRng;
    let mut seed_bytes = vec![0u8; 64];
    rng.fill_bytes(&mut seed_bytes[..]);
    let seed = SecretVec::new(seed_bytes);
    let private_key = PrivateKey::new(&mut rng).expect("Should have generated an Aleo private key");
    UnencryptedKeys { seed, private_key }
}

pub fn generate(passphrase: SecretString) -> AleoSetupKeys {
    let unencrypted = generate_unencrypted();
    encrypt_keys(&unencrypted, passphrase)
}

/// If `cli_passphrase` is `None`, request passphrase via pinentry or tty
pub fn read_passphrase(cli_passphrase: Option<SecretString>) -> Result<SecretString> {
    if let Some(passphrase) = cli_passphrase {
        Ok(passphrase)
    } else {
        age::cli_common::read_secret("Enter your Aleo setup passphrase", "Passphrase", None)
            .map_err(|_| ContributeError::CouldNotReadPassphraseError.into())
    }
}

/// If `cli_passphrase` is `None`, call `cli_common::read_or_generate_passphrase`.
/// Panics if `read_or_generate_passphrase` returns an error
pub fn read_or_generate_passphrase(cli_passphrase: Option<SecretString>) -> SecretString {
    if let Some(passphrase) = cli_passphrase {
        passphrase
    } else {
        match age::cli_common::read_or_generate_passphrase() {
            Ok(Passphrase::Typed(passphrase)) => passphrase,
            Ok(Passphrase::Generated(new_passphrase)) => {
                println!("\n\nGenerated new passphrase: {}", new_passphrase.expose_secret());
                new_passphrase
            }
            Err(_) => panic!("Should have read or generated passphrase"),
        }
    }
}
