use std::path::Path;

use anyhow::Result;
use blake2::{Blake2s, Digest};
use serde::{Deserialize, Serialize};

const CONFIRMATION_KEY_FILE: &str = ".confirmation_key";

/// The data structure to persist the information
/// about the private key in case of contributor restart.
/// Private key is used to prove the participation
/// in the setup
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ConfirmationKey {
    address: String,
    private_key: String,
}

impl ConfirmationKey {
    /// Return blake2s hash of confirmation key as hex string
    ///
    /// If there's `CONFIRMATION_KEY_FILE`, and the key_file.address
    /// is the same as contributor address, use the file contents
    /// to produce the hash.
    ///
    /// If there's no `CONFIRMATION_KEY_FILE` or key_file.address
    /// is different, generate a new private_key, write it
    /// to disk with contributor address, and use these private_key
    /// and address to produce the hash
    pub fn for_current_round(address: String) -> Result<String> {
        let key_file = if Path::new(CONFIRMATION_KEY_FILE).exists() {
            let key_file = Self::read_from_disk()?;
            if key_file.address == address {
                key_file
            } else {
                Self::create_new(address)?
            }
        } else {
            Self::create_new(address)?
        };
        Ok(key_file.hash_contents())
    }

    /// Create a new confirmation key and write it to disk
    fn create_new(address: String) -> Result<Self> {
        let additional_keys = super::generate_unencrypted();
        let private_key = additional_keys.private_key.to_string();
        let key_file = ConfirmationKey { address, private_key };
        key_file.write_to_disk()?;
        Ok(key_file)
    }

    fn read_from_disk() -> Result<Self> {
        let bytes = std::fs::read(CONFIRMATION_KEY_FILE)?;
        let key_file = serde_json::from_slice(&bytes)?;
        Ok(key_file)
    }

    fn write_to_disk(&self) -> Result<()> {
        let bytes = serde_json::to_vec(self)?;
        std::fs::write(CONFIRMATION_KEY_FILE, bytes)?;
        Ok(())
    }

    /// Produce the blake2s hash of concatenated address and private key
    fn hash_contents(&self) -> String {
        let concatenated = format!("{}{}", self.address, self.private_key);
        let mut hasher = Blake2s::new();
        hasher.update(concatenated.as_bytes());
        let bytes = hasher.finalize().to_vec();
        hex::encode(&bytes)
    }
}

pub fn print_key_and_remove_the_file() -> Result<()> {
    let key_file = ConfirmationKey::read_from_disk()?;
    println!("|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||");
    println!("Your address is {}", key_file.address);
    println!("And confirmation key is {}", key_file.private_key);
    println!("Store this information in order to prove your participation in the setup");
    println!("|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||");
    std::fs::remove_file(CONFIRMATION_KEY_FILE)?;
    Ok(())
}
