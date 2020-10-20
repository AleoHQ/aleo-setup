use crate::errors::VerifierError;
use snarkos_toolkit::account::{Address, ViewKey};

use rand::thread_rng;
use std::fmt;
use tracing::trace;

/// The header used for authenticating requests sent to the coordinator
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuthenticationHeader {
    pub auth_type: String,
    pub address: String,
    pub signature: String,
}

impl AuthenticationHeader {
    pub fn new(auth_type: String, address: String, signature: String) -> Self {
        Self {
            auth_type,
            address,
            signature,
        }
    }
}

/// The authentication format in the header
impl fmt::Display for AuthenticationHeader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}:{}", self.auth_type, self.address, self.signature)
    }
}

/// Generate the authentication header with the request method, request path, and view key.
/// Returns the authorization header "Aleo <address>:<signature>"
pub fn authenticate(view_key: &ViewKey, method: &str, path: &str) -> Result<AuthenticationHeader, VerifierError> {
    // TODO (raychu86) make this user defined RNG
    let rng = &mut thread_rng();

    // Derive the Aleo address used to verify the signature.
    let address = Address::from_view_key(&view_key)?;

    // Form the message that is signed
    let message = format!("{} {}", method.to_lowercase(), path.to_lowercase());

    trace!(
        "Request authentication - (message: {}) (address: {})",
        message,
        address.to_string()
    );

    // Construct the authentication signature.
    let signature = view_key.sign(&message.into_bytes(), rng)?;

    // Construct the authentication header.

    Ok(AuthenticationHeader::new(
        "Aleo".to_string(),
        address.to_string(),
        signature.to_string(),
    ))
}
