use crate::{objects::Round, Coordinator, Storage};

use rocket::{http::Status, State};
use tracing::{error, info};
use url::Url;

// TODO (howardwu): Add authentication.
#[post("/chunks/<chunk_id>/contribution", data = "<participant_id>")]
pub fn chunk_post(
    coordinator: State<Coordinator>,
    chunk_id: u64,
    participant_id: String,
    // contribution_id: u64,
) -> Result<String, Status> {
    match coordinator.contribute_chunk(chunk_id, participant_id) {
        Ok(_) => Ok(json!({ "status": "ok" }).to_string()),
        Err(error) => {
            error!("Unable to store the contribution");
            Err(Status::BadRequest)
        }
    }
    // Err(Status::BadRequest)
}
