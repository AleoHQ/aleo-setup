use crate::{objects::Participant, Coordinator};

use rocket::{http::Status, State};
use tracing::error;

// TODO (howardwu): Add authentication.
#[get("/chunks/<chunk_id>/contribution", data = "<participant>")]
pub fn chunk_get(coordinator: State<Coordinator>, chunk_id: u64, participant: Participant) -> Result<String, Status> {
    // Check that the participant ID is authorized.
    match coordinator.is_current_contributor(&participant) {
        // Case 1 - Participant is authorized.
        Ok(true) => (),
        // Case 2 - Participant is not authorized.
        Ok(false) => {
            error!(
                "{} is not authorized for /chunks/{}/lock",
                participant.clone(),
                chunk_id
            );
            return Err(Status::Unauthorized);
        }
        // Case 3 - Ceremony has not started, or storage has been corrupted.
        Err(_) => return Err(Status::InternalServerError),
    }

    // Return the next contribution locator for the given chunk ID.
    match coordinator.next_contribution_locator_strict(chunk_id) {
        Ok(contribution_locator) => {
            let response = json!({
                "status": "ok",
                "result": {
                    "chunkId": chunk_id,
                    "participantId": participant,
                    "writeUrl": contribution_locator.to_string()
                }
            });
            Ok(response.to_string())
        }
        _ => return Err(Status::Unauthorized),
    }
}
