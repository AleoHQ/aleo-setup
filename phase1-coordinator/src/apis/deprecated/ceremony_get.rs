use crate::{objects::Round, Coordinator};

use rocket::{http::Status, State};
use rocket_contrib::json::Json;
use std::sync::Arc;

#[get("/ceremony")]
pub fn ceremony_get(coordinator: State<Arc<Coordinator>>) -> Result<Json<Round>, Status> {
    match coordinator.current_round() {
        Ok(round) => Ok(Json(round.clone())),
        _ => return Err(Status::InternalServerError),
    }
}

#[cfg(test)]
mod test {
    use crate::{objects::Round, testing::prelude::*};

    #[test]
    #[serial]
    fn test_ceremony_get() {
        clear_test_transcript();

        let (client, coordinator) = test_client(&TEST_ENVIRONMENT).unwrap();
        test_initialize_to_round_1(&coordinator).unwrap();

        let mut response = client.get("/ceremony").dispatch();
        let response_body = response.body_string();
        assert_eq!(Status::Ok, response.status());
        assert_eq!(Some(ContentType::JSON), response.content_type());
        assert!(response_body.is_some());

        let candidate: Round = serde_json::from_str(&response_body.unwrap()).unwrap();
        let expected = test_round_1_initial_json().unwrap();
        if candidate != expected {
            print_diff(&expected, &candidate);
        }
        assert_eq!(candidate, expected);
    }
}
