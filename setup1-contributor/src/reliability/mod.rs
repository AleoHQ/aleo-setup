//! Functions and data structures related to reliability score checks

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use http::Request;
use setup1_shared::reliability::{
    ContributorMessage,
    ContributorMessageName,
    CoordinatorMessage,
    CoordinatorMessageName,
    MAXIMUM_MESSAGE_SIZE,
};
use snarkos_toolkit::account::PrivateKey;
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::protocol::{Message, WebSocketConfig},
};
use url::Url;

use crate::utils::get_authorization_value;

mod bandwidth;
mod cpu;
mod latency;

/// Builds a request with authorization header to initialize
/// a WebSocket handshake later
fn prepare_request(api_url: &Url, api_path: &str, private_key: &PrivateKey) -> Result<Request<()>> {
    let mut url = api_url.join(api_path)?;
    url.set_scheme("ws")
        .map_err(|e| anyhow!("Failed to set url scheme to ws: {:?}", e))?;

    let auth_rng = &mut rand::rngs::OsRng;
    let authorization = get_authorization_value(private_key, "GET", api_path, auth_rng)?;

    Request::builder()
        .uri(url.as_str())
        .header(http::header::AUTHORIZATION, authorization)
        .body(())
        .map_err(Into::into)
}

/// Check reliability score before starting
/// to contribute
pub(crate) async fn check(api_base_url: &Url, private_key: &PrivateKey) -> Result<()> {
    let api_path = "/v1/contributor/reliability";
    let request = prepare_request(api_base_url, api_path, private_key)?;

    let mut config = WebSocketConfig::default();
    config.max_frame_size = Some(MAXIMUM_MESSAGE_SIZE);
    config.max_message_size = Some(MAXIMUM_MESSAGE_SIZE);

    let (ws_stream, _response) = connect_async_with_config(request, Some(config))
        .await
        .map_err(|e| anyhow!("Failed to connect to coordinator via WebSocket: {:?}", e))?;
    tracing::trace!("WebSocket handshake has been successfully completed");

    let (mut write, mut read) = ws_stream.split();

    loop {
        let raw_message = if let Some(next) = read.next().await {
            next?
        } else {
            tracing::trace!("Got None in reliability checks, closing the connection");
            break;
        };
        if raw_message.is_close() {
            break;
        }
        if raw_message.is_ping() | raw_message.is_pong() {
            // do nothing for now, but check the standard
            // and tungstenite implementation details
            continue;
        }
        let decoded = CoordinatorMessage::from_slice(&raw_message.into_data()).map_err(|e| anyhow!("{:?}", e))?;
        tracing::trace!("Got message from coordinator: {:?}", decoded.name);

        match decoded.name {
            CoordinatorMessageName::Ping => latency::check(&mut write, decoded.data).await?,
            CoordinatorMessageName::BandwidthChallenge => bandwidth::check(&mut write, decoded.data).await?,
            CoordinatorMessageName::CpuChallenge => cpu::check(&mut write, decoded.data).await?,
            other => {
                let text = format!(
                    "Wrong message, expected one of Ping | BandwidthChallenge | CpuChallenge, got {:?}",
                    other,
                );
                tracing::warn!("{}", text);
                let error = ContributorMessage {
                    name: ContributorMessageName::Error,
                    data: text.into_bytes(),
                };
                let response = Message::binary(error.to_vec());
                write.send(response).await.map_err(|e| anyhow!("{:?}", e))?;
            }
        }
    }
    Ok(())
}
