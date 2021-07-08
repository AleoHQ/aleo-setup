use setup1_verifier::{utils::init_logger, verifier::Verifier};

use phase1_coordinator::environment::{Development, Environment, Parameters, Production};
use setup1_shared::structures::{PublicSettings, SetupKind};
use structopt::StructOpt;
use url::Url;

use snarkvm_dpc::{
    testnet1::{instantiated::Components, SystemParameters},
    Address,
    ViewKey,
};
use std::{path::PathBuf, str::FromStr};
use tracing::info;

fn development() -> Environment {
    Development::from(Parameters::TestCustom {
        number_of_chunks: 64,
        power: 16,
        batch_size: 512,
    })
    .into()
}

fn inner() -> Environment {
    Production::from(Parameters::AleoInner).into()
}

fn outer() -> Environment {
    Production::from(Parameters::AleoOuter).into()
}

fn universal() -> Environment {
    Production::from(Parameters::AleoUniversal).into()
}

#[derive(Debug, StructOpt)]
#[structopt(name = "Aleo setup verifier")]
struct Options {
    #[structopt(long, help = "Path to a file containing verifier view key")]
    view_key: PathBuf,
    #[structopt(long, help = "Coordinator api url, for example http://localhost:9000")]
    api_url: Url,
}

async fn request_coordinator_public_settings(coordinator_url: &Url) -> anyhow::Result<PublicSettings> {
    let settings_endpoint_url = coordinator_url.join("/v1/coordinator/settings")?;
    let client = reqwest::Client::new();
    let bytes = client
        .post(settings_endpoint_url)
        .header(http::header::CONTENT_LENGTH, 0)
        .send()
        .await?
        .bytes()
        .await?;
    PublicSettings::decode(&bytes.to_vec())
        .map_err(|e| anyhow::anyhow!("Error decoding coordinator PublicSettings: {}", e))
}

#[tokio::main]
async fn main() {
    let options = Options::from_args();

    init_logger();

    let public_settings = request_coordinator_public_settings(&options.api_url)
        .await
        .expect("Failed to fetch the coordinator public settings");

    let environment = match public_settings.setup {
        SetupKind::Development => development(),
        SetupKind::Inner => inner(),
        SetupKind::Outer => outer(),
        SetupKind::Universal => universal(),
    };

    let storage_prefix = format!("{:?}", public_settings.setup).to_lowercase();
    let tasks_storage_path = format!("{}_verifier.tasks", storage_prefix);

    let raw_view_key = std::fs::read_to_string(options.view_key).expect("View key not found");
    let view_key = ViewKey::from_str(&raw_view_key).expect("Invalid view key");
    let parameters = SystemParameters::<Components>::load().unwrap();
    let address =
        Address::from_view_key(&parameters.account_encryption, &view_key).expect("Address not derived correctly");

    // Initialize the verifier
    info!("Initializing verifier...");
    let verifier = Verifier::new(
        options.api_url.clone(),
        view_key,
        address,
        environment,
        tasks_storage_path,
    )
    .expect("Failed to initialize verifier");

    verifier.start_verifier().await;
}
