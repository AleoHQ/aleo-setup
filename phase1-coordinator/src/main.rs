#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;
#[macro_use]
extern crate serde_json;

use phase1_coordinator::{
    apis::*,
    environment::{Environment, Parameters},
    Coordinator,
    Participant,
};

use chrono::Utc;
use rocket::{
    config::{Config, Environment as RocketEnvironment},
    Rocket,
};
use tracing::{info, Level};

#[inline]
fn logger() {
    let subscriber = tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

#[inline]
fn coordinator(environment: &Environment) -> anyhow::Result<Coordinator> {
    info!("Starting coordinator");
    let mut coordinator = Coordinator::new(environment.clone())?;

    let contributors = vec![Participant::Contributor(
        "0xd0FaDc3C5899c28c581c0e06819f4113cb08b0e4".to_string(),
    )];
    let verifiers = vec![Participant::Verifier(
        "0xd0FaDc3C5899c28c581c0e06819f4113cb08b0e4".to_string(),
    )];
    let chunk_verifiers = (0..environment.number_of_chunks())
        .into_iter()
        .map(|_| verifiers[0].clone())
        .collect();
    let chunk_verifier_base_urls = (0..environment.number_of_chunks())
        .into_iter()
        .map(|_| "http://localhost:8080")
        .collect();

    // If this is the first time running the ceremony, start by initializing one round.
    if coordinator.current_round_height()? == 0 {
        coordinator.next_round(
            Utc::now(),
            contributors,
            verifiers,
            chunk_verifiers,
            chunk_verifier_base_urls,
        )?;
    }
    info!("Coordinator is ready");
    info!("{}", serde_json::to_string_pretty(&coordinator.current_round()?)?);

    Ok(coordinator)
}

#[inline]
fn server(environment: &Environment) -> anyhow::Result<Rocket> {
    info!("Starting server...");
    let builder = match environment {
        Environment::Test(_) => Config::build(RocketEnvironment::Development),
        Environment::Development(_) => Config::build(RocketEnvironment::Production),
        Environment::Production(_) => Config::build(RocketEnvironment::Production),
    };

    let config = builder
        .address(environment.address())
        .port(environment.port())
        .finalize()?;

    let server = rocket::custom(config)
        .manage(coordinator(environment)?)
        .mount("/", routes![
            chunk_get,
            chunk_post,
            lock_post,
            ping_get,
            timestamp_get, // transcript_get,
            round_get,
        ])
        .attach(environment.cors());
    info!("Server is ready");
    Ok(server)
}

#[inline]
pub fn main() -> anyhow::Result<()> {
    logger();
    server(&Environment::Test(Parameters::AleoTest))?.launch();
    Ok(())
}
