#[cfg(feature = "azure")]
mod blobstore;

use i18n_embed::{DesktopLanguageRequester, LanguageRequester};
use structopt::StructOpt;

mod cli;
mod commands;
mod errors;
mod objects;
mod reliability;
mod setup_keys;
mod tasks;
mod utils;

use cli::{Command, Options};
use commands::{contribute_subcommand, generate_keys};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Setup localization of the `age` dependency.
    let age_localizer = age::localizer();
    let language_requester = DesktopLanguageRequester::new();
    age_localizer.select(&language_requester.requested_languages())?;

    let opts = Options::from_args();

    match opts.subcommand {
        Command::Generate(generate_opts) => generate_keys(generate_opts),
        Command::Contribute(contribute_opts) => {
            contribute_subcommand(&contribute_opts).await?;
        }
    }

    Ok(())
}
