use crate::cli::commands::generate::GenerateOptions;

use std::io::Write;

pub fn generate_keys(opts: GenerateOptions) {
    let mut file = std::fs::File::create(&opts.keys_path).expect("Should have created keys file");
    let passphrase = crate::setup_keys::read_or_generate_passphrase(opts.passphrase);

    println!("\nDO NOT FORGET YOUR PASSPHRASE!\n\nYou will need your passphrase to access your keys.\n\n");

    let aleo_setup_keys = crate::setup_keys::generate(passphrase);

    file.write_all(&serde_json::to_vec(&aleo_setup_keys).expect("Should have converted setup keys to vector"))
        .expect("Should have written setup keys successfully to file");
    println!("Done! Your keys are ready in {:?}.", &opts.keys_path);
}
