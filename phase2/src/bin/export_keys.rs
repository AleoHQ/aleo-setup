extern crate bellman_ce;
extern crate rand;
extern crate phase2;
extern crate exitcode;
extern crate serde;
extern crate serde_json;
extern crate num_bigint;
extern crate num_traits;

use std::fs;
use std::fs::OpenOptions;
use serde::{Deserialize, Serialize};
use phase2::parameters::MPCParameters;
use phase2::utils::repr_to_big;
use bellman_ce::pairing::{
    Engine,
    CurveAffine,
    ff::PrimeField,
    bn256::{
        Bn256,
    }
};

#[derive(Serialize, Deserialize)]
struct ProvingKeyJson {
    #[serde(rename = "A")]
    pub a: Vec<Vec<String>>,
    #[serde(rename = "B1")]
    pub b1: Vec<Vec<String>>,
    #[serde(rename = "B2")]
    pub b2: Vec<Vec<Vec<String>>>,
    #[serde(rename = "C")]
    pub c: Vec<Option<Vec<String>>>,
    pub vk_alfa_1: Vec<String>,
    pub vk_beta_1: Vec<String>,
    pub vk_delta_1: Vec<String>,
    pub vk_beta_2: Vec<Vec<String>>,
    pub vk_delta_2: Vec<Vec<String>>,
    #[serde(rename = "hExps")]
    pub h: Vec<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
struct VerifyingKeyJson {
    #[serde(rename = "IC")]
    pub ic: Vec<Vec<String>>,
    pub vk_alfa_1: Vec<String>,
    pub vk_beta_2: Vec<Vec<String>>,
    pub vk_gamma_2: Vec<Vec<String>>,
    pub vk_delta_2: Vec<Vec<String>>,
    pub vk_alfabeta_12: Vec<Vec<Vec<String>>>,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        println!("Usage: \n<in_params.params> <out_vk.json> <out_pk.json>");
        std::process::exit(exitcode::USAGE);
    }
    let params_filename = &args[1];
    let vk_filename = &args[2];
    let pk_filename = &args[3];

    let disallow_points_at_infinity = false;

    println!("Exporting {}...", params_filename);

    let reader = OpenOptions::new()
                            .read(true)
                            .open(params_filename)
                            .expect("unable to open.");
    let params = MPCParameters::read(reader, disallow_points_at_infinity, true).expect("unable to read params");
    let params = params.get_params();

    let mut proving_key = ProvingKeyJson {
        a: vec![],
        b1: vec![],
        b2: vec![],
        c: vec![],
        vk_alfa_1: vec![],
        vk_beta_1: vec![],
        vk_delta_1: vec![],
        vk_beta_2: vec![],
        vk_delta_2: vec![],
        h: vec![],
    };

    let p1_to_vec = |p : &<Bn256 as Engine>::G1Affine| {
        vec![
            repr_to_big(p.get_x().into_repr()),
            repr_to_big(p.get_y().into_repr()),
            if p.is_zero() { "0".to_string() } else { "1".to_string() }
        ]
    };
    let p2_to_vec = |p : &<Bn256 as Engine>::G2Affine| {
        vec![
            vec![
                repr_to_big(p.get_x().c0.into_repr()),
                repr_to_big(p.get_x().c1.into_repr()),
            ],
            vec![
                repr_to_big(p.get_y().c0.into_repr()),
                repr_to_big(p.get_y().c1.into_repr()),
            ],
            if p.is_zero() {
                vec!["0".to_string(), "0".to_string()]
            } else {
                vec!["1".to_string(), "0".to_string()]
            }
        ]
    };
    let pairing_to_vec = |p : bellman_ce::pairing::bn256::Fq12| {
        vec![
            vec![
                vec![
                    repr_to_big(p.c0.c0.c0.into_repr()),
                    repr_to_big(p.c0.c0.c1.into_repr()),
                ],
                vec![
                    repr_to_big(p.c0.c1.c0.into_repr()),
                    repr_to_big(p.c0.c1.c1.into_repr()),
                ],
                vec![
                    repr_to_big(p.c0.c2.c0.into_repr()),
                    repr_to_big(p.c0.c2.c1.into_repr()),
                ]
            ],
            vec![
                vec![
                    repr_to_big(p.c1.c0.c0.into_repr()),
                    repr_to_big(p.c1.c0.c1.into_repr()),
                ],
                vec![
                    repr_to_big(p.c1.c1.c0.into_repr()),
                    repr_to_big(p.c1.c1.c1.into_repr()),
                ],
                vec![
                    repr_to_big(p.c1.c2.c0.into_repr()),
                    repr_to_big(p.c1.c2.c1.into_repr()),
                ]
            ],
        ]
    };
    let a = params.a.clone();
    for e in a.iter() {
        proving_key.a.push(p1_to_vec(e));
    }
    let b1 = params.b_g1.clone();
    for e in b1.iter() {
        proving_key.b1.push(p1_to_vec(e));
    }
    let b2 = params.b_g2.clone();
    for e in b2.iter() {
        proving_key.b2.push(p2_to_vec(e));
    }
    let c = params.l.clone();
    for _ in 0..params.vk.ic.len() {
        proving_key.c.push(None);
    }
    for e in c.iter() {
        proving_key.c.push(Some(p1_to_vec(e)));
    }

    let vk_alfa_1 = params.vk.alpha_g1.clone();
    proving_key.vk_alfa_1 = p1_to_vec(&vk_alfa_1);

    let vk_beta_1 = params.vk.beta_g1.clone();
    proving_key.vk_beta_1 = p1_to_vec(&vk_beta_1);

    let vk_delta_1 = params.vk.delta_g1.clone();
    proving_key.vk_delta_1 = p1_to_vec(&vk_delta_1);

    let vk_beta_2 = params.vk.beta_g2.clone();
    proving_key.vk_beta_2 = p2_to_vec(&vk_beta_2);

    let vk_delta_2 = params.vk.delta_g2.clone();
    proving_key.vk_delta_2 = p2_to_vec(&vk_delta_2);

    let h = params.h.clone();
    for e in h.iter() {
        proving_key.h.push(p1_to_vec(e));
    }

    let mut verification_key = VerifyingKeyJson {
        ic: vec![],
        vk_alfa_1: vec![],
        vk_beta_2: vec![],
        vk_gamma_2: vec![],
        vk_delta_2: vec![],
        vk_alfabeta_12: vec![],
    };

    let ic = params.vk.ic.clone();
    for e in ic.iter() {
        verification_key.ic.push(p1_to_vec(e));
    }

    verification_key.vk_alfa_1 = p1_to_vec(&vk_alfa_1);
    verification_key.vk_beta_2 = p2_to_vec(&vk_beta_2);
    let vk_gamma_2 = params.vk.gamma_g2.clone();
    verification_key.vk_gamma_2 = p2_to_vec(&vk_gamma_2);
    verification_key.vk_delta_2 = p2_to_vec(&vk_delta_2);
    verification_key.vk_alfabeta_12 = pairing_to_vec(Bn256::pairing(vk_alfa_1, vk_beta_2));

    let pk_json = serde_json::to_string(&proving_key).unwrap();
    fs::write(pk_filename, pk_json.as_bytes()).unwrap();

    let vk_json = serde_json::to_string(&verification_key).unwrap();
    fs::write(vk_filename, vk_json.as_bytes()).unwrap();

    println!("Created {} and {}.", pk_filename, vk_filename);
}
