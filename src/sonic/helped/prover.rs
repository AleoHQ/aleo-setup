use ff::{Field};
use pairing::{Engine, CurveProjective};
use std::marker::PhantomData;

use super::{Proof, SxyAdvice};
use super::batch::Batch;
use super::poly::{SxEval, SyEval};
use super::parameters::{Parameters};

use crate::SynthesisError;

use crate::sonic::transcript::{Transcript, TranscriptProtocol};
use crate::sonic::util::*;
use crate::sonic::cs::{Backend, SynthesisDriver};
use crate::sonic::cs::{Circuit, Variable, Coeff};
use crate::sonic::srs::SRS;

pub fn create_advice_on_information_and_srs<E: Engine, C: Circuit<E>, S: SynthesisDriver>(
    circuit: &C,
    proof: &Proof<E>,
    srs: &SRS<E>,
    n: usize
) -> SxyAdvice<E>
{
    let z: E::Fr;
    let y: E::Fr;
    {
        let mut transcript = Transcript::new(&[]);
        transcript.commit_point(&proof.r);
        y = transcript.get_challenge_scalar();
        transcript.commit_point(&proof.t);
        z = transcript.get_challenge_scalar();
    }
    let z_inv = z.inverse().unwrap(); // TODO

    let (s_poly_negative, s_poly_positive) = {
        let mut tmp = SxEval::new(y, n);
        S::synthesize(&mut tmp, circuit).unwrap(); // TODO

        tmp.poly()
    };

    // Compute S commitment
    let s = multiexp(
        srs.g_positive_x_alpha[0..(2 * n)]
            .iter()
            .chain_ext(srs.g_negative_x_alpha[0..(n)].iter()),
        s_poly_positive.iter().chain_ext(s_poly_negative.iter())
    ).into_affine();

    // Compute s(z, y)
    let mut szy = E::Fr::zero();
    {
        let mut tmp = z;
        for &p in &s_poly_positive {
            let mut p = p;
            p.mul_assign(&tmp);
            szy.add_assign(&p);
            tmp.mul_assign(&z);
        }
        let mut tmp = z_inv;
        for &p in &s_poly_negative {
            let mut p = p;
            p.mul_assign(&tmp);
            szy.add_assign(&p);
            tmp.mul_assign(&z_inv);
        }
    }

    // Compute kate opening
    let opening = {
        let mut open = szy;
        open.negate();

        let poly = kate_divison(
            s_poly_negative.iter().rev().chain_ext(Some(open).iter()).chain_ext(s_poly_positive.iter()),
            z,
        );

        let negative_poly = poly[0..n].iter().rev();
        let positive_poly = poly[n..].iter();
        multiexp(
            srs.g_negative_x[1..(negative_poly.len() + 1)].iter().chain_ext(
                srs.g_positive_x[0..positive_poly.len()].iter()
            ),
            negative_poly.chain_ext(positive_poly)
        ).into_affine()
    };

    SxyAdvice {
        s,
        szy,
        opening
    }
}

pub fn create_advice<E: Engine, C: Circuit<E>, S: SynthesisDriver>(
    circuit: &C,
    proof: &Proof<E>,
    parameters: &Parameters<E>,
) -> SxyAdvice<E>
{
    let n = parameters.vk.n;
    create_advice_on_information_and_srs::<E, C, S>(circuit, proof, &parameters.srs, n)   
}

pub fn create_advice_on_srs<E: Engine, C: Circuit<E>, S: SynthesisDriver>(
    circuit: &C,
    proof: &Proof<E>,
    srs: &SRS<E>
) -> SxyAdvice<E>
{
    // annoying, but we need n to compute s(z, y), and this isn't
    // precomputed anywhere yet
    let n = {
        struct CountN {
            n: usize
        }

        impl<'a, E: Engine> Backend<E> for &'a mut CountN {
            fn new_multiplication_gate(&mut self) {
                self.n += 1;
            }
        }

        let mut tmp = CountN{n:0};
        S::synthesize(&mut tmp, circuit).unwrap(); // TODO

        tmp.n
    };

    create_advice_on_information_and_srs::<E, C, S>(circuit, proof, srs, n)   
}

pub fn create_proof<E: Engine, C: Circuit<E>, S: SynthesisDriver>(
    circuit: &C,
    srs: &SRS<E>
) -> Result<Proof<E>, SynthesisError>
{
    struct Wires<E: Engine> {
        a: Vec<E::Fr>,
        b: Vec<E::Fr>,
        c: Vec<E::Fr>
    }

    impl<'a, E: Engine> Backend<E> for &'a mut Wires<E> {
        fn new_multiplication_gate(&mut self) {
            self.a.push(E::Fr::zero());
            self.b.push(E::Fr::zero());
            self.c.push(E::Fr::zero());
        }

        fn get_var(&self, variable: Variable) -> Option<E::Fr> {
            Some(match variable {
                Variable::A(index) => {
                    self.a[index - 1]
                },
                Variable::B(index) => {
                    self.b[index - 1]
                },
                Variable::C(index) => {
                    self.c[index - 1]
                }
            })
        }

        fn set_var<F>(&mut self, variable: Variable, value: F) -> Result<(), SynthesisError>
            where F: FnOnce() -> Result<E::Fr, SynthesisError>
        {
            let value = value()?;

            match variable {
                Variable::A(index) => {
                    self.a[index - 1] = value;
                },
                Variable::B(index) => {
                    self.b[index - 1] = value;
                },
                Variable::C(index) => {
                    self.c[index - 1] = value;
                }
            }

            Ok(())
        }
    }

    let mut wires = Wires {
        a: vec![],
        b: vec![],
        c: vec![],
    };

    S::synthesize(&mut wires, circuit)?;

    let n = wires.a.len();

    let mut transcript = Transcript::new(&[]);

    let r = multiexp(
        srs.g_positive_x_alpha[(srs.d - 3*n - 1)..].iter(),
        wires.c.iter().rev()
            .chain_ext(wires.b.iter().rev())
            .chain_ext(Some(E::Fr::zero()).iter())
            .chain_ext(wires.a.iter()),
    ).into_affine();

    transcript.commit_point(&r);

    let y: E::Fr = transcript.get_challenge_scalar();

    let mut rx1 = wires.b;
    rx1.extend(wires.c);
    rx1.reverse();
    rx1.push(E::Fr::zero());
    rx1.extend(wires.a);

    let mut rxy = rx1.clone();
    let y_inv = y.inverse().unwrap(); // TODO
    let mut tmp = y.pow(&[n as u64]);

    for rxy in rxy.iter_mut().rev() {
        rxy.mul_assign(&tmp);
        tmp.mul_assign(&y_inv);
    }

    let (s_poly_negative, s_poly_positive) = {
        let mut tmp = SxEval::new(y, n);
        S::synthesize(&mut tmp, circuit).unwrap(); // TODO

        tmp.poly()
    };

    let mut rxy_prime = rxy.clone();
    {
        rxy_prime.resize(4 * n + 1, E::Fr::zero());
        // Add s(x, y)
        for (r, s) in rxy_prime[0..(2 * n)]
            .iter_mut()
            .rev()
            .zip(s_poly_negative)
        {
            r.add_assign(&s);
        }
        for (r, s) in rxy_prime[(2 * n + 1)..].iter_mut().zip(s_poly_positive) {
            r.add_assign(&s);
        }
    }

    let mut txy = multiply_polynomials::<E>(rx1.clone(), rxy_prime);
    txy[4 * n] = E::Fr::zero(); // -k(y)

    let t = multiexp(
        srs.g_positive_x_alpha[0..(3 * n)]
            .iter()
            .chain_ext(srs.g_negative_x_alpha[0..(4 * n)].iter()),
        txy[(4 * n + 1)..]
            .iter()
            .chain_ext(txy[0..4 * n].iter().rev()),
    ).into_affine();

    transcript.commit_point(&t);

    let z: E::Fr = transcript.get_challenge_scalar();
    let z_inv = z.inverse().unwrap(); // TODO

    // TODO: use the faster way to evaluate the polynomials
    let mut rz = E::Fr::zero();
    {
        let mut tmp = z.pow(&[n as u64]);

        for coeff in rx1.iter().rev() {
            let mut coeff = *coeff;
            coeff.mul_assign(&tmp);
            rz.add_assign(&coeff);
            tmp.mul_assign(&z_inv);
        }
    }

    let mut rzy = E::Fr::zero();
    {
        let mut tmp = z.pow(&[n as u64]);

        for mut coeff in rxy.into_iter().rev() {
            coeff.mul_assign(&tmp);
            rzy.add_assign(&coeff);
            tmp.mul_assign(&z_inv);
        }
    }

    transcript.commit_scalar(&rz);
    transcript.commit_scalar(&rzy);

    let r1: E::Fr = transcript.get_challenge_scalar();

    let zy_opening = {
        // r(X, 1) - r(z, y)
        rx1[2 * n].sub_assign(&rzy);

        let mut point = y;
        point.mul_assign(&z);
        let poly = kate_divison(
            rx1.iter(),
            point,
        );

        let negative_poly = poly[0..2*n].iter().rev();
        let positive_poly = poly[2*n..].iter();
        multiexp(
            srs.g_negative_x[1..(negative_poly.len() + 1)].iter().chain_ext(
                srs.g_positive_x[0..positive_poly.len()].iter()
            ),
            negative_poly.chain_ext(positive_poly)
        ).into_affine()
    };

    let z_opening = {
        rx1[2 * n].add_assign(&rzy); // restore

        for (t, &r) in txy[2 * n..].iter_mut().zip(rx1.iter()) {
            let mut r = r;
            r.mul_assign(&r1);
            t.add_assign(&r);
        }

        let mut val = E::Fr::zero();
        {
            assert_eq!(txy.len(), 3*n + 1 + 4*n);
            let mut tmp = z.pow(&[(3*n) as u64]);

            for coeff in txy.iter().rev() {
                let mut coeff = *coeff;
                coeff.mul_assign(&tmp);
                val.add_assign(&coeff);
                tmp.mul_assign(&z_inv);
            }
        }

        txy[4 * n].sub_assign(&val);

        let poly = kate_divison(
            txy.iter(),
            z,
        );

        let negative_poly = poly[0..4*n].iter().rev();
        let positive_poly = poly[4*n..].iter();
        multiexp(
            srs.g_negative_x[1..(negative_poly.len() + 1)].iter().chain_ext(
                srs.g_positive_x[0..positive_poly.len()].iter()
            ),
            negative_poly.chain_ext(positive_poly)
        ).into_affine()
    };

    Ok(Proof {
        r, rz, rzy, t, z_opening, zy_opening
    })
}

#[test]
fn my_fun_circuit_test() {
    use ff::PrimeField;
    use pairing::bls12_381::{Bls12, Fr};
    use super::*;
    use crate::sonic::cs::{Basic, ConstraintSystem, LinearCombination};

    struct MyCircuit;

    impl<E: Engine> Circuit<E> for MyCircuit {
        fn synthesize<CS: ConstraintSystem<E>>(&self, cs: &mut CS) -> Result<(), SynthesisError> {
            let (a, b, _) = cs.multiply(|| {
                Ok((
                    E::Fr::from_str("10").unwrap(),
                    E::Fr::from_str("20").unwrap(),
                    E::Fr::from_str("200").unwrap(),
                ))
            })?;

            cs.enforce_zero(LinearCombination::from(a) + a - b);

            //let multiplier = cs.alloc_input(|| Ok(E::Fr::from_str("20").unwrap()))?;

            //cs.enforce_zero(LinearCombination::from(b) - multiplier);

            Ok(())
        }
    }

    let srs = SRS::<Bls12>::new(
        20,
        Fr::from_str("22222").unwrap(),
        Fr::from_str("33333333").unwrap(),
    );
    let proof = create_proof::<Bls12, _, Basic>(&MyCircuit, &srs).unwrap();

    use std::time::{Instant};
    let start = Instant::now();
    let mut batch = MultiVerifier::<Bls12, _, Basic>::new(MyCircuit, &srs).unwrap();

    for _ in 0..1 {
        batch.add_proof(&proof, &[/*Fr::from_str("20").unwrap()*/], |_, _| None);
    }

    assert!(batch.check_all());

    let elapsed = start.elapsed();
    println!("time to verify: {:?}", elapsed);
}
