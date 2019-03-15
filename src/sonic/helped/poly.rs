use pairing::ff::{Field};
use pairing::{Engine, CurveProjective};
use std::marker::PhantomData;

use crate::sonic::cs::{Backend};
use crate::sonic::cs::{Coeff, Variable, LinearCombination};
use crate::sonic::util::*;

/*
s(X, Y) =   \sum\limits_{i=1}^N u_i(Y) X^{-i}
          + \sum\limits_{i=1}^N v_i(Y) X^{i}
          + \sum\limits_{i=1}^N w_i(Y) X^{i+N}

where

    u_i(Y) =        \sum\limits_{q=1}^Q Y^{q+N} u_{i,q}
    v_i(Y) =        \sum\limits_{q=1}^Q Y^{q+N} v_{i,q}
    w_i(Y) = -Y^i + -Y^{-i} + \sum\limits_{q=1}^Q Y^{q+N} w_{i,q}

*/
#[derive(Clone)]
pub struct SxEval<E: Engine> {
    y: E::Fr,

    // current value of y^{q+N}
    yqn: E::Fr,

    // x^{-i} (\sum\limits_{q=1}^Q y^{q+N} u_{q,i})
    u: Vec<E::Fr>,
    // x^{i} (\sum\limits_{q=1}^Q y^{q+N} v_{q,i})
    v: Vec<E::Fr>,
    // x^{i+N} (-y^i -y^{-i} + \sum\limits_{q=1}^Q y^{q+N} w_{q,i})
    w: Vec<E::Fr>,
}

impl<E: Engine> SxEval<E> {
    pub fn new(y: E::Fr, n: usize) -> Self {
        let y_inv = y.inverse().unwrap(); // TODO

        let yqn = y.pow(&[n as u64]);

        let u = vec![E::Fr::zero(); n];
        let v = vec![E::Fr::zero(); n];
        let mut w = vec![E::Fr::zero(); n];

        let mut tmp1 = y;
        let mut tmp2 = y_inv;
        for w in &mut w {
            let mut new = tmp1;
            new.add_assign(&tmp2);
            new.negate();
            *w = new;
            tmp1.mul_assign(&y);
            tmp2.mul_assign(&y_inv);
        }

        SxEval {
            y,
            yqn,
            u,
            v,
            w,
        }
    }

    pub fn poly(mut self) -> (Vec<E::Fr>, Vec<E::Fr>) {
        self.v.extend(self.w);

        (self.u, self.v)
    }

    pub fn finalize(self, x: E::Fr) -> E::Fr {
        let x_inv = x.inverse().unwrap(); // TODO

        let mut acc = E::Fr::zero();

        let tmp = x_inv;
        acc.add_assign(&evaluate_at_consequitive_powers(& self.u[..], tmp, tmp));
        let tmp = x;
        acc.add_assign(&evaluate_at_consequitive_powers(& self.v[..], tmp, tmp));
        let tmp = x.pow(&[(self.v.len()+1) as u64]);
        acc.add_assign(&evaluate_at_consequitive_powers(& self.w[..], tmp, x));

        // let mut tmp = x_inv;
        // for mut u in self.u {
        //     u.mul_assign(&tmp);
        //     acc.add_assign(&u);
        //     tmp.mul_assign(&x_inv);
        // }

        // let mut tmp = x;
        // for mut v in self.v {
        //     v.mul_assign(&tmp);
        //     acc.add_assign(&v);
        //     tmp.mul_assign(&x);
        // }
        // for mut w in self.w {
        //     w.mul_assign(&tmp);
        //     acc.add_assign(&w);
        //     tmp.mul_assign(&x);
        // }

        acc
    }
}

impl<'a, E: Engine> Backend<E> for &'a mut SxEval<E> {
    fn new_linear_constraint(&mut self) {
        self.yqn.mul_assign(&self.y);
    }

    fn insert_coefficient(&mut self, var: Variable, coeff: Coeff<E>) {
        let acc = match var {
            Variable::A(index) => {
                &mut self.u[index - 1]
            }
            Variable::B(index) => {
                &mut self.v[index - 1]
            }
            Variable::C(index) => {
                &mut self.w[index - 1]
            }
        };

        match coeff {
            Coeff::Zero => { },
            Coeff::One => {
                acc.add_assign(&self.yqn);
            },
            Coeff::NegativeOne => {
                acc.sub_assign(&self.yqn);
            },
            Coeff::Full(mut val) => {
                val.mul_assign(&self.yqn);
                acc.add_assign(&val);
            }
        }
    }
}

/*
s(X, Y) =   \sum\limits_{i=1}^N \sum\limits_{q=1}^Q Y^{q+N} u_{i,q} X^{-i}
          + \sum\limits_{i=1}^N \sum\limits_{q=1}^Q Y^{q+N} v_{i,q} X^{i}
          + \sum\limits_{i=1}^N \sum\limits_{q=1}^Q Y^{q+N} w_{i,q} X^{i+N}
          - \sum\limits_{i=1}^N Y^i X^{i+N}
          - \sum\limits_{i=1}^N Y^{-i} X^{i+N}
*/
pub struct SyEval<E: Engine> {
    max_n: usize,
    current_q: usize,

    // x^{-1}, ..., x^{-N}
    a: Vec<E::Fr>,

    // x^1, ..., x^{N}
    b: Vec<E::Fr>,

    // x^{N+1}, ..., x^{2*N}
    c: Vec<E::Fr>,

    // coeffs for y^1, ..., y^{N+Q}
    positive_coeffs: Vec<E::Fr>,

    // coeffs for y^{-1}, y^{-2}, ..., y^{-N}
    negative_coeffs: Vec<E::Fr>,
}


impl<E: Engine> SyEval<E> {
    pub fn new(x: E::Fr, n: usize, q: usize) -> Self {
        let xinv = x.inverse().unwrap();
        let mut tmp = E::Fr::one();
        let mut a = vec![E::Fr::zero(); n];
        for a in &mut a {
            tmp.mul_assign(&xinv); // tmp = x^{-i}
            *a = tmp;
        }

        let mut tmp = E::Fr::one();
        let mut b = vec![E::Fr::zero(); n];
        for b in &mut b {
            tmp.mul_assign(&x); // tmp = x^{i}
            *b = tmp;
        }

        let mut positive_coeffs = vec![E::Fr::zero(); n + q];
        let mut negative_coeffs = vec![E::Fr::zero(); n];

        let mut c = vec![E::Fr::zero(); n];
        for ((c, positive_coeff), negative_coeff) in c.iter_mut().zip(&mut positive_coeffs).zip(&mut negative_coeffs) {
            tmp.mul_assign(&x); // tmp = x^{i+N}
            *c = tmp;

            // - \sum\limits_{i=1}^N Y^i X^{i+N}
            let mut tmp = tmp;
            tmp.negate();
            *positive_coeff = tmp;

            // - \sum\limits_{i=1}^N Y^{-i} X^{i+N}
            *negative_coeff = tmp;
        }

        SyEval {
            a,
            b,
            c,
            positive_coeffs,
            negative_coeffs,
            current_q: 0,
            max_n: n,
        }
    }

    pub fn poly(self) -> (Vec<E::Fr>, Vec<E::Fr>) {
        (self.negative_coeffs, self.positive_coeffs)
    }

    pub fn finalize(self, y: E::Fr) -> E::Fr {
        let mut acc = E::Fr::zero();
        let yinv = y.inverse().unwrap(); // TODO

        let positive_powers_contrib = evaluate_at_consequitive_powers(& self.positive_coeffs[..], y, y);
        let negative_powers_contrib = evaluate_at_consequitive_powers(& self.negative_coeffs[..], yinv, yinv);
        acc.add_assign(&positive_powers_contrib);
        acc.add_assign(&negative_powers_contrib);

        // let mut tmp = y;
        // for mut coeff in self.positive_coeffs {
        //     coeff.mul_assign(&tmp);
        //     acc.add_assign(&coeff);
        //     tmp.mul_assign(&y);
        // }

        // let mut tmp = yinv;
        // for mut coeff in self.negative_coeffs {
        //     coeff.mul_assign(&tmp);
        //     acc.add_assign(&coeff);
        //     tmp.mul_assign(&yinv);
        // }

        acc
    }
}

impl<'a, E: Engine> Backend<E> for &'a mut SyEval<E> {
    fn new_linear_constraint(&mut self) {
        self.current_q += 1;
    }

    fn insert_coefficient(&mut self, var: Variable, coeff: Coeff<E>) {
        match var {
            Variable::A(index) => {
                let index = index - 1;
                // Y^{q+N} += X^{-i} * coeff
                let mut tmp = self.a[index];
                coeff.multiply(&mut tmp);
                let yindex = self.current_q + self.max_n;
                self.positive_coeffs[yindex - 1].add_assign(&tmp);
            }
            Variable::B(index) => {
                let index = index - 1;
                // Y^{q+N} += X^{i} * coeff
                let mut tmp = self.b[index];
                coeff.multiply(&mut tmp);
                let yindex = self.current_q + self.max_n;
                self.positive_coeffs[yindex - 1].add_assign(&tmp);
            }
            Variable::C(index) => {
                let index = index - 1;
                // Y^{q+N} += X^{i+N} * coeff
                let mut tmp = self.c[index];
                coeff.multiply(&mut tmp);
                let yindex = self.current_q + self.max_n;
                self.positive_coeffs[yindex - 1].add_assign(&tmp);
            }
        };
    }
}