/// Largeley this module is implementation of provable evaluation of s(z, y), that is represented in two parts
/// s2(X, Y) = \sum_{i=1}^{N} (Y^{-i} + Y^{i})X^{i}
/// s1(X, Y) = ...
/// s1 part requires grand product and permutation arguments, that are also implemented

mod s2_proof;
mod wellformed_argument;
mod grand_product_argument;

pub use self::wellformed_argument::{WellformednessArgument, WellformednessProof};