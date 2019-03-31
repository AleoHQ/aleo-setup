# Description 

Initial SONIC proof system integration using the code from the [original implementation](https://github.com/zknuckles/sonic.git). It's here for experimental reasons and evaluation of the following properties:

- How applicable is "helped" procedure for a case of Ethereum
- What is a final verification cost for "helped" and "unhelped" procedures
- Prover efficiency in both cases
- Implementation of a memory constrained prover and helper
- Smart-contract implementation of verifiers
- Code cleanup
- Migration for smart-contract compatible transcripts

## TODO Plan

- [x] Test with public inputs
- [x] Test on BN256 
- [x] Parallelize using existing primitives
- [x] Implement polynomial parallelized evaluation
- [x] Make custom transcriptor that is easy to transform into the smart-contract
- [x] Basic Ethereum smart-contract
- [x] Add blinding factors
- [ ] Implement unhelped version
  - [x] Implement a part of S poly precomputation (S_2)
  - [x] Implement a "well formed" argument 
  - [x] Implement a coefficients product argument
  - [x] Implement a premutation argument
  - [ ] Implement synthesizer for proper form of S polynomial
- [ ] Finer tuning
  - [x] Parallelized evaluation of S polynomial
  - [ ] Parallelize some polynomial operations in the protocol itself
  - [x] Parallelized kate division  
  - [ ] Implement specialized version of polynomial multiplication by degree 1 polynomial
  - [ ] Non-assigning variant of the "adaptor" (may require a change of SONIC CS trait)