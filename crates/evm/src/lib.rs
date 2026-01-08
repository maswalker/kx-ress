//! Ress evm implementation.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![allow(missing_docs, unused_crate_dependencies)]

mod db;
pub use db::WitnessDatabase;

mod executor;
pub use executor::BlockExecutor;
