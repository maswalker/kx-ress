//! Ress verifier for validating ExecutionResult without accessing engine cache.

mod kasplex;
mod provider;
mod verify;

pub use verify::{verify, VerificationError};
pub use kasplex::{get_vault_address, get_vault_address_from_spec, allow_equal_timestamp, validate_timestamp_for_kasplex, adjust_base_fee_for_kasplex_engine};

