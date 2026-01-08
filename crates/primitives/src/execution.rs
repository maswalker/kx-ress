//! Execution result types.

use alloy_primitives::B256;
use crate::witness::ExecutionWitness;
use reth_primitives::{Block, Bytecode, RecoveredBlock};
use thiserror::Error;

/// Result of executing a block.
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub block: RecoveredBlock<Block>,
    pub parent_block: RecoveredBlock<Block>,
    pub witness: ExecutionWitness,
    pub bytecodes: Vec<(B256, Bytecode)>,
}

/// Error type for execution engine.
#[derive(Debug, Error)]
pub enum ExecuteEngineError {
    #[error("Task error: {0}")]
    TaskError(String),
}

impl From<&str> for ExecuteEngineError {
    fn from(s: &str) -> Self {
        ExecuteEngineError::TaskError(s.to_string())
    }
}

