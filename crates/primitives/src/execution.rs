//! Execution result types.

use alloy_primitives::B256;
use crate::witness::ExecutionWitness;
use reth_primitives::{Block, Bytecode, RecoveredBlock};
use thiserror::Error;

/// Result of executing blocks.
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    /// Blocks in the same order as requested
    pub blocks: Vec<RecoveredBlock<Block>>,
    /// Parent block of the first block
    pub parent_block: RecoveredBlock<Block>,
    /// Witnesses for each block (same order as blocks)
    pub witnesses: Vec<ExecutionWitness>,
    /// All bytecodes from all blocks (merged and deduplicated by code hash)
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

