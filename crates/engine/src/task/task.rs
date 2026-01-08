use alloy_primitives::{B256, BlockNumber};
use ress_primitives::witness::ExecutionWitness;
use reth_primitives::{Block, Bytecode, RecoveredBlock};
use reth_consensus::ConsensusError;
use reth_errors::ProviderError;
use std::time::Instant;
use tokio::sync::oneshot;
use thiserror::Error;

pub type TaskId = u64;

#[derive(Debug, Clone)]
pub struct TaskRequest {
    pub block_hash: B256,
    pub block_height: BlockNumber,
    pub parent_hash: B256,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskState {
    pub block_ready: bool,
    pub parent_block_ready: bool,
    pub witness_ready: bool,
}

#[derive(Debug)]
pub struct TaskResult {
    pub block: RecoveredBlock<Block>,
    pub parent_block: RecoveredBlock<Block>,
    pub witness: ExecutionWitness,
    pub bytecodes: Vec<(B256, Bytecode)>,
}

#[derive(Debug, Error)]
pub enum TaskError {
    #[error("Block not found: {0}")]
    BlockNotFound(B256),
    #[error("Witness not found for block: {0}")]
    WitnessNotFound(B256),
    #[error("Parent block not found: {0}")]
    ParentNotFound(B256),
    #[error("Bytecode not found: {0}")]
    BytecodeNotFound(B256),
    #[error("Consensus error: {0}")]
    ConsensusError(#[from] ConsensusError),
    #[error("Execution error: {0}")]
    ExecutionError(String),
    #[error("State root mismatch for block {block}: got {got}, expected {expected}")]
    StateRootMismatch { block: B256, got: B256, expected: B256 },
    #[error("Provider error: {0}")]
    ProviderError(#[from] ProviderError),
}

pub struct Task {
    pub id: TaskId,
    pub block_hash: B256,
    pub block_height: BlockNumber,
    pub parent_hash: B256,
    pub result_tx: oneshot::Sender<Result<TaskResult, TaskError>>,
    pub state: TaskState,
    pub created_at: Instant,
}

