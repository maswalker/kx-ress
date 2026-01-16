use alloy_primitives::{B256, BlockNumber};
use ress_primitives::witness::ExecutionWitness;
use reth_primitives::{Block, Bytecode, RecoveredBlock};
use reth_consensus::ConsensusError;
use reth_errors::ProviderError;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::oneshot;
use thiserror::Error;

pub type TaskId = u64;

#[derive(Debug, Clone)]
pub struct TaskRequest {
    /// Parent hash of the first block in the batch
    pub parent_hash: B256,
    /// Block hashes to process (for single block, this is a vec with one element)
    pub block_hashes: Vec<B256>,
    /// Starting block height (first block's height)
    pub start_block_height: BlockNumber,
    /// Whether to download witness for each block (must have same length as block_hashes)
    pub download_witnesses: Vec<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockState {
    pub block_ready: bool,
    pub witness_ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskState {
    pub parent_block_ready: bool,
    /// State for each block in the task, indexed by block hash
    pub block_states: HashMap<B256, BlockState>,
}

#[derive(Debug)]
pub struct TaskResult {
    /// Blocks in the same order as block_hashes in the request
    pub blocks: Vec<RecoveredBlock<Block>>,
    /// Parent block of the first block
    pub parent_block: RecoveredBlock<Block>,
    /// Witnesses for each block (same order as blocks)
    pub witnesses: Vec<ExecutionWitness>,
    /// All bytecodes from all blocks (merged and deduplicated by code hash)
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
    pub parent_hash: B256,
    pub block_hashes: Vec<B256>,
    pub start_block_height: BlockNumber,
    pub download_witnesses: Vec<bool>,
    pub result_tx: oneshot::Sender<Result<TaskResult, TaskError>>,
    pub state: TaskState,
    pub created_at: Instant,
}

