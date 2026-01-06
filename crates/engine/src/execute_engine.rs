//! Simplified execution engine for executing blocks by hash.
//!
//! This module provides a simplified interface for executing blocks:
//! 1. Accepts block hash
//! 2. Downloads block, witness, and bytecode asynchronously using EngineDownloader
//! 3. When all data is ready, executes block with full validation (similar to EngineTree::insert_block)
//! 4. Validates state root and sends result via channel

use crate::download::{DownloadData, DownloadOutcome, EngineDownloader};
use crate::tree::{BlockBuffer, BlockStatus, InsertPayloadOk};
use alloy_eips::BlockNumHash;
use alloy_primitives::{keccak256, map::{B256Map, B256HashSet}, B256};
use ress_network::RessNetworkHandle;
use ress_primitives::witness::ExecutionWitness;
use ress_provider::RessProvider;
use reth_chainspec::ChainSpec;
use reth_consensus::{Consensus, ConsensusError, FullConsensus, HeaderValidator};
use reth_engine_tree::tree::error::InsertBlockErrorKind;
use reth_errors::ProviderError;
use reth_node_ethereum::{consensus::EthBeaconConsensus, EthereumEngineValidator};
use reth_primitives::{Block, EthPrimitives, GotExpected, Receipt, RecoveredBlock, SealedBlock, SealedHeader};
use reth_provider::BlockExecutionOutput;
use reth_trie::{HashedPostState, KeccakKeyHasher};
use reth_trie_sparse::{
    provider::DefaultTrieNodeProviderFactory,
    SparseStateTrie,
};
use ress_evm::BlockExecutor;
use crate::tree::root::calculate_state_root;
use rayon::iter::IntoParallelRefIterator;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::oneshot;
use tracing::*;

/// Result of block execution
#[derive(Debug)]
pub struct ExecutionResult {
    /// The executed block
    pub block: RecoveredBlock<Block>,
    /// Calculated state root
    pub state_root: B256,
    /// Execution output
    pub output: BlockExecutionOutput<Receipt>,
    /// Insert result
    pub insert_result: InsertPayloadOk,
}

/// Error types for ExecuteEngine
#[derive(Debug, thiserror::Error)]
pub enum ExecuteEngineError {
    #[error("Block not found: {0}")]
    BlockNotFound(B256),
    #[error("Witness not found for block: {0}")]
    WitnessNotFound(B256),
    #[error("Parent block not found: {0}")]
    ParentNotFound(B256),
    #[error("Consensus error: {0}")]
    ConsensusError(#[from] ConsensusError),
    #[error("Execution error: {0}")]
    ExecutionError(String),
    #[error("State root mismatch: got {got}, expected {expected}")]
    StateRootMismatch { got: B256, expected: B256 },
    #[error("Trie error: {0}")]
    TrieError(String),
    #[error("Provider error: {0}")]
    ProviderError(#[from] ProviderError),
    #[error("Insert block error: {0:?}")]
    InsertBlockError(InsertBlockErrorKind),
}

impl From<InsertBlockErrorKind> for ExecuteEngineError {
    fn from(err: InsertBlockErrorKind) -> Self {
        ExecuteEngineError::InsertBlockError(err)
    }
}

/// Pending execution request
struct PendingExecution {
    /// Channel to send execution result
    result_tx: oneshot::Sender<Result<ExecutionResult, ExecuteEngineError>>,
}

/// Simplified execution engine for executing blocks by hash
#[allow(missing_debug_implementations)]
pub struct ExecuteEngine {
    provider: RessProvider,
    downloader: EngineDownloader,
    consensus: EthBeaconConsensus<ChainSpec>,
    engine_validator: EthereumEngineValidator,
    
    /// Block buffer for tracking downloaded blocks, witnesses, and bytecodes
    block_buffer: BlockBuffer<Block>,
    
    /// Pending execution requests by block hash
    pending_executions: HashMap<B256, PendingExecution>,
}

impl ExecuteEngine {
    /// Create new execution engine
    pub fn new(
        provider: RessProvider,
        network: RessNetworkHandle,
        consensus: EthBeaconConsensus<ChainSpec>,
        engine_validator: EthereumEngineValidator,
    ) -> Self {
        Self {
            provider,
            downloader: EngineDownloader::new(network, consensus.clone()),
            consensus,
            engine_validator,
            block_buffer: BlockBuffer::new(256),
            pending_executions: HashMap::new(),
        }
    }

    /// Start executing block by hash
    /// Returns a receiver channel that will receive the execution result when ready
    pub fn execute_block(
        &mut self,
        block_hash: B256,
    ) -> oneshot::Receiver<Result<ExecutionResult, ExecuteEngineError>> {
        let (tx, rx) = oneshot::channel();
        
        info!(target: "ress::execute_engine", %block_hash, "Starting block execution");

        // Check if block already exists
        if let Some(existing_block) = self.provider.recovered_block(&block_hash) {
            // Block exists, check if we have witness
            // For now, we'll still download witness to ensure we have the latest
            // In production, you might want to check if witness exists first
        }

        // Register pending execution
        self.pending_executions.insert(block_hash, PendingExecution { result_tx: tx });

        // Start downloading block and witness
        self.downloader.download_full_block(block_hash);
        self.downloader.download_witness(block_hash);

        rx
    }

    /// Handle download outcome (similar to ConsensusEngine::on_download_outcome)
    fn on_download_outcome(
        &mut self,
        outcome: DownloadOutcome,
    ) -> Result<(), ExecuteEngineError> {
        let elapsed = outcome.elapsed;
        let mut unlocked_block_hashes = B256HashSet::default();

        match outcome.data {
            DownloadData::FullBlock(block) => {
                let block_num_hash = block.num_hash();
                trace!(target: "ress::execute_engine", ?block_num_hash, ?elapsed, "Downloaded block");
                let recovered = match block.try_recover() {
                    Ok(block) => block,
                    Err(_error) => {
                        debug!(target: "ress::execute_engine", ?block_num_hash, "Error recovering downloaded block");
                        return Ok(());
                    }
                };
                self.block_buffer.insert_block(recovered);
                unlocked_block_hashes.insert(block_num_hash.hash);
            }
            DownloadData::Witness(block_hash, witness) => {
                let code_hashes = witness.bytecode_hashes().clone();
                let missing_code_hashes = self.provider.missing_code_hashes(code_hashes)
                    .map_err(|error| ExecuteEngineError::ProviderError(ProviderError::Database(error)))?;
                let missing_bytecodes_len = missing_code_hashes.len();
                let witness_nodes_count = witness.state_witness().len();

                trace!(
                    target: "ress::execute_engine",
                    %block_hash,
                    missing_bytecodes_len,
                    witness_nodes_count,
                    ?elapsed,
                    "Downloaded witness"
                );

                self.block_buffer.insert_witness(
                    block_hash,
                    witness,
                    missing_code_hashes.clone(),
                );

                if missing_code_hashes.is_empty() {
                    unlocked_block_hashes.insert(block_hash);
                } else {
                    // Download missing bytecodes
                    for code_hash in missing_code_hashes {
                        self.downloader.download_bytecode(code_hash);
                    }
                }
            }
            DownloadData::Bytecode(code_hash, bytecode) => {
                trace!(target: "ress::execute_engine", %code_hash, ?elapsed, "Downloaded bytecode");
                self.provider.insert_bytecode(code_hash, bytecode)
                    .map_err(|e| ExecuteEngineError::ProviderError(ProviderError::Database(e)))?;
                unlocked_block_hashes.extend(self.block_buffer.on_bytecode_received(code_hash));
            }
            DownloadData::FinalizedBlock(_, _) => {
                // Not used in simplified execution
            }
        }

        // Check if any pending blocks are ready for execution
        for unlocked_hash in unlocked_block_hashes {
            if let Some(pending) = self.pending_executions.remove(&unlocked_hash) {
                if let Some((block, witness)) = self.block_buffer.remove_block(&unlocked_hash) {
                    trace!(target: "ress::execute_engine", block = ?block.num_hash(), "Block ready for execution");
                    
                    // Execute block in background (spawn task)
                    let provider = self.provider.clone();
                    let consensus = self.consensus.clone();
                    let engine_validator = self.engine_validator.clone();
                    
                    tokio::spawn(async move {
                        let result = Self::execute_block_internal(
                            &provider,
                            &consensus,
                            &engine_validator,
                            block,
                            Some(witness),
                        );
                        let _ = pending.result_tx.send(result);
                    });
                } else {
                    // Block not in buffer, put pending back
                    self.pending_executions.insert(unlocked_hash, pending);
                }
            }
        }

        Ok(())
    }

    /// Execute block with full validation (similar to EngineTree::insert_block)
    fn execute_block_internal(
        provider: &RessProvider,
        consensus: &EthBeaconConsensus<ChainSpec>,
        _engine_validator: &EthereumEngineValidator,
        block: RecoveredBlock<Block>,
        maybe_witness: Option<ExecutionWitness>,
    ) -> Result<ExecutionResult, ExecuteEngineError> {
        let start = std::time::Instant::now();
        let block_num_hash = block.num_hash();
        debug!(
            target: "ress::execute_engine",
            block = ?block_num_hash,
            parent_hash = %block.parent_hash,
            state_root = %block.state_root,
            "Executing block"
        );

        // Check if block already exists
        if provider.sealed_header(block.hash_ref()).is_some() {
            return Ok(ExecutionResult {
                block: block.clone(),
                state_root: block.state_root,
                output: BlockExecutionOutput {
                    state: Default::default(),
                    result: Default::default(),
                },
                insert_result: InsertPayloadOk::AlreadySeen,
            });
        }

        // ===================== Validation =====================
        trace!(target: "ress::execute_engine", block = ?block_num_hash, "Validating block consensus");
        Self::validate_block(consensus, block.sealed_block())?;

        // Check parent exists
        let Some(parent) = provider.sealed_header(&block.parent_hash) else {
            return Err(ExecuteEngineError::ParentNotFound(block.parent_hash));
        };

        // Validate header against parent
        let parent_sealed = SealedHeader::new(parent, block.parent_hash);
        if let Err(error) = consensus.validate_header_against_parent(block.sealed_header(), &parent_sealed) {
            error!(target: "ress::execute_engine", %error, "Failed to validate header against parent");
            return Err(ExecuteEngineError::ConsensusError(error));
        }

        // ===================== Witness =====================
        let Some(execution_witness) = maybe_witness else {
            return Err(ExecuteEngineError::WitnessNotFound(block.hash()));
        };

        let mut trie = SparseStateTrie::default();
        let mut state_witness = B256Map::default();
        for encoded in execution_witness.state_witness() {
            state_witness.insert(keccak256(encoded), encoded.clone());
        }
        trie.reveal_witness(parent_sealed.state_root, &state_witness)
            .map_err(|error| ExecuteEngineError::TrieError(format!("Failed to reveal witness: {}", error)))?;

        // ===================== Execution =====================
        let start_time = std::time::Instant::now();
        let block_executor = BlockExecutor::new(
            provider.clone(),
            block.parent_num_hash(),
            &trie,
        );
        let output = block_executor.execute(&block)
            .map_err(|e| ExecuteEngineError::ExecutionError(format!("Block execution failed: {}", e)))?;
        debug!(
            target: "ress::execute_engine",
            block = ?block_num_hash,
            elapsed = ?start_time.elapsed(),
            "Executed block"
        );

        // ===================== Post Execution Validation =====================
        <EthBeaconConsensus<ChainSpec> as FullConsensus<EthPrimitives>>::validate_block_post_execution(
            consensus,
            &block,
            &output.result,
        )
        .map_err(ExecuteEngineError::ConsensusError)?;

        // ===================== State Root =====================
        let hashed_state = HashedPostState::from_bundle_state::<KeccakKeyHasher>(
            output.state.state.par_iter()
        );
        let provider_factory = DefaultTrieNodeProviderFactory;
        let state_root = calculate_state_root(&mut trie, hashed_state, &provider_factory)
            .map_err(|error| ExecuteEngineError::TrieError(format!("Failed to calculate state root: {}", error)))?;

        if state_root != block.state_root {
            return Err(ExecuteEngineError::StateRootMismatch {
                got: state_root,
                expected: block.state_root,
            });
        }

        // ===================== Update Node State =====================
        provider.insert_block(block.clone(), Some(execution_witness.into_state_witness()));

        debug!(
            target: "ress::execute_engine",
            block = ?block_num_hash,
            elapsed = ?start.elapsed(),
            "Finished executing block"
        );

        Ok(ExecutionResult {
            block,
            state_root,
            output,
            insert_result: InsertPayloadOk::Inserted(BlockStatus::Valid),
        })
    }

    /// Validate block (similar to EngineTree::validate_block)
    fn validate_block(consensus: &EthBeaconConsensus<ChainSpec>, block: &SealedBlock) -> Result<(), ConsensusError> {
        if let Err(error) = consensus.validate_header(block.sealed_header()) {
            error!(target: "ress::execute_engine", %error, "Failed to validate header");
            return Err(error);
        }

        if let Err(error) = consensus.validate_block_pre_execution(block) {
            error!(target: "ress::execute_engine", %error, "Failed to validate block");
            return Err(error);
        }

        Ok(())
    }
}

impl Future for ExecuteEngine {
    type Output = Result<(), ExecuteEngineError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Poll downloader for new outcomes
        loop {
            match self.downloader.poll(cx) {
                Poll::Ready(outcome) => {
                    if let Err(e) = self.on_download_outcome(outcome) {
                        return Poll::Ready(Err(e));
                    }
                    // Continue polling for more outcomes
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}
