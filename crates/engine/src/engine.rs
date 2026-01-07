use crate::download::{DownloadData, DownloadOutcome, EngineDownloader};
use crate::tree::{BlockStatus, InsertPayloadOk, EngineTree, TreeEvent, DownloadRequest};
use alloy_primitives::map::B256HashSet;
use alloy_primitives::B256;
use ress_network::RessNetworkHandle;
use ress_primitives::witness::ExecutionWitness;
use ress_provider::RessProvider;
use reth_chainspec::ChainSpec;
use reth_consensus::ConsensusError;
use reth_engine_tree::tree::error::{InsertBlockErrorKind, InsertBlockFatalError};
use alloy_rpc_types_engine::{PayloadStatus, PayloadStatusEnum};
use reth_errors::ProviderError;
use reth_node_ethereum::{consensus::EthBeaconConsensus, EthereumEngineValidator};
use reth_primitives::{Block, Receipt, RecoveredBlock};
use reth_provider::BlockExecutionOutput;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::{mpsc, oneshot};
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
    #[error("Fatal error during block insertion: {0}")]
    InsertBlockFatalError(#[from] InsertBlockFatalError),
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
    tree: EngineTree,
    downloader: EngineDownloader,
    
    /// Pending execution requests by block hash
    pending_executions: HashMap<B256, PendingExecution>,
}

impl ExecuteEngine {
    /// Create new execution engine
    pub fn new(
        provider: RessProvider,
        network: RessNetworkHandle,
        consensus: EthBeaconConsensus<ChainSpec>,
    ) -> Self {
        Self {
            tree: EngineTree::new(
                provider,
                consensus.clone(),
            ),
            downloader: EngineDownloader::new(network, consensus),
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
        if let Some(_existing_block) = self.tree.provider.recovered_block(&block_hash) {
            // Block exists, check if we have witness
            // For now, we'll still download witness to ensure we have the latest
            // In production, you might want to check if witness exists first
        }

        // Register pending execution
        self.pending_executions.insert(block_hash, PendingExecution { result_tx: tx });

        // Start downloading block and witness
        debug!(target: "ress::execute_engine", %block_hash, "Triggering download of block and witness");
        self.downloader.download_full_block(block_hash);
        self.downloader.download_witness(block_hash);
        debug!(target: "ress::execute_engine", %block_hash, "Download requests sent, waiting for data...");

        rx
    }

    /// Handle download outcome
    fn on_download_outcome(
        &mut self,
        outcome: DownloadOutcome,
    ) -> Result<(), ExecuteEngineError> {
        let elapsed = outcome.elapsed;
        let mut unlocked_block_hashes = B256HashSet::default();

        match outcome.data {
            DownloadData::FullBlock(block) => {
                let block_num_hash = block.num_hash();
                info!(target: "ress::execute_engine", ?block_num_hash, ?elapsed, "Downloaded block");
                let recovered = match block.try_recover() {
                    Ok(block) => block,
                    Err(_error) => {
                        debug!(target: "ress::execute_engine", ?block_num_hash, "Error recovering downloaded block");
                        return Ok(());
                    }
                };
                self.tree.block_buffer.insert_block(recovered);
                
                // Only unlock if witness is also ready and all bytecodes are downloaded
                if self.tree.block_buffer.witnesses.contains_key(&block_num_hash.hash) {
                    let missing_bytecodes = self.tree.block_buffer.missing_bytecodes
                        .get(&block_num_hash.hash)
                        .map(|s| s.is_empty())
                        .unwrap_or(true);
                    if missing_bytecodes {
                        info!(target: "ress::execute_engine", ?block_num_hash, "Block ready, witness ready, all bytecodes ready, unlocking");
                        unlocked_block_hashes.insert(block_num_hash.hash);
                    } else {
                        info!(target: "ress::execute_engine", ?block_num_hash, "Block ready, witness ready, waiting for bytecodes");
                    }
                } else {
                    info!(target: "ress::execute_engine", ?block_num_hash, "Block ready, waiting for witness");
                }
            }
            DownloadData::Witness(block_hash, witness) => {
                let code_hashes = witness.bytecode_hashes().clone();
                let missing_code_hashes = self.tree.provider.missing_code_hashes(code_hashes)
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

                self.tree.block_buffer.insert_witness(
                    block_hash,
                    witness,
                    missing_code_hashes.clone(),
                );

                if missing_code_hashes.is_empty() {
                    // Only unlock if block is also ready in buffer
                    // Block will be unlocked when it's downloaded
                    if self.tree.block_buffer.blocks.contains_key(&block_hash) {
                        info!(target: "ress::execute_engine", %block_hash, "Witness ready, block also ready, unlocking");
                        unlocked_block_hashes.insert(block_hash);
                    } else {
                        info!(target: "ress::execute_engine", %block_hash, "Witness ready, waiting for block");
                    }
                } else {
                    // Download missing bytecodes
                    for code_hash in missing_code_hashes {
                        self.downloader.download_bytecode(code_hash);
                    }
                }
            }
            DownloadData::Bytecode(code_hash, bytecode) => {
                trace!(target: "ress::execute_engine", %code_hash, ?elapsed, "Downloaded bytecode");
                self.tree.provider.insert_bytecode(code_hash, bytecode)
                    .map_err(|e| ExecuteEngineError::ProviderError(ProviderError::Database(e)))?;
                // on_bytecode_received returns block hashes that are now ready (all bytecodes downloaded)
                // But we still need to check if block and witness are both ready
                for block_hash in self.tree.block_buffer.on_bytecode_received(code_hash) {
                    if self.tree.block_buffer.blocks.contains_key(&block_hash) &&
                       self.tree.block_buffer.witnesses.contains_key(&block_hash) {
                        info!(target: "ress::execute_engine", %block_hash, "All bytecodes ready, block and witness ready, unlocking");
                        unlocked_block_hashes.insert(block_hash);
                    } else {
                        info!(target: "ress::execute_engine", %block_hash, "All bytecodes ready, but block or witness not ready yet");
                    }
                }
            }
            DownloadData::FinalizedBlock(_, _) => {
                // Not used in simplified execution
            }
        }

        // Check if any pending blocks are ready for execution
        // Use tree.on_downloaded_block to process downloaded blocks
        info!(target: "ress::execute_engine", unlocked_count = unlocked_block_hashes.len(), "Processing unlocked blocks");
        for unlocked_hash in unlocked_block_hashes {
            let Some((block, witness)) = self.tree.block_buffer.remove_block(&unlocked_hash) else {
                warn!(target: "ress::execute_engine", %unlocked_hash, "Block not found in buffer");
                continue;
            };
            let block_num_hash = block.num_hash();
            info!(target: "ress::execute_engine", block = ?block_num_hash, "Inserting block after download");
            
            // Check if this block is a pending execution
            let is_pending = self.pending_executions.contains_key(&unlocked_hash);
            
            // For pending blocks, connect children and trigger parent downloads
            // For non-pending blocks, don't connect children or download parents to avoid recursion
            let mut result = self
                .tree
                .on_downloaded_block_with_options(block.clone(), witness, is_pending)
                .map_err(ExecuteEngineError::InsertBlockFatalError);
            
            // Handle tree events and pending execution
            if let Some(pending) = self.pending_executions.remove(&unlocked_hash) {
                match result {
                    Ok(mut outcome) => {
                        // Handle tree events (e.g., download parent block, download witness)
                        self.on_maybe_tree_event(outcome.event.take());
                        
                        // Check payload status to determine if execution was successful
                        match outcome.outcome.status {
                            PayloadStatusEnum::Valid => {
                                // Success - construct ExecutionResult
                                // Note: output is set to Default::default() because tree.insert_block
                                // doesn't return the execution output. The execution was successful
                                // (indicated by PayloadStatusEnum::Valid) and state_root is correct.
                                let state_root = block.state_root;
                                let _ = pending.result_tx.send(Ok(ExecutionResult {
                                    block: block.clone(),
                                    state_root,
                                    output: BlockExecutionOutput {
                                        state: Default::default(),
                                        result: Default::default(),
                                    },
                                    insert_result: InsertPayloadOk::Inserted(BlockStatus::Valid),
                                }));
                            }
                            PayloadStatusEnum::Syncing => {
                                // Still syncing (missing witness or parent) - put pending back
                                // Tree events already triggered download of missing data
                                info!(target: "ress::execute_engine", block = ?block_num_hash, "Block still syncing, re-queuing pending execution");
                                self.pending_executions.insert(unlocked_hash, pending);
                            }
                            PayloadStatusEnum::Invalid { .. } => {
                                // Invalid - send error via channel
                                let _ = pending.result_tx.send(Err(ExecuteEngineError::ConsensusError(
                                    ConsensusError::Other("Block validation failed".into())
                                )));
                            }
                            _ => {
                                // Other status - put pending back
                                self.pending_executions.insert(unlocked_hash, pending);
                            }
                        }
                    }
                    Err(e) => {
                        error!(target: "ress::execute_engine", block = ?block_num_hash, %e, "Error inserting downloaded block");
                        let _ = pending.result_tx.send(Err(e));
                    }
                }
            } else {
                // No pending execution for this block
                // For non-pending blocks, we don't trigger parent downloads to avoid recursive downloading
                // Only buffer the block if it's disconnected, don't download its parent
                if let Ok(mut outcome) = result {
                    match outcome.outcome.status {
                        PayloadStatusEnum::Valid => {
                            // Block inserted successfully, but don't connect buffered children for non-pending blocks
                            // to avoid recursive insertion of ancestor blocks
                            info!(target: "ress::execute_engine", block = ?block_num_hash, "Non-pending block inserted successfully (skipping buffered children connection)");
                        }
                        PayloadStatusEnum::Syncing => {
                            // Block is disconnected, but we don't download parent for non-pending blocks
                            // Just ignore the tree event (parent download request)
                            if let Some(event) = outcome.event.take() {
                                info!(target: "ress::execute_engine", block = ?block_num_hash, ?event, "Non-pending block disconnected, not downloading parent to avoid recursion");
                            }
                        }
                        _ => {
                            // Other status, ignore tree events
                            if let Some(event) = outcome.event.take() {
                                info!(target: "ress::execute_engine", block = ?block_num_hash, ?event, "Non-pending block other status, ignoring tree event");
                            }
                        }
                    }
                }
            }
        }

        // After processing unlocked blocks, check if any pending executions are now ready
        // This handles the case where ancestor blocks were inserted, which may have connected
        // buffered blocks, making pending executions ready
        self.check_pending_executions_ready();

        Ok(())
    }

    /// Check if any pending executions are now ready (block, witness, and all bytecodes are ready)
    fn check_pending_executions_ready(&mut self) {
        let ready_hashes: Vec<B256> = self.pending_executions
            .keys()
            .filter(|&hash| {
                let has_block = self.tree.block_buffer.blocks.contains_key(hash);
                let has_witness = self.tree.block_buffer.witnesses.contains_key(hash);
                let has_all_bytecodes = self.tree.block_buffer.missing_bytecodes
                    .get(hash)
                    .map(|missing| missing.is_empty())
                    .unwrap_or(true);
                has_block && has_witness && has_all_bytecodes
            })
            .copied()
            .collect();

        if !ready_hashes.is_empty() {
            info!(target: "ress::execute_engine", ready_count = ready_hashes.len(), "Found ready pending executions after ancestor blocks inserted");
            for hash in ready_hashes {
                // Check if block is still in buffer (might have been removed by try_connect_buffered_blocks)
                let Some((block, witness)) = self.tree.block_buffer.remove_block(&hash) else {
                    // Block was already removed from buffer, likely by try_connect_buffered_blocks
                    // Check if it was successfully inserted into provider
                    if let Some(inserted_block) = self.tree.provider.recovered_block(&hash) {
                        info!(target: "ress::execute_engine", block = ?hash, "Block was already inserted by try_connect_buffered_blocks, marking as successful");
                        if let Some(pending) = self.pending_executions.remove(&hash) {
                            let state_root = inserted_block.state_root;
                            let _ = pending.result_tx.send(Ok(ExecutionResult {
                                block: inserted_block,
                                state_root,
                                output: BlockExecutionOutput {
                                    state: Default::default(),
                                    result: Default::default(),
                                },
                                insert_result: InsertPayloadOk::Inserted(BlockStatus::Valid),
                            }));
                        }
                    } else {
                        warn!(target: "ress::execute_engine", block = ?hash, "Block not in buffer and not in provider, skipping");
                    }
                    continue;
                };
                
                let block_num_hash = block.num_hash();
                let parent_hash = block.parent_hash;
                let parent_exists = self.tree.provider.sealed_header(&parent_hash).is_some();
                info!(target: "ress::execute_engine", block = ?block_num_hash, parent_hash = %parent_hash, parent_exists = parent_exists, "Re-processing pending execution after ancestor blocks ready");
                
                // This is a pending execution, so we should connect children and download parents
                let mut result = self
                    .tree
                    .on_downloaded_block_with_options(block.clone(), witness, true)
                    .map_err(ExecuteEngineError::InsertBlockFatalError);
                
                if let Some(pending) = self.pending_executions.remove(&hash) {
                    match result {
                        Ok(mut outcome) => {
                            info!(target: "ress::execute_engine", block = ?block_num_hash, status = ?outcome.outcome.status, "Re-processed pending execution result");
                            self.on_maybe_tree_event(outcome.event.take());
                            
                            match outcome.outcome.status {
                                PayloadStatusEnum::Valid => {
                                    info!(target: "ress::execute_engine", block = ?block_num_hash, "Pending execution now successful, sending result");
                                    let state_root = block.state_root;
                                    let _ = pending.result_tx.send(Ok(ExecutionResult {
                                        block: block.clone(),
                                        state_root,
                                        output: BlockExecutionOutput {
                                            state: Default::default(),
                                            result: Default::default(),
                                        },
                                        insert_result: InsertPayloadOk::Inserted(BlockStatus::Valid),
                                    }));
                                }
                                PayloadStatusEnum::Syncing => {
                                    info!(target: "ress::execute_engine", block = ?block_num_hash, parent_hash = %parent_hash, "Pending execution still syncing, re-queuing. Parent exists: {}", self.tree.provider.sealed_header(&parent_hash).is_some());
                                    self.pending_executions.insert(hash, pending);
                                }
                                PayloadStatusEnum::Invalid { .. } => {
                                    error!(target: "ress::execute_engine", block = ?block_num_hash, "Pending execution failed validation");
                                    let _ = pending.result_tx.send(Err(ExecuteEngineError::ConsensusError(
                                        ConsensusError::Other("Block validation failed".into())
                                    )));
                                }
                                _ => {
                                    info!(target: "ress::execute_engine", block = ?block_num_hash, status = ?outcome.outcome.status, "Pending execution other status, re-queuing");
                                    self.pending_executions.insert(hash, pending);
                                }
                            }
                        }
                        Err(e) => {
                            error!(target: "ress::execute_engine", block = ?block_num_hash, %e, "Error re-processing pending execution");
                            let _ = pending.result_tx.send(Err(e));
                        }
                    }
                }
            }
        }
    }

    /// Handle tree events
    fn on_maybe_tree_event(&mut self, maybe_event: Option<TreeEvent>) {
        if let Some(event) = maybe_event {
            info!(target: "ress::execute_engine", ?event, "Handling tree event");
            self.on_tree_event(event);
        }
    }

        /// Handle tree events
        fn on_tree_event(&mut self, event: TreeEvent) {
            match event {
                TreeEvent::Download(DownloadRequest::Block { block_hash }) => {
                    // Skip zero hash (invalid block hash, likely from genesis block's parent)
                    if block_hash.is_zero() {
                        warn!(target: "ress::execute_engine", "Tree event: skipping zero hash block download");
                        return;
                    }
                    info!(target: "ress::execute_engine", %block_hash, "Tree event: downloading block");
                    self.downloader.download_full_block(block_hash);
                    if !self.tree.block_buffer.witnesses.contains_key(&block_hash) {
                        info!(target: "ress::execute_engine", %block_hash, "Tree event: also downloading witness");
                        self.downloader.download_witness(block_hash);
                    }
                }
            TreeEvent::Download(DownloadRequest::Witness { block_hash }) => {
                info!(target: "ress::execute_engine", %block_hash, "Tree event: downloading witness");
                self.downloader.download_witness(block_hash);
            }
            TreeEvent::Download(DownloadRequest::Finalized { block_hash }) => {
                info!(target: "ress::execute_engine", %block_hash, "Tree event: downloading finalized block with ancestors");
                self.downloader.download_finalized_with_ancestors(block_hash);
            }
            TreeEvent::TreeAction(_) => {
                // Not used in ExecuteEngine
            }
        }
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

