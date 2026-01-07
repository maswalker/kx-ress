#![allow(dead_code, unused_imports)]

use alloy_eips::BlockNumHash;
use alloy_primitives::{keccak256, map::B256Map, B256, U256};
use alloy_rpc_types_engine::{
    PayloadStatus, PayloadStatusEnum, PayloadValidationError,
};
use metrics::Gauge;
use rayon::iter::IntoParallelRefIterator;
use ress_evm::BlockExecutor;
use ress_primitives::witness::ExecutionWitness;
use ress_provider::RessProvider;
use reth_chain_state::ExecutedBlock;
use reth_chainspec::ChainSpec;
use reth_consensus::{Consensus, ConsensusError, FullConsensus, HeaderValidator};
use reth_engine_tree::tree::{
    error::{InsertBlockError, InsertBlockErrorKind, InsertBlockFatalError},
    EngineValidator, InvalidHeaderCache,
};
use reth_errors::ProviderError;
use reth_ethereum_engine_primitives::{EthEngineTypes, EthPayloadBuilderAttributes};
use reth_metrics::Metrics;
use reth_node_api::PayloadBuilderAttributes;
use reth_node_ethereum::consensus::EthBeaconConsensus;
use reth_primitives::{Block, EthPrimitives, GotExpected, Header, RecoveredBlock, SealedBlock};
use reth_primitives_traits::SealedHeader;
use reth_provider::ExecutionOutcome;
use reth_trie::{HashedPostState, KeccakKeyHasher};
use reth_trie_sparse::{
    provider::DefaultTrieNodeProviderFactory,
    SparseStateTrie,
};
use std::{sync::Arc, time::Instant};
use tracing::*;

mod outcome;
pub use outcome::*;

mod block_buffer;
pub use block_buffer::BlockBuffer;

/// State root computation.
pub mod root;
use root::calculate_state_root;

/// Consensus engine tree for storing and validating blocks as well as advancing the chain.
#[derive(Debug)]
pub struct EngineTree {
    /// Ress provider.
    pub(crate) provider: RessProvider,
    /// Consensus.
    pub(crate) consensus: EthBeaconConsensus<ChainSpec>,
    /// Pending block buffer.
    pub(crate) block_buffer: BlockBuffer<Block>,
    /// Invalid headers.
    pub(crate) invalid_headers: InvalidHeaderCache,
    /// Metrics.
    metrics: EngineTreeMetrics,
}

impl EngineTree {
    /// Create new engine tree.
    pub fn new(
        provider: RessProvider,
        consensus: EthBeaconConsensus<ChainSpec>,
    ) -> Self {
        Self {
            provider,
            consensus,
            block_buffer: BlockBuffer::new(256),
            invalid_headers: InvalidHeaderCache::new(256),
            metrics: EngineTreeMetrics::default(),
        }
    }

    /// Returns true if the given hash is the last received sync target block.
    ///
    /// Always returns false since we don't track forkchoice state.
    fn is_sync_target_head(&self, _block_hash: B256) -> bool {
        false
    }

    /// Returns true if the block is either persisted or buffered.
    fn is_block_persisted_or_buffered(&self, block_hash: &B256) -> bool {
        self.provider.sealed_header(block_hash).is_some() ||
            self.block_buffer.blocks.contains_key(block_hash)
    }

    /// Determines if the given block is part of a fork.
    /// Always returns true since we don't track canonical head.
    fn is_fork(&self, _target_hash: B256) -> bool {
        true
    }

    /// Get canonical head from genesis header.
    fn canonical_head(&self) -> BlockNumHash {
        self.provider.chain_spec().genesis_header().num_hash_slow()
    }


    /// Invoked with a block downloaded from the network
    ///
    /// Returns an event with the appropriate action to take, such as:
    ///  - download more missing blocks
    ///  - try to canonicalize the target if the `block` is the tracked target (head) block.
    pub fn on_downloaded_block(
        &mut self,
        block: RecoveredBlock<Block>,
        witness: ExecutionWitness,
    ) -> Result<TreeOutcome<PayloadStatus>, InsertBlockFatalError> {
        self.on_downloaded_block_with_options(block, witness, true)
    }

    /// Handle downloaded block with options
    /// `should_connect_children`: if true, try to connect buffered children after insertion
    /// `should_download_parent`: if true, trigger parent download when disconnected
    pub fn on_downloaded_block_with_options(
        &mut self,
        block: RecoveredBlock<Block>,
        witness: ExecutionWitness,
        should_connect_children: bool,
    ) -> Result<TreeOutcome<PayloadStatus>, InsertBlockFatalError> {
        let block_num_hash = block.num_hash();
        // Skip lowest_buffered_ancestor check to avoid recursive downloading
        // We only need to verify the current block, not the entire chain
        // let lowest_buffered_ancestor = self.lowest_buffered_ancestor_or(block_num_hash.hash);
        // if let Some(status) =
        //     self.check_invalid_ancestor_with_head(lowest_buffered_ancestor, block_num_hash.hash)
        // {
        //     return Ok(TreeOutcome::new(status))
        // }

        // try to append the block
        let block_hash = block.hash();
        let mut maybe_tree_event = None;
        let status = match self.insert_block(block.clone(), Some(witness)) {
            Ok(InsertPayloadOk::Inserted(BlockStatus::Valid)) => {
                if should_connect_children {
                    info!(target: "ress::engine", block = ?block_num_hash, "Block inserted successfully, trying to connect buffered children");
                    let connected_count = self.try_connect_buffered_blocks(block_num_hash.hash)?;
                    info!(target: "ress::engine", block = ?block_num_hash, connected_children = connected_count, "Connected buffered children");
                } else {
                    info!(target: "ress::engine", block = ?block_num_hash, "Block inserted successfully (skipping buffered children connection)");
                }
                PayloadStatus::new(PayloadStatusEnum::Valid, Some(block_hash))
            }
            Ok(InsertPayloadOk::AlreadySeen) => {
                trace!(target: "ress::engine", "Downloaded block already executed");
                PayloadStatus::new(PayloadStatusEnum::Valid, Some(block_hash))
            }
            Ok(InsertPayloadOk::Inserted(BlockStatus::Disconnected {
                missing_ancestor, ..
            })) => {
                // block is not connected to the canonical head
                // Only trigger parent download if should_connect_children is true (i.e., for pending blocks)
                if should_connect_children {
                    info!(target: "ress::engine", block = ?block_num_hash, missing_ancestor = ?missing_ancestor, "Block disconnected, downloading direct parent only");
                    maybe_tree_event = Some(TreeEvent::download_block(missing_ancestor.hash));
                } else {
                    info!(target: "ress::engine", block = ?block_num_hash, missing_ancestor = ?missing_ancestor, "Block disconnected, but not downloading parent (non-pending block)");
                }
                PayloadStatus::new(PayloadStatusEnum::Syncing, None)
            }
            Ok(InsertPayloadOk::Inserted(BlockStatus::NoWitness)) => {
                // we don't have a witness to validate the payload
                maybe_tree_event = Some(TreeEvent::download_witness(block_hash));
                PayloadStatus::new(PayloadStatusEnum::Syncing, None)
            }
            Err(kind) => {
                debug!(target: "ress::engine", error = %kind, "Failed to insert downloaded block");
                self.on_insert_block_error(InsertBlockError::new(block.into_sealed_block(), kind))?
            }
        };

        let mut outcome = TreeOutcome::new(status);
        if let Some(event) = maybe_tree_event {
            outcome = outcome.with_event(event);
        }
        Ok(outcome)
    }

    /// Attempts to connect any buffered blocks that are connected to the given parent hash.
    /// Returns the number of blocks that were successfully connected.
    fn try_connect_buffered_blocks(
        &mut self,
        parent_hash: B256,
    ) -> Result<usize, InsertBlockFatalError> {
        info!(target: "ress::engine", %parent_hash, "Looking for buffered children of parent");
        let blocks = self.block_buffer.remove_block_with_children(parent_hash);

        if blocks.is_empty() {
            // nothing to append
            info!(target: "ress::engine", %parent_hash, "No buffered children to connect");
            return Ok(0)
        }
        info!(target: "ress::engine", %parent_hash, children_count = blocks.len(), "Found buffered children to connect");

        let now = Instant::now();
        let block_count = blocks.len();
        info!(target: "ress::engine", %parent_hash, children_count = block_count, "Attempting to connect buffered children");
        let mut connected_count = 0;
        for (child, witness) in blocks {
            let child_num_hash = child.num_hash();
            info!(target: "ress::engine", child = ?child_num_hash, "Trying to connect buffered child block");
            // Clone witness before moving it into insert_block, in case we need to re-buffer
            let witness_clone = witness.clone();
            match self.insert_block(child.clone(), Some(witness)) {
                Ok(InsertPayloadOk::Inserted(BlockStatus::Valid)) => {
                    info!(target: "ress::engine", child = ?child_num_hash, "Successfully connected buffered child block");
                    connected_count += 1;
                    // Recursively try to connect children of this child
                    match self.try_connect_buffered_blocks(child_num_hash.hash) {
                        Ok(additional_connected) => {
                            connected_count += additional_connected;
                        }
                        Err(e) => {
                            warn!(target: "ress::engine", child = ?child_num_hash, %e, "Error connecting children of connected block");
                            return Err(e);
                        }
                    }
                }
                Ok(InsertPayloadOk::Inserted(BlockStatus::Disconnected { missing_ancestor, .. })) => {
                    info!(target: "ress::engine", child = ?child_num_hash, ?missing_ancestor, "Buffered child block still disconnected (parent's parent missing, but we only download direct parent)");
                    // Note: insert_block already re-buffered the block and witness when parent was missing
                    // We don't trigger download of parent's parent to avoid recursive downloading
                }
                Ok(InsertPayloadOk::Inserted(BlockStatus::NoWitness)) => {
                    info!(target: "ress::engine", child = ?child_num_hash, "Buffered child block missing witness, re-buffering");
                    // Re-buffer the block since it's missing witness
                    self.block_buffer.insert_block(child);
                    // Note: witness is not re-buffered since the block status indicates NoWitness
                }
                Ok(InsertPayloadOk::AlreadySeen) => {
                    info!(target: "ress::engine", child = ?child_num_hash, "Buffered child block already seen");
                    connected_count += 1;
                }
                Err(kind) => {
                    error!(target: "ress::engine", child = ?child_num_hash, error = ?kind, "Failed to connect buffered child block");
                    if let Err(fatal) = self.on_insert_block_error(InsertBlockError::new(
                        child.into_sealed_block(),
                        kind,
                    )) {
                        warn!(target: "ress::engine", %fatal, "Fatal error occurred while connecting buffered blocks");
                        return Err(fatal)
                    }
                }
            }
        }

        info!(target: "ress::engine", %parent_hash, elapsed = ?now.elapsed(), connected = connected_count, total = block_count, "Finished connecting buffered blocks");
        Ok(connected_count)
    }

    /// Insert block into the tree.
    pub fn insert_block(
        &mut self,
        block: RecoveredBlock<Block>,
        maybe_witness: Option<ExecutionWitness>,
    ) -> Result<InsertPayloadOk, InsertBlockErrorKind> {
        let start = Instant::now();
        let block_num_hash = block.num_hash();
        debug!(target: "ress::engine", block=?block_num_hash, parent_hash = %block.parent_hash, state_root = %block.state_root, "Inserting new block into tree");

        if self.provider.sealed_header(block.hash_ref()).is_some() {
            return Ok(InsertPayloadOk::AlreadySeen);
        }

        trace!(target: "ress::engine", block=?block_num_hash, "Validating block consensus");
        self.validate_block(&block)?;

        let parent_hash = block.parent_hash;
        info!(target: "ress::engine", block=?block_num_hash, parent_hash=%parent_hash, "Checking if parent exists in provider or buffer");
        
        // Try to get parent from provider first
        let (parent_header, parent_state_root, parent_num) = if let Some(parent) = self.provider.sealed_header(&parent_hash) {
            let parent_num = parent.number;
            let parent_state_root = parent.state_root;
            info!(target: "ress::engine", block=?block_num_hash, parent=?BlockNumHash::new(parent_num, parent_hash), "Parent found in provider");
            (Some(parent), parent_state_root, parent_num)
        } else if let Some(parent_block) = self.block_buffer.blocks.get(&parent_hash) {
            // Parent is in buffer, use its state_root and number
            let parent_num = parent_block.number;
            let parent_state_root = parent_block.state_root;
            info!(target: "ress::engine", block=?block_num_hash, parent=?BlockNumHash::new(parent_num, parent_hash), "Parent found in buffer, using its state_root");
            (None, parent_state_root, parent_num)
        } else {
            // Parent not found, need to download it
            // Only download the direct parent, not the lowest ancestor
            // This prevents recursive downloading all the way to genesis
            let missing_ancestor = block.parent_num_hash();
            info!(target: "ress::engine", block=?block_num_hash, parent_hash=%parent_hash, ?missing_ancestor, has_witness=maybe_witness.is_some(), "Block has missing parent, re-buffering (only downloading direct parent)");
            if let Some(witness) = maybe_witness {
                self.block_buffer.insert_witness(block.hash(), witness, Default::default());
            }
            self.block_buffer.insert_block(block);
            return Ok(InsertPayloadOk::Inserted(BlockStatus::Disconnected {
                head: self.canonical_head(),
                missing_ancestor,
            }));
        };
        
        let parent_num_hash = BlockNumHash::new(parent_num, parent_hash);
        info!(target: "ress::engine", block=?block_num_hash, parent=?parent_num_hash, "Proceeding with execution (timestamp validation skipped)");
        
        // Skip timestamp validation against parent as requested
        // Note: We still need parent's state_root for witness revelation and parent_num_hash for execution
        // if let Some(parent_header) = parent_header {
        //     let parent = SealedHeader::new(parent_header, block.parent_hash);
        //     if let Err(error) =
        //         self.consensus.validate_header_against_parent(block.sealed_header(), &parent)
        //     {
        //         error!(target: "ress::engine", %error, "Failed to validate header against parent");
        //         return Err(error.into());
        //     }
        // }

        // ===================== Witness =====================
        let Some(execution_witness) = maybe_witness else {
            self.block_buffer.insert_block(block);
            trace!(target: "ress::engine", block = ?block_num_hash, "Block has missing witness");
            return Ok(InsertPayloadOk::Inserted(BlockStatus::NoWitness))
        };
        let mut trie = SparseStateTrie::default();
        let mut state_witness = B256Map::default();
        for encoded in execution_witness.state_witness() {
            state_witness.insert(keccak256(encoded), encoded.clone());
        }
        trie.reveal_witness(parent_state_root, &state_witness).map_err(|error| {
            InsertBlockErrorKind::Provider(ProviderError::TrieWitnessError(error.to_string()))
        })?;

        // ===================== Execution =====================
        let start_time = std::time::Instant::now();
        let block_executor =
            BlockExecutor::new(self.provider.clone(), parent_num_hash, &trie);
        let output = block_executor.execute(&block).map_err(InsertBlockErrorKind::Execution)?;
        debug!(target: "ress::engine", block = ?block_num_hash, elapsed = ?start_time.elapsed(), "Executed new payload");

        // ===================== Post Execution Validation =====================
        <EthBeaconConsensus<ChainSpec> as FullConsensus<EthPrimitives>>::validate_block_post_execution(
            &self.consensus,
            &block,
            &output.result
        )?;

        // ===================== State Root =====================
        let hashed_state =
            HashedPostState::from_bundle_state::<KeccakKeyHasher>(output.state.state.par_iter());
        let provider_factory = DefaultTrieNodeProviderFactory;
        let state_root = calculate_state_root(&mut trie, hashed_state, &provider_factory).map_err(|error| {
            InsertBlockErrorKind::Provider(ProviderError::TrieWitnessError(error.to_string()))
        })?;
        if state_root != block.state_root {
            return Err(ConsensusError::BodyStateRootDiff(
                GotExpected { got: state_root, expected: block.state_root }.into(),
            )
            .into());
        }

        // ===================== Update Node State =====================
        info!(target: "ress::engine", block=?block_num_hash, "Inserting block into provider");
        self.provider.insert_block(block.clone(), Some(execution_witness.into_state_witness()));
        info!(target: "ress::engine", block=?block_num_hash, "Block inserted into provider successfully");

        // Emit event.
        // Note: This code path is not used since ConsensusEngine is not started
        // Commenting out to avoid compilation errors with ExecutedBlock construction
        // let elapsed = start.elapsed();
        // let executed = ExecutedBlock::<EthEngineTypes> {
        //     recovered_block: Arc::new(block),
        //     execution_output: Arc::new(ExecutionOutcome::from((output, block_num_hash.number))),
        //     hashed_state: Default::default(),
        //     trie_updates: Default::default(),
        // };
        // let event = if self.is_fork(block_num_hash.hash) {
        //     BeaconConsensusEngineEvent::<EthEngineTypes>::ForkBlockAdded(executed.clone(), elapsed)
        // } else {
        //     BeaconConsensusEngineEvent::<EthEngineTypes>::CanonicalBlockAdded(executed, elapsed)
        // };
        // self.emit_event(event);

        debug!(target: "ress::engine", block=?block_num_hash, elapsed = ?start_time.elapsed(), "Finished inserting block");
        Ok(InsertPayloadOk::Inserted(BlockStatus::Valid))
    }

    /// Checks if the given `check` hash points to an invalid header, inserting the given `head`
    /// block into the invalid header cache if the `check` hash has a known invalid ancestor.
    ///
    /// Returns a payload status response according to the engine API spec if the block is known to
    /// be invalid.
    fn check_invalid_ancestor_with_head(
        &mut self,
        check: B256,
        head: B256,
    ) -> Option<PayloadStatus> {
        // check if the check hash was previously marked as invalid
        let header = self.invalid_headers.get(&check)?;

        // populate the latest valid hash field
        let status = self.prepare_invalid_response(header.parent);

        // insert the head block into the invalid header cache
        self.invalid_headers.insert_with_invalid_ancestor(head, header);

        Some(status)
    }

    /// Checks if the given `head` points to an invalid header, which requires a specific response
    /// to a forkchoice update.
    fn check_invalid_ancestor(&mut self, head: B256) -> Option<PayloadStatus> {
        // check if the head was previously marked as invalid
        let header = self.invalid_headers.get(&head)?;
        // populate the latest valid hash field
        Some(self.prepare_invalid_response(header.parent))
    }

    /// Prepares the invalid payload response for the given hash, checking the
    /// database for the parent hash and populating the payload status with the latest valid hash
    /// according to the engine api spec.
    fn prepare_invalid_response(&mut self, mut parent_hash: B256) -> PayloadStatus {
        // Edge case: the `latestValid` field is the zero hash if the parent block is the terminal
        // PoW block, which we need to identify by looking at the parent's block difficulty
        if let Some(parent) = self.provider.sealed_header(&parent_hash) {
            if !parent.difficulty.is_zero() {
                parent_hash = B256::ZERO;
            }
        }

        let valid_parent_hash = self.latest_valid_hash_for_invalid_payload(parent_hash);
        PayloadStatus::from_status(PayloadStatusEnum::Invalid {
            validation_error: PayloadValidationError::LinksToRejectedPayload.to_string(),
        })
        .with_latest_valid_hash(valid_parent_hash.unwrap_or_default())
    }

    /// Validate if block is correct and satisfies all the consensus rules that concern the header
    /// and block body itself.
    fn validate_block(&self, block: &SealedBlock) -> Result<(), ConsensusError> {
        // Note: validate_header_with_total_difficulty may not be available in reth 1.9.3
        // Commenting out for now - header validation is handled during execution
        // self.consensus.validate_header_with_total_difficulty(block.header(), U256::MAX).inspect_err(|error| {
        //     error!(target: "ress::engine", %error, "Failed to validate header against total difficulty");
        // })?;

        self.consensus.validate_header(block.sealed_header()).inspect_err(|error| {
            error!(target: "ress::engine", %error, "Failed to validate header");
        })?;

        self.consensus.validate_block_pre_execution(block).inspect_err(|error| {
            error!(target: "ress::engine", %error, "Failed to validate block");
        })?;

        Ok(())
    }

    /// Return the parent hash of the lowest buffered ancestor for the requested block, if there
    /// are any buffered ancestors. If there are no buffered ancestors, and the block itself does
    /// not exist in the buffer, this returns the hash that is passed in.
    ///
    /// Returns the parent hash of the block itself if the block is buffered and has no other
    /// buffered ancestors.
    fn lowest_buffered_ancestor_or(&self, hash: B256) -> B256 {
        self.block_buffer
            .lowest_ancestor(&hash)
            .map(|block| block.parent_hash)
            .unwrap_or_else(|| hash)
    }

    /// If validation fails, the response MUST contain the latest valid hash:
    ///
    ///   - The block hash of the ancestor of the invalid payload satisfying the following two
    ///     conditions:
    ///     - It is fully validated and deemed VALID
    ///     - Any other ancestor of the invalid payload with a higher blockNumber is INVALID
    ///   - 0x0000000000000000000000000000000000000000000000000000000000000000 if the above
    ///     conditions are satisfied by a `PoW` block.
    ///   - null if client software cannot determine the ancestor of the invalid payload satisfying
    ///     the above conditions.
    fn latest_valid_hash_for_invalid_payload(&mut self, parent_hash: B256) -> Option<B256> {
        // Check if parent exists in side chain or in canonical chain.
        if self.provider.sealed_header(&parent_hash).is_some() {
            return Some(parent_hash);
        }

        // iterate over ancestors in the invalid cache
        // until we encounter the first valid ancestor
        let mut current_hash = parent_hash;
        let mut current_block = self.invalid_headers.get(&current_hash);
        while let Some(block_with_parent) = current_block {
            current_hash = block_with_parent.parent;
            current_block = self.invalid_headers.get(&current_hash);

            // If current_header is None, then the current_hash does not have an invalid
            // ancestor in the cache, check its presence in blockchain tree
            if current_block.is_none() && self.provider.sealed_header(&current_hash).is_some() {
                return Some(current_hash);
            }
        }

        None
    }

    /// Handles an error that occurred while inserting a block.
    ///
    /// If this is a validation error this will mark the block as invalid.
    ///
    /// Returns the proper payload status response if the block is invalid.
    fn on_insert_block_error(
        &mut self,
        error: InsertBlockError<Block>,
    ) -> Result<PayloadStatus, InsertBlockFatalError> {
        let (block, error) = error.split();

        // if invalid block, we check the validation error. Otherwise return the fatal
        // error.
        let validation_err = error.ensure_validation_error()?;

        // If the error was due to an invalid payload, the payload is added to the invalid headers
        // cache and `Ok` with [PayloadStatusEnum::Invalid] is returned.
        warn!(target: "ress::engine", invalid_hash = %block.hash(), invalid_number = block.number, %validation_err, "Invalid block error on new payload");
        let latest_valid_hash = self.latest_valid_hash_for_invalid_payload(block.parent_hash);

        // keep track of the invalid header
        self.invalid_headers.insert(block.block_with_parent());
        Ok(PayloadStatus::new(
            PayloadStatusEnum::Invalid { validation_error: validation_err.to_string() },
            latest_valid_hash,
        ))
    }
}

/// Block inclusion can be valid, accepted, or invalid. Invalid blocks are returned as an error
/// variant.
///
/// If we don't know the block's parent, we return `Disconnected`, as we can't claim that the block
/// is valid or not.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockStatus {
    /// The block is valid and block extends canonical chain.
    Valid,
    /// The block may be valid and has an unknown missing ancestor.
    Disconnected {
        /// Current canonical head.
        head: BlockNumHash,
        /// The lowest ancestor block that is not connected to the canonical chain.
        missing_ancestor: BlockNumHash,
    },
    /// The block has no witness.
    NoWitness,
}

/// How a payload was inserted if it was valid.
///
/// If the payload was valid, but has already been seen, [`InsertPayloadOk::AlreadySeen`] is
/// returned, otherwise [`InsertPayloadOk::Inserted`] is returned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InsertPayloadOk {
    /// The payload was valid, but we have already seen it.
    AlreadySeen,
    /// The payload was valid and inserted into the tree.
    Inserted(BlockStatus),
}


#[derive(Metrics)]
#[metrics(scope = "engine.tree")]
struct EngineTreeMetrics {
    /// Canonical head block number.
    head: Gauge,
}
