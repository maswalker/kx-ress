//! Verification logic for ExecutionResult.

use crate::provider::{VerifierDatabase, VerifierProvider};
use alloy_eips::BlockNumHash;
use alloy_primitives::{keccak256, map::B256Map, B256};
use ress_evm::EvmConfigWrapper;
use ress_primitives::execution::ExecutionResult;
use kasplex_reth_chainspec::spec::KasplexChainSpec;
use reth_chainspec::ChainSpec;
use reth_consensus::FullConsensus;
use reth_evm::{
    execute::BlockExecutor as _,
    ConfigureEvm,
};
use reth_primitives::EthPrimitives;
use reth_provider::BlockExecutionOutput;
use reth_revm::db::{states::bundle_state::BundleRetention, State};
use reth_trie::{HashedPostState, KeccakKeyHasher};
use reth_trie_sparse::{provider::DefaultTrieNodeProviderFactory, SparseStateTrie};
use std::sync::Arc;

// Import calculate_state_root from engine task module
// We'll need to re-export or copy this function
use ress_engine::task::calculate_state_root;

use thiserror::Error;

/// Errors that can occur during execution result verification.
#[derive(Debug, Error)]
pub enum VerificationError {
    /// Consensus validation failed.
    #[error("Consensus error: {0}")]
    ConsensusError(#[from] reth_consensus::ConsensusError),
    /// Block execution failed.
    #[error("Execution error: {0}")]
    ExecutionError(String),
    /// Calculated state root does not match the expected state root.
    #[error("State root mismatch: got {got}, expected {expected}")]
    StateRootMismatch {
        /// The calculated state root.
        got: B256,
        /// The expected state root from the block.
        expected: B256,
    },
    /// Provider/database error.
    #[error("Provider error: {0}")]
    ProviderError(#[from] reth_errors::ProviderError),
}

/// Verify an ExecutionResult by re-executing the block and checking the state root.
/// This function does not access any cached data from the engine.
///
/// # Arguments
/// * `result` - The ExecutionResult to verify
/// * `chain_spec` - The chain specification
/// * `kasplex_chain_spec` - Optional Kasplex chain specification (for Kasplex chains)
///
/// # Returns
/// Ok(()) if verification succeeds, Err(VerificationError) otherwise
pub fn verify(
    result: &ExecutionResult,
    chain_spec: Arc<ChainSpec>,
    kasplex_chain_spec: Option<Arc<KasplexChainSpec>>,
) -> Result<(), VerificationError> {
    if result.blocks.is_empty() {
        return Err(VerificationError::ExecutionError(
            "ExecutionResult must have at least one block".to_string()
        ));
    }

    if result.blocks.len() != result.witnesses.len() {
        return Err(VerificationError::ExecutionError(format!(
            "Blocks count ({}) does not match witnesses count ({})",
            result.blocks.len(),
            result.witnesses.len()
        )));
    }

    // Verify parent block hash equals parent header hash (Ethereum rule)
    let computed_parent_hash = result.parent_block.hash();
    let parent_sealed_header = result.parent_block.clone_sealed_header();
    let parent_header_hash = parent_sealed_header.hash();
    if computed_parent_hash != parent_header_hash {
        return Err(VerificationError::ExecutionError(format!(
            "Parent block hash does not match parent header hash (Ethereum rule violation): block hash {}, header hash {}",
            computed_parent_hash,
            parent_header_hash
        )));
    }

    // Verify all blocks sequentially
    let mut current_parent = result.parent_block.clone();
    for (i, block) in result.blocks.iter().enumerate() {
        // Verify block hash equals header hash (Ethereum rule)
        let computed_block_hash = block.hash();
        let sealed_header = block.clone_sealed_header();
        let header_hash = sealed_header.hash();
        if computed_block_hash != header_hash {
            return Err(VerificationError::ExecutionError(format!(
                "Block hash does not match header hash (Ethereum rule violation): block hash {}, header hash {}",
                computed_block_hash,
                header_hash
            )));
        }

        // Verify block's parent_hash matches current parent's hash
        // Skip for genesis block (block.number == 0)
        if block.number > 0 && block.parent_hash != current_parent.hash() {
            return Err(VerificationError::ExecutionError(format!(
                "Block parent_hash does not match parent block hash: block parent_hash {}, parent block hash {}",
                block.parent_hash,
                current_parent.hash()
            )));
        }

        // Verify block number relationship (skip for genesis block)
        if block.number > 0 && block.number != current_parent.number + 1 {
            return Err(VerificationError::ExecutionError(format!(
                "Block number relationship invalid: block number {}, parent block number {}",
                block.number,
                current_parent.number
            )));
        }

        // Check if this is an empty block (no transactions and no ommers)
        let is_empty = block.body().transactions.is_empty() && block.body().ommers.is_empty();

        if is_empty {
            // For empty blocks, only perform basic validations:
            // 1. block.hash() correctness (already verified above)
            // 2. parent_hash correctness (already verified above)
            // 3. block_number continuity (already verified above)
            // 4. state_root == parent_state_root (empty block characteristic)
            if block.state_root != current_parent.state_root {
                return Err(VerificationError::StateRootMismatch {
                    got: current_parent.state_root,
                    expected: block.state_root,
                });
            }
            // Empty block verification complete for this block
            current_parent = block.clone();
            continue;
        }

        // Normal verification path for blocks with transactions
        let witness = &result.witnesses[i];
        
        // Create provider with all bytecodes from result
        let provider = VerifierProvider::new(
            chain_spec.clone(),
            result.bytecodes.clone(),
        );

        // Build trie from witness
        let mut trie = SparseStateTrie::default();
        let mut state_witness = B256Map::default();
        for encoded in witness.state_witness() {
            state_witness.insert(keccak256(encoded), encoded.clone());
        }

        let parent_state_root = current_parent.state_root;
        trie.reveal_witness(parent_state_root, &state_witness)
            .map_err(|e| VerificationError::ExecutionError(format!("Failed to reveal witness: {}", e)))?;

        // Create block executor
        let parent = BlockNumHash::new(block.number.saturating_sub(1), current_parent.hash());
        let db = VerifierDatabase::new(&provider, parent, &trie);
        let state = State::builder()
            .with_database(db)
            .with_bundle_update()
            .without_state_clear()
            .build();

        // Use EvmConfigWrapper to support both Ethereum and Kasplex chains
        let evm_config = if let Some(ref kasplex_chain_spec) = kasplex_chain_spec {
            EvmConfigWrapper::from_kasplex_chain_spec(kasplex_chain_spec.clone())
        } else {
            EvmConfigWrapper::from_ethereum_chain_spec(provider.chain_spec())
        };

        let mut executor_state = state;

        // Execute block using the appropriate EVM config
        let exec_result = match evm_config {
            EvmConfigWrapper::Ethereum(config) => {
                let mut strategy = config
                    .executor_for_block(&mut executor_state, block)
                    .map_err(|e| VerificationError::ExecutionError(format!("Failed to create executor: {}", e)))?;

                strategy
                    .apply_pre_execution_changes()
                    .map_err(|e| VerificationError::ExecutionError(format!("Pre-execution failed: {}", e)))?;

                for tx in block.transactions_recovered() {
                    strategy
                        .execute_transaction(tx)
                        .map_err(|e| VerificationError::ExecutionError(format!("Transaction execution failed: {}", e)))?;
                }

                strategy
                    .apply_post_execution_changes()
                    .map_err(|e| VerificationError::ExecutionError(format!("Post-execution failed: {}", e)))?
            }
            EvmConfigWrapper::Kasplex(config) => {
                let mut strategy = config
                    .executor_for_block(&mut executor_state, block)
                    .map_err(|e| VerificationError::ExecutionError(format!("Failed to create executor: {}", e)))?;

                strategy
                    .apply_pre_execution_changes()
                    .map_err(|e| VerificationError::ExecutionError(format!("Pre-execution failed: {}", e)))?;

                for tx in block.transactions_recovered() {
                    strategy
                        .execute_transaction(tx)
                        .map_err(|e| VerificationError::ExecutionError(format!("Transaction execution failed: {}", e)))?;
                }

                strategy
                    .apply_post_execution_changes()
                    .map_err(|e| VerificationError::ExecutionError(format!("Post-execution failed: {}", e)))?
            }
        };

        // Validate consensus before merging
        let consensus = reth_node_ethereum::consensus::EthBeaconConsensus::new(chain_spec.clone());
        <reth_node_ethereum::consensus::EthBeaconConsensus<ChainSpec> as FullConsensus<EthPrimitives>>::validate_block_post_execution(
            &consensus,
            block,
            &exec_result,
        )
        .map_err(VerificationError::ConsensusError)?;

        executor_state.merge_transitions(BundleRetention::PlainState);
        let bundle = executor_state.take_bundle();

        // Create BlockExecutionOutput
        let output = BlockExecutionOutput {
            state: bundle,
            result: exec_result,
        };

        // Calculate state root
        use rayon::iter::IntoParallelRefIterator;
        let hashed_state = HashedPostState::from_bundle_state::<KeccakKeyHasher>(
            output.state.state.par_iter(),
        );
        let provider_factory = DefaultTrieNodeProviderFactory;
        let calculated_state_root = calculate_state_root(&mut trie, hashed_state, &provider_factory)
            .map_err(|e| VerificationError::ExecutionError(format!("Failed to calculate state root: {}", e)))?;

        // Verify state root matches
        let expected_state_root = block.state_root;
        if calculated_state_root != expected_state_root {
            return Err(VerificationError::StateRootMismatch {
                got: calculated_state_root,
                expected: expected_state_root,
            });
        }

        // Update current_parent for next iteration
        current_parent = block.clone();
    }
    
    Ok(())
}

