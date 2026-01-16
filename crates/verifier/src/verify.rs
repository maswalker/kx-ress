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
    // Verify block hash equals header hash (Ethereum rule)
    // In Ethereum, block hash is the keccak256 hash of the block header
    let computed_block_hash = result.block.hash();
    let sealed_header = result.block.clone_sealed_header();
    let header_hash = sealed_header.hash();
    if computed_block_hash != header_hash {
        return Err(VerificationError::ExecutionError(format!(
            "Block hash does not match header hash (Ethereum rule violation): block hash {}, header hash {}",
            computed_block_hash,
            header_hash
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

    // Verify block's parent_hash matches parent block's hash
    // Skip for genesis block (block.number == 0)
    if result.block.number > 0 && result.block.parent_hash != computed_parent_hash {
        return Err(VerificationError::ExecutionError(format!(
            "Block parent_hash does not match parent block hash: block parent_hash {}, parent block hash {}",
            result.block.parent_hash,
            computed_parent_hash
        )));
    }

    // Verify block number relationship (skip for genesis block)
    if result.block.number > 0 && result.block.number != result.parent_block.number + 1 {
        return Err(VerificationError::ExecutionError(format!(
            "Block number relationship invalid: block number {}, parent block number {}",
            result.block.number,
            result.parent_block.number
        )));
    }

    // Check if this is an empty block (no transactions and no ommers)
    let is_empty = result.block.body().transactions.is_empty() && result.block.body().ommers.is_empty();

    if is_empty {
        // For empty blocks, only perform basic validations:
        // 1. block.hash() correctness (already verified above)
        // 2. parent_hash correctness (already verified above)
        // 3. block_number continuity (already verified above)
        // 4. state_root == parent_state_root (empty block characteristic)
        if result.block.state_root != result.parent_block.state_root {
            return Err(VerificationError::StateRootMismatch {
                got: result.parent_block.state_root,
                expected: result.block.state_root,
            });
        }
        // Empty block verification complete
        return Ok(());
    }

    // Normal verification path for blocks with transactions
    // Create provider with only bytecodes from result
    let provider = VerifierProvider::new(
        chain_spec.clone(),
        result.bytecodes.clone(),
    );

    // Build trie from witness
    let mut trie = SparseStateTrie::default();
    let mut state_witness = B256Map::default();
    for encoded in result.witness.state_witness() {
        state_witness.insert(keccak256(encoded), encoded.clone());
    }

    let parent_state_root = result.parent_block.state_root;
    trie.reveal_witness(parent_state_root, &state_witness)
        .map_err(|e| VerificationError::ExecutionError(format!("Failed to reveal witness: {}", e)))?;

    // Create block executor
    let parent = BlockNumHash::new(result.block.number.saturating_sub(1), result.parent_block.hash());
    let db = VerifierDatabase::new(&provider, parent, &trie);
    let state = State::builder()
        .with_database(db)
        .with_bundle_update()
        .without_state_clear()
        .build();

    // Use EvmConfigWrapper to support both Ethereum and Kasplex chains
    // For Kasplex chains, this will use KasplexEvmConfig which distributes base fees to treasury
    let evm_config = if let Some(kasplex_chain_spec) = kasplex_chain_spec {
        EvmConfigWrapper::from_kasplex_chain_spec(kasplex_chain_spec)
    } else {
        EvmConfigWrapper::from_ethereum_chain_spec(provider.chain_spec())
    };

    let mut executor_state = state;

    // Execute block using the appropriate EVM config
    let exec_result = match evm_config {
        EvmConfigWrapper::Ethereum(config) => {
            let mut strategy = config
                .executor_for_block(&mut executor_state, &result.block)
                .map_err(|e| VerificationError::ExecutionError(format!("Failed to create executor: {}", e)))?;

            // Execute block
            strategy
                .apply_pre_execution_changes()
                .map_err(|e| VerificationError::ExecutionError(format!("Pre-execution failed: {}", e)))?;

            for tx in result.block.transactions_recovered() {
                strategy
                    .execute_transaction(tx)
                    .map_err(|e| VerificationError::ExecutionError(format!("Transaction execution failed: {}", e)))?;
            }

            strategy
                .apply_post_execution_changes()
                .map_err(|e| VerificationError::ExecutionError(format!("Post-execution failed: {}", e)))?
        }
        EvmConfigWrapper::Kasplex(config) => {
            // Use KasplexEvmConfig which uses KasplexBlockExecutorFactory
            // This will use KasplexEvmFactory and KasplexEvmHandler
            // to distribute base fees to treasury address
            let mut strategy = config
                .executor_for_block(&mut executor_state, &result.block)
                .map_err(|e| VerificationError::ExecutionError(format!("Failed to create executor: {}", e)))?;

            // Execute block
            strategy
                .apply_pre_execution_changes()
                .map_err(|e| VerificationError::ExecutionError(format!("Pre-execution failed: {}", e)))?;

            for tx in result.block.transactions_recovered() {
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
        &result.block,
        &exec_result,
    )
    .map_err(VerificationError::ConsensusError)?;

    executor_state.merge_transitions(BundleRetention::PlainState);
    let bundle = executor_state.take_bundle();

    // Create BlockExecutionOutput to match the pattern in engine
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
    let expected_state_root = result.block.state_root;
    if calculated_state_root != expected_state_root {
        return Err(VerificationError::StateRootMismatch {
            got: calculated_state_root,
            expected: expected_state_root,
        });
    }
    Ok(())
}

