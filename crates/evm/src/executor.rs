use alloy_eips::BlockNumHash;
use ress_provider::RessProvider;
use reth_evm::{
    execute::{BlockExecutionError, BlockExecutor as _},
    ConfigureEvm,
};
use reth_primitives::{Block, Receipt, RecoveredBlock};
use reth_provider::BlockExecutionOutput;
use reth_revm::db::{states::bundle_state::BundleRetention, State};
use reth_trie_sparse::SparseStateTrie;

use crate::config::EvmConfigWrapper;
use crate::db::WitnessDatabase;

/// An evm block executor that uses a reth's block executor to execute blocks by
/// using state from [`SparseStateTrie`].
#[allow(missing_debug_implementations)]
pub struct BlockExecutor<'a> {
    evm_config: EvmConfigWrapper,
    state: State<WitnessDatabase<'a>>,
}

impl<'a> BlockExecutor<'a> {
    /// Instantiate new block executor with chain spec and witness database.
    /// For Kasplex chains, this will use KasplexEvmConfig which distributes base fees to treasury.
    pub fn new(provider: RessProvider, parent: BlockNumHash, trie: &'a SparseStateTrie) -> Self {
        // Check if this is a Kasplex chain
        let evm_config = if let Some(kasplex_chain_spec) = provider.kasplex_chain_spec() {
            // Use KasplexEvmConfig for Kasplex chains (distributes base fees to treasury)
            EvmConfigWrapper::from_kasplex_chain_spec(kasplex_chain_spec)
        } else {
            // Use standard Ethereum config
            EvmConfigWrapper::from_ethereum_chain_spec(provider.chain_spec())
        };
        
        let db = WitnessDatabase::new(provider, parent, trie);
        let state =
            State::builder().with_database(db).with_bundle_update().without_state_clear().build();
        Self { evm_config, state }
    }

    /// Execute a block.
    pub fn execute(
        mut self,
        block: &RecoveredBlock<Block>,
    ) -> Result<BlockExecutionOutput<Receipt>, BlockExecutionError> {
        use crate::config::EvmConfigWrapper;
        
        // Handle both Ethereum and Kasplex configs separately
        // KasplexEvmConfig uses KasplexBlockExecutorFactory with KasplexEvmFactory
        // which distributes base fees to treasury via KasplexEvmHandler
        match &self.evm_config {
            EvmConfigWrapper::Ethereum(config) => {
                let mut strategy = config.executor_for_block(&mut self.state, block).unwrap();
                strategy.apply_pre_execution_changes()?;
                for tx in block.transactions_recovered() {
                    strategy.execute_transaction(tx)?;
                }
                let result = strategy.apply_post_execution_changes()?;
                self.state.merge_transitions(BundleRetention::PlainState);
                Ok(BlockExecutionOutput { state: self.state.take_bundle(), result })
            }
            EvmConfigWrapper::Kasplex(config) => {
                // Use KasplexEvmConfig which uses KasplexBlockExecutorFactory
                // This will use KasplexEvmFactory and KasplexEvmHandler
                // to distribute base fees to treasury address
                let mut strategy = config.executor_for_block(&mut self.state, block).unwrap();
                strategy.apply_pre_execution_changes()?;
                for tx in block.transactions_recovered() {
                    strategy.execute_transaction(tx)?;
                }
                let result = strategy.apply_post_execution_changes()?;
                self.state.merge_transitions(BundleRetention::PlainState);
                Ok(BlockExecutionOutput { state: self.state.take_bundle(), result })
            }
        }
    }
}
