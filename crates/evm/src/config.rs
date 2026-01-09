//! EVM configuration wrapper to support both Ethereum and Kasplex EVM configs.

use kasplex_reth_block::KasplexEvmConfig as KasplexBlockEvmConfig;
use kasplex_reth_chainspec::spec::KasplexChainSpec;
use reth_chainspec::ChainSpec;
use reth_evm_ethereum::EthEvmConfig;
use std::sync::Arc;

/// A wrapper that supports both Ethereum and Kasplex EVM configurations.
#[derive(Clone, Debug)]
pub enum EvmConfigWrapper {
    /// Standard Ethereum EVM configuration.
    Ethereum(EthEvmConfig),
    /// Kasplex-specific EVM configuration (distributes base fee to treasury).
    Kasplex(KasplexEvmConfig),
}

impl EvmConfigWrapper {
    /// Create an EVM config wrapper from a Kasplex chain spec.
    pub fn from_kasplex_chain_spec(chain_spec: Arc<KasplexChainSpec>) -> Self {
        Self::Kasplex(KasplexEvmConfig::new(chain_spec))
    }

    /// Create an EVM config wrapper from a generic chain spec (Ethereum).
    pub fn from_ethereum_chain_spec(chain_spec: Arc<ChainSpec>) -> Self {
        Self::Ethereum(EthEvmConfig::new(chain_spec))
    }
}

// Note: EvmConfigWrapper does not implement ConfigureEvm because the two config types
// have incompatible associated types (BlockExecutorFactory, BlockAssembler, etc.).
// Instead, we handle both types directly in BlockExecutor::execute.

/// Kasplex EVM configuration.
///
/// This is a type alias for the KasplexEvmConfig from kasplex-reth-block,
/// which uses `KasplexBlockExecutorFactory` with `KasplexEvmFactory` to
/// distribute base fees to the treasury address instead of the block beneficiary.
pub type KasplexEvmConfig = KasplexBlockEvmConfig;

