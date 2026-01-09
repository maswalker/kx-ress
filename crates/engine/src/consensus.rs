//! Consensus wrapper to support both Ethereum and Kasplex consensus.

use kasplex_reth_chainspec::spec::KasplexChainSpec;
use kasplex_reth_consensus::KasplexBeaconConsensus;
use reth_chainspec::ChainSpec;
use reth_consensus::{Consensus as ConsensusTrait, FullConsensus, HeaderValidator};
use reth_node_ethereum::consensus::EthBeaconConsensus;
use reth_primitives::{EthPrimitives, Header, RecoveredBlock, SealedBlock, SealedHeader};
use reth_primitives_traits::{Block, BlockHeader};
use reth_provider::BlockExecutionResult;
use std::sync::Arc;

/// A wrapper that supports both Ethereum and Kasplex consensus.
#[derive(Clone, Debug)]
pub enum ConsensusWrapper {
    /// Standard Ethereum consensus.
    Ethereum(EthBeaconConsensus<ChainSpec>),
    /// Kasplex-specific consensus (allows timestamp == parent.timestamp).
    Kasplex(KasplexBeaconConsensus),
}

impl ConsensusWrapper {
    /// Create a consensus wrapper from a Kasplex chain spec.
    /// This is the preferred way when we know it's a Kasplex chain.
    pub fn from_kasplex_chain_spec(chain_spec: Arc<KasplexChainSpec>) -> Self {
        Self::Kasplex(KasplexBeaconConsensus::new(chain_spec))
    }

    /// Create a consensus wrapper from a generic chain spec (Ethereum).
    pub fn from_ethereum_chain_spec(chain_spec: Arc<ChainSpec>) -> Self {
        Self::Ethereum(EthBeaconConsensus::new(chain_spec))
    }
}

impl<N> FullConsensus<N> for ConsensusWrapper
where
    N: reth_node_api::NodePrimitives<BlockHeader = Header, Block = reth_primitives::Block>,
{
    fn validate_block_post_execution(
        &self,
        block: &RecoveredBlock<N::Block>,
        result: &BlockExecutionResult<N::Receipt>,
    ) -> Result<(), reth_consensus::ConsensusError> {
        match self {
            Self::Ethereum(consensus) => {
                <EthBeaconConsensus<ChainSpec> as FullConsensus<N>>::validate_block_post_execution(
                    consensus, block, result,
                )
            }
            Self::Kasplex(consensus) => {
                <KasplexBeaconConsensus as FullConsensus<N>>::validate_block_post_execution(
                    consensus, block, result,
                )
            }
        }
    }
}

// Implement Consensus trait for reth_primitives::Block (Ethereum block type)
impl ConsensusTrait<reth_primitives::Block> for ConsensusWrapper {
    type Error = reth_consensus::ConsensusError;

    fn validate_body_against_header(
        &self,
        body: &<reth_primitives::Block as Block>::Body,
        header: &SealedHeader<<reth_primitives::Block as Block>::Header>,
    ) -> Result<(), Self::Error> {
        match self {
            Self::Ethereum(consensus) => {
                <EthBeaconConsensus<ChainSpec> as ConsensusTrait<reth_primitives::Block>>::validate_body_against_header(
                    consensus, body, header
                )
            }
            Self::Kasplex(consensus) => {
                <KasplexBeaconConsensus as ConsensusTrait<reth_primitives::Block>>::validate_body_against_header(
                    consensus, body, header
                )
            }
        }
    }

    fn validate_block_pre_execution(
        &self,
        block: &SealedBlock<reth_primitives::Block>,
    ) -> Result<(), Self::Error> {
        match self {
            Self::Ethereum(consensus) => {
                <EthBeaconConsensus<ChainSpec> as ConsensusTrait<reth_primitives::Block>>::validate_block_pre_execution(
                    consensus, block
                )
            }
            Self::Kasplex(consensus) => {
                <KasplexBeaconConsensus as ConsensusTrait<reth_primitives::Block>>::validate_block_pre_execution(
                    consensus, block
                )
            }
        }
    }
}

// Implement HeaderValidator trait for Header (Ethereum header type)
impl HeaderValidator<Header> for ConsensusWrapper {
    fn validate_header(&self, header: &SealedHeader<Header>) -> Result<(), reth_consensus::ConsensusError> {
        match self {
            Self::Ethereum(consensus) => {
                <EthBeaconConsensus<ChainSpec> as HeaderValidator<Header>>::validate_header(
                    consensus, header
                )
            }
            Self::Kasplex(consensus) => {
                <KasplexBeaconConsensus as HeaderValidator<Header>>::validate_header(
                    consensus, header
                )
            }
        }
    }

    fn validate_header_against_parent(
        &self,
        header: &SealedHeader<Header>,
        parent: &SealedHeader<Header>,
    ) -> Result<(), reth_consensus::ConsensusError> {
        match self {
            Self::Ethereum(consensus) => {
                <EthBeaconConsensus<ChainSpec> as HeaderValidator<Header>>::validate_header_against_parent(
                    consensus, header, parent
                )
            }
            Self::Kasplex(consensus) => {
                <KasplexBeaconConsensus as HeaderValidator<Header>>::validate_header_against_parent(
                    consensus, header, parent
                )
            }
        }
    }
}

