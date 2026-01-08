use crate::{chain_state::ChainState, database::RessDatabase};
use alloy_eips::BlockNumHash;
use alloy_primitives::{map::B256HashSet, BlockHash, BlockNumber, Bytes, B256};
use reth_chainspec::ChainSpec;
use reth_db::DatabaseError;
use reth_primitives::{Block, BlockBody, Bytecode, Header, RecoveredBlock, SealedHeader};
use reth_ress_protocol::RessProtocolProvider;
use reth_storage_errors::provider::ProviderResult;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct RessProvider {
    chain_spec: Arc<ChainSpec>,
    database: RessDatabase,
    chain_state: ChainState,
}

impl RessProvider {
    pub fn new(chain_spec: Arc<ChainSpec>, database: RessDatabase) -> Self {
        Self { chain_spec, database, chain_state: ChainState::default() }
    }

    pub fn chain_spec(&self) -> Arc<ChainSpec> {
        self.chain_spec.clone()
    }

    pub fn is_hash_canonical(&self, hash: &BlockHash) -> bool {
        self.chain_state.is_hash_canonical(hash)
    }

    pub fn block_hash(&self, parent: BlockNumHash, number: BlockNumber) -> Option<BlockHash> {
        self.chain_state.block_hash(parent, number)
    }

    pub fn block_number(&self, hash: &B256) -> Option<BlockNumber> {
        self.chain_state.block_number(hash)
    }

    pub fn sealed_header(&self, hash: &B256) -> Option<SealedHeader> {
        self.chain_state.sealed_header(hash)
    }

    pub fn recovered_block(&self, hash: &B256) -> Option<RecoveredBlock<Block>> {
        self.chain_state.recovered_block(hash)
    }

    pub fn insert_block(&self, block: RecoveredBlock<Block>, maybe_witness: Option<Vec<Bytes>>) {
        self.chain_state.insert_block(block, maybe_witness);
    }

    /// Insert witness for a block hash without requiring the block to exist
    pub fn insert_witness(&self, block_hash: B256, witness: Vec<Bytes>) {
        self.chain_state.insert_witness(block_hash, witness);
    }

    pub fn bytecode_exists(&self, code_hash: B256) -> Result<bool, DatabaseError> {
        self.database.bytecode_exists(code_hash)
    }

    pub fn get_bytecode(&self, code_hash: B256) -> Result<Option<Bytecode>, DatabaseError> {
        self.database.get_bytecode(code_hash)
    }

    pub fn insert_bytecode(
        &self,
        code_hash: B256,
        bytecode: Bytecode,
    ) -> Result<(), DatabaseError> {
        self.database.insert_bytecode(code_hash, bytecode)
    }

    pub fn missing_code_hashes(
        &self,
        code_hashes: B256HashSet,
    ) -> Result<B256HashSet, DatabaseError> {
        self.database.missing_code_hashes(code_hashes)
    }

    pub fn insert_canonical_hash(&self, number: BlockNumber, hash: BlockHash) {
        self.chain_state.insert_canonical_hash(number, hash);
    }

    pub fn on_chain_update(&self, new: Vec<SealedHeader>, old: Vec<SealedHeader>) {
        for header in old {
            self.chain_state.remove_canonical_hash(header.number, header.hash());
        }
        for header in new {
            self.chain_state.insert_canonical_hash(header.number, header.hash());
        }
    }

    pub fn on_finalized(&self, finalized_hash: &B256) {
        if !finalized_hash.is_zero() {
            self.chain_state.remove_blocks_on_finalized(finalized_hash);
        }
    }
}

impl RessProtocolProvider for RessProvider {
    fn header(&self, block_hash: B256) -> ProviderResult<Option<Header>> {
        Ok(self.chain_state.header(&block_hash))
    }

    fn block_body(&self, block_hash: B256) -> ProviderResult<Option<BlockBody>> {
        Ok(self.chain_state.block_body(&block_hash))
    }

    fn bytecode(&self, code_hash: B256) -> ProviderResult<Option<Bytes>> {
        Ok(self.database.get_bytecode(code_hash)?.map(|b| b.original_bytes()))
    }

    async fn witness(&self, block_hash: B256) -> ProviderResult<Vec<Bytes>> {
        Ok(self.chain_state.witness(&block_hash).unwrap_or_default())
    }
}
