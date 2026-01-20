//! Simple provider implementation that only uses data from ExecutionResult.

use alloy_eips::BlockNumHash;
use alloy_primitives::{keccak256, Address, B256, U256};
use alloy_rlp::Decodable;
use reth_primitives::Bytecode;
use reth_provider::ProviderError;
use reth_revm::{bytecode::Bytecode as RevmBytecode, state::AccountInfo, Database};
use reth_trie_sparse::SparseStateTrie;
use std::collections::HashMap;

/// A simple provider that only uses bytecodes from ExecutionResult.
/// This does not access any cached data from the engine.
#[derive(Debug)]
pub(crate) struct VerifierProvider {
    bytecodes: HashMap<B256, Bytecode>,
}

impl VerifierProvider {
    pub(crate) fn new(bytecodes: Vec<(B256, Bytecode)>) -> Self {
        let bytecodes_map: HashMap<B256, Bytecode> = bytecodes.into_iter().collect();
        Self {
            bytecodes: bytecodes_map,
        }
    }
}

/// Database implementation for verifier that uses only data from ExecutionResult.
#[derive(Debug)]
pub(crate) struct VerifierDatabase<'a> {
    provider: &'a VerifierProvider,
    parent: BlockNumHash,
    trie: &'a SparseStateTrie,
}

impl<'a> VerifierDatabase<'a> {
    pub(crate) fn new(provider: &'a VerifierProvider, parent: BlockNumHash, trie: &'a SparseStateTrie) -> Self {
        Self { provider, parent, trie }
    }
}

impl Database for VerifierDatabase<'_> {
    type Error = ProviderError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let hashed_address = keccak256(address);
        let Some(bytes) = self.trie.get_account_value(&hashed_address) else {
            return Ok(None);
        };
        let account = reth_trie::TrieAccount::decode(&mut bytes.as_slice())
            .map_err(|e| ProviderError::TrieWitnessError(format!("Failed to decode account: {}", e)))?;
        let account_info = AccountInfo {
            balance: account.balance,
            nonce: account.nonce,
            code_hash: account.code_hash,
            code: None,
            account_id: None,
        };
        Ok(Some(account_info))
    }

    fn storage(&mut self, address: Address, slot: U256) -> Result<U256, Self::Error> {
        let slot = B256::from(slot);
        let hashed_address = keccak256(address);
        let hashed_slot = keccak256(slot);
        let value = match self.trie.get_storage_slot_value(&hashed_address, &hashed_slot) {
            Some(value) => U256::decode(&mut value.as_slice())
                .map_err(|e| ProviderError::TrieWitnessError(format!("Failed to decode storage value: {}", e)))?,
            None => U256::ZERO,
        };
        Ok(value)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<RevmBytecode, Self::Error> {
        let bytecode = self.provider.bytecodes.get(&code_hash).ok_or_else(|| {
            ProviderError::TrieWitnessError(format!("Bytecode for {} not found in ExecutionResult", code_hash))
        })?;
        // Convert reth_primitives::Bytecode to reth_revm::bytecode::Bytecode
        // Bytecode implements Deref to reth_revm::bytecode::Bytecode
        Ok(bytecode.0.clone())
    }

    fn block_hash(&mut self, block_number: u64) -> Result<B256, Self::Error> {
        // For verification, we can only provide the parent block hash
        // Other block hashes would need to be computed or provided separately
        if block_number == self.parent.number {
            Ok(self.parent.hash)
        } else {
            Err(ProviderError::StateForNumberNotFound(block_number))
        }
    }
}

