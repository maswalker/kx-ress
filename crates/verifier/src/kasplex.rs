//! Kasplex-specific verification logic

use alloy_primitives::{Address, U256};
use reth_chainspec::ChainSpec;
use reth_primitives::BlockHeader;
use std::str::FromStr;

/// Calculate the vault address for a Kasplex chain based on chain ID.
/// 
/// Vault address format: `0x{ChainID}{padding}{10001}`
/// 
/// Example for chain ID 202555:
/// - Result: 0x2025550000000000000000000000000000010001
pub fn get_vault_address(chain_id: u64) -> Address {
    let chain_id_str = format!("{}", chain_id); // Use decimal representation
    let suffix = "10001";
    let address_length: usize = 40; // 20 bytes = 40 hex characters
    
    // Calculate padding needed
    let padding_needed = address_length.saturating_sub(chain_id_str.len() + suffix.len());
    let padding = "0".repeat(padding_needed);
    
    // Construct address: 0x + chain_id (as decimal string) + padding + suffix
    let address_hex = format!("{}{}{}", chain_id_str, padding, suffix);
    Address::from_str(&format!("0x{}", address_hex))
        .unwrap_or_else(|_| Address::ZERO)
}

/// Calculate the vault address from a chain spec.
pub fn get_vault_address_from_spec(chain_spec: &ChainSpec) -> Address {
    get_vault_address(chain_spec.chain.id())
}

/// Check if timestamp validation should allow equal timestamps for Kasplex.
/// For Kasplex, block.timestamp == parent.timestamp is allowed.
pub fn allow_equal_timestamp(chain_spec: &ChainSpec) -> bool {
    // Check if this is a Kasplex chain by checking chain ID
    // Kasplex Mainnet has chain ID 202555
    let chain_id = chain_spec.chain.id();
    chain_id == 202555 || chain_id == 168001 // Kasplex Mainnet or Internal L2
}

/// Validate timestamp against parent timestamp for Kasplex.
/// Returns Ok(()) if valid, Err with message if invalid.
pub fn validate_timestamp_for_kasplex(
    block_timestamp: u64,
    parent_timestamp: u64,
    chain_spec: &ChainSpec,
) -> Result<(), String> {
    if allow_equal_timestamp(chain_spec) {
        // Kasplex allows block.timestamp == parent.timestamp
        if block_timestamp < parent_timestamp {
            return Err(format!(
                "Block timestamp {} is less than parent timestamp {}",
                block_timestamp, parent_timestamp
            ));
        }
    } else {
        // Standard Ethereum requires block.timestamp > parent.timestamp
        if block_timestamp <= parent_timestamp {
            return Err(format!(
                "Block timestamp {} must be greater than parent timestamp {}",
                block_timestamp, parent_timestamp
            ));
        }
    }
    Ok(())
}

/// Calculate total base fee collected from all transactions in a block.
pub fn calculate_total_base_fee(
    gas_used: u64,
    base_fee_per_gas: Option<u64>,
) -> U256 {
    if let Some(base_fee) = base_fee_per_gas {
        U256::from(base_fee) * U256::from(gas_used)
    } else {
        U256::ZERO
    }
}

/// Adjust base fee distribution for Kasplex chains in engine verification.
/// Transfers all base fee from beneficiary to vault address.
pub fn adjust_base_fee_for_kasplex_engine(
    hashed_state: &mut reth_trie::HashedPostState,
    block: &reth_primitives::RecoveredBlock<reth_primitives::Block>,
    chain_spec: &ChainSpec,
) -> Result<(), String> {
    adjust_base_fee_for_kasplex_impl(hashed_state, block, chain_spec)
}

/// Internal implementation for adjusting base fee distribution.
pub(crate) fn adjust_base_fee_for_kasplex_impl(
    hashed_state: &mut reth_trie::HashedPostState,
    block: &reth_primitives::RecoveredBlock<reth_primitives::Block>,
    chain_spec: &ChainSpec,
) -> Result<(), String> {
    use alloy_primitives::keccak256;
    use reth_trie::Account;
    use reth_revm::state::AccountInfo;
    use reth_primitives::B256;

    // Calculate total base fee collected
    let base_fee_per_gas = block.base_fee_per_gas.unwrap_or(0);
    let total_base_fee = calculate_total_base_fee(block.gas_used, Some(base_fee_per_gas));
    
    if total_base_fee.is_zero() {
        // No base fee to distribute
        return Ok(());
    }

    let vault_address = get_vault_address_from_spec(chain_spec);
    let beneficiary = block.beneficiary;
    let hashed_beneficiary = keccak256(beneficiary);
    let hashed_vault = keccak256(vault_address);

    // Get current account states
    let beneficiary_account = hashed_state.accounts.get(&hashed_beneficiary);
    let vault_account = hashed_state.accounts.get(&hashed_vault);

    // Calculate new balances
    let beneficiary_balance = beneficiary_account
        .as_ref()
        .and_then(|acc| acc.as_ref())
        .map(|acc| acc.info.balance)
        .unwrap_or(U256::ZERO);
    
    let vault_balance = vault_account
        .as_ref()
        .and_then(|acc| acc.as_ref())
        .map(|acc| acc.info.balance)
        .unwrap_or(U256::ZERO);

    // Transfer base fee from beneficiary to vault
    let new_beneficiary_balance = beneficiary_balance.saturating_sub(total_base_fee);
    let new_vault_balance = vault_balance.saturating_add(total_base_fee);

    // Update beneficiary account
    if let Some(account) = hashed_state.accounts.get_mut(&hashed_beneficiary) {
        if let Some(ref mut acc) = account {
            acc.info.balance = new_beneficiary_balance;
        } else {
            // Create new account if it doesn't exist (shouldn't happen for beneficiary)
            *account = Some(Account {
                info: AccountInfo {
                    balance: new_beneficiary_balance,
                    nonce: 0,
                    code_hash: B256::ZERO,
                    code: None,
                },
                storage: Default::default(),
            });
        }
    } else {
        // Beneficiary account not in state changes, which means it's unchanged from parent
        // We still need to modify it, so we need to load it from the trie or mark it as changed
        // For now, we'll create a new entry
        hashed_state.accounts.insert(hashed_beneficiary, Some(Account {
            info: AccountInfo {
                balance: new_beneficiary_balance,
                nonce: 0,
                code_hash: B256::ZERO,
                code: None,
            },
            storage: Default::default(),
        }));
    }

    // Update vault account
    if let Some(account) = hashed_state.accounts.get_mut(&hashed_vault) {
        if let Some(ref mut acc) = account {
            acc.info.balance = new_vault_balance;
        } else {
            // Create new account if it doesn't exist
            *account = Some(Account {
                info: AccountInfo {
                    balance: new_vault_balance,
                    nonce: 0,
                    code_hash: B256::ZERO,
                    code: None,
                },
                storage: Default::default(),
            });
        }
    } else {
        // Vault account not in state changes, create new entry
        hashed_state.accounts.insert(hashed_vault, Some(Account {
            info: AccountInfo {
                balance: new_vault_balance,
                nonce: 0,
                code_hash: B256::ZERO,
                code: None,
            },
            storage: Default::default(),
        }));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vault_address_calculation() {
        // Test for Kasplex Mainnet (chain ID 202555)
        let mainnet_vault = get_vault_address(202555);
        // Should be: 0x2025550000000000000000000000000000010001
        assert!(mainnet_vault != Address::ZERO);
        assert_eq!(
            mainnet_vault.to_string().to_lowercase(),
            "0x2025550000000000000000000000000000010001"
        );
        
        // Test for another chain ID
        let other_vault = get_vault_address(123);
        assert!(other_vault != Address::ZERO);
        assert_ne!(mainnet_vault, other_vault);
    }
}

