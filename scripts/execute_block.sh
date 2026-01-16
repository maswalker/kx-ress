#!/bin/bash

# Script to execute a block via HTTP API
# Usage: ./execute_block.sh [block_number]
# Example: ./execute_block.sh 10
# If no argument provided, defaults to block 10

set -e

RPC_URL="${RPC_URL:-http://localhost:8545}"
API_URL="${API_URL:-http://localhost:8080/execute_block}"

# Get block number from argument or default to 10
BLOCK_NUMBER="${1:-47020}"

echo "Fetching block info for block $BLOCK_NUMBER from $RPC_URL..."

# Fetch block info using eth_getBlockByNumber
BLOCK_DATA=$(curl -s -X POST "$RPC_URL" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\":[\"0x$(printf '%x' $BLOCK_NUMBER)\", false],\"id\":1}")

# Check if request was successful
if [ "$(echo "$BLOCK_DATA" | jq -r '.error')" != "null" ] && [ -n "$(echo "$BLOCK_DATA" | jq -r '.error')" ]; then
    echo "Error: RPC request failed"
    echo "$BLOCK_DATA" | jq '.error'
    exit 1
fi

# Extract block hash
BLOCK_HASH=$(echo "$BLOCK_DATA" | jq -r '.result.hash')

# Check if block hash was retrieved
if [ -z "$BLOCK_HASH" ] || [ "$BLOCK_HASH" = "null" ]; then
    echo "Error: Failed to fetch block hash for block $BLOCK_NUMBER"
    echo "Make sure the RPC endpoint is accessible at $RPC_URL"
    echo "Response: $BLOCK_DATA"
    exit 1
fi

# Extract parent hash
PARENT_HASH=$(echo "$BLOCK_DATA" | jq -r '.result.parentHash')

# Check if parent hash was retrieved
if [ -z "$PARENT_HASH" ] || [ "$PARENT_HASH" = "null" ]; then
    echo "Error: Failed to fetch parent hash for block $BLOCK_NUMBER"
    echo "Make sure the RPC endpoint is accessible at $RPC_URL"
    echo "Response: $BLOCK_DATA"
    exit 1
fi

# Extract transaction count to determine if block has transactions
# Note: When params=[block_number, false], transactions is returned as array length
# When params=[block_number, true], transactions is returned as full transaction objects
# Since we're using false, we need to get the transactions array and check its length
TRANSACTIONS=$(echo "$BLOCK_DATA" | jq -r '.result.transactions // []')
TRANSACTION_COUNT=$(echo "$TRANSACTIONS" | jq 'length')

# Set with_tx based on transaction count
WITH_TX="false"
if [ "$TRANSACTION_COUNT" -gt 0 ]; then
    WITH_TX="true"
fi

echo "Block info:"
echo "  Number: $BLOCK_NUMBER"
echo "  Hash: $BLOCK_HASH"
echo "  Parent hash: $PARENT_HASH"
echo "  Transaction count: $TRANSACTION_COUNT"
echo "  Has transactions (with_tx): $WITH_TX"
echo ""
echo "Executing block via API at $API_URL..."
echo ""

# Execute the block with all required fields including with_tx
RESPONSE=$(curl -s -X POST "$API_URL" \
    -H 'Content-Type: application/json' \
    -d "{\"block_hash\": \"$BLOCK_HASH\", \"block_height\": $BLOCK_NUMBER, \"parent_hash\": \"$PARENT_HASH\", \"with_tx\": $WITH_TX}")

# Pretty print JSON response
echo "Response:"
echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"
echo ""

# Check if execution was successful
SUCCESS=$(echo "$RESPONSE" | jq -r '.success' 2>/dev/null || echo "false")
if [ "$SUCCESS" = "true" ]; then
    echo "✓ Block execution successful!"
    echo ""
    echo "=== Execution Result ==="
    echo "Block hash: $(echo "$RESPONSE" | jq -r '.block_hash')"
    echo "State root: $(echo "$RESPONSE" | jq -r '.state_root')"
    echo ""
    
    # Print block info
    if echo "$RESPONSE" | jq -e '.block' > /dev/null 2>&1; then
        BLOCK_RLP=$(echo "$RESPONSE" | jq -r '.block.rlp_encoded')
        BLOCK_RLP_LEN=${#BLOCK_RLP}
        echo "Block (RLP encoded):"
        echo "  Length: $BLOCK_RLP_LEN characters"
        echo "  Preview: ${BLOCK_RLP:0:100}..."
        echo ""
    fi
    
    # Print parent block info
    if echo "$RESPONSE" | jq -e '.parent_block' > /dev/null 2>&1; then
        PARENT_BLOCK_RLP=$(echo "$RESPONSE" | jq -r '.parent_block.rlp_encoded')
        PARENT_BLOCK_RLP_LEN=${#PARENT_BLOCK_RLP}
        echo "Parent Block (RLP encoded):"
        echo "  Length: $PARENT_BLOCK_RLP_LEN characters"
        echo "  Preview: ${PARENT_BLOCK_RLP:0:100}..."
        echo ""
    fi
    
    # Print witness info
    if echo "$RESPONSE" | jq -e '.witness' > /dev/null 2>&1; then
        WITNESS_COUNT=$(echo "$RESPONSE" | jq '.witness.state_witness | length')
        echo "Witness:"
        echo "  State witness entries: $WITNESS_COUNT"
        if [ "$WITNESS_COUNT" -gt 0 ]; then
            FIRST_WITNESS=$(echo "$RESPONSE" | jq -r '.witness.state_witness[0]')
            FIRST_WITNESS_LEN=${#FIRST_WITNESS}
            echo "  First entry length: $FIRST_WITNESS_LEN characters"
            echo "  First entry preview: ${FIRST_WITNESS:0:80}..."
        fi
        echo ""
    fi
    
    # Print bytecodes info
    if echo "$RESPONSE" | jq -e '.bytecodes' > /dev/null 2>&1; then
        BYTECODE_COUNT=$(echo "$RESPONSE" | jq '.bytecodes | length')
        echo "Bytecodes:"
        echo "  Count: $BYTECODE_COUNT"
        if [ "$BYTECODE_COUNT" -gt 0 ]; then
            echo "  Details:"
            echo "$RESPONSE" | jq -r '.bytecodes[] | "    - Hash: \(.hash), Code length: \(.code | length) chars"' | head -10
            if [ "$BYTECODE_COUNT" -gt 10 ]; then
                echo "    ... and $((BYTECODE_COUNT - 10)) more"
            fi
        fi
        echo ""
    fi
    
    echo "=== Full JSON Response ==="
    echo "$RESPONSE" | jq '.'
    exit 0
else
    echo "✗ Block execution failed!"
    echo ""
    echo "Response fields:"
    echo "$RESPONSE" | jq -r 'to_entries[] | "  \(.key): \(.value)"' 2>/dev/null || echo "$RESPONSE"
    exit 1
fi
