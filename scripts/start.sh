#!/bin/bash

# Ress node startup script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if cargo is available
if ! command -v cargo &> /dev/null; then
    echo -e "${RED}Error: cargo is not installed or not in PATH${NC}"
    exit 1
fi

# Path to the binary
BINARY_PATH="./target/release/ress"

# Check if binary exists
if [ ! -f "$BINARY_PATH" ]; then
    echo -e "${RED}Error: Binary not found at $BINARY_PATH${NC}"
    exit 1
fi

# Trusted peer
TRUSTED_PEER="enode://f103bbd625f5376284f31d04645f7c506cb9b2b0953cda712e7c1ec7d416a4c273516fe9399f074e886ce83814921bf682f7e5b8a99f511227a34854bf1eb3ab@127.0.0.1:31310"

# Run the node
echo -e "${GREEN}Starting ress node...${NC}"
echo -e "${GREEN}Trusted peer: $TRUSTED_PEER${NC}"
echo ""

exec "$BINARY_PATH" \
    --trusted-peers $TRUSTED_PEER \
    --datadir ./data \
    --port 32310
