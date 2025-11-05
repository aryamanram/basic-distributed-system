#!/bin/bash
# HyDFS Startup Script

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=================================="
echo "  HyDFS Startup Script"
echo "=================================="
echo ""

# Check if VM ID is provided
if [ -z "$1" ]; then
    echo -e "${RED}Error: VM ID not provided${NC}"
    echo "Usage: ./start_hydfs.sh <vm_id>"
    echo "Example: ./start_hydfs.sh 1"
    exit 1
fi

VM_ID=$1

# Validate VM ID
if [ "$VM_ID" -lt 1 ] || [ "$VM_ID" -gt 10 ]; then
    echo -e "${RED}Error: VM ID must be between 1 and 10${NC}"
    exit 1
fi

echo -e "${GREEN}Starting HyDFS for VM${VM_ID}...${NC}"
echo ""

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 not found${NC}"
    exit 1
fi

# Check if required files exist
if [ ! -f "main.py" ]; then
    echo -e "${RED}Error: main.py not found${NC}"
    echo "Please ensure you are in the MP3 directory"
    exit 1
fi

if [ ! -f "../MP2/membership.py" ]; then
    echo -e "${YELLOW}Warning: MP2/membership.py not found${NC}"
    echo "Make sure MP2 is in the parent directory"
fi

# Create storage directory if it doesn't exist
mkdir -p "hydfs_storage_${VM_ID}"

echo -e "${GREEN}Configuration:${NC}"
echo "  VM ID: ${VM_ID}"
echo "  Hostname: $(hostname)"
echo "  Storage: hydfs_storage_${VM_ID}/"
echo "  Log: machine.${VM_ID}.log"
echo ""

# Start HyDFS
echo -e "${GREEN}Starting HyDFS node...${NC}"
echo "Press Ctrl+C to stop"
echo ""

python3 main.py --vm-id ${VM_ID}

echo ""
echo -e "${YELLOW}HyDFS stopped${NC}"