#!/bin/bash
# HyDFS Helper Script - Common operations and test scenarios

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "=================================="
echo "  HyDFS Helper Script"
echo "=================================="
echo ""

show_menu() {
    echo -e "${BLUE}Available Operations:${NC}"
    echo "  1. Generate test files"
    echo "  2. Quick test (create, append, get, merge)"
    echo "  3. Consistency test (concurrent appends)"
    echo "  4. Check all VMs status"
    echo "  5. Clean up test files"
    echo "  6. Show logs"
    echo "  0. Exit"
    echo ""
}

generate_test_files() {
    echo -e "${GREEN}Generating test files...${NC}"
    
    # Small test file
    echo "This is a test file for HyDFS" > test_small.txt
    echo "Line 2 of the test file" >> test_small.txt
    echo "Line 3 of the test file" >> test_small.txt
    
    # Medium test file (1KB)
    python3 -c "print('A' * 1024)" > test_1kb.txt
    
    # Large test file (100KB)
    python3 -c "print('B' * 102400)" > test_100kb.txt
    
    # Append files
    for i in {1..5}; do
        echo "Append $i data" > append_${i}.txt
    done
    
    echo -e "${GREEN}Test files generated:${NC}"
    echo "  - test_small.txt (3 lines)"
    echo "  - test_1kb.txt (1 KB)"
    echo "  - test_100kb.txt (100 KB)"
    echo "  - append_1.txt to append_5.txt"
}

quick_test() {
    echo -e "${GREEN}Running quick test...${NC}"
    echo ""
    
    echo "1. Creating file..."
    echo "   Command: create test_small.txt hydfs_quicktest.txt"
    echo ""
    
    echo "2. Appending to file..."
    echo "   Command: append append_1.txt hydfs_quicktest.txt"
    echo ""
    
    echo "3. Getting file..."
    echo "   Command: get hydfs_quicktest.txt retrieved.txt"
    echo ""
    
    echo "4. Listing replicas..."
    echo "   Command: ls hydfs_quicktest.txt"
    echo ""
    
    echo "5. Merging file..."
    echo "   Command: merge hydfs_quicktest.txt"
    echo ""
    
    echo -e "${YELLOW}Copy these commands into the HyDFS prompt${NC}"
}

consistency_test() {
    echo -e "${GREEN}Consistency Test Instructions:${NC}"
    echo ""
    
    echo "Step 1: Create initial file"
    echo "  create test_small.txt consistency_test.txt"
    echo ""
    
    echo "Step 2: Sequential appends (tests per-client ordering)"
    for i in {1..5}; do
        echo "  append append_${i}.txt consistency_test.txt"
    done
    echo ""
    
    echo "Step 3: Get and verify"
    echo "  get consistency_test.txt result.txt"
    echo "  cat result.txt  # Should show all appends in order"
    echo ""
    
    echo "Step 4: Merge"
    echo "  merge consistency_test.txt"
    echo ""
    
    echo "Step 5: Verify all replicas identical"
    echo "  getfromreplica fa25-cs425-a701.cs.illinois.edu consistency_test.txt vm1.txt"
    echo "  getfromreplica fa25-cs425-a702.cs.illinois.edu consistency_test.txt vm2.txt"
    echo "  getfromreplica fa25-cs425-a703.cs.illinois.edu consistency_test.txt vm3.txt"
    echo "  diff vm1.txt vm2.txt  # Should show no differences"
}

check_vms() {
    echo -e "${GREEN}Checking VM status...${NC}"
    echo ""
    
    HOSTS=(
        "fa25-cs425-a701.cs.illinois.edu"
        "fa25-cs425-a702.cs.illinois.edu"
        "fa25-cs425-a703.cs.illinois.edu"
        "fa25-cs425-a704.cs.illinois.edu"
        "fa25-cs425-a705.cs.illinois.edu"
        "fa25-cs425-a706.cs.illinois.edu"
        "fa25-cs425-a707.cs.illinois.edu"
        "fa25-cs425-a708.cs.illinois.edu"
        "fa25-cs425-a709.cs.illinois.edu"
        "fa25-cs425-a710.cs.illinois.edu"
    )
    
    for i in "${!HOSTS[@]}"; do
        VM_ID=$((i + 1))
        HOST="${HOSTS[$i]}"
        
        # Check if HyDFS port is listening
        if timeout 2 bash -c "echo > /dev/tcp/${HOST}/7000" 2>/dev/null; then
            echo -e "  VM${VM_ID} (${HOST}): ${GREEN}RUNNING${NC}"
        else
            echo -e "  VM${VM_ID} (${HOST}): ${RED}NOT RUNNING${NC}"
        fi
    done
}

clean_test_files() {
    echo -e "${YELLOW}Cleaning up test files...${NC}"
    
    rm -f test_small.txt test_1kb.txt test_100kb.txt
    rm -f append_*.txt
    rm -f retrieved.txt result.txt
    rm -f vm*.txt
    
    echo -e "${GREEN}Test files cleaned up${NC}"
}

show_logs() {
    echo -e "${GREEN}Recent log entries:${NC}"
    echo ""
    
    if [ -f "../MP2/machine.${1}.log" ]; then
        tail -20 ../MP2/machine.${1}.log
    else
        echo -e "${RED}Log file not found${NC}"
        echo "Make sure MP2 logs are in ../MP2/"
    fi
}

# Main loop
while true; do
    show_menu
    read -p "Select option (0-6): " choice
    echo ""
    
    case $choice in
        1)
            generate_test_files
            ;;
        2)
            quick_test
            ;;
        3)
            consistency_test
            ;;
        4)
            check_vms
            ;;
        5)
            clean_test_files
            ;;
        6)
            read -p "Enter VM ID (1-10): " vm_id
            show_logs $vm_id
            ;;
        0)
            echo "Goodbye!"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid option${NC}"
            ;;
    esac
    
    echo ""
    read -p "Press Enter to continue..."
    echo ""
done