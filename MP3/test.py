"""
Test script for HyDFS functionality
"""
import os
import time
import random
import string

def generate_test_file(filename, size_kb=1):
    """
    Generate a test file with random content.
    """
    content = ''.join(random.choices(string.ascii_letters + string.digits + '\n', k=size_kb * 1024))
    with open(filename, 'w') as f:
        f.write(content)
    print(f"Generated test file: {filename} ({size_kb} KB)")

def test_basic_operations():
    """
    Test basic file operations.
    """
    print("\n=== Testing Basic Operations ===\n")
    
    # Generate test files
    generate_test_file('test1.txt', 1)
    generate_test_file('append1.txt', 1)
    
    print("\nTest 1: Create file")
    print("Run: create test1.txt hydfs_test1.txt")
    
    print("\nTest 2: Get file")
    print("Run: get hydfs_test1.txt retrieved1.txt")
    
    print("\nTest 3: Append to file")
    print("Run: append append1.txt hydfs_test1.txt")
    
    print("\nTest 4: List file replicas")
    print("Run: ls hydfs_test1.txt")
    
    print("\nTest 5: List stored files")
    print("Run: liststore")
    
    print("\nTest 6: Merge file")
    print("Run: merge hydfs_test1.txt")

def test_consistency():
    """
    Test consistency guarantees.
    """
    print("\n=== Testing Consistency ===\n")
    
    # Generate multiple append files
    for i in range(5):
        generate_test_file(f'append_{i}.txt', 1)
    
    print("\nTest 1: Sequential appends from same client")
    print("Run: create test1.txt consistency_test.txt")
    for i in range(5):
        print(f"Run: append append_{i}.txt consistency_test.txt")
    
    print("\nTest 2: Get and verify ordering")
    print("Run: get consistency_test.txt result.txt")
    print("Check: Verify appends appear in order")
    
    print("\nTest 3: Multi-append (concurrent)")
    print("Generate 3 different files on 3 VMs:")
    print("  VM1: append_vm1.txt")
    print("  VM2: append_vm2.txt")
    print("  VM3: append_vm3.txt")
    print("Run: multiappend multitest.txt vm1,vm2,vm3 append_vm1.txt,append_vm2.txt,append_vm3.txt")
    print("Run: merge multitest.txt")
    
    print("\nTest 4: Verify replicas are identical")
    print("Run: getfromreplica fa25-cs425-a701.cs.illinois.edu multitest.txt test_vm1.txt")
    print("Run: getfromreplica fa25-cs425-a702.cs.illinois.edu multitest.txt test_vm2.txt")
    print("Run: getfromreplica fa25-cs425-a703.cs.illinois.edu multitest.txt test_vm3.txt")
    print("Check: diff test_vm1.txt test_vm2.txt (should be identical)")

def test_failure_handling():
    """
    Test failure handling and re-replication.
    """
    print("\n=== Testing Failure Handling ===\n")
    
    generate_test_file('failure_test.txt', 10)
    
    print("\nTest 1: Create file and check replicas")
    print("Run: create failure_test.txt hydfs_failure.txt")
    print("Run: ls hydfs_failure.txt")
    print("Note: Record which 3 VMs have the file")
    
    print("\nTest 2: Simulate failure")
    print("Action: Kill one of the VMs storing the file (Ctrl+C)")
    print("Wait: ~10 seconds for failure detection and re-replication")
    
    print("\nTest 3: Verify re-replication")
    print("Run: ls hydfs_failure.txt")
    print("Check: File should still have 3 replicas")
    
    print("\nTest 4: Verify file is still accessible")
    print("Run: get hydfs_failure.txt retrieved_after_failure.txt")
    print("Check: Compare with original file")

def test_rebalancing():
    """
    Test rebalancing when nodes join.
    """
    print("\n=== Testing Rebalancing ===\n")
    
    # Generate multiple test files
    for i in range(10):
        generate_test_file(f'rebalance_{i}.txt', 1)
    
    print("\nTest 1: Create files with initial nodes")
    print("Start with 8 VMs")
    for i in range(10):
        print(f"Run: create rebalance_{i}.txt hydfs_rebal_{i}.txt")
    
    print("\nTest 2: Record file distribution")
    print("Run on each VM: liststore")
    print("Note: Record how many files each VM has")
    
    print("\nTest 3: Add new nodes")
    print("Action: Start 2 more VMs")
    print("Wait: ~10 seconds for rebalancing")
    
    print("\nTest 4: Verify rebalancing")
    print("Run on each VM: liststore")
    print("Check: Files should be redistributed to new nodes")
    
    print("\nTest 5: Verify files are still accessible")
    for i in range(10):
        print(f"Run: get hydfs_rebal_{i}.txt retrieved_rebal_{i}.txt")

def test_merge_performance():
    """
    Test merge performance with varying append counts.
    """
    print("\n=== Testing Merge Performance ===\n")
    
    print("\nTest for varying numbers of concurrent appends (1, 2, 5, 10)")
    
    for num_appends in [1, 2, 5, 10]:
        print(f"\nTest with {num_appends} concurrent appends:")
        
        # Generate test files
        generate_test_file('merge_base.txt', 128)  # 128KB base file
        for i in range(num_appends):
            generate_test_file(f'merge_append_{i}.txt', 4)  # 4KB appends
        
        print(f"1. Create: create merge_base.txt merge_test_{num_appends}.txt")
        
        # Generate multiappend command
        vms = ','.join([f'vm{i+1}' for i in range(num_appends)])
        files = ','.join([f'merge_append_{i}.txt' for i in range(num_appends)])
        print(f"2. MultiAppend: multiappend merge_test_{num_appends}.txt {vms} {files}")
        
        print(f"3. Merge: merge merge_test_{num_appends}.txt")
        print("4. Record merge time")
        
        print(f"5. Second merge: merge merge_test_{num_appends}.txt")
        print("6. Record second merge time (should be faster)")
        print()

def generate_measurement_guide():
    """
    Generate guide for measurements required in report.
    """
    print("\n=== Measurement Guide for Report ===\n")
    
    print("Measurement 1: Re-replication overheads")
    print("- Preload system with 100 files")
    print("- Fail one node")
    print("- Measure time until re-replication complete")
    print("- Measure bandwidth used")
    print("- Repeat for file sizes: 100KB, 250KB, 500KB, 750KB, 1MB")
    print()
    
    print("Measurement 2: Rebalancing overheads")
    print("- Preload with X files of 128KB each")
    print("- X values: 10, 50, 100, 200")
    print("- Add a new node")
    print("- Measure bandwidth for rebalancing")
    print()
    
    print("Measurement 3: Merge performance")
    print("- Create file with 128KB initial size")
    print("- Concurrent appends: 1, 2, 5, 10 clients")
    print("- Append sizes: 4KB and 32KB")
    print("- Measure merge completion time")
    print()
    
    print("Measurement 4: Subsequent merge performance")
    print("- Same setup as Measurement 3")
    print("- Call merge twice")
    print("- Compare first and second merge times")

def main():
    print("="*70)
    print("HyDFS Test Suite")
    print("="*70)
    
    while True:
        print("\n\nTest Categories:")
        print("1. Basic Operations")
        print("2. Consistency")
        print("3. Failure Handling")
        print("4. Rebalancing")
        print("5. Merge Performance")
        print("6. Measurement Guide")
        print("7. Generate All Test Files")
        print("0. Exit")
        
        choice = input("\nSelect test category (0-7): ").strip()
        
        if choice == '1':
            test_basic_operations()
        elif choice == '2':
            test_consistency()
        elif choice == '3':
            test_failure_handling()
        elif choice == '4':
            test_rebalancing()
        elif choice == '5':
            test_merge_performance()
        elif choice == '6':
            generate_measurement_guide()
        elif choice == '7':
            print("\nGenerating all test files...")
            generate_test_file('test1.txt', 1)
            for i in range(5):
                generate_test_file(f'append_{i}.txt', 1)
            generate_test_file('failure_test.txt', 10)
            for i in range(10):
                generate_test_file(f'rebalance_{i}.txt', 1)
            generate_test_file('merge_base.txt', 128)
            for i in range(10):
                generate_test_file(f'merge_append_{i}.txt', 4)
            print("All test files generated!")
        elif choice == '0':
            break
        else:
            print("Invalid choice")

if __name__ == '__main__':
    main()