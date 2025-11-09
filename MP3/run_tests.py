#!/usr/bin/env python3
"""
HyDFS Demo Master Test Runner
Provides menu interface to run all tests
"""
import subprocess
import sys
import os

TESTS = {
    '1': {
        'name': 'Test 1: Create 5 Files',
        'script': 'test1_create.py',
        'description': 'Create 5 files from dataset sequentially',
        'points': '5%'
    },
    '2': {
        'name': 'Test 2: Get + Verify Replicas',
        'script': 'test2_get_replicas.py',
        'description': 'Get file, verify with diff, check replicas on ring',
        'points': '10%'
    },
    '3': {
        'name': 'Test 3: Post-Failure Re-Replication',
        'script': 'test3_failure.py',
        'description': 'Fail 2 VMs, verify re-replication',
        'points': '5%'
    },
    '4': {
        'name': 'Test 4: Read-My-Writes + Ordering',
        'script': 'test4_read_my_writes.py',
        'description': 'Sequential appends, verify read-my-writes and ordering',
        'points': '15%'
    },
    '5': {
        'name': 'Test 5: Concurrent Appends + Merge',
        'script': 'test5_concurrent_merge.py',
        'description': 'Concurrent appends from 4 VMs, merge, verify',
        'points': '15%'
    }
}

def print_header():
    print("="*70)
    print("HyDFS Demo Test Suite")
    print("CS425 Fall 2025 - Group 7")
    print("="*70)
    print()

def print_menu():
    print("\n" + "="*70)
    print("Test Menu")
    print("="*70)
    print()
    
    for test_id in sorted(TESTS.keys()):
        test = TESTS[test_id]
        print(f"{test_id}. {test['name']} [{test['points']}]")
        print(f"   {test['description']}")
        print()
    
    print("a. Run ALL tests sequentially")
    print("c. Check test scripts exist")
    print("e. Edit configuration")
    print("h. Show help and tips")
    print("q. Quit")
    print()

def check_scripts():
    """Check if all test scripts exist"""
    print("\n" + "="*70)
    print("Checking Test Scripts")
    print("="*70)
    print()
    
    all_exist = True
    for test_id, test in TESTS.items():
        script = test['script']
        exists = os.path.exists(script)
        status = "✓ Found" if exists else "✗ NOT FOUND"
        print(f"Test {test_id}: {script:30s} {status}")
        if not exists:
            all_exist = False
    
    # Check config
    config_exists = os.path.exists('test_config.ini')
    status = "✓ Found" if config_exists else "✗ NOT FOUND"
    print(f"Config:  test_config.ini{' '*18s} {status}")
    
    if not all_exist or not config_exists:
        print("\n⚠  Some files are missing!")
        print("Make sure you're in the correct directory with all test scripts.")
    else:
        print("\n✓ All test scripts found!")
    
    return all_exist

def run_test(test_id):
    """Run a specific test"""
    if test_id not in TESTS:
        print(f"Error: Invalid test ID {test_id}")
        return
    
    test = TESTS[test_id]
    script = test['script']
    
    if not os.path.exists(script):
        print(f"\nError: Script {script} not found!")
        return
    
    print("\n" + "="*70)
    print(f"Running {test['name']}")
    print("="*70)
    print()
    
    try:
        result = subprocess.run(['python3', script])
        
        if result.returncode == 0:
            print("\n✓ Test completed successfully")
        else:
            print("\n⚠  Test exited with errors")
    
    except KeyboardInterrupt:
        print("\n\n⚠  Test interrupted by user")
    except Exception as e:
        print(f"\nError running test: {e}")

def run_all_tests():
    """Run all tests sequentially"""
    print("\n" + "="*70)
    print("Running ALL Tests Sequentially")
    print("="*70)
    print("\nThis will run all 5 tests in order.")
    print("Some tests require manual actions (like failing VMs).")
    print()
    
    response = input("Continue? (y/n): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
        return
    
    for test_id in sorted(TESTS.keys()):
        run_test(test_id)
        
        if test_id != '5':  # Don't prompt after last test
            print("\n" + "-"*70)
            response = input("Continue to next test? (y/n): ").strip().lower()
            if response != 'y':
                print("Stopping test sequence.")
                return
    
    print("\n" + "="*70)
    print("ALL TESTS COMPLETED")
    print("="*70)

def show_help():
    """Show help and tips"""
    print("\n" + "="*70)
    print("HyDFS Demo Help and Tips")
    print("="*70)
    print()
    
    print("BEFORE THE DEMO:")
    print("-"*70)
    print("1. Ensure all 10 VMs are running HyDFS")
    print("   ssh to each VM and run: cd MP3 && python3 main.py --vm-id <N>")
    print()
    print("2. Verify dataset files exist in MP3 directory")
    print("   ls business_*.txt (should see 20 files)")
    print()
    print("3. Edit test_config.ini to customize test parameters")
    print("   Update VMs, filenames, keywords as needed")
    print()
    print("4. Run a practice test to ensure everything works")
    print()
    
    print("\nDURING THE DEMO:")
    print("-"*70)
    print("1. TA will pick files and VMs - update scripts accordingly")
    print("2. For Test 3, have separate terminals open to each VM")
    print("3. For Test 5, show terminal timestamps to prove concurrency")
    print()
    
    print("\nQUICK PARAMETER UPDATES:")
    print("-"*70)
    print("Each test script has parameters at the top:")
    print("  - test1_create.py: WRITER_VM, DATASET_FILES")
    print("  - test2_get_replicas.py: HYDFS_FILE, READER_VM")
    print("  - test3_failure.py: VMS_TO_FAIL")
    print("  - test4_read_my_writes.py: CLIENT_VM, APPEND_FILE_1/2, KEYWORDS")
    print("  - test5_concurrent_merge.py: APPEND_VMS, APPEND_FILES, KEYWORDS")
    print()
    print("Edit these directly in the Python files for quick changes.")
    print()
    
    print("\nTROUBLESHOOTING:")
    print("-"*70)
    print("- If a test fails, check HyDFS is running on all VMs")
    print("- Verify membership protocol is working (use list_mem_ids)")
    print("- Check logs in machine.<vm_id>.log files")
    print("- Ensure dataset files are in the correct directory")
    print()

def edit_config():
    """Open configuration file"""
    if not os.path.exists('test_config.ini'):
        print("\nConfiguration file not found!")
        return
    
    editor = os.environ.get('EDITOR', 'nano')
    print(f"\nOpening test_config.ini with {editor}...")
    print("(Close editor to return to menu)")
    
    try:
        subprocess.run([editor, 'test_config.ini'])
    except Exception as e:
        print(f"Error opening editor: {e}")
        print("\nYou can manually edit test_config.ini")

def main():
    print_header()
    
    # Check if we're in the right directory
    if not os.path.exists('business_1.txt'):
        print("⚠  Warning: Dataset files not found in current directory.")
        print("Please run this script from the MP3 directory.\n")
    
    while True:
        print_menu()
        choice = input("Select option: ").strip().lower()
        
        if choice == 'q':
            print("\nExiting demo test suite. Good luck with your demo!")
            break
        
        elif choice in TESTS:
            run_test(choice)
        
        elif choice == 'a':
            run_all_tests()
        
        elif choice == 'c':
            check_scripts()
        
        elif choice == 'e':
            edit_config()
        
        elif choice == 'h':
            show_help()
        
        else:
            print(f"\nInvalid option: {choice}")
        
        if choice != 'q':
            input("\nPress ENTER to continue...")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nExiting...")
        sys.exit(0)