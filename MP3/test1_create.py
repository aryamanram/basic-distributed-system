#!/usr/bin/env python3
"""
Test 1: Create 5 files one after the other
Configurable parameters at the top
"""
import socket
import time
import sys

# ============= CONFIGURABLE PARAMETERS =============
# VM to use for creating files
WRITER_VM = 1

# Files from dataset to create (modify these as needed)
DATASET_FILES = [
    "business_1.txt",
    "business_2.txt", 
    "business_3.txt",
    "business_4.txt",
    "business_5.txt"
]

# Corresponding HyDFS filenames
HYDFS_FILES = [
    "hydfs_business_1.txt",
    "hydfs_business_2.txt",
    "hydfs_business_3.txt",
    "hydfs_business_4.txt",
    "hydfs_business_5.txt"
]

# Wait time after creates complete (for background writes)
WAIT_TIME = 5  # seconds

HYDFS_CONTROL_PORT = 9091

VM_HOSTS = {
    1: "fa25-cs425-a701.cs.illinois.edu",
    2: "fa25-cs425-a702.cs.illinois.edu",
    3: "fa25-cs425-a703.cs.illinois.edu",
    4: "fa25-cs425-a704.cs.illinois.edu",
    5: "fa25-cs425-a705.cs.illinois.edu",
    6: "fa25-cs425-a706.cs.illinois.edu",
    7: "fa25-cs425-a707.cs.illinois.edu",
    8: "fa25-cs425-a708.cs.illinois.edu",
    9: "fa25-cs425-a709.cs.illinois.edu",
    10: "fa25-cs425-a710.cs.illinois.edu"
}
# ===================================================

def send_command(vm_id, command, timeout=60.0):
    """Send command to VM and return response"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((VM_HOSTS[vm_id], HYDFS_CONTROL_PORT))
        sock.sendall(command.encode('utf-8'))
        response = sock.recv(65536).decode('utf-8')
        sock.close()
        return response
    except Exception as e:
        return f"Error: {e}"

def main():
    print("="*70)
    print("TEST 1: Creating 5 Files")
    print("="*70)
    print(f"\nWriter VM: VM{WRITER_VM} ({VM_HOSTS[WRITER_VM]})")
    print(f"Files to create: {len(DATASET_FILES)}")
    print()
    
    # Verify dataset files exist
    print("Step 0: Verifying dataset files exist...")
    for local_file in DATASET_FILES:
        try:
            with open(local_file, 'r') as f:
                pass
            print(f"  ✓ {local_file} found")
        except FileNotFoundError:
            print(f"  ✗ {local_file} NOT FOUND!")
            print(f"\nError: Dataset file {local_file} not found in current directory")
            print("Please ensure you're running this from the MP3 directory")
            sys.exit(1)
    
    print("\n" + "-"*70)
    
    # Create each file
    for i, (local_file, hydfs_file) in enumerate(zip(DATASET_FILES, HYDFS_FILES), 1):
        print(f"\nStep {i}: Creating {hydfs_file}")
        print(f"  Local file: {local_file}")
        print(f"  HyDFS file: {hydfs_file}")
        
        cmd = f"create {local_file} {hydfs_file}"
        print(f"  Command: {cmd}")
        
        response = send_command(WRITER_VM, cmd)
        print(f"  Response from VM{WRITER_VM}:")
        print("  " + response.replace("\n", "\n  "))
        
        # Check if successful
        if "created" in response.lower() or "success" in response.lower():
            print(f"  ✓ File {hydfs_file} created successfully")
        else:
            print(f"  ⚠ File creation may have failed - check response")
        
        print("-"*70)
    
    # Wait for background writes
    print(f"\nStep 6: Waiting {WAIT_TIME} seconds for background writes to complete...")
    for remaining in range(WAIT_TIME, 0, -1):
        print(f"  {remaining} seconds remaining...", end='\r')
        time.sleep(1)
    print(f"  Background writes complete!{' '*30}")
    
    print("\n" + "="*70)
    print("TEST 1 COMPLETE")
    print("="*70)
    print("\nSummary:")
    print(f"  Created {len(HYDFS_FILES)} files on VM{WRITER_VM}")
    print("\nFiles created:")
    for local_file, hydfs_file in zip(DATASET_FILES, HYDFS_FILES):
        print(f"  {local_file} → {hydfs_file}")
    print("\nNext: Run Test 2 to verify file retrieval and replication")

if __name__ == "__main__":
    main()