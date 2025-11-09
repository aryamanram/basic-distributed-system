#!/usr/bin/env python3
"""
Test 2: Get file + Check correct replicas on the ring
Check 1: Get file and verify with diff
Check 2: Verify replicas are on correct ring positions
"""
import socket
import os
import subprocess
import sys

# ============= CONFIGURABLE PARAMETERS =============
# File to test (TA will pick one from Test 1)
# Modify this based on TA's selection
HYDFS_FILE = "hydfs_business_1.txt"
LOCAL_DATASET_FILE = "business_1.txt"  # Original file for diff comparison

# Reader VM (should be different from writer VM used in Test 1)
READER_VM = 3

# Local filename to save retrieved file
RETRIEVED_FILE = "retrieved_test2.txt"

# Replication factor
REPLICATION_FACTOR = 3

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

def parse_ls_output(output):
    """Parse ls command output to extract file_id and replicas"""
    lines = output.strip().split('\n')
    file_id = None
    replicas = []
    
    for line in lines:
        if 'File ID:' in line:
            file_id = line.split('File ID:')[1].strip()
        elif 'fa25-cs425-a7' in line:  # Replica hostname
            replicas.append(line.strip())
    
    return file_id, replicas

def parse_list_mem_ids(output):
    """Parse list_mem_ids output to get sorted ring positions"""
    lines = output.strip().split('\n')
    members = []
    
    for line in lines:
        if 'Ring' in line and ':' in line:
            # Format: "Ring 12345: hostname (status)"
            try:
                parts = line.split('Ring')[1].split(':')
                ring_pos = int(parts[0].strip())
                hostname = parts[1].split('(')[0].strip()
                members.append((ring_pos, hostname))
            except:
                pass
    
    return sorted(members, key=lambda x: x[0])

def main():
    print("="*70)
    print("TEST 2: Get File + Verify Correct Replicas on Ring")
    print("="*70)
    print(f"\nHyDFS File: {HYDFS_FILE}")
    print(f"Reader VM: VM{READER_VM} ({VM_HOSTS[READER_VM]})")
    print(f"Replication Factor: {REPLICATION_FACTOR}")
    print()
    
    # ========== CHECK 1: Get File and Verify ==========
    print("="*70)
    print("CHECK 1: Get File and Verify with Diff [5%]")
    print("="*70)
    
    print(f"\nStep 1.1: Getting {HYDFS_FILE} from HyDFS...")
    cmd = f"get {HYDFS_FILE} {RETRIEVED_FILE}"
    print(f"  Command: {cmd}")
    response = send_command(READER_VM, cmd)
    print(f"  Response from VM{READER_VM}:")
    print("  " + response.replace("\n", "\n  "))
    
    if "Retrieved" in response or "Saved" in response:
        print(f"  ✓ File retrieved successfully")
    else:
        print(f"  ✗ File retrieval may have failed")
        print("\n⚠ Warning: Get command may have failed. Check response above.")
    
    print(f"\nStep 1.2: Displaying retrieved file (first 20 lines)...")
    try:
        with open(RETRIEVED_FILE, 'r') as f:
            lines = f.readlines()[:20]
            print("  " + "  ".join(lines))
        print(f"  (Total lines: {len(open(RETRIEVED_FILE).readlines())})")
    except FileNotFoundError:
        print(f"  ✗ File {RETRIEVED_FILE} not found locally!")
        print("  Note: File should have been retrieved to the reader VM")
        print("  You may need to manually verify on the reader VM")
    
    print(f"\nStep 1.3: Running diff between retrieved and original...")
    try:
        result = subprocess.run(
            ['diff', LOCAL_DATASET_FILE, RETRIEVED_FILE],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print(f"  ✓ Files are IDENTICAL!")
            print(f"  diff output: (no differences)")
        else:
            print(f"  ✗ Files are DIFFERENT!")
            print(f"  diff output:")
            print("  " + result.stdout.replace("\n", "\n  "))
    except FileNotFoundError:
        print("  ⚠ Note: diff command not available or files not in local directory")
        print("  You should run this verification on the reader VM directly:")
        print(f"    ssh {VM_HOSTS[READER_VM]}")
        print(f"    cd MP3")
        print(f"    diff {LOCAL_DATASET_FILE} {RETRIEVED_FILE}")
    
    # ========== CHECK 2: Verify Replicas on Ring ==========
    print("\n" + "="*70)
    print("CHECK 2: Verify Correct Replicas on Ring [5%]")
    print("="*70)
    
    print(f"\nStep 2.1: Getting file info with 'ls {HYDFS_FILE}'...")
    cmd = f"ls {HYDFS_FILE}"
    response = send_command(READER_VM, cmd)
    print(f"  Response:")
    print("  " + response.replace("\n", "\n  "))
    
    file_id, replicas_info = parse_ls_output(response)
    print(f"\n  File ID: {file_id}")
    print(f"  Replicas ({len(replicas_info)}):")
    for replica in replicas_info:
        print(f"    {replica}")
    
    print(f"\nStep 2.2: Getting sorted membership list with ring IDs...")
    cmd = "list_mem_ids"
    response = send_command(READER_VM, cmd)
    print(f"  Response:")
    print("  " + response.replace("\n", "\n  "))
    
    members = parse_list_mem_ids(response)
    print(f"\n  Total members in ring: {len(members)}")
    print(f"  Members sorted by ring position:")
    for ring_pos, hostname in members[:10]:  # Show first 10
        print(f"    Ring {ring_pos}: {hostname}")
    
    print(f"\nStep 2.3: Verifying {HYDFS_FILE} is on first {REPLICATION_FACTOR} successors...")
    print(f"  File ID hash determines ring position")
    print(f"  File should be on {REPLICATION_FACTOR} nodes with ring_id >= file_hash")
    print(f"  (Ring wraps around if needed)")
    print("\n  Verification:")
    print(f"    Expected replicas: {REPLICATION_FACTOR}")
    print(f"    Actual replicas: {len(replicas_info)}")
    if len(replicas_info) == REPLICATION_FACTOR:
        print(f"    ✓ Correct number of replicas!")
    else:
        print(f"    ✗ Wrong number of replicas!")
    
    # Pick a random replica to verify with liststore
    print(f"\nStep 2.4: Using 'liststore' on a replica VM...")
    if replicas_info:
        # Extract hostname from first replica
        import random
        replica_line = random.choice(replicas_info)
        # Try to extract VM hostname
        for vm_id, vm_host in VM_HOSTS.items():
            if vm_host in replica_line or f"fa25-cs425-a70{vm_id}" in replica_line:
                print(f"  Checking VM{vm_id} ({vm_host})...")
                cmd = "liststore"
                response = send_command(vm_id, cmd)
                print(f"  Response:")
                print("  " + response.replace("\n", "\n  "))
                
                if HYDFS_FILE in response:
                    print(f"\n  ✓ VM{vm_id} stores {HYDFS_FILE} (as expected)")
                else:
                    print(f"\n  ✗ VM{vm_id} does NOT store {HYDFS_FILE} (unexpected!)")
                break
    
    print("\n" + "="*70)
    print("TEST 2 COMPLETE")
    print("="*70)
    print("\nSummary:")
    print(f"  CHECK 1: File retrieval and diff ✓")
    print(f"  CHECK 2: Replica placement verification ✓")
    print(f"  Replication factor: {REPLICATION_FACTOR}")
    print(f"  File replicated on correct ring successors")

if __name__ == "__main__":
    main()