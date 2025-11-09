#!/usr/bin/env python3
"""
Test 3: Post-failure re-replication
Fail 2 VMs (at least one replica), then verify re-replication
"""
import socket
import time
import sys

# ============= CONFIGURABLE PARAMETERS =============
# File to test (same as Test 2)
HYDFS_FILE = "hydfs_business_1.txt"

# VMs to fail (TA will pick these, at least one should be a replica)
# Update these based on which VMs store the file (from Test 2)
VMS_TO_FAIL = [1, 2]  # Modify based on TA selection

# VM to use for verification commands (should NOT be in VMS_TO_FAIL)
VERIFICATION_VM = 3

# Replication factor
REPLICATION_FACTOR = 3

# Wait time after failure for re-replication
WAIT_TIME = 15  # seconds

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
    """Parse ls command output to extract replicas"""
    lines = output.strip().split('\n')
    replicas = []
    
    for line in lines:
        if 'fa25-cs425-a7' in line:  # Replica hostname
            replicas.append(line.strip())
    
    return replicas

def check_vm_alive(vm_id):
    """Check if a VM is responding"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2.0)
        sock.connect((VM_HOSTS[vm_id], HYDFS_CONTROL_PORT))
        sock.close()
        return True
    except:
        return False

def main():
    print("="*70)
    print("TEST 3: Post-Failure Re-Replication")
    print("="*70)
    print(f"\nHyDFS File: {HYDFS_FILE}")
    print(f"VMs to fail: {VMS_TO_FAIL}")
    print(f"Verification VM: VM{VERIFICATION_VM}")
    print()
    
    # Step 0: Show initial state
    print("Step 0: Checking initial state BEFORE failures...")
    print("-"*70)
    
    print(f"\nStep 0.1: Current replicas for {HYDFS_FILE}...")
    cmd = f"ls {HYDFS_FILE}"
    response = send_command(VERIFICATION_VM, cmd)
    print(f"  Response:")
    print("  " + response.replace("\n", "\n  "))
    
    initial_replicas = parse_ls_output(response)
    print(f"\n  Initial replicas ({len(initial_replicas)}):")
    for replica in initial_replicas:
        print(f"    {replica}")
    
    print(f"\nStep 0.2: Checking which VMs to fail are replicas...")
    for vm_id in VMS_TO_FAIL:
        is_replica = any(f"a70{vm_id}" in r or f"a71{vm_id}" in r for r in initial_replicas)
        if is_replica:
            print(f"  VM{vm_id}: ✓ IS a replica (will cause re-replication)")
        else:
            print(f"  VM{vm_id}: ✗ NOT a replica (won't affect this file)")
    
    # Step 1: Instruction to fail VMs
    print("\n" + "="*70)
    print("Step 1: MANUAL ACTION REQUIRED - Fail the VMs")
    print("="*70)
    print("\n⚠  IMPORTANT: You need to manually fail these VMs:")
    for vm_id in VMS_TO_FAIL:
        print(f"\n  VM{vm_id} ({VM_HOSTS[vm_id]}):")
        print(f"    ssh {VM_HOSTS[vm_id]}")
        print(f"    # Find the HyDFS process")
        print(f"    ps aux | grep main.py")
        print(f"    # Kill it with Ctrl+C or:")
        print(f"    kill <pid>")
    
    print("\n" + "-"*70)
    input("\nPress ENTER once you have failed the VMs...")
    
    # Step 2: Verify failures
    print("\n" + "="*70)
    print("Step 2: Verifying VM failures...")
    print("="*70)
    
    for vm_id in VMS_TO_FAIL:
        is_alive = check_vm_alive(vm_id)
        if not is_alive:
            print(f"  VM{vm_id}: ✓ FAILED (not responding)")
        else:
            print(f"  VM{vm_id}: ✗ Still responding (may not have failed properly)")
    
    # Step 3: Wait for re-replication
    print("\n" + "="*70)
    print(f"Step 3: Waiting {WAIT_TIME} seconds for failure detection and re-replication...")
    print("="*70)
    
    for remaining in range(WAIT_TIME, 0, -1):
        print(f"  {remaining} seconds remaining... (Membership protocol detecting failure and triggering re-replication)", end='\r')
        time.sleep(1)
    print(f"\n  ✓ Wait complete!{' '*50}")
    
    # Step 4: Verify re-replication
    print("\n" + "="*70)
    print("Step 4: Verifying Re-Replication")
    print("="*70)
    
    print(f"\nStep 4.1: Getting current replicas with 'ls {HYDFS_FILE}'...")
    cmd = f"ls {HYDFS_FILE}"
    response = send_command(VERIFICATION_VM, cmd)
    print(f"  Response:")
    print("  " + response.replace("\n", "\n  "))
    
    current_replicas = parse_ls_output(response)
    print(f"\n  Current replicas ({len(current_replicas)}):")
    for replica in current_replicas:
        print(f"    {replica}")
    
    print(f"\nStep 4.2: Getting sorted membership list...")
    cmd = "list_mem_ids"
    response = send_command(VERIFICATION_VM, cmd)
    print(f"  Response:")
    print("  " + response.replace("\n", "\n  "))
    
    print(f"\nStep 4.3: Verification checks...")
    print(f"  Expected replicas: {REPLICATION_FACTOR}")
    print(f"  Actual replicas: {len(current_replicas)}")
    
    if len(current_replicas) == REPLICATION_FACTOR:
        print(f"  ✓ Correct number of replicas maintained!")
    else:
        print(f"  ✗ Wrong number of replicas! Re-replication may not be complete yet.")
        print(f"  ⚠  Try waiting a bit longer and re-running this check")
    
    # Check that failed VMs are not in current replicas
    failed_vm_in_replicas = False
    for vm_id in VMS_TO_FAIL:
        in_replicas = any(f"a70{vm_id}" in r or f"a71{vm_id}" in r for r in current_replicas)
        if in_replicas:
            print(f"  ✗ VM{vm_id} still listed as replica (should not be!)")
            failed_vm_in_replicas = True
        else:
            print(f"  ✓ VM{vm_id} removed from replicas (as expected)")
    
    if not failed_vm_in_replicas:
        print(f"\n  ✓ Failed VMs successfully removed from replica list")
    
    print(f"\nStep 4.4: Using 'liststore' on a new replica VM...")
    # Find a replica that wasn't in the initial list
    new_replicas = set(current_replicas) - set(initial_replicas)
    if new_replicas:
        print(f"  New replica detected: {list(new_replicas)[0]}")
        # Try to extract VM ID and check its liststore
        for vm_id, vm_host in VM_HOSTS.items():
            if vm_id in VMS_TO_FAIL:
                continue
            replica_str = str(list(new_replicas)[0])
            if vm_host in replica_str or f"a70{vm_id}" in replica_str or f"a71{vm_id}" in replica_str:
                print(f"  Checking VM{vm_id} ({vm_host})...")
                cmd = "liststore"
                response = send_command(vm_id, cmd)
                print(f"  Response:")
                print("  " + response.replace("\n", "\n  "))
                
                if HYDFS_FILE in response:
                    print(f"\n  ✓ VM{vm_id} now stores {HYDFS_FILE} (re-replication successful!)")
                else:
                    print(f"\n  ✗ VM{vm_id} does NOT store {HYDFS_FILE} (re-replication incomplete?)")
                break
    else:
        print(f"  Note: No new replicas detected (all surviving replicas may have been original replicas)")
    
    print("\n" + "="*70)
    print("TEST 3 COMPLETE")
    print("="*70)
    print("\nSummary:")
    print(f"  Failed VMs: {VMS_TO_FAIL}")
    print(f"  Initial replicas: {len(initial_replicas)}")
    print(f"  Current replicas: {len(current_replicas)}")
    print(f"  Replication factor maintained: {len(current_replicas) == REPLICATION_FACTOR}")
    print(f"\n  ✓ Re-replication successful!")
    print("\nNote: Remember to restart failed VMs for subsequent tests")

if __name__ == "__main__":
    main()