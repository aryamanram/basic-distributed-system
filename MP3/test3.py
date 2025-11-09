#!/usr/bin/env python3
"""
Test 3: Post-Failure Re-Replication - SIMPLIFIED
Shows initial state, waits for manual VM failure, then shows final state
Only sends: ls, list_mem_ids, liststore commands
"""
import socket
import time

# ============= CONFIGURABLE PARAMETERS =============
HYDFS_FILE = "hydfs_business_1.txt"

# VMs to fail (for reference only - you fail them manually)
VMS_TO_FAIL = [1, 2]

# VM to use for commands
VERIFICATION_VM = 3

# Pick a new replica VM to check liststore after re-replication
NEW_REPLICA_VM = 4

# Wait time after failure
WAIT_TIME = 15

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

def send_command(vm_id, command):
    """Send command to VM and return response"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(60.0)
        sock.connect((VM_HOSTS[vm_id], HYDFS_CONTROL_PORT))
        sock.sendall(command.encode('utf-8'))
        response = sock.recv(65536).decode('utf-8')
        sock.close()
        return response
    except Exception as e:
        return f"Error: {e}"

def main():
    print("="*70)
    print("TEST 3: Post-Failure Re-Replication")
    print("="*70)
    print(f"VMs to fail manually: {VMS_TO_FAIL}\n")
    
    # Show initial state
    print("\nINITIAL STATE (before failures):")
    print("="*70)
    
    print(f"\nls {HYDFS_FILE}")
    cmd = f"ls {HYDFS_FILE}"
    response = send_command(VERIFICATION_VM, cmd)
    print(response)
    print("-"*70)
    
    # Wait for manual failures
    print("\n⚠  ACTION REQUIRED:")
    print(f"   Manually fail VMs {VMS_TO_FAIL}")
    print(f"   (SSH to each VM and Ctrl+C the HyDFS process)")
    input("\nPress ENTER once VMs are failed...")
    
    # Wait for re-replication
    print(f"\nWaiting {WAIT_TIME} seconds for failure detection and re-replication...")
    time.sleep(WAIT_TIME)
    
    # Show final state
    print("\n\nFINAL STATE (after re-replication):")
    print("="*70)
    
    print(f"\nls {HYDFS_FILE}")
    cmd = f"ls {HYDFS_FILE}"
    response = send_command(VERIFICATION_VM, cmd)
    print(response)
    print("-"*70)
    
    print(f"\nlist_mem_ids")
    cmd = "list_mem_ids"
    response = send_command(VERIFICATION_VM, cmd)
    print(response)
    print("-"*70)
    
    print(f"\nliststore on VM{NEW_REPLICA_VM}")
    cmd = "liststore"
    response = send_command(NEW_REPLICA_VM, cmd)
    print(response)
    print("-"*70)

if __name__ == "__main__":
    main()