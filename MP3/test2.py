#!/usr/bin/env python3
"""
Test 2: Get + List Replicas - SIMPLIFIED
Only sends: get, ls, list_mem_ids, liststore commands
"""
import socket

# ============= CONFIGURABLE PARAMETERS =============
READER_VM = 3

HYDFS_FILE = "hydfs_business_1.txt"
RETRIEVED_FILE = "retrieved_test2.txt"

# Pick one replica VM to check liststore (TA will pick)
REPLICA_VM = 1

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
    print("TEST 2: Get + List Replicas")
    print("="*70)
    print(f"Reader VM: VM{READER_VM}\n")
    
    # Get file
    print(f"\n1. get {HYDFS_FILE} {RETRIEVED_FILE}")
    cmd = f"get {HYDFS_FILE} {RETRIEVED_FILE}"
    response = send_command(READER_VM, cmd)
    print(response)
    print("-"*70)
    
    # List replicas
    print(f"\n2. ls {HYDFS_FILE}")
    cmd = f"ls {HYDFS_FILE}"
    response = send_command(READER_VM, cmd)
    print(response)
    print("-"*70)
    
    # List membership IDs
    print(f"\n3. list_mem_ids")
    cmd = "list_mem_ids"
    response = send_command(READER_VM, cmd)
    print(response)
    print("-"*70)
    
    # Check liststore on replica
    print(f"\n4. liststore on VM{REPLICA_VM}")
    cmd = "liststore"
    response = send_command(REPLICA_VM, cmd)
    print(response)
    print("-"*70)

if __name__ == "__main__":
    main()