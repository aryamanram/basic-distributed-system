#!/usr/bin/env python3
"""
Test 4: Read-My-Writes + Ordering - SIMPLIFIED
Only sends: create, append (2x), get commands
"""
import socket
import time

# ============= CONFIGURABLE PARAMETERS =============
CLIENT_VM = 5

APPEND_FILE_1 = "business_10.txt"
APPEND_FILE_2 = "business_11.txt"

HYDFS_FILE = "test4_foo.txt"
RETRIEVED_FILE = "test4_retrieved.txt"

# Initial file to create
INITIAL_FILE = "test4_initial.txt"

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
    print("TEST 4: Read-My-Writes + Ordering")
    print("="*70)
    print(f"Client VM: VM{CLIENT_VM}\n")
    
    # Create initial file
    with open(INITIAL_FILE, 'w') as f:
        f.write("Initial content for Test 4\n")
    
    print(f"0. create {INITIAL_FILE} {HYDFS_FILE}")
    cmd = f"create {INITIAL_FILE} {HYDFS_FILE}"
    response = send_command(CLIENT_VM, cmd)
    print(response)
    print("-"*70)
    
    time.sleep(1)
    
    # First append
    print(f"\n1. append {APPEND_FILE_1} {HYDFS_FILE}")
    cmd = f"append {APPEND_FILE_1} {HYDFS_FILE}"
    start1 = time.time()
    response = send_command(CLIENT_VM, cmd)
    end1 = time.time()
    print(response)
    print(f"   (completed at {end1:.3f})")
    print("-"*70)
    
    # Second append (immediately after)
    print(f"\n2. append {APPEND_FILE_2} {HYDFS_FILE}")
    cmd = f"append {APPEND_FILE_2} {HYDFS_FILE}"
    start2 = time.time()
    response = send_command(CLIENT_VM, cmd)
    end2 = time.time()
    print(response)
    print(f"   (started at {start2:.3f}, completed at {end2:.3f})")
    print(f"   (second append started {start2-end1:.3f}s after first completed)")
    print("-"*70)
    
    # Get from same VM
    print(f"\n3. get {HYDFS_FILE} {RETRIEVED_FILE}")
    cmd = f"get {HYDFS_FILE} {RETRIEVED_FILE}"
    response = send_command(CLIENT_VM, cmd)
    print(response)
    print("-"*70)

if __name__ == "__main__":
    main()