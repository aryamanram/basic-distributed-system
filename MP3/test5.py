#!/usr/bin/env python3
"""
Test 5: Concurrent Appends + Merge - SIMPLIFIED
Only sends: create, append (concurrent), merge, getfromreplica commands
"""
import socket
import time
import threading

# ============= CONFIGURABLE PARAMETERS =============
HYDFS_FILE = "test5_multiappend.txt"

APPEND_VMS = [1, 2, 3, 4]

APPEND_FILES = [
    "business_12.txt",
    "business_13.txt",
    "business_14.txt",
    "business_15.txt"
]

COORDINATOR_VM = 5

# Initial file
INITIAL_FILE = "test5_initial.txt"

# Replicas to fetch from (pick 2 different VMs)
REPLICA_VM_1 = 1
REPLICA_VM_2 = 2

RETRIEVED_FILE_1 = "test5_replica1.txt"
RETRIEVED_FILE_2 = "test5_replica2.txt"

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
    print("TEST 5: Concurrent Appends + Merge")
    print("="*70)
    print(f"Concurrent appends from VMs: {APPEND_VMS}\n")
    
    # Create initial file
    with open(INITIAL_FILE, 'w') as f:
        f.write("Initial content for Test 5\n")
    
    print(f"0. create {INITIAL_FILE} {HYDFS_FILE}")
    cmd = f"create {INITIAL_FILE} {HYDFS_FILE}"
    response = send_command(COORDINATOR_VM, cmd)
    print(response)
    print("-"*70)
    
    time.sleep(1)
    
    # Concurrent appends
    print("\n1. Launching concurrent appends...")
    
    results = {}
    timestamps = {}
    
    def do_append(vm_id, append_file):
        start = time.time()
        cmd = f"append {append_file} {HYDFS_FILE}"
        response = send_command(vm_id, cmd)
        end = time.time()
        results[vm_id] = response
        timestamps[vm_id] = (start, end)
    
    threads = []
    global_start = time.time()
    
    for vm_id, append_file in zip(APPEND_VMS, APPEND_FILES):
        print(f"   Launching: VM{vm_id} append {append_file}")
        thread = threading.Thread(target=do_append, args=(vm_id, append_file))
        thread.start()
        threads.append(thread)
    
    # Wait for all
    for thread in threads:
        thread.join()
    
    global_end = time.time()
    
    print(f"\n   All appends completed in {global_end - global_start:.3f}s")
    
    # Show timing
    print("\n   Timing details:")
    for vm_id in APPEND_VMS:
        if vm_id in timestamps:
            start, end = timestamps[vm_id]
            print(f"   VM{vm_id}: {start:.3f} - {end:.3f} (duration: {end-start:.3f}s)")
    
    print("-"*70)
    
    # Merge
    print(f"\n2. merge {HYDFS_FILE}")
    cmd = f"merge {HYDFS_FILE}"
    merge_start = time.time()
    response = send_command(COORDINATOR_VM, cmd)
    merge_end = time.time()
    print(response)
    print(f"   (merge took {merge_end - merge_start:.3f}s)")
    print("-"*70)
    
    time.sleep(1)
    
    # Get from two replicas
    print(f"\n3. getfromreplica {VM_HOSTS[REPLICA_VM_1]} {HYDFS_FILE} {RETRIEVED_FILE_1}")
    cmd = f"getfromreplica {VM_HOSTS[REPLICA_VM_1]} {HYDFS_FILE} {RETRIEVED_FILE_1}"
    response = send_command(COORDINATOR_VM, cmd)
    print(response)
    print("-"*70)
    
    print(f"\n4. getfromreplica {VM_HOSTS[REPLICA_VM_2]} {HYDFS_FILE} {RETRIEVED_FILE_2}")
    cmd = f"getfromreplica {VM_HOSTS[REPLICA_VM_2]} {HYDFS_FILE} {RETRIEVED_FILE_2}"
    response = send_command(COORDINATOR_VM, cmd)
    print(response)
    print("-"*70)

if __name__ == "__main__":
    main()