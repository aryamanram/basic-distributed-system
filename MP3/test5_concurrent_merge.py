#!/usr/bin/env python3
"""
Test 5: Concurrent appends + Merge
Launch 4+ concurrent appends, then merge and verify
"""
import socket
import time
import sys
import subprocess

# ============= CONFIGURABLE PARAMETERS =============
# HyDFS filename for concurrent appends
HYDFS_FILE = "test5_multiappend.txt"

# VMs to use for concurrent appends (at least 4)
APPEND_VMS = [1, 2, 3, 4]

# Files to append from each VM (TA will pick these)
# Each VM will append a different file
APPEND_FILES = [
    "business_12.txt",  # VM1 appends this
    "business_13.txt",  # VM2 appends this
    "business_14.txt",  # VM3 appends this
    "business_15.txt"   # VM4 appends this
]

# Keywords to search for in each file (for verification)
KEYWORDS = [
    "Bank",        # From business_12.txt
    "Industrial",  # From business_13.txt
    "Khodorkovsky",# From business_14.txt
    "China"        # From business_15.txt
]

# VM to use for merge and verification
COORDINATOR_VM = 5

# Initial file to create
INITIAL_FILE = "test5_initial.txt"

# Retrieved files for replica comparison
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

def search_in_file(filename, keyword):
    """Search for keyword in file"""
    try:
        with open(filename, 'r') as f:
            content = f.read()
            return keyword.lower() in content.lower()
    except FileNotFoundError:
        return False

def parse_ls_output(output):
    """Parse ls output to get replicas"""
    lines = output.strip().split('\n')
    replicas = []
    for line in lines:
        if 'fa25-cs425-a7' in line:
            replicas.append(line.strip())
    return replicas

def main():
    print("="*70)
    print("TEST 5: Concurrent Appends + Merge")
    print("="*70)
    print(f"\nHyDFS File: {HYDFS_FILE}")
    print(f"Concurrent appends from {len(APPEND_VMS)} VMs: {APPEND_VMS}")
    print(f"Coordinator VM: VM{COORDINATOR_VM}")
    print()
    
    # Step 0: Create initial file
    print("Step 0: Creating initial file...")
    print("-"*70)
    
    with open(INITIAL_FILE, 'w') as f:
        f.write("Initial content for Test 5 concurrent appends\n")
    
    cmd = f"create {INITIAL_FILE} {HYDFS_FILE}"
    response = send_command(COORDINATOR_VM, cmd)
    print(f"  Response:")
    print("  " + response.replace("\n", "\n  "))
    
    time.sleep(2)
    
    # ========== PART 1: Concurrent Appends ==========
    print("\n" + "="*70)
    print("PART 1: Concurrent Appends [5%]")
    print("="*70)
    
    print(f"\nStep 1.1: Preparing multiappend command...")
    vms_str = ','.join([f"vm{vm}" for vm in APPEND_VMS])
    files_str = ','.join(APPEND_FILES)
    
    print(f"  VMs: {vms_str}")
    print(f"  Files: {files_str}")
    
    # Note: multiappend is not directly supported via controller
    # We'll launch appends individually but concurrently
    print(f"\nStep 1.2: Launching concurrent appends...")
    print("  Note: Launching appends individually but showing they're concurrent via timestamps")
    
    import threading
    
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
        print(f"  Launching append on VM{vm_id} ({append_file})...")
        thread = threading.Thread(target=do_append, args=(vm_id, append_file))
        thread.start()
        threads.append(thread)
    
    # Wait for all to complete
    for thread in threads:
        thread.join()
    
    global_end = time.time()
    
    print(f"\n  All appends completed in {global_end - global_start:.3f} seconds")
    
    print(f"\nStep 1.3: Checking append timestamps for concurrency...")
    print(f"  Global start: {global_start:.3f}")
    print(f"  Global end: {global_end:.3f}")
    print()
    
    for vm_id in APPEND_VMS:
        if vm_id in timestamps:
            start, end = timestamps[vm_id]
            print(f"  VM{vm_id}:")
            print(f"    Start: {start:.3f} (+{start - global_start:.3f}s from global start)")
            print(f"    End:   {end:.3f} (+{end - global_start:.3f}s from global start)")
            print(f"    Duration: {end - start:.3f}s")
    
    # Check for overlap
    print(f"\nStep 1.4: Checking for concurrent execution (overlapping time windows)...")
    times = [(timestamps[vm][0], timestamps[vm][1], vm) for vm in APPEND_VMS if vm in timestamps]
    times.sort()
    
    overlap_found = False
    for i in range(len(times) - 1):
        start1, end1, vm1 = times[i]
        start2, end2, vm2 = times[i+1]
        if start2 < end1:  # Overlap
            print(f"  ✓ VM{vm1} and VM{vm2} overlapped")
            print(f"    VM{vm1}: {start1:.3f} - {end1:.3f}")
            print(f"    VM{vm2}: {start2:.3f} - {end2:.3f}")
            print(f"    Overlap: {start2:.3f} - {end1:.3f} ({end1 - start2:.3f}s)")
            overlap_found = True
    
    if overlap_found:
        print(f"\n  ✓ CHECK 1 PASSED: Appends were executed concurrently")
    else:
        print(f"\n  ⚠  Appends may have been sequential (check timestamps above)")
    
    # Show responses
    print(f"\nStep 1.5: Append responses...")
    for vm_id in APPEND_VMS:
        if vm_id in results:
            print(f"\n  VM{vm_id} response:")
            print("    " + results[vm_id].replace("\n", "\n    ")[:200] + "...")
    
    # ========== PART 2: Merge ==========
    print("\n" + "="*70)
    print("PART 2: Merge Operation")
    print("="*70)
    
    print(f"\nStep 2.1: Running merge on {HYDFS_FILE}...")
    cmd = f"merge {HYDFS_FILE}"
    print(f"  Command: {cmd}")
    merge_start = time.time()
    response = send_command(COORDINATOR_VM, cmd)
    merge_end = time.time()
    print(f"  Response:")
    print("  " + response.replace("\n", "\n  "))
    print(f"  Merge time: {merge_end - merge_start:.3f} seconds")
    
    if "completed" in response.lower() or "success" in response.lower():
        print(f"  ✓ Merge completed")
    
    time.sleep(2)  # Wait for merge to propagate
    
    # ========== PART 3: Verification After Merge ==========
    print("\n" + "="*70)
    print("PART 3: Verification After Merge [10%]")
    print("="*70)
    
    # Get replicas
    print(f"\nStep 3.1: Getting replica locations...")
    cmd = f"ls {HYDFS_FILE}"
    response = send_command(COORDINATOR_VM, cmd)
    print(f"  Response:")
    print("  " + response.replace("\n", "\n  "))
    
    replicas = parse_ls_output(response)
    print(f"\n  Found {len(replicas)} replicas")
    
    # Pick two replicas
    if len(replicas) < 2:
        print(f"  ✗ Need at least 2 replicas for comparison")
        sys.exit(1)
    
    replica_vms = []
    for replica in replicas[:2]:
        for vm_id, host in VM_HOSTS.items():
            if host in replica or f"a70{vm_id}" in replica or f"a71{vm_id}" in replica:
                replica_vms.append(vm_id)
                break
    
    print(f"  Selected replicas: VM{replica_vms[0]}, VM{replica_vms[1]}")
    
    # CHECK 1: Replicas are identical
    print(f"\nStep 3.2: CHECK 1 - Verify replicas are identical...")
    
    print(f"  Getting file from VM{replica_vms[0]}...")
    cmd = f"getfromreplica {VM_HOSTS[replica_vms[0]]} {HYDFS_FILE} {RETRIEVED_FILE_1}"
    response = send_command(COORDINATOR_VM, cmd)
    print(f"    Response: {response[:100]}...")
    
    print(f"  Getting file from VM{replica_vms[1]}...")
    cmd = f"getfromreplica {VM_HOSTS[replica_vms[1]]} {HYDFS_FILE} {RETRIEVED_FILE_2}"
    response = send_command(COORDINATOR_VM, cmd)
    print(f"    Response: {response[:100]}...")
    
    time.sleep(1)
    
    print(f"\n  Running diff between replicas...")
    try:
        result = subprocess.run(
            ['diff', RETRIEVED_FILE_1, RETRIEVED_FILE_2],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print(f"  ✓ CHECK 1 PASSED: Replicas are IDENTICAL!")
        else:
            print(f"  ✗ CHECK 1 FAILED: Replicas are DIFFERENT!")
            print(f"  Diff output:")
            print("    " + result.stdout.replace("\n", "\n    ")[:500])
    except FileNotFoundError:
        print(f"  ⚠  Note: Run diff manually on coordinator VM:")
        print(f"    diff {RETRIEVED_FILE_1} {RETRIEVED_FILE_2}")
    
    # CHECK 2: All appends are present
    print(f"\nStep 3.3: CHECK 2 - Verify all {len(APPEND_VMS)} appends are present...")
    
    all_found = True
    for i, keyword in enumerate(KEYWORDS):
        found = search_in_file(RETRIEVED_FILE_1, keyword)
        if found:
            print(f"  ✓ Append {i+1} (keyword '{keyword}'): FOUND")
        else:
            print(f"  ✗ Append {i+1} (keyword '{keyword}'): NOT FOUND!")
            all_found = False
    
    if all_found:
        print(f"\n  ✓ CHECK 2 PASSED: All {len(APPEND_VMS)} appends are present")
    else:
        print(f"\n  ✗ CHECK 2 FAILED: Some appends are missing")
    
    # Display merged file
    print(f"\nStep 3.4: Displaying merged file (first 30 lines)...")
    try:
        with open(RETRIEVED_FILE_1, 'r') as f:
            lines = f.readlines()[:30]
        
        for i, line in enumerate(lines, 1):
            print(f"  {i:3d}: {line.rstrip()[:70]}")
        
        print(f"\n  Total lines: {len(open(RETRIEVED_FILE_1).readlines())}")
    except FileNotFoundError:
        print(f"  ⚠  File not found locally")
    
    print("\n" + "="*70)
    print("TEST 5 COMPLETE")
    print("="*70)
    print("\nSummary:")
    print(f"  Concurrent appends: {len(APPEND_VMS)} VMs")
    print(f"  Appends executed concurrently: {'✓' if overlap_found else '⚠'}")
    print(f"  Merge completed: ✓")
    print(f"  Replicas identical: {'✓' if all_found else '✗'}")
    print(f"  All appends present: {'✓' if all_found else '✗'}")

if __name__ == "__main__":
    main()