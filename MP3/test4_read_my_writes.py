#!/usr/bin/env python3
"""
Test 4: Read-my-writes + Client-append ordering
Sequential appends from same VM, then verify ordering
"""
import socket
import time
import sys

# ============= CONFIGURABLE PARAMETERS =============
# VM to use for appends and reads
CLIENT_VM = 5

# Files to append (TA will pick these from dataset)
APPEND_FILE_1 = "business_10.txt"  # foo1.txt
APPEND_FILE_2 = "business_11.txt"  # foo2.txt

# HyDFS filename to append to
HYDFS_FILE = "test4_foo.txt"

# Keywords to search for (update based on actual file content)
# These will be used to verify both appends are present and ordered
KEYWORD_1 = "Winn-Dixie"  # From business_10.txt
KEYWORD_2 = "Saab"         # From business_11.txt

# Local file to save retrieved content
RETRIEVED_FILE = "test4_retrieved.txt"

# Initial file to create (can be empty or small)
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
    """Search for keyword in file, return (found, line_number)"""
    try:
        with open(filename, 'r') as f:
            for i, line in enumerate(f, 1):
                if keyword.lower() in line.lower():
                    return True, i, line.strip()
        return False, 0, ""
    except FileNotFoundError:
        return False, 0, ""

def main():
    print("="*70)
    print("TEST 4: Read-My-Writes + Client-Append Ordering")
    print("="*70)
    print(f"\nClient VM: VM{CLIENT_VM} ({VM_HOSTS[CLIENT_VM]})")
    print(f"HyDFS File: {HYDFS_FILE}")
    print(f"First append: {APPEND_FILE_1} (keyword: {KEYWORD_1})")
    print(f"Second append: {APPEND_FILE_2} (keyword: {KEYWORD_2})")
    print()
    
    # Step 0: Create initial file
    print("Step 0: Creating initial file...")
    print("-"*70)
    
    # Create a small initial file
    with open(INITIAL_FILE, 'w') as f:
        f.write("Initial content for Test 4\n")
    
    cmd = f"create {INITIAL_FILE} {HYDFS_FILE}"
    print(f"  Command: {cmd}")
    response = send_command(CLIENT_VM, cmd)
    print(f"  Response:")
    print("  " + response.replace("\n", "\n  "))
    
    if "created" in response.lower() or "success" in response.lower():
        print(f"  ✓ Initial file created")
    
    time.sleep(2)  # Brief wait
    
    # ========== PART 1: Sequential Appends ==========
    print("\n" + "="*70)
    print("PART 1: Sequential Appends [5%]")
    print("="*70)
    
    print(f"\nStep 1.1: First append ({APPEND_FILE_1})...")
    cmd = f"append {APPEND_FILE_1} {HYDFS_FILE}"
    print(f"  Command: {cmd}")
    start_time_1 = time.time()
    response = send_command(CLIENT_VM, cmd)
    end_time_1 = time.time()
    print(f"  Response:")
    print("  " + response.replace("\n", "\n  "))
    print(f"  Time: {end_time_1 - start_time_1:.3f} seconds")
    
    if "Append completed" in response or "success" in response.lower():
        print(f"  ✓ First append completed")
    
    print(f"\nStep 1.2: Second append ({APPEND_FILE_2}) - immediately after first...")
    cmd = f"append {APPEND_FILE_2} {HYDFS_FILE}"
    print(f"  Command: {cmd}")
    start_time_2 = time.time()
    response = send_command(CLIENT_VM, cmd)
    end_time_2 = time.time()
    print(f"  Response:")
    print("  " + response.replace("\n", "\n  "))
    print(f"  Time: {end_time_2 - start_time_2:.3f} seconds")
    
    if "Append completed" in response or "success" in response.lower():
        print(f"  ✓ Second append completed")
    
    print(f"\n  Summary:")
    print(f"    First append started: {start_time_1:.3f}")
    print(f"    First append ended: {end_time_1:.3f}")
    print(f"    Second append started: {start_time_2:.3f}")
    print(f"    Second append ended: {end_time_2:.3f}")
    print(f"    Appends were sequential (second started after first completed)")
    
    # ========== PART 2: Read-My-Writes and Ordering ==========
    print("\n" + "="*70)
    print("PART 2: Read-My-Writes and Ordering Verification [10%]")
    print("="*70)
    
    print(f"\nStep 2.1: Getting file from same client VM...")
    cmd = f"get {HYDFS_FILE} {RETRIEVED_FILE}"
    print(f"  Command: {cmd}")
    response = send_command(CLIENT_VM, cmd)
    print(f"  Response:")
    print("  " + response.replace("\n", "\n  "))
    
    if "Retrieved" in response or "Saved" in response:
        print(f"  ✓ File retrieved successfully")
    
    time.sleep(1)  # Brief wait for file to be written
    
    # CHECK 1: Read-my-writes (both appends present)
    print(f"\nStep 2.2: CHECK 1 - Verify both appends are present (read-my-writes)...")
    print(f"  Searching for keyword from first append: '{KEYWORD_1}'")
    
    found_1, line_1, content_1 = search_in_file(RETRIEVED_FILE, KEYWORD_1)
    if found_1:
        print(f"  ✓ Found at line {line_1}: {content_1[:80]}...")
    else:
        print(f"  ✗ NOT FOUND!")
    
    print(f"\n  Searching for keyword from second append: '{KEYWORD_2}'")
    found_2, line_2, content_2 = search_in_file(RETRIEVED_FILE, KEYWORD_2)
    if found_2:
        print(f"  ✓ Found at line {line_2}: {content_2[:80]}...")
    else:
        print(f"  ✗ NOT FOUND!")
    
    if found_1 and found_2:
        print(f"\n  ✓ CHECK 1 PASSED: Both appends are present (read-my-writes guaranteed)")
    else:
        print(f"\n  ✗ CHECK 1 FAILED: One or both appends missing")
    
    # CHECK 2: Ordering (first before second)
    print(f"\nStep 2.3: CHECK 2 - Verify correct ordering...")
    if found_1 and found_2:
        print(f"  First append keyword at line: {line_1}")
        print(f"  Second append keyword at line: {line_2}")
        
        if line_1 < line_2:
            print(f"  ✓ CHECK 2 PASSED: Correct order (first append before second)")
        else:
            print(f"  ✗ CHECK 2 FAILED: Wrong order (second append before first!)")
    else:
        print(f"  ⚠  Cannot verify ordering (one or both appends not found)")
    
    # Display retrieved file for manual inspection
    print(f"\nStep 2.4: Displaying retrieved file for manual inspection...")
    try:
        with open(RETRIEVED_FILE, 'r') as f:
            lines = f.readlines()
        
        print(f"  Total lines: {len(lines)}")
        print(f"  First 10 lines:")
        for i, line in enumerate(lines[:10], 1):
            print(f"    {i:3d}: {line.rstrip()[:70]}")
        
        print(f"\n  Around first keyword (line {line_1}):")
        start = max(0, line_1 - 3)
        end = min(len(lines), line_1 + 2)
        for i in range(start, end):
            marker = " → " if i == line_1 - 1 else "   "
            print(f"  {marker}{i+1:3d}: {lines[i].rstrip()[:70]}")
        
        print(f"\n  Around second keyword (line {line_2}):")
        start = max(0, line_2 - 3)
        end = min(len(lines), line_2 + 2)
        for i in range(start, end):
            marker = " → " if i == line_2 - 1 else "   "
            print(f"  {marker}{i+1:3d}: {lines[i].rstrip()[:70]}")
        
    except FileNotFoundError:
        print(f"  ⚠  File {RETRIEVED_FILE} not found locally")
        print(f"  You should check this on the client VM directly")
    
    print("\n" + "="*70)
    print("TEST 4 COMPLETE")
    print("="*70)
    print("\nSummary:")
    print(f"  Sequential appends: ✓")
    print(f"  Both appends present (read-my-writes): {'✓' if (found_1 and found_2) else '✗'}")
    print(f"  Correct ordering: {'✓' if (found_1 and found_2 and line_1 < line_2) else '✗'}")
    print(f"\n  First append keyword '{KEYWORD_1}' at line {line_1}")
    print(f"  Second append keyword '{KEYWORD_2}' at line {line_2}")

if __name__ == "__main__":
    main()