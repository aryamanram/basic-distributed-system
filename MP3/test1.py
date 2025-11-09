#!/usr/bin/env python3
"""
Test 1: Create 5 Files - SIMPLIFIED
Only sends create commands, nothing else
"""
import socket
import time

# ============= CONFIGURABLE PARAMETERS =============
WRITER_VM = 1

DATASET_FILES = [
    "business_1.txt",
    "business_2.txt",
    "business_3.txt",
    "business_4.txt",
    "business_5.txt"
]

HYDFS_FILES = [
    "hydfs_business_1.txt",
    "hydfs_business_2.txt",
    "hydfs_business_3.txt",
    "hydfs_business_4.txt",
    "hydfs_business_5.txt"
]

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
    print("TEST 1: Creating 5 Files")
    print("="*70)
    print(f"Writer VM: VM{WRITER_VM}\n")
    
    for i, (local_file, hydfs_file) in enumerate(zip(DATASET_FILES, HYDFS_FILES), 1):
        print(f"\nFile {i}: create {local_file} {hydfs_file}")
        cmd = f"create {local_file} {hydfs_file}"
        response = send_command(WRITER_VM, cmd)
        print(response)
        print("-"*70)

if __name__ == "__main__":
    main()