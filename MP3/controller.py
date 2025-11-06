"""
HyDFS Controller - Remote command interface for HyDFS operations
"""
import socket
import json
import os
import sys
from typing import List, Set

CONTROL_PORT = 9090

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

class HyDFSController:
    def __init__(self):
        self.target_vms: Set[int] = set()
    
    def parse_vm_selection(self, selection: str) -> Set[int]:
        """
        Parse VM selection string into set of VM IDs.
        Supports: single (1), list (1,2,3), range (1-5), all
        """
        vms = set()
        
        if selection.lower() == "all":
            return set(range(1, 11))
        
        # Split by comma for multiple selections
        parts = selection.split(',')
        for part in parts:
            part = part.strip()
            
            # Check for range (e.g., 1-5)
            if '-' in part:
                try:
                    start, end = map(int, part.split('-'))
                    vms.update(range(start, end + 1))
                except ValueError:
                    print(f"Invalid range: {part}")
            else:
                # Single VM
                try:
                    vm_id = int(part)
                    if 1 <= vm_id <= 10:
                        vms.add(vm_id)
                    else:
                        print(f"Invalid VM ID: {vm_id} (must be 1-10)")
                except ValueError:
                    print(f"Invalid VM ID: {part}")
        
        return vms
    
    def send_command(self, vm_id: int, command: dict) -> dict:
        """
        Send a command to a specific VM and get response.
        """
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30.0)
            sock.connect((VM_HOSTS[vm_id], CONTROL_PORT))
            
            # Send command as JSON
            command_data = json.dumps(command).encode('utf-8')
            size_header = f"{len(command_data):010d}".encode('utf-8')
            sock.sendall(size_header + command_data)
            
            # Receive response
            size_header = sock.recv(10)
            if not size_header:
                return {"status": "error", "message": "No response from server"}
            
            # Debug: Check if we got a valid size header
            try:
                size = int(size_header.decode('utf-8'))
            except ValueError as e:
                # Got unexpected data instead of size header
                error_msg = f"Invalid size header: {size_header[:50]}"
                return {"status": "error", "message": error_msg}
            
            response_data = b''
            while len(response_data) < size:
                chunk = sock.recv(min(4096, size - len(response_data)))
                if not chunk:
                    break
                response_data += chunk
            
            sock.close()
            
            # Parse response
            try:
                return json.loads(response_data.decode('utf-8'))
            except json.JSONDecodeError as e:
                return {"status": "error", "message": f"Invalid JSON response: {str(e)}"}
        
        except socket.timeout:
            return {"status": "error", "message": "Connection timeout"}
        except ConnectionRefusedError:
            return {"status": "error", "message": "Connection refused - is the node running?"}
        except Exception as e:
            return {"status": "error", "message": f"Connection error: {str(e)}"}
        finally:
            if sock:
                try:
                    sock.close()
                except:
                    pass
    
    def execute_on_vms(self, vm_ids: Set[int], command: dict):
        """
        Execute command on multiple VMs and display results.
        """
        if not vm_ids:
            print("No VMs selected. Use 'vm <id>' first.")
            return
        
        for vm_id in sorted(vm_ids):
            print(f"\n[VM{vm_id} - {VM_HOSTS[vm_id]}]")
            response = self.send_command(vm_id, command)
            
            if response.get('status') == 'success':
                # Pretty print the response
                if 'message' in response:
                    print(f"  {response['message']}")
                if 'data' in response:
                    # Don't print raw data, just size
                    data = response.get('data', [])
                    if isinstance(data, list):
                        print(f"  Data: {len(data)} bytes")
                    else:
                        print(f"  {response['data']}")
                if 'files' in response:
                    files = response['files']
                    print(f"  Files ({len(files)}):")
                    for f in files:
                        print(f"    - {f['filename']} (ID: {f['file_id']})")
                if 'replicas' in response:
                    replicas = response['replicas']
                    print(f"  Replicas ({len(replicas)}):")
                    for r in replicas:
                        print(f"    - {r['hostname']} (Ring: {r['ring_position']})")
                if 'members' in response:
                    members = response['members']
                    print(f"  Members ({len(members)}):")
                    for m in members:
                        print(f"    - Ring {m['ring_position']}: {m['hostname']} ({m['status']})")
            else:
                error_msg = response.get('message', 'Unknown error')
                print(f"  Error: {error_msg}")
                # If error is long (like a traceback), print first few lines
                if '\n' in error_msg:
                    lines = error_msg.split('\n')
                    print(f"  Error details:")
                    for line in lines[:10]:  # Show first 10 lines
                        if line.strip():
                            print(f"    {line}")
                    if len(lines) > 10:
                        print(f"    ... ({len(lines) - 10} more lines)")

    
    def read_local_file(self, filename: str) -> bytes:
        """
        Read a local file and return its contents.
        """
        try:
            with open(filename, 'rb') as f:
                return f.read()
        except Exception as e:
            print(f"Error reading file {filename}: {e}")
            return None
    
    def write_local_file(self, filename: str, data: bytes) -> bool:
        """
        Write data to a local file.
        """
        try:
            with open(filename, 'wb') as f:
                f.write(data)
            return True
        except Exception as e:
            print(f"Error writing file {filename}: {e}")
            return False
    
    def run(self):
        """
        Main command loop.
        """
        print("="*70)
        print("HyDFS Controller")
        print("="*70)
        print("\nCommands:")
        print("  VM Selection:")
        print("    vm <id>           - Select single VM (e.g., vm 1)")
        print("    vm <id1,id2,...>  - Select multiple VMs (e.g., vm 1,2,3)")
        print("    vm <start-end>    - Select range of VMs (e.g., vm 1-5)")
        print("    vm all            - Select all VMs")
        print("    show              - Show currently selected VMs")
        print()
        print("  File Operations:")
        print("    create <local> <hydfs>")
        print("    get <hydfs> <local>")
        print("    append <local> <hydfs>")
        print("    merge <hydfs>")
        print()
        print("  Info Commands:")
        print("    ls <hydfs>")
        print("    liststore")
        print("    list_mem_ids")
        print("    getfromreplica <vm_address> <hydfs> <local>")
        print()
        print("  Other:")
        print("    quit / exit       - Exit controller")
        print("="*70 + "\n")
        
        while True:
            try:
                cmd = input("hydfs-ctrl> ").strip()
                if not cmd:
                    continue
                
                parts = cmd.split()
                command = parts[0].lower()
                
                # VM selection
                if command == 'vm':
                    if len(parts) < 2:
                        print("Usage: vm <id|id1,id2,...|start-end|all>")
                        continue
                    
                    self.target_vms = self.parse_vm_selection(parts[1])
                    if self.target_vms:
                        print(f"Selected VMs: {sorted(self.target_vms)}")
                    else:
                        print("No valid VMs selected")
                    continue
                
                # Show selected VMs
                if command == 'show':
                    if self.target_vms:
                        print(f"Selected VMs: {sorted(self.target_vms)}")
                    else:
                        print("No VMs selected. Use 'vm <id>' first.")
                    continue
                
                # Exit
                if command in ('quit', 'exit'):
                    break
                
                # File operations
                if command == 'create' and len(parts) == 3:
                    local_file = parts[1]
                    hydfs_file = parts[2]
                    
                    # Read local file
                    data = self.read_local_file(local_file)
                    if data is None:
                        continue
                    
                    cmd_dict = {
                        'type': 'CREATE',
                        'filename': hydfs_file,
                        'data': list(data)
                    }
                    self.execute_on_vms(self.target_vms, cmd_dict)
                
                elif command == 'get' and len(parts) == 3:
                    hydfs_file = parts[1]
                    local_file = parts[2]
                    
                    # Get from first selected VM
                    if not self.target_vms:
                        print("No VMs selected. Use 'vm <id>' first.")
                        continue
                    
                    vm_id = min(self.target_vms)
                    print(f"\n[VM{vm_id} - {VM_HOSTS[vm_id]}]")
                    
                    cmd_dict = {
                        'type': 'GET',
                        'filename': hydfs_file
                    }
                    response = self.send_command(vm_id, cmd_dict)
                    
                    if response.get('status') == 'success':
                        data = bytes(response.get('data', []))
                        if self.write_local_file(local_file, data):
                            print(f"  Retrieved {len(data)} bytes")
                            print(f"  Saved to {local_file}")
                        else:
                            print(f"  Failed to write to {local_file}")
                    else:
                        print(f"  Error: {response.get('message', 'Unknown error')}")
                
                elif command == 'append' and len(parts) == 3:
                    local_file = parts[1]
                    hydfs_file = parts[2]
                    
                    # Read local file
                    data = self.read_local_file(local_file)
                    if data is None:
                        continue
                    
                    cmd_dict = {
                        'type': 'APPEND',
                        'filename': hydfs_file,
                        'data': list(data)
                    }
                    self.execute_on_vms(self.target_vms, cmd_dict)
                
                elif command == 'merge' and len(parts) == 2:
                    hydfs_file = parts[1]
                    
                    # Merge from first selected VM
                    if not self.target_vms:
                        print("No VMs selected. Use 'vm <id>' first.")
                        continue
                    
                    vm_id = min(self.target_vms)
                    print(f"\n[VM{vm_id} - {VM_HOSTS[vm_id]}]")
                    
                    cmd_dict = {
                        'type': 'MERGE',
                        'filename': hydfs_file
                    }
                    response = self.send_command(vm_id, cmd_dict)
                    
                    if response.get('status') == 'success':
                        print(f"  {response.get('message', 'Merge completed')}")
                    else:
                        print(f"  Error: {response.get('message', 'Unknown error')}")
                
                elif command == 'ls' and len(parts) == 2:
                    hydfs_file = parts[1]
                    
                    cmd_dict = {
                        'type': 'LS',
                        'filename': hydfs_file
                    }
                    self.execute_on_vms(self.target_vms or {1}, cmd_dict)
                
                elif command == 'liststore':
                    cmd_dict = {
                        'type': 'LISTSTORE'
                    }
                    self.execute_on_vms(self.target_vms, cmd_dict)
                
                elif command == 'list_mem_ids':
                    cmd_dict = {
                        'type': 'LIST_MEM_IDS'
                    }
                    self.execute_on_vms(self.target_vms or {1}, cmd_dict)
                
                elif command == 'getfromreplica' and len(parts) == 4:
                    vm_address = parts[1]
                    hydfs_file = parts[2]
                    local_file = parts[3]
                    
                    # Find VM ID from address
                    target_vm = None
                    for vm_id, hostname in VM_HOSTS.items():
                        if hostname == vm_address:
                            target_vm = vm_id
                            break
                    
                    if target_vm is None:
                        print(f"Unknown VM address: {vm_address}")
                        continue
                    
                    print(f"\n[VM{target_vm} - {VM_HOSTS[target_vm]}]")
                    
                    cmd_dict = {
                        'type': 'GET_FROM_REPLICA',
                        'filename': hydfs_file
                    }
                    response = self.send_command(target_vm, cmd_dict)
                    
                    if response.get('status') == 'success':
                        data = bytes(response.get('data', []))
                        if self.write_local_file(local_file, data):
                            print(f"  Retrieved {len(data)} bytes from {vm_address}")
                            print(f"  Saved to {local_file}")
                        else:
                            print(f"  Failed to write to {local_file}")
                    else:
                        print(f"  Error: {response.get('message', 'Unknown error')}")
                
                else:
                    print(f"Unknown command or invalid arguments: {cmd}")
                    print("Type 'help' or see command list above")
            
            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except Exception as e:
                print(f"Error: {e}")
                import traceback
                traceback.print_exc()
        
        print("Goodbye!")

def main():
    controller = HyDFSController()
    controller.run()

if __name__ == '__main__':
    main()