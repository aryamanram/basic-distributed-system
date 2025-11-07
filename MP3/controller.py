"""
HyDFS Controller - Control interface for managing HyDFS operations across VMs
Similar to MP2's command_functions.py
Enhanced with multi-VM selection support
"""
import socket
import json
import sys

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

def send_command(vm_id, command):
    """
    Send a command to a specific VM and receive response.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(60.0)  # Longer timeout for file operations
        sock.connect((VM_HOSTS[vm_id], HYDFS_CONTROL_PORT))
        
        # Send command
        sock.sendall(command.encode('utf-8'))
        
        # Receive response
        response = sock.recv(65536).decode('utf-8')
        sock.close()
        
        print(f"\n[VM{vm_id} ({VM_HOSTS[vm_id]})]")
        print(response)
        return response
    except socket.timeout:
        print(f"\n[VM{vm_id}] Timeout - operation may still be in progress")
        return None
    except Exception as e:
        print(f"\n[VM{vm_id}] Error: {e}")
        return None

def print_help():
    """
    Print available commands.
    """
    print("\n" + "="*70)
    print("HyDFS Controller Commands")
    print("="*70)
    print("\nVM Selection:")
    print("  vm <id>              - Select single VM (1-10)")
    print("  vm <id1,id2,...>     - Select multiple VMs (e.g., vm 1,2,5,8)")
    print("  vm all               - Select all VMs")
    print("\nFile Operations:")
    print("  create <local> <hydfs>")
    print("  get <hydfs> <local>")
    print("  append <local> <hydfs>")
    print("  merge <hydfs>")
    print("\nQuery Operations:")
    print("  ls <hydfs>")
    print("  liststore")
    print("  getfromreplica <vm_address> <hydfs> <local>")
    print("  list_mem_ids")
    print("\nAdvanced:")
    print("  multiappend <hydfs> <vm1,vm2,...> <file1,file2,...>")
    print("\nControl:")
    print("  help             - Show this help")
    print("  status           - Check VM connectivity")
    print("  quit/exit        - Exit controller")
    print("="*70)
    print("\nNote: File paths are relative to each VM's working directory")
    print("Ensure test files exist on target VMs before operations")
    print("="*70 + "\n")

def check_vm_status():
    """
    Check connectivity to all VMs.
    """
    print("\nChecking VM status...")
    print("-" * 60)
    
    for vm_id in range(1, 11):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((VM_HOSTS[vm_id], HYDFS_CONTROL_PORT))
            
            # Send status command
            sock.sendall("status".encode('utf-8'))
            response = sock.recv(1024).decode('utf-8')
            sock.close()
            
            print(f"  VM{vm_id:2d} ({VM_HOSTS[vm_id]:35s}): ✓ ONLINE - {response}")
        except socket.timeout:
            print(f"  VM{vm_id:2d} ({VM_HOSTS[vm_id]:35s}): ✗ TIMEOUT")
        except ConnectionRefusedError:
            print(f"  VM{vm_id:2d} ({VM_HOSTS[vm_id]:35s}): ✗ NOT RUNNING")
        except Exception as e:
            print(f"  VM{vm_id:2d} ({VM_HOSTS[vm_id]:35s}): ✗ ERROR - {e}")
    
    print("-" * 60)

def parse_command(cmd_str):
    """
    Parse command string and validate.
    Returns (command_type, args) or (None, None) if invalid.
    """
    parts = cmd_str.strip().split()
    if not parts:
        return None, None
    
    cmd = parts[0].lower()
    
    # Commands with no args
    if cmd in ['liststore', 'list_mem_ids', 'status', 'help', 'quit', 'exit']:
        return cmd, []
    
    # Commands with specific arg counts
    cmd_args = {
        'create': 2,
        'get': 2,
        'append': 2,
        'merge': 1,
        'ls': 1,
        'getfromreplica': 3,
        'multiappend': 3
    }
    
    if cmd in cmd_args:
        expected = cmd_args[cmd]
        if len(parts) - 1 != expected:
            print(f"Error: '{cmd}' expects {expected} arguments, got {len(parts) - 1}")
            return None, None
        return cmd, parts[1:]
    
    return None, None

def main():
    print("\n" + "="*70)
    print("HyDFS Controller")
    print("="*70)
    print("\nType 'help' for available commands")
    print("Type 'status' to check VM connectivity")
    print("\nVM Selection Examples:")
    print("  vm 1         - Select VM 1")
    print("  vm 1,2,5,8   - Select VMs 1, 2, 5, and 8")
    print("  vm all       - Select all VMs")
    print("="*70 + "\n")
    
    target = None
    
    while True:
        try:
            cmd_str = input("hydfs-ctrl> ").strip()
            if not cmd_str:
                continue
            
            # Handle VM selection
            if cmd_str.startswith("vm "):
                arg = cmd_str.split()[1] if len(cmd_str.split()) > 1 else ""
                if arg == "all":
                    target = "all"
                    print(f"Target set to: all VMs")
                else:
                    # Check if comma-separated list
                    if ',' in arg:
                        try:
                            vm_ids = [int(x.strip()) for x in arg.split(',')]
                            # Validate all IDs
                            invalid = [vid for vid in vm_ids if vid < 1 or vid > 10]
                            if invalid:
                                print(f"Error: Invalid VM IDs: {invalid}. Must be between 1 and 10")
                            else:
                                target = vm_ids
                                vm_list = ', '.join([f"VM{vid}" for vid in vm_ids])
                                print(f"Target set to: {vm_list}")
                        except ValueError:
                            print("Error: Invalid VM ID format. Use comma-separated numbers (e.g., 1,2,5)")
                    else:
                        # Single VM
                        try:
                            vm_id = int(arg)
                            if 1 <= vm_id <= 10:
                                target = vm_id
                                print(f"Target set to: VM{vm_id} ({VM_HOSTS[vm_id]})")
                            else:
                                print("Error: VM ID must be between 1 and 10")
                        except ValueError:
                            print("Error: Invalid VM ID")
                continue
            
            # Handle special commands
            if cmd_str.lower() in ['quit', 'exit']:
                print("Exiting controller...")
                break
            
            if cmd_str.lower() == 'help':
                print_help()
                continue
            
            if cmd_str.lower() == 'status':
                check_vm_status()
                continue
            
            # Validate target is set
            if target is None:
                print("Error: No target VM selected. Use 'vm <id>', 'vm <id1,id2,...>', or 'vm all'")
                continue
            
            # Parse and validate command
            cmd_type, args = parse_command(cmd_str)
            if cmd_type is None:
                print("Error: Invalid command. Type 'help' for available commands")
                continue
            
            # Execute command on target VM(s)
            if target == "all":
                print(f"\nExecuting '{cmd_str}' on all VMs...")
                print("=" * 60)
                for vm_id in range(1, 11):
                    send_command(vm_id, cmd_str)
                print("=" * 60)
            elif isinstance(target, list):
                # Multiple specific VMs
                print(f"\nExecuting '{cmd_str}' on VMs: {', '.join(map(str, target))}...")
                print("=" * 60)
                for vm_id in target:
                    send_command(vm_id, cmd_str)
                print("=" * 60)
            else:
                # Single VM
                send_command(target, cmd_str)
        
        except KeyboardInterrupt:
            print("\n\nExiting controller...")
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()