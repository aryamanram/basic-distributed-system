"""
HyDFS Main - Command Line Interface
"""
import argparse
import os
import sys
import time
import socket
from typing import Optional

from node import HyDFSNode, HYDFS_PORT
from storage import FileBlock
from utils import get_file_id

def read_local_file(filename: str) -> Optional[bytes]:
    """
    Read a local file and return its contents.
    """
    try:
        with open(filename, 'rb') as f:
            return f.read()
    except Exception as e:
        print(f"Error reading file {filename}: {e}")
        return None

def write_local_file(filename: str, data: bytes) -> bool:
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

class HyDFSClient:
    """
    Client interface for HyDFS operations.
    """
    def __init__(self, node: HyDFSNode):
        self.node = node
        self.client_id = f"{node.hostname}_{os.getpid()}_{time.time()}"
    
    def create(self, local_filename: str, hydfs_filename: str):
        """
        Create a file on HyDFS.
        """
        print(f"Creating {hydfs_filename} from {local_filename}...")
        
        # Read local file
        data = read_local_file(local_filename)
        if data is None:
            print(f"Failed to read {local_filename}")
            return
        
        # Get replica nodes
        replicas = self.node.get_replicas_for_file(hydfs_filename)
        
        if not replicas:
            print("No replica nodes available")
            return
        
        print(f"Replicas: {replicas}")
        
        # Send create request to all replicas
        success_count = 0
        message = {
            'type': 'CREATE',
            'filename': hydfs_filename,
            'data': list(data)
        }
        
        for node_id in replicas:
            try:
                hostname = node_id.split(':')[0]
                port = int(node_id.split(':')[1])
                
                response = self.node.network.send_message(hostname, port, message)
                
                if response and response.get('status') == 'success':
                    print(f"✓ Created on {hostname}")
                    success_count += 1
                else:
                    print(f"✗ Failed on {hostname}: {response.get('message') if response else 'No response'}")
            except Exception as e:
                print(f"✗ Error sending to {node_id}: {e}")
        
        print(f"\nCreate completed: {success_count}/{len(replicas)} replicas")
        print(f"File replicated to: {replicas}")
    
    def get(self, hydfs_filename: str, local_filename: str):
        """
        Get a file from HyDFS.
        """
        print(f"Getting {hydfs_filename} to {local_filename}...")
        
        # Get replica nodes
        replicas = self.node.get_replicas_for_file(hydfs_filename)
        
        if not replicas:
            print("No replica nodes available")
            return
        
        # Try each replica until success
        for node_id in replicas:
            try:
                hostname = node_id.split(':')[0]
                port = int(node_id.split(':')[1])
                
                print(f"Trying replica {hostname}...")
                
                message = {
                    'type': 'GET',
                    'filename': hydfs_filename,
                    'client_id': self.client_id
                }
                
                response = self.node.network.send_message(hostname, port, message, timeout=30.0)
                
                if response and response.get('status') == 'success':
                    data = bytes(response.get('data', []))
                    
                    if write_local_file(local_filename, data):
                        print(f"✓ Retrieved {len(data)} bytes from {hostname}")
                        print(f"Saved to {local_filename}")
                        return
                    else:
                        print(f"✗ Failed to write to {local_filename}")
                        return
                else:
                    print(f"✗ Failed from {hostname}: {response.get('message') if response else 'No response'}")
            
            except Exception as e:
                print(f"✗ Error from {node_id}: {e}")
        
        print("Failed to retrieve file from any replica")
    
    def append(self, local_filename: str, hydfs_filename: str):
        """
        Append to a file on HyDFS.
        """
        print(f"Appending {local_filename} to {hydfs_filename}...")
        
        # Read local file
        data = read_local_file(local_filename)
        if data is None:
            print(f"Failed to read {local_filename}")
            return
        
        # Mark write as pending
        self.node.consistency.mark_write_pending(hydfs_filename, self.client_id)
        
        try:
            # Get replica nodes
            replicas = self.node.get_replicas_for_file(hydfs_filename)
            
            if not replicas:
                print("No replica nodes available")
                return
            
            # Get sequence number
            seq_num = self.node.consistency.get_next_sequence(self.client_id)
            
            # Create block
            block_id = f"{get_file_id(hydfs_filename)}_{self.client_id}_{seq_num}"
            block = FileBlock(
                block_id=block_id,
                client_id=self.client_id,
                sequence_num=seq_num,
                timestamp=time.time(),
                data=data,
                size=len(data)
            )
            
            # Send append request to all replicas
            success_count = 0
            message = {
                'type': 'APPEND',
                'filename': hydfs_filename,
                'client_id': self.client_id,
                'block': {
                    'block_id': block.block_id,
                    'client_id': block.client_id,
                    'sequence_num': block.sequence_num,
                    'timestamp': block.timestamp,
                    'data': list(block.data),
                    'size': block.size
                }
            }
            
            for node_id in replicas:
                try:
                    hostname = node_id.split(':')[0]
                    port = int(node_id.split(':')[1])
                    
                    response = self.node.network.send_message(hostname, port, message)
                    
                    if response and response.get('status') == 'success':
                        print(f"✓ Appended to {hostname}")
                        success_count += 1
                    else:
                        print(f"✗ Failed on {hostname}: {response.get('message') if response else 'No response'}")
                except Exception as e:
                    print(f"✗ Error sending to {node_id}: {e}")
            
            print(f"\nAppend completed: {success_count}/{len(replicas)} replicas")
            print(f"File replicated to: {replicas}")
        
        finally:
            # Mark write as complete
            self.node.consistency.mark_write_complete(hydfs_filename, self.client_id)
    
    def merge(self, hydfs_filename: str):
        """
        Merge all replicas of a file.
        """
        print(f"Merging {hydfs_filename}...")
        start_time = time.time()
        
        # Get replica nodes
        replicas = self.node.get_replicas_for_file(hydfs_filename)
        
        if not replicas:
            print("No replica nodes available")
            return
        
        # Send merge request to first replica (coordinator)
        coordinator = replicas[0]
        hostname = coordinator.split(':')[0]
        port = int(coordinator.split(':')[1])
        
        message = {
            'type': 'MERGE',
            'filename': hydfs_filename
        }
        
        print(f"Sending merge request to coordinator {hostname}...")
        
        response = self.node.network.send_message(hostname, port, message, timeout=60.0)
        
        elapsed = time.time() - start_time
        
        if response and response.get('status') == 'success':
            print(f"✓ Merge completed in {elapsed:.3f} seconds")
        else:
            print(f"✗ Merge failed: {response.get('message') if response else 'No response'}")
    
    def ls(self, hydfs_filename: str):
        """
        List machines storing a file.
        """
        print(f"Listing replicas for {hydfs_filename}...")
        
        # Get replica nodes
        replicas = self.node.get_replicas_for_file(hydfs_filename)
        
        if not replicas:
            print("No replica nodes available")
            return
        
        # Query first replica for info
        coordinator = replicas[0]
        hostname = coordinator.split(':')[0]
        port = int(coordinator.split(':')[1])
        
        message = {
            'type': 'LS',
            'filename': hydfs_filename
        }
        
        response = self.node.network.send_message(hostname, port, message)
        
        if response and response.get('status') == 'success':
            file_id = response.get('file_id')
            replica_info = response.get('replicas', [])
            
            print(f"\nFile ID: {file_id}")
            print(f"Stored on {len(replica_info)} machines:")
            for info in replica_info:
                print(f"  - {info['hostname']} (Node ID: {info['node_id'][:50]}..., Ring: {info['ring_position']})")
        else:
            print(f"Failed to get file info: {response.get('message') if response else 'No response'}")
    
    def liststore(self):
        """
        List files stored on local node.
        """
        print(f"Listing files stored on {self.node.hostname}...")
        
        message = {
            'type': 'LISTSTORE'
        }
        
        response = self.node.network.send_message(self.node.hostname, self.node.port, message)
        
        if response and response.get('status') == 'success':
            node_id = response.get('node_id')
            ring_pos = response.get('ring_position')
            files = response.get('files', [])
            
            print(f"\nNode ID: {node_id[:50]}...")
            print(f"Ring Position: {ring_pos}")
            print(f"Files stored ({len(files)}):")
            for file_info in files:
                print(f"  - {file_info['filename']} (ID: {file_info['file_id']})")
        else:
            print(f"Failed to get store info: {response.get('message') if response else 'No response'}")
    
    def getfromreplica(self, vm_address: str, hydfs_filename: str, local_filename: str):
        """
        Get file from a specific replica.
        """
        print(f"Getting {hydfs_filename} from {vm_address} to {local_filename}...")
        
        # Resolve VM address to hostname
        hostname = vm_address
        port = HYDFS_PORT
        
        message = {
            'type': 'GET_FROM_REPLICA',
            'filename': hydfs_filename,
            'client_id': self.client_id
        }
        
        response = self.node.network.send_message(hostname, port, message, timeout=30.0)
        
        if response and response.get('status') == 'success':
            data = bytes(response.get('data', []))
            
            if write_local_file(local_filename, data):
                print(f"✓ Retrieved {len(data)} bytes from {hostname}")
                print(f"Saved to {local_filename}")
            else:
                print(f"✗ Failed to write to {local_filename}")
        else:
            print(f"✗ Failed: {response.get('message') if response else 'No response'}")
    
    def list_mem_ids(self):
        """
        List membership with ring IDs.
        """
        print("Listing membership with ring IDs...")
        
        message = {
            'type': 'LIST_MEM_IDS'
        }
        
        response = self.node.network.send_message(self.node.hostname, self.node.port, message)
        
        if response and response.get('status') == 'success':
            members = response.get('members', [])
            
            print(f"\nTotal members: {len(members)}")
            print("Sorted by Ring Position:")
            for member in members:
                print(f"  Ring {member['ring_position']}: {member['hostname']} ({member['status']})")
        else:
            print(f"Failed to get membership: {response.get('message') if response else 'No response'}")
    
    def multiappend(self, hydfs_filename: str, vm_addresses: list, local_filenames: list):
        """
        Launch multiple appends simultaneously.
        """
        print(f"Launching multi-append to {hydfs_filename}...")
        print(f"VMs: {vm_addresses}")
        print(f"Files: {local_filenames}")
        
        import threading
        
        def append_from_vm(vm_addr, local_file):
            # Create a temporary client
            temp_client = HyDFSClient(self.node)
            temp_client.client_id = f"{vm_addr}_{os.getpid()}_{time.time()}"
            temp_client.append(local_file, hydfs_filename)
        
        threads = []
        for vm_addr, local_file in zip(vm_addresses, local_filenames):
            thread = threading.Thread(target=append_from_vm, args=(vm_addr, local_file))
            thread.start()
            threads.append(thread)
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        print("Multi-append completed")

def main():
    parser = argparse.ArgumentParser(description='HyDFS - Hybrid Distributed File System')
    parser.add_argument('--vm-id', type=int, required=True, help='VM ID (1-10)')
    
    args = parser.parse_args()
    
    # Create and start node
    node = HyDFSNode(args.vm_id)
    node.start()
    
    # Join the group
    print("\nJoining group...")
    node.membership.join_group()
    time.sleep(2)  # Wait for membership to stabilize
    
    # Create client
    client = HyDFSClient(node)
    
    print("\n" + "="*60)
    print("HyDFS Command Interface")
    print("="*60)
    print("Commands:")
    print("  create <local> <hydfs>")
    print("  get <hydfs> <local>")
    print("  append <local> <hydfs>")
    print("  merge <hydfs>")
    print("  ls <hydfs>")
    print("  liststore")
    print("  getfromreplica <vm_address> <hydfs> <local>")
    print("  list_mem_ids")
    print("  multiappend <hydfs> <vm1,vm2,...> <file1,file2,...>")
    print("  quit")
    print("="*60 + "\n")
    
    # Command loop
    while True:
        try:
            cmd = input("hydfs> ").strip()
            if not cmd:
                continue
            
            parts = cmd.split()
            command = parts[0].lower()
            
            if command == 'quit' or command == 'exit':
                break
            
            elif command == 'create' and len(parts) == 3:
                client.create(parts[1], parts[2])
            
            elif command == 'get' and len(parts) == 3:
                client.get(parts[1], parts[2])
            
            elif command == 'append' and len(parts) == 3:
                client.append(parts[1], parts[2])
            
            elif command == 'merge' and len(parts) == 2:
                client.merge(parts[1])
            
            elif command == 'ls' and len(parts) == 2:
                client.ls(parts[1])
            
            elif command == 'liststore':
                client.liststore()
            
            elif command == 'getfromreplica' and len(parts) == 4:
                client.getfromreplica(parts[1], parts[2], parts[3])
            
            elif command == 'list_mem_ids':
                client.list_mem_ids()
            
            elif command == 'multiappend' and len(parts) == 4:
                vms = parts[2].split(',')
                files = parts[3].split(',')
                if len(vms) == len(files):
                    client.multiappend(parts[1], vms, files)
                else:
                    print("Error: Number of VMs and files must match")
            
            else:
                print(f"Unknown command or invalid arguments: {cmd}")
                print("Type 'help' for command list")
        
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
    
    # Cleanup
    print("\nLeaving group...")
    node.membership.leave_group()
    time.sleep(1)
    node.stop()
    print("Goodbye!")

if __name__ == '__main__':
    main()