"""
HyDFS Node - Main orchestrator
Coordinates between all components
"""

import time
from ring import ConsistentHashRing
from storage import FileStore
from replication import ReplicationManager
from consistency import ConsistencyManager
from network import NetworkManager
from utils import setup_logging, log_operation

class HyDFSNode:
    def __init__(self, vm_id, membership_service):
        self.vm_id = vm_id
        self.membership = membership_service
        self.node_id = None  # Will be set on start
        
        # Initialize components
        self.ring = ConsistentHashRing()
        self.storage = FileStore(f"/tmp/hydfs_vm{vm_id}")
        self.network = NetworkManager(self.get_address(), port=5000 + vm_id)
        self.replication = ReplicationManager(self, self.ring, self.storage, self.network)
        self.consistency = ConsistencyManager(self, self.ring, self.storage, self.network)
        
        # Setup logging
        self.logger = setup_logging(vm_id)
        
        # Register callbacks with membership
        self.membership.register_failure_callback(self.handle_failure)
        self.membership.register_join_callback(self.handle_join)
    
    def start(self):
        """Start the HyDFS node"""
        # Generate node ID
        self.node_id = self.generate_node_id()
        
        # Add self to ring
        self.ring.add_node(self.node_id, self.get_address())
        
        # Initialize ring with current membership
        self.sync_ring_with_membership()
        
        # Start network server
        self.network.start_server()
        
        # Start background merge process
        self.consistency.start_background_merge()
        
        log_operation(self.logger, "START", f"Node {self.vm_id} started with ID {self.node_id}")
    
    def stop(self):
        """Stop the HyDFS node"""
        self.consistency.stop_background_merge()
        self.network.stop_server()
        log_operation(self.logger, "STOP", f"Node {self.vm_id} stopped")
    
    def generate_node_id(self):
        """Generate unique node ID with timestamp"""
        import socket
        hostname = socket.gethostname()
        timestamp = int(time.time() * 1000)
        return f"{hostname}:{5000 + self.vm_id}:{timestamp}"
    
    def get_address(self):
        """Get node address"""
        import socket
        return socket.gethostname()
    
    def sync_ring_with_membership(self):
        """Sync ring with current membership list"""
        members = self.membership.membership.get_members()
        for host, member_data in members.items():
            if member_data['status'] == 'ACTIVE':
                # Extract node_id from member data
                node_id = member_data.get('member_id')
                self.ring.add_node(node_id, host)
    
    def handle_failure(self, failed_node):
        """Handle node failure - trigger re-replication"""
        log_operation(self.logger, "FAILURE", f"Node {failed_node} failed")
        
        # Remove from ring
        self.ring.remove_node(failed_node)
        
        # Trigger re-replication for affected files
        affected_files = self.storage.list_files()
        for filename in affected_files:
            self.replication.re_replicate(filename, [failed_node])
    
    def handle_join(self, new_node):
        """Handle node join - trigger re-balancing"""
        log_operation(self.logger, "JOIN", f"Node {new_node} joined")
        
        # Add to ring
        # Note: Extract node_id from membership data
        self.ring.add_node(new_node, new_node)  # Simplified
        
        # Trigger re-balancing
        # TODO: Implement re-balancing logic
        pass
    
    # File operations
    
    def create_file(self, local_filename, hydfs_filename):
        """Create a file in HyDFS"""
        log_operation(self.logger, "CREATE", f"Creating {hydfs_filename} from {local_filename}")
        success = self.replication.create(local_filename, hydfs_filename)
        if success:
            print(f"✓ File {hydfs_filename} created successfully")
        else:
            print(f"✗ Failed to create {hydfs_filename}")
    
    def get_file(self, hydfs_filename, local_filename):
        """Get a file from HyDFS"""
        log_operation(self.logger, "GET", f"Getting {hydfs_filename} to {local_filename}")
        success = self.replication.get(hydfs_filename, local_filename)
        if success:
            print(f"✓ File {hydfs_filename} retrieved successfully")
        else:
            print(f"✗ Failed to get {hydfs_filename}")
    
    def append_file(self, local_filename, hydfs_filename):
        """Append to a file in HyDFS"""
        log_operation(self.logger, "APPEND", f"Appending {local_filename} to {hydfs_filename}")
        success = self.replication.append(local_filename, hydfs_filename)
        if success:
            print(f"✓ File {hydfs_filename} appended successfully")
        else:
            print(f"✗ Failed to append to {hydfs_filename}")
    
    def merge_file(self, hydfs_filename):
        """Merge replicas of a file"""
        log_operation(self.logger, "MERGE", f"Merging {hydfs_filename}")
        success = self.consistency.merge(hydfs_filename)
        if success:
            print(f"✓ File {hydfs_filename} merged successfully")
        else:
            print(f"✗ Failed to merge {hydfs_filename}")
    
    def ls_file(self, hydfs_filename):
        """List VMs storing a file"""
        replicas = self.ring.get_replicas_for_file(hydfs_filename)
        file_id = self.ring.hash_file(hydfs_filename)
        
        print(f"File: {hydfs_filename}")
        print(f"FileID: {file_id}")
        print(f"Replicas ({len(replicas)}):")
        for vm_address in replicas:
            node_id = self.ring.get_node_id_for_address(vm_address)
            print(f"  - {vm_address} (NodeID: {node_id})")
    
    def liststore(self):
        """List files stored locally"""
        files = self.storage.list_files()
        node_id = self.node_id
        
        print(f"Node ID: {node_id}")
        print(f"Files stored ({len(files)}):")
        for filename in files:
            file_id = self.ring.hash_file(filename)
            print(f"  - {filename} (FileID: {file_id})")
    
    def getfromreplica(self, vm_address, hydfs_filename, local_filename):
        """Get file from specific replica"""
        # TODO: Implement
        pass
    
    def list_mem_ids(self):
        """List membership with ring IDs"""
        members = self.membership.membership.get_members()
        print("Membership List (sorted by NodeID):")
        
        member_list = []
        for host, member_data in members.items():
            if member_data['status'] == 'ACTIVE':
                node_id = member_data.get('member_id')
                ring_id = self.ring.get_node_position(node_id)
                member_list.append((ring_id, host, node_id))
        
        member_list.sort()
        
        for ring_id, host, node_id in member_list:
            print(f"  {ring_id}: {host} ({node_id})")