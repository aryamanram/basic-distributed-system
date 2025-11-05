"""
Replication Manager
Handles file operations across replicas
"""

import time
from storage import Block

class ReplicationManager:
    def __init__(self, node, ring, storage, network):
        self.node = node
        self.ring = ring
        self.storage = storage
        self.network = network
        
        # Track client operations for read-my-writes
        self.client_sequences = {}  # client_id -> sequence_number
    
    def create(self, local_filename, hydfs_filename):
        """Create a file in HyDFS"""
        # Check if file already exists
        replicas = self.ring.get_replicas_for_file(hydfs_filename)
        
        # TODO: Check with replicas if file exists
        # If exists, return False
        
        # Read local file
        try:
            with open(local_filename, 'rb') as f:
                data = f.read()
        except Exception as e:
            print(f"Error reading {local_filename}: {e}")
            return False
        
        # Create initial block
        client_id = self.node.node_id
        sequence = self._get_next_sequence(client_id)
        block = Block(data, client_id, sequence, time.time())
        
        # Send to all replicas
        success_count = 0
        for replica in replicas:
            message = {
                'type': 'CREATE_REQUEST',
                'filename': hydfs_filename,
                'block': block.to_dict()
            }
            
            try:
                response = self.network.send_request(replica, message)
                if response and response.get('success'):
                    success_count += 1
            except Exception as e:
                print(f"Error sending to {replica}: {e}")
        
        # Print replicas
        print(f"Replicated to: {replicas}")
        
        return success_count == len(replicas)
    
    def get(self, hydfs_filename, local_filename):
        """Get a file from HyDFS"""
        replicas = self.ring.get_replicas_for_file(hydfs_filename)
        
        if not replicas:
            print(f"No replicas found for {hydfs_filename}")
            return False
        
        # Try to get from first replica
        # TODO: Implement read-my-writes logic
        replica = replicas[0]
        
        message = {
            'type': 'GET_REQUEST',
            'filename': hydfs_filename,
            'client_id': self.node.node_id,
            'sequence': self.client_sequences.get(self.node.node_id, 0)
        }
        
        try:
            response = self.network.send_request(replica, message)
            if response and response.get('success'):
                blocks = [Block.from_dict(b) for b in response['blocks']]
                
                # Write to local file
                with open(local_filename, 'wb') as f:
                    for block in blocks:
                        f.write(block.data)
                
                return True
        except Exception as e:
            print(f"Error getting from {replica}: {e}")
        
        return False
    
    def append(self, local_filename, hydfs_filename):
        """Append to a file in HyDFS"""
        # Read local file
        try:
            with open(local_filename, 'rb') as f:
                data = f.read()
        except Exception as e:
            print(f"Error reading {local_filename}: {e}")
            return False
        
        # Create append block
        client_id = self.node.node_id
        sequence = self._get_next_sequence(client_id)
        block = Block(data, client_id, sequence, time.time())
        
        # Get replicas
        replicas = self.ring.get_replicas_for_file(hydfs_filename)
        
        # Send to all replicas
        success_count = 0
        for replica in replicas:
            message = {
                'type': 'APPEND_REQUEST',
                'filename': hydfs_filename,
                'block': block.to_dict()
            }
            
            try:
                response = self.network.send_request(replica, message)
                if response and response.get('success'):
                    success_count += 1
            except Exception as e:
                print(f"Error sending to {replica}: {e}")
        
        return success_count == len(replicas)
    
    def handle_create_request(self, request):
        """Handle CREATE request"""
        filename = request['filename']
        block = Block.from_dict(request['block'])
        
        # Store file
        self.storage.store_file(filename, [block])
        
        print(f"[REPLICA] Created {filename}")
        
        return {'success': True}
    
    def handle_get_request(self, request):
        """Handle GET request"""
        filename = request['filename']
        
        blocks = self.storage.get_file(filename)
        if blocks is None:
            return {'success': False, 'error': 'File not found'}
        
        print(f"[REPLICA] Served {filename}")
        
        return {
            'success': True,
            'blocks': [b.to_dict() for b in blocks]
        }
    
    def handle_append_request(self, request):
        """Handle APPEND request"""
        filename = request['filename']
        block = Block.from_dict(request['block'])
        
        success = self.storage.append_block(filename, block)
        
        print(f"[REPLICA] Appended to {filename}")
        
        return {'success': success}
    
    def re_replicate(self, filename, failed_nodes):
        """Re-replicate file after failure"""
        # TODO: Implement re-replication logic
        pass
    
    def re_balance(self, filename, new_node):
        """Re-balance files after node join"""
        # TODO: Implement re-balancing logic
        pass
    
    def _get_next_sequence(self, client_id):
        """Get next sequence number for client"""
        if client_id not in self.client_sequences:
            self.client_sequences[client_id] = 0
        self.client_sequences[client_id] += 1
        return self.client_sequences[client_id]