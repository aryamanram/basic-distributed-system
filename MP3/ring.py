"""
Consistent Hashing Ring
Maps nodes and files to ring positions
"""

import hashlib
from bisect import bisect_left, bisect_right

class ConsistentHashRing:
    def __init__(self):
        self.ring = []  # Sorted list of (position, node_id)
        self.nodes = {}  # node_id -> address mapping
    
    def hash_node(self, node_id):
        """Hash a node ID to ring position"""
        return int(hashlib.sha256(node_id.encode()).hexdigest(), 16) % (2**32)
    
    def hash_file(self, filename):
        """Hash a filename to ring position"""
        return int(hashlib.sha256(filename.encode()).hexdigest(), 16) % (2**32)
    
    def add_node(self, node_id, address):
        """Add a node to the ring"""
        position = self.hash_node(node_id)
        self.ring.append((position, node_id))
        self.ring.sort()
        self.nodes[node_id] = address
    
    def remove_node(self, node_id):
        """Remove a node from the ring"""
        position = self.hash_node(node_id)
        self.ring = [(pos, nid) for pos, nid in self.ring if nid != node_id]
        if node_id in self.nodes:
            del self.nodes[node_id]
    
    def get_node_position(self, node_id):
        """Get ring position for a node"""
        return self.hash_node(node_id)
    
    def get_file_position(self, filename):
        """Get ring position for a file"""
        return self.hash_file(filename)
    
    def get_n_successors(self, file_id, n):
        """Get n successor nodes for a given file ID"""
        if not self.ring:
            return []
        
        # Find position in ring
        positions = [pos for pos, _ in self.ring]
        idx = bisect_right(positions, file_id) % len(self.ring)
        
        # Get n successors (wrap around)
        # But don't return more nodes than available (no duplicates)
        successors = []
        num_to_return = min(n, len(self.ring))  # ✅ FIX: Only return available nodes
        for i in range(num_to_return):
            successor_idx = (idx + i) % len(self.ring)
            successors.append(self.ring[successor_idx][1])
        
        return successors
    
    def get_replicas_for_file(self, filename):
        """Get replica addresses for a file"""
        file_id = self.hash_file(filename)
        successor_ids = self.get_n_successors(file_id, 3)  # n=3 replicas
        return [self.nodes[node_id] for node_id in successor_ids if node_id in self.nodes]
    
    def get_node_id_for_address(self, address):
        """Get node ID for an address"""
        for node_id, addr in self.nodes.items():
            if addr == address:
                return node_id
        return None