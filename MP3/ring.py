"""
Consistent Hashing Ring for HyDFS
"""
from typing import Dict, List, Optional, Tuple
from utils import hash_to_ring

class ConsistentHashRing:
    """
    Implements a consistent hashing ring for node and file placement.
    """
    def __init__(self):
        self.nodes: Dict[int, str] = {}  # ring_position -> node_id
        self.sorted_positions: List[int] = []
        
    def add_node(self, node_id: str) -> int:
        """
        Add a node to the ring.
        Returns the position on the ring.
        """
        position = hash_to_ring(node_id)
        self.nodes[position] = node_id
        self.sorted_positions = sorted(self.nodes.keys())
        return position
    
    def remove_node(self, node_id: str) -> bool:
        """
        Remove a node from the ring.
        Returns True if node was found and removed.
        """
        position = hash_to_ring(node_id)
        if position in self.nodes and self.nodes[position] == node_id:
            del self.nodes[position]
            self.sorted_positions = sorted(self.nodes.keys())
            return True
        return False
    
    def get_successors(self, key: str, n: int) -> List[str]:
        """
        Get the first n successor nodes for a given key.
        Returns a list of node IDs.
        """
        if not self.sorted_positions:
            return []
        
        key_position = hash_to_ring(key)
        successors = []
        seen_nodes = set()
        
        # Find the starting position
        start_idx = 0
        for i, pos in enumerate(self.sorted_positions):
            if pos >= key_position:
                start_idx = i
                break
        
        # Collect n unique successors (wrap around the ring)
        idx = start_idx
        while len(successors) < n and len(seen_nodes) < len(self.nodes):
            pos = self.sorted_positions[idx % len(self.sorted_positions)]
            node_id = self.nodes[pos]
            
            if node_id not in seen_nodes:
                successors.append(node_id)
                seen_nodes.add(node_id)
            
            idx += 1
        
        return successors
    
    def get_node_position(self, node_id: str) -> Optional[int]:
        """
        Get the ring position for a node ID.
        """
        position = hash_to_ring(node_id)
        if position in self.nodes and self.nodes[position] == node_id:
            return position
        return None
    
    def get_all_nodes(self) -> List[Tuple[int, str]]:
        """
        Get all nodes as (position, node_id) tuples, sorted by position.
        """
        return [(pos, self.nodes[pos]) for pos in self.sorted_positions]
    
    def get_node_count(self) -> int:
        """
        Get the number of nodes in the ring.
        """
        return len(self.nodes)
    
    def get_file_position(self, filename: str) -> int:
        """
        Get the ring position for a file.
        """
        return hash_to_ring(filename)