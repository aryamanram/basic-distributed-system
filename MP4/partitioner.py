"""
Partitioner - Hash-based partitioning for load distribution
"""
import hashlib

class HashPartitioner:
    """
    Hash-based partitioner for distributing tuples across tasks.
    Uses MD5 hash and modulo operation.
    """
    
    def __init__(self, num_partitions: int):
        self.num_partitions = num_partitions
    
    def partition(self, key: str) -> int:
        """
        Partition a key to a task index.
        Returns index in range [0, num_partitions).
        """
        if self.num_partitions <= 0:
            return 0
        
        # Hash the key
        hash_obj = hashlib.md5(key.encode('utf-8'))
        hash_value = int(hash_obj.hexdigest(), 16)
        
        # Modulo to get partition
        return hash_value % self.num_partitions