"""
Consistency management and merge operations for HyDFS
"""
import threading
import time
from typing import Dict, List, Optional, Set
from storage import FileBlock

class ClientSequenceTracker:
    """
    Tracks sequence numbers for each client to enforce per-client ordering.
    """
    def __init__(self):
        self.lock = threading.Lock()
        # client_id -> next expected sequence number
        self.sequences: Dict[str, int] = {}
    
    def get_next_sequence(self, client_id: str) -> int:
        """
        Get and increment the sequence number for a client.
        """
        with self.lock:
            if client_id not in self.sequences:
                self.sequences[client_id] = 0
            seq = self.sequences[client_id]
            self.sequences[client_id] += 1
            return seq
    
    def reset_client(self, client_id: str):
        """
        Reset sequence counter for a client.
        """
        with self.lock:
            if client_id in self.sequences:
                del self.sequences[client_id]

class ConsistencyManager:
    """
    Manages consistency guarantees for file operations.
    """
    def __init__(self, logger):
        self.logger = logger
        self.tracker = ClientSequenceTracker()
        
        # Track pending writes per file for read-my-writes
        # filename -> Set[client_id]
        self.pending_writes: Dict[str, Set[str]] = {}
        self.pending_lock = threading.Lock()
        
        # Track completed writes per client for read-my-writes
        # (client_id, filename) -> timestamp
        self.completed_writes: Dict[tuple, float] = {}
        self.completed_lock = threading.Lock()
    
    def get_next_sequence(self, client_id: str) -> int:
        """
        Get next sequence number for a client.
        """
        return self.tracker.get_next_sequence(client_id)
    
    def mark_write_pending(self, filename: str, client_id: str):
        """
        Mark a write as pending for read-my-writes guarantee.
        """
        with self.pending_lock:
            if filename not in self.pending_writes:
                self.pending_writes[filename] = set()
            self.pending_writes[filename].add(client_id)
    
    def mark_write_complete(self, filename: str, client_id: str):
        """
        Mark a write as complete.
        """
        with self.pending_lock:
            if filename in self.pending_writes:
                self.pending_writes[filename].discard(client_id)
                if not self.pending_writes[filename]:
                    del self.pending_writes[filename]
        
        with self.completed_lock:
            self.completed_writes[(client_id, filename)] = time.time()
    
    def has_pending_writes(self, filename: str, client_id: str) -> bool:
        """
        Check if a client has pending writes for a file.
        """
        with self.pending_lock:
            if filename in self.pending_writes:
                return client_id in self.pending_writes[filename]
            return False
    
    def merge_blocks(self, blocks_list: List[List[FileBlock]]) -> List[FileBlock]:
        """
        Merge blocks from multiple replicas.
        
        Algorithm:
        1. Collect all unique blocks from all replicas
        2. Sort by (client_id, sequence_num, timestamp)
        3. This ensures per-client ordering and eventual consistency
        
        Returns the merged, ordered list of blocks.
        """
        if not blocks_list:
            return []
        
        # Collect all unique blocks
        block_map: Dict[str, FileBlock] = {}
        for blocks in blocks_list:
            for block in blocks:
                # Use block_id as unique identifier
                if block.block_id not in block_map:
                    block_map[block.block_id] = block
                else:
                    # If we see the same block_id, keep the one with earlier timestamp
                    existing = block_map[block.block_id]
                    if block.timestamp < existing.timestamp:
                        block_map[block.block_id] = block
        
        # Sort blocks by client_id, then sequence_num, then timestamp
        sorted_blocks = sorted(
            block_map.values(),
            key=lambda b: (b.client_id, b.sequence_num, b.timestamp)
        )
        
        self.logger.log(f"MERGE: Merged {len(block_map)} unique blocks from {len(blocks_list)} replicas")
        
        return sorted_blocks
    
    def validate_block_ordering(self, blocks: List[FileBlock]) -> bool:
        """
        Validate that blocks maintain per-client ordering.
        """
        # Track last sequence number for each client
        client_sequences: Dict[str, int] = {}
        
        for block in blocks:
            client_id = block.client_id
            seq_num = block.sequence_num
            
            if client_id in client_sequences:
                # Check if sequence number is increasing
                if seq_num <= client_sequences[client_id]:
                    self.logger.log(
                        f"CONSISTENCY ERROR: Out-of-order blocks for client {client_id}: "
                        f"{client_sequences[client_id]} -> {seq_num}"
                    )
                    return False
            
            client_sequences[client_id] = seq_num
        
        return True
    
    def detect_conflicts(self, blocks: List[FileBlock]) -> List[str]:
        """
        Detect any potential conflicts in block ordering.
        Returns list of conflict descriptions.
        """
        conflicts = []
        
        # Check for duplicate block IDs
        block_ids = [b.block_id for b in blocks]
        if len(block_ids) != len(set(block_ids)):
            conflicts.append("Duplicate block IDs detected")
        
        # Check for gaps in sequence numbers per client
        client_sequences: Dict[str, List[int]] = {}
        for block in blocks:
            if block.client_id not in client_sequences:
                client_sequences[block.client_id] = []
            client_sequences[block.client_id].append(block.sequence_num)
        
        for client_id, seqs in client_sequences.items():
            seqs_sorted = sorted(seqs)
            for i in range(len(seqs_sorted) - 1):
                if seqs_sorted[i+1] - seqs_sorted[i] > 1:
                    conflicts.append(
                        f"Gap in sequence for client {client_id}: "
                        f"{seqs_sorted[i]} -> {seqs_sorted[i+1]}"
                    )
        
        return conflicts

class MergeCoordinator:
    """
    Coordinates merge operations across replicas.
    """
    def __init__(self, logger):
        self.logger = logger
        self.merges_in_progress: Set[str] = set()
        self.merge_lock = threading.Lock()
    
    def start_merge(self, filename: str) -> bool:
        """
        Try to start a merge for a file.
        Returns True if merge started, False if already in progress.
        """
        with self.merge_lock:
            if filename in self.merges_in_progress:
                return False
            self.merges_in_progress.add(filename)
            return True
    
    def finish_merge(self, filename: str):
        """
        Mark a merge as finished.
        """
        with self.merge_lock:
            self.merges_in_progress.discard(filename)
    
    def is_merge_in_progress(self, filename: str) -> bool:
        """
        Check if a merge is in progress for a file.
        """
        with self.merge_lock:
            return filename in self.merges_in_progress