"""
Tuple Tracker - Tracks processed tuples for exactly-once semantics
"""
import time
from typing import Set, Dict
import json

class TupleTracker:
    """
    Tracks which tuples have been processed to prevent duplicates.
    """
    
    def __init__(self, task_id: str, logger):
        self.task_id = task_id
        self.logger = logger
        
        # Set of processed tuple IDs
        self.processed: Set[str] = set()
        
        # Timestamp of last processing for cleanup
        self.last_processed_time: Dict[str, float] = {}
        
        # Cleanup threshold (1 hour)
        self.cleanup_threshold = 3600.0
    
    def is_duplicate(self, tuple_id: str) -> bool:
        """Check if a tuple has already been processed."""
        return tuple_id in self.processed
    
    def mark_processed(self, tuple_id: str):
        """Mark a tuple as processed."""
        self.processed.add(tuple_id)
        self.last_processed_time[tuple_id] = time.time()
        
        # Periodic cleanup
        if len(self.processed) % 1000 == 0:
            self._cleanup_old_entries()
    
    def _cleanup_old_entries(self):
        """Remove old entries to prevent unbounded growth."""
        now = time.time()
        to_remove = []
        
        for tuple_id, timestamp in self.last_processed_time.items():
            if now - timestamp > self.cleanup_threshold:
                to_remove.append(tuple_id)
        
        for tuple_id in to_remove:
            self.processed.discard(tuple_id)
            del self.last_processed_time[tuple_id]
        
        if to_remove:
            self.logger.log(f"Cleaned up {len(to_remove)} old tuple entries")
    
    def serialize_state(self) -> str:
        """Serialize state for persistence."""
        state = {
            'processed': list(self.processed),
            'timestamps': self.last_processed_time
        }
        return json.dumps(state)
    
    def deserialize_state(self, data: str):
        """Deserialize state from persistence."""
        try:
            state = json.loads(data)
            self.processed = set(state.get('processed', []))
            self.last_processed_time = state.get('timestamps', {})
            
            self.logger.log(f"Loaded {len(self.processed)} processed tuples from state")
        except Exception as e:
            self.logger.log(f"Error deserializing state: {e}")