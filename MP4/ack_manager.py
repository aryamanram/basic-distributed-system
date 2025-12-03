"""
ACK Manager - Manages acknowledgments for exactly-once semantics
"""
import time
import threading
from typing import Dict, Set, Optional, Callable

class AckManager:
    """
    Manages acknowledgments for sent tuples.
    Tracks which tuples have been acked and handles retries.
    """
    
    def __init__(self, task_id: str, logger, ack_timeout: float = 2.0, max_retries: int = 3):
        self.task_id = task_id
        self.logger = logger
        self.ack_timeout = ack_timeout
        self.max_retries = max_retries
        
        # Tracking
        self.pending_acks: Dict[str, float] = {}  # tuple_id -> sent_time
        self.acked: Set[str] = set()
        self.retry_count: Dict[str, int] = {}  # tuple_id -> retry_count
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        # Retry callback
        self.retry_callback: Optional[Callable] = None
    
    def set_retry_callback(self, callback: Callable):
        """Set callback function for retrying tuples."""
        self.retry_callback = callback
    
    def track_sent(self, tuple_id: str):
        """Track a sent tuple that needs acknowledgment."""
        with self.lock:
            self.pending_acks[tuple_id] = time.time()
            self.retry_count[tuple_id] = 0
    
    def mark_acked(self, tuple_id: str):
        """Mark a tuple as acknowledged."""
        with self.lock:
            if tuple_id in self.pending_acks:
                del self.pending_acks[tuple_id]
            
            self.acked.add(tuple_id)
            
            if tuple_id in self.retry_count:
                del self.retry_count[tuple_id]
    
    def check_timeouts(self):
        """
        Check for tuples that have timed out and need retry.
        Should be called periodically.
        """
        now = time.time()
        timed_out = []
        
        with self.lock:
            for tuple_id, sent_time in self.pending_acks.items():
                if now - sent_time > self.ack_timeout:
                    timed_out.append(tuple_id)
        
        # Handle timeouts
        for tuple_id in timed_out:
            self._handle_timeout(tuple_id)
    
    def _handle_timeout(self, tuple_id: str):
        """Handle a timed-out tuple."""
        with self.lock:
            retry_count = self.retry_count.get(tuple_id, 0)
            
            if retry_count < self.max_retries:
                # Retry
                self.retry_count[tuple_id] = retry_count + 1
                self.pending_acks[tuple_id] = time.time()
                
                self.logger.log(f"Retrying tuple {tuple_id} (attempt {retry_count + 1})")
                
                # Call retry callback
                if self.retry_callback:
                    self.retry_callback(tuple_id)
            else:
                # Give up
                self.logger.log(f"Tuple {tuple_id} failed after {self.max_retries} retries")
                del self.pending_acks[tuple_id]
                del self.retry_count[tuple_id]
    
    def get_pending_count(self) -> int:
        """Get number of pending acknowledgments."""
        with self.lock:
            return len(self.pending_acks)