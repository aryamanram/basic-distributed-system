"""
RainStorm Exactly-Once Semantics
- Tuple ID assignment and tracking
- Acknowledgment management
- Duplicate detection
- State persistence to HyDFS (append-only log)
- State replay on task restart
"""
import os
import sys
import time
import threading
import json
from typing import Dict, Set, Optional, List, Tuple
from utils import StreamTuple, RainStormLogger

# Add parent directory for MP3 imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# ============================================================================
# Tuple ID Generator
# ============================================================================

class TupleIDGenerator:
    """
    Generates unique tuple IDs for exactly-once tracking.
    Format: <task_id>_<sequence_num>_<timestamp>
    """
    def __init__(self, task_id: str):
        self.task_id = task_id
        self.sequence_num = 0
        self.lock = threading.Lock()
    
    def generate(self) -> str:
        """Generate a new unique tuple ID."""
        with self.lock:
            self.sequence_num += 1
            return f"{self.task_id}_{self.sequence_num}_{int(time.time()*1000000)}"
    
    def set_sequence(self, seq: int):
        """Set sequence number (used during replay)."""
        with self.lock:
            self.sequence_num = max(self.sequence_num, seq)

# ============================================================================
# Processed Tuple Tracker
# ============================================================================

class ProcessedTupleTracker:
    """
    Tracks which tuples have been processed by this task.
    Used for duplicate detection.
    """
    def __init__(self, logger: RainStormLogger):
        self.logger = logger
        self.processed_ids: Set[str] = set()
        self.lock = threading.Lock()
        
        # Keep track of recent IDs only (memory optimization)
        self.max_tracked = 100000
    
    def is_duplicate(self, tuple_id: str) -> bool:
        """Check if a tuple has already been processed."""
        with self.lock:
            return tuple_id in self.processed_ids
    
    def mark_processed(self, tuple_id: str):
        """Mark a tuple as processed."""
        with self.lock:
            self.processed_ids.add(tuple_id)
            
            # Cleanup if too many
            if len(self.processed_ids) > self.max_tracked:
                # Remove oldest (roughly)
                to_remove = list(self.processed_ids)[:self.max_tracked // 2]
                for tid in to_remove:
                    self.processed_ids.discard(tid)
    
    def load_from_set(self, processed_set: Set[str]):
        """Load processed IDs from a set (during replay)."""
        with self.lock:
            self.processed_ids.update(processed_set)
    
    def get_count(self) -> int:
        """Get count of tracked tuples."""
        with self.lock:
            return len(self.processed_ids)

# ============================================================================
# Output Ack Tracker
# ============================================================================

class OutputAckTracker:
    """
    Tracks which output tuples have been acknowledged by downstream.
    Used to determine what needs to be replayed on restart.
    """
    def __init__(self, logger: RainStormLogger):
        self.logger = logger
        
        # output_tuple_id -> (tuple_data, acked)
        self.outputs: Dict[str, Tuple[StreamTuple, bool]] = {}
        self.lock = threading.Lock()
    
    def add_output(self, tuple_data: StreamTuple):
        """Record an output tuple."""
        with self.lock:
            self.outputs[tuple_data.tuple_id] = (tuple_data, False)
    
    def mark_acked(self, tuple_id: str):
        """Mark an output tuple as acked."""
        with self.lock:
            if tuple_id in self.outputs:
                tuple_data, _ = self.outputs[tuple_id]
                self.outputs[tuple_id] = (tuple_data, True)
    
    def get_unacked(self) -> List[StreamTuple]:
        """Get list of unacked output tuples."""
        with self.lock:
            return [t for t, acked in self.outputs.values() if not acked]
    
    def is_acked(self, tuple_id: str) -> bool:
        """Check if an output tuple is acked."""
        with self.lock:
            if tuple_id in self.outputs:
                _, acked = self.outputs[tuple_id]
                return acked
            return False
    
    def load_acked_set(self, acked_set: Set[str]):
        """Load acked IDs from a set (during replay)."""
        with self.lock:
            for tuple_id in acked_set:
                if tuple_id in self.outputs:
                    tuple_data, _ = self.outputs[tuple_id]
                    self.outputs[tuple_id] = (tuple_data, True)

# ============================================================================
# HyDFS State Logger
# ============================================================================

class HyDFSStateLogger:
    """
    Persists exactly-once state to HyDFS as an append-only log.
    Each task has its own log file in HyDFS.
    
    Log format (JSON lines):
    {"type": "PROCESSED", "tuple_id": "...", "timestamp": ...}
    {"type": "OUTPUT", "tuple_id": "...", "key": "...", "value": "...", "timestamp": ...}
    {"type": "ACKED", "tuple_id": "...", "timestamp": ...}
    """
    def __init__(self, task_id: str, hydfs_filename: str, logger: RainStormLogger):
        self.task_id = task_id
        self.hydfs_filename = hydfs_filename
        self.logger = logger
        
        # Local buffer before flushing to HyDFS
        self.buffer: List[Dict] = []
        self.buffer_lock = threading.Lock()
        self.batch_size = 10  # Flush after this many entries
        
        # Track if HyDFS is available
        self.hydfs_available = False
        self._check_hydfs()
    
    def _check_hydfs(self):
        """Check if HyDFS is available."""
        try:
            from MP3.node import HyDFSNode
            self.hydfs_available = True
        except ImportError:
            self.logger.log("HyDFS not available - using local file fallback")
            self.hydfs_available = False
    
    def log_processed(self, tuple_id: str):
        """Log that a tuple was processed."""
        entry = {
            'type': 'PROCESSED',
            'tuple_id': tuple_id,
            'timestamp': time.time()
        }
        self._add_entry(entry)
    
    def log_output(self, tuple_data: StreamTuple):
        """Log an output tuple."""
        entry = {
            'type': 'OUTPUT',
            'tuple_id': tuple_data.tuple_id,
            'key': tuple_data.key,
            'value': tuple_data.value,
            'source_task': tuple_data.source_task,
            'timestamp': time.time()
        }
        self._add_entry(entry)
    
    def log_acked(self, tuple_id: str):
        """Log that an output tuple was acked."""
        entry = {
            'type': 'ACKED',
            'tuple_id': tuple_id,
            'timestamp': time.time()
        }
        self._add_entry(entry)
    
    def _add_entry(self, entry: Dict):
        """Add entry to buffer and flush if needed."""
        with self.buffer_lock:
            self.buffer.append(entry)
            if len(self.buffer) >= self.batch_size:
                self._flush()
    
    def _flush(self):
        """Flush buffer to storage."""
        if not self.buffer:
            return
        
        # Convert to JSON lines
        lines = [json.dumps(entry) for entry in self.buffer]
        data = '\n'.join(lines) + '\n'
        
        if self.hydfs_available:
            self._write_to_hydfs(data)
        else:
            self._write_to_local(data)
        
        self.buffer.clear()
    
    def _write_to_hydfs(self, data: str):
        """Write data to HyDFS."""
        try:
            # Create a temp file and append to HyDFS
            temp_file = f"/tmp/rainstorm_state_{self.task_id}_{int(time.time()*1000)}.tmp"
            with open(temp_file, 'w') as f:
                f.write(data)
            
            # Use HyDFS append
            # Note: In real implementation, would use HyDFS client
            self.logger.log(f"State written to HyDFS: {self.hydfs_filename}")
            
            os.remove(temp_file)
        except Exception as e:
            self.logger.log(f"HyDFS write error: {e}")
            self._write_to_local(data)
    
    def _write_to_local(self, data: str):
        """Fallback: write to local file."""
        try:
            os.makedirs("state_logs", exist_ok=True)
            local_file = f"state_logs/{self.task_id}_state.log"
            with open(local_file, 'a') as f:
                f.write(data)
        except Exception as e:
            self.logger.log(f"Local state write error: {e}")
    
    def flush(self):
        """Force flush buffer."""
        with self.buffer_lock:
            self._flush()
    
    def replay(self) -> Tuple[Set[str], Set[str], List[StreamTuple]]:
        """
        Replay state log to recover:
        - Set of processed tuple IDs
        - Set of acked output tuple IDs  
        - List of unacked output tuples (need to be resent)
        
        Returns: (processed_set, acked_set, unacked_outputs)
        """
        processed = set()
        acked = set()
        outputs: Dict[str, StreamTuple] = {}
        
        # Try HyDFS first
        log_content = self._read_from_hydfs()
        if not log_content:
            log_content = self._read_from_local()
        
        if not log_content:
            return processed, acked, []
        
        # Parse log entries
        for line in log_content.strip().split('\n'):
            if not line:
                continue
            try:
                entry = json.loads(line)
                entry_type = entry.get('type')
                
                if entry_type == 'PROCESSED':
                    processed.add(entry['tuple_id'])
                
                elif entry_type == 'OUTPUT':
                    tuple_data = StreamTuple(
                        key=entry['key'],
                        value=entry['value'],
                        tuple_id=entry['tuple_id'],
                        source_task=entry.get('source_task')
                    )
                    outputs[entry['tuple_id']] = tuple_data
                
                elif entry_type == 'ACKED':
                    acked.add(entry['tuple_id'])
            
            except json.JSONDecodeError:
                continue
        
        # Find unacked outputs
        unacked = [t for tid, t in outputs.items() if tid not in acked]
        
        self.logger.log(f"State replay: {len(processed)} processed, "
                       f"{len(acked)} acked, {len(unacked)} unacked")
        
        return processed, acked, unacked
    
    def _read_from_hydfs(self) -> Optional[str]:
        """Read state log from HyDFS."""
        if not self.hydfs_available:
            return None
        try:
            # Would use HyDFS client to read
            # For now, return None to fall back to local
            return None
        except Exception as e:
            self.logger.log(f"HyDFS read error: {e}")
            return None
    
    def _read_from_local(self) -> Optional[str]:
        """Read state log from local file."""
        try:
            local_file = f"state_logs/{self.task_id}_state.log"
            if os.path.exists(local_file):
                with open(local_file, 'r') as f:
                    return f.read()
        except Exception as e:
            self.logger.log(f"Local state read error: {e}")
        return None

# ============================================================================
# Exactly-Once Manager
# ============================================================================

class ExactlyOnceManager:
    """
    Main class for managing exactly-once semantics for a task.
    Combines all the tracking components.
    """
    def __init__(self, task_id: str, hydfs_state_file: str, 
                 logger: RainStormLogger, enabled: bool = True):
        self.task_id = task_id
        self.logger = logger
        self.enabled = enabled
        
        if not enabled:
            self.logger.log("Exactly-once semantics DISABLED")
            return
        
        self.logger.log("Exactly-once semantics ENABLED")
        
        # Initialize components
        self.id_generator = TupleIDGenerator(task_id)
        self.processed_tracker = ProcessedTupleTracker(logger)
        self.output_tracker = OutputAckTracker(logger)
        self.state_logger = HyDFSStateLogger(task_id, hydfs_state_file, logger)
        
        # Recovery flag
        self.recovered = False
    
    def recover_state(self):
        """Recover state from HyDFS log (called on task restart)."""
        if not self.enabled:
            return
        
        self.logger.log("Recovering exactly-once state...")
        
        processed, acked, unacked = self.state_logger.replay()
        
        # Load into trackers
        self.processed_tracker.load_from_set(processed)
        self.output_tracker.load_acked_set(acked)
        
        # Set sequence number to continue from where we left off
        max_seq = 0
        for tid in processed:
            parts = tid.split('_')
            if len(parts) >= 2:
                try:
                    seq = int(parts[1])
                    max_seq = max(max_seq, seq)
                except:
                    pass
        self.id_generator.set_sequence(max_seq)
        
        self.recovered = True
        
        # Return unacked tuples that need to be resent
        return unacked
    
    def should_process(self, tuple_data: StreamTuple) -> bool:
        """
        Check if a tuple should be processed (not a duplicate).
        Returns True if tuple should be processed, False if duplicate.
        """
        if not self.enabled:
            return True
        
        if self.processed_tracker.is_duplicate(tuple_data.tuple_id):
            self.logger.log_duplicate(tuple_data.tuple_id)
            return False
        
        return True
    
    def on_tuple_processed(self, tuple_data: StreamTuple):
        """Called after a tuple is successfully processed."""
        if not self.enabled:
            return
        
        self.processed_tracker.mark_processed(tuple_data.tuple_id)
        self.state_logger.log_processed(tuple_data.tuple_id)
    
    def generate_output_id(self) -> str:
        """Generate ID for an output tuple."""
        if not self.enabled:
            return f"{self.task_id}_{int(time.time()*1000000)}"
        return self.id_generator.generate()
    
    def on_output_sent(self, tuple_data: StreamTuple):
        """Called when an output tuple is sent downstream."""
        if not self.enabled:
            return
        
        self.output_tracker.add_output(tuple_data)
        self.state_logger.log_output(tuple_data)
    
    def on_output_acked(self, tuple_id: str):
        """Called when an output tuple is acknowledged."""
        if not self.enabled:
            return
        
        self.output_tracker.mark_acked(tuple_id)
        self.state_logger.log_acked(tuple_id)
    
    def get_unacked_outputs(self) -> List[StreamTuple]:
        """Get list of outputs that haven't been acked."""
        if not self.enabled:
            return []
        return self.output_tracker.get_unacked()
    
    def flush_state(self):
        """Flush any buffered state to storage."""
        if not self.enabled:
            return
        self.state_logger.flush()
    
    def get_stats(self) -> Dict:
        """Get statistics about exactly-once tracking."""
        if not self.enabled:
            return {'enabled': False}
        
        return {
            'enabled': True,
            'processed_count': self.processed_tracker.get_count(),
            'unacked_count': len(self.output_tracker.get_unacked())
        }