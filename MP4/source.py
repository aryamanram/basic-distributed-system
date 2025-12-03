"""
Source Process - Reads from HyDFS and feeds into first stage
"""
import time
import sys
import os
from typing import List, Optional
import threading

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.hydfs_client import HyDFSClient
from partitioner import HashPartitioner

class SourceProcess:
    """
    Source process that reads files from HyDFS and produces tuples.
    
    Produces tuples in format: <filename:linenumber, line>
    """
    
    def __init__(self, vm_id: int, config: dict, logger, 
                 hydfs_directory: str, input_rate: float):
        self.vm_id = vm_id
        self.config = config
        self.logger = logger
        self.hydfs_directory = hydfs_directory
        self.input_rate = input_rate  # tuples per second
        
        # Components
        self.hydfs_client = HyDFSClient(logger)
        
        # State
        self.running = False
        self.downstream_tasks: List[Dict] = []
        self.partitioner: Optional[HashPartitioner] = None
        
        # Metrics
        self.tuples_sent = 0
        self.start_time = time.time()
    
    def start(self):
        """Start the source process."""
        self.logger.log("Source process starting")
        self.running = True
        
        threading.Thread(target=self._stream_loop, daemon=True).start()
        
        self.logger.log("Source process started")
    
    def stop(self):
        """Stop the source process."""
        self.logger.log("Source process stopping")
        self.running = False
        self.logger.log("Source process stopped")
    
    def set_downstream_tasks(self, tasks: List[Dict]):
        """Set downstream tasks to send tuples to."""
        self.downstream_tasks = tasks
        self.partitioner = HashPartitioner(len(tasks))
        self.logger.log(f"Source configured with {len(tasks)} downstream tasks")
    
    def _stream_loop(self):
        """Main streaming loop."""
        # Get list of files from HyDFS
        files = self._get_hydfs_files()
        
        if not files:
            self.logger.log("No files found in HyDFS directory")
            return
        
        self.logger.log(f"Source streaming from {len(files)} files")
        
        # Calculate delay between tuples
        delay = 1.0 / self.input_rate if self.input_rate > 0 else 0
        
        # Stream all files
        for filename in files:
            if not self.running:
                break
            
            self._stream_file(filename, delay)
        
        self.logger.log(f"Source finished streaming {self.tuples_sent} tuples")
    
    def _get_hydfs_files(self) -> List[str]:
        """Get list of files from HyDFS directory."""
        try:
            # Use HyDFS client to list files
            # For now, we'll use a simple approach
            # In real implementation, this would query HyDFS
            
            # List files in local directory for testing
            import glob
            pattern = os.path.join(self.hydfs_directory, "*.txt")
            files = glob.glob(pattern)
            
            return files
        
        except Exception as e:
            self.logger.log(f"Error listing HyDFS files: {e}")
            return []
    
    def _stream_file(self, filename: str, delay: float):
        """Stream a single file line by line."""
        try:
            with open(filename, 'r') as f:
                line_number = 0
                
                for line in f:
                    if not self.running:
                        break
                    
                    line_number += 1
                    
                    # Create tuple
                    key = f"{os.path.basename(filename)}:{line_number}"
                    value = line.strip()
                    
                    # Send tuple
                    self._send_tuple(key, value)
                    
                    # Rate limiting
                    if delay > 0:
                        time.sleep(delay)
        
        except Exception as e:
            self.logger.log(f"Error streaming file {filename}: {e}")
    
    def _send_tuple(self, key: str, value: str):
        """Send a tuple to downstream tasks."""
        if not self.downstream_tasks:
            return
        
        # Generate tuple ID
        tuple_id = f"source_{self.tuples_sent}"
        
        # Partition to determine target task
        target_idx = self.partitioner.partition(key)
        target_task = self.downstream_tasks[target_idx]
        
        # Create message
        message = {
            'type': 'TUPLE',
            'target_task_id': target_task['task_id'],
            'tuple_id': tuple_id,
            'key': key,
            'value': value,
            'source_task': 'source'
        }
        
        # Send to target worker
        try:
            # Import here to avoid circular dependency
            from network_manager import NetworkManager
            network = NetworkManager(8502, self.logger)
            
            network.send_message(
                target_task['worker_hostname'],
                8501,
                message
            )
            
            self.tuples_sent += 1
        
        except Exception as e:
            self.logger.log(f"Error sending tuple: {e}")