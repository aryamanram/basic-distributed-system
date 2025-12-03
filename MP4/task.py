"""
Task - Executes operator logic and forwards tuples
"""
import subprocess
import threading
import time
import queue
from typing import Dict, List, Optional, Tuple
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from tuple_tracker import TupleTracker
from ack_manager import AckManager
from partitioner import HashPartitioner
from utils.hydfs_client import HyDFSClient

class Task:
    """
    A single task that executes an operator.
    
    Responsibilities:
    - Execute operator on incoming tuples
    - Forward output tuples to next stage
    - Track processed tuples (for exactly-once)
    - Handle ACKs from downstream
    - Persist state to HyDFS (for recovery)
    - Measure throughput (for autoscaling)
    """
    
    def __init__(self, task_config: Dict, worker, logger):
        self.task_config = task_config
        self.worker = worker
        self.logger = logger
        
        self.task_id = task_config['task_id']
        self.stage_id = task_config['stage_id']
        self.task_index = task_config['task_index']
        
        # Operator config
        self.operator_exe = task_config['operator']['executable']
        self.operator_args = task_config['operator']['arguments']
        
        # Downstream stage info
        self.downstream_tasks = task_config.get('downstream_tasks', [])
        
        # Exactly-once components
        self.exactly_once = task_config.get('exactly_once', False)
        self.tuple_tracker = TupleTracker(self.task_id, logger) if self.exactly_once else None
        self.ack_manager = AckManager(self.task_id, logger) if self.exactly_once else None
        
        # Partitioning
        self.partitioner = HashPartitioner(len(self.downstream_tasks))
        
        # Tuple queues
        self.input_queue = queue.Queue(maxsize=1000)
        self.output_queue = queue.Queue(maxsize=1000)
        
        # State
        self.running = False
        self.operator_process: Optional[subprocess.Popen] = None
        
        # Metrics
        self.tuples_processed = 0
        self.tuples_forwarded = 0
        self.start_time = time.time()
        
        # HyDFS for state persistence
        self.hydfs_client = HyDFSClient(logger)
        self.state_file = f"rainstorm_state_{self.task_id}.log"
        
        # Load previous state if recovering
        if self.exactly_once:
            self._load_state()
    
    def start(self):
        """Start the task."""
        self.logger.log(f"Task {self.task_id} starting")
        self.running = True
        
        # Start operator process
        self._start_operator()
        
        # Start processing threads
        threading.Thread(target=self._process_tuples, daemon=True).start()
        threading.Thread(target=self._forward_tuples, daemon=True).start()
        threading.Thread(target=self._report_metrics, daemon=True).start()
        
        if self.exactly_once:
            threading.Thread(target=self._persist_state, daemon=True).start()
        
        self.logger.log(f"Task {self.task_id} started")
    
    def stop(self):
        """Stop the task."""
        self.logger.log(f"Task {self.task_id} stopping")
        self.running = False
        
        if self.operator_process:
            self.operator_process.terminate()
            self.operator_process.wait()
        
        self.logger.log(f"Task {self.task_id} stopped")
    
    def is_running(self) -> bool:
        """Check if task is running."""
        return self.running and (self.operator_process is None or 
                                self.operator_process.poll() is None)
    
    def receive_tuple(self, message: Dict):
        """Receive a tuple from upstream task or source."""
        try:
            self.input_queue.put(message, timeout=1.0)
        except queue.Full:
            self.logger.log(f"Task {self.task_id} input queue full, dropping tuple")
    
    def receive_ack(self, message: Dict):
        """Receive ACK from downstream task."""
        if self.ack_manager:
            tuple_id = message.get('tuple_id')
            self.ack_manager.mark_acked(tuple_id)
    
    def _start_operator(self):
        """Start the operator process."""
        try:
            cmd = [self.operator_exe]
            if self.operator_args:
                cmd.extend(self.operator_args.split())
            
            self.operator_process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            
            self.logger.log(f"Task {self.task_id} started operator: {cmd}")
        
        except Exception as e:
            self.logger.log(f"Error starting operator for {self.task_id}: {e}")
            raise
    
    def _process_tuples(self):
        """Process tuples from input queue."""
        while self.running:
            try:
                # Get tuple from queue
                message = self.input_queue.get(timeout=1.0)
                
                tuple_id = message.get('tuple_id')
                key = message.get('key')
                value = message.get('value')
                
                # Check for duplicate (if exactly-once)
                if self.exactly_once:
                    if self.tuple_tracker.is_duplicate(tuple_id):
                        self.logger.log(f"Task {self.task_id} dropping duplicate {tuple_id}")
                        # Still send ACK
                        self._send_ack(message.get('source_task'), tuple_id)
                        continue
                    
                    self.tuple_tracker.mark_processed(tuple_id)
                
                # Execute operator
                output_tuples = self._execute_operator(key, value)
                
                # Forward output tuples
                for out_key, out_value in output_tuples:
                    self.output_queue.put({
                        'tuple_id': f"{tuple_id}_out",
                        'key': out_key,
                        'value': out_value,
                        'source_task': self.task_id
                    })
                
                # Send ACK to source
                if self.exactly_once:
                    self._send_ack(message.get('source_task'), tuple_id)
                
                self.tuples_processed += 1
            
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.log(f"Error processing tuple in {self.task_id}: {e}")
    
    def _execute_operator(self, key: str, value: str) -> List[Tuple[str, str]]:
        """
        Execute operator on a tuple.
        Returns list of output tuples.
        """
        try:
            # Write input to operator stdin
            input_line = f"{key}\t{value}\n"
            self.operator_process.stdin.write(input_line)
            self.operator_process.stdin.flush()
            
            # Read output from operator stdout
            output_tuples = []
            output_line = self.operator_process.stdout.readline()
            
            if output_line:
                parts = output_line.strip().split('\t', 1)
                if len(parts) == 2:
                    output_tuples.append((parts[0], parts[1]))
            
            return output_tuples
        
        except Exception as e:
            self.logger.log(f"Error executing operator in {self.task_id}: {e}")
            return []
    
    def _forward_tuples(self):
        """Forward output tuples to downstream tasks."""
        while self.running:
            try:
                # Get output tuple
                message = self.output_queue.get(timeout=1.0)
                
                # Determine target task using partitioning
                key = message['key']
                target_idx = self.partitioner.partition(key)
                target_task_config = self.downstream_tasks[target_idx]
                
                # Forward tuple
                forward_message = {
                    'type': 'TUPLE',
                    'target_task_id': target_task_config['task_id'],
                    'tuple_id': message['tuple_id'],
                    'key': message['key'],
                    'value': message['value'],
                    'source_task': self.task_id
                }
                
                # Send to target worker
                target_worker = target_task_config['worker_hostname']
                self.worker.network.send_message(target_worker, 8501, forward_message)
                
                self.tuples_forwarded += 1
                
                # Track for ACK (if exactly-once)
                if self.ack_manager:
                    self.ack_manager.track_sent(message['tuple_id'])
            
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.log(f"Error forwarding tuple from {self.task_id}: {e}")
    
    def _send_ack(self, target_task_id: str, tuple_id: str):
        """Send ACK to upstream task."""
        # Find target task's worker
        # (This requires topology information, simplified here)
        message = {
            'type': 'ACK',
            'task_id': target_task_id,
            'tuple_id': tuple_id
        }
        
        # Send to source task (implementation depends on topology tracking)
        # For now, simplified
    
    def _report_metrics(self):
        """Report metrics to leader for autoscaling."""
        while self.running:
            try:
                time.sleep(5.0)
                
                elapsed = time.time() - self.start_time
                throughput = self.tuples_processed / elapsed if elapsed > 0 else 0
                
                message = {
                    'type': 'TASK_METRICS',
                    'task_id': self.task_id,
                    'stage_id': self.stage_id,
                    'throughput': throughput,
                    'tuples_processed': self.tuples_processed,
                    'tuples_forwarded': self.tuples_forwarded
                }
                
                # Send to leader
                self.worker.network.send_message(
                    self.worker.leader_hostname, 8500, message
                )
            
            except Exception as e:
                self.logger.log(f"Error reporting metrics from {self.task_id}: {e}")
    
    def _persist_state(self):
        """Persist state to HyDFS for recovery."""
        while self.running:
            try:
                time.sleep(10.0)
                
                if self.tuple_tracker:
                    state_data = self.tuple_tracker.serialize_state()
                    
                    # Append state to HyDFS
                    self.hydfs_client.append(state_data, self.state_file)
            
            except Exception as e:
                self.logger.log(f"Error persisting state for {self.task_id}: {e}")
    
    def _load_state(self):
        """Load previous state from HyDFS."""
        try:
            # Get state file from HyDFS
            state_data = self.hydfs_client.get(self.state_file)
            
            if state_data and self.tuple_tracker:
                self.tuple_tracker.deserialize_state(state_data)
                self.logger.log(f"Task {self.task_id} loaded previous state")
        
        except Exception as e:
            self.logger.log(f"Task {self.task_id} could not load state: {e}")