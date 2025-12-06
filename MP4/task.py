#!/usr/bin/env python3
"""
RainStorm Task - Individual task process that runs an operator
Handles tuple processing, exactly-once semantics, and rate reporting
"""
import argparse
import json
import os
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Set

# Add parent directory to path for MP3 imports
_parent_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, _parent_dir)
_mp3_dir = os.path.join(_parent_dir, 'MP3')
sys.path.insert(0, _mp3_dir)

RAINSTORM_PORT = 8000
TASK_BASE_PORT = 8100


def get_timestamp() -> str:
    """Get formatted timestamp."""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


class TaskLogger:
    """Logger for task operations."""
    
    def __init__(self, task_id: str, run_id: str, vm_id: int):
        self.task_id = task_id
        self.log_file = f"task_{task_id}_{run_id}.log"
        self.vm_id = vm_id
        self.lock = threading.Lock()
    
    def log(self, msg: str):
        """Log a message with timestamp."""
        line = f"[{get_timestamp()}] [{self.task_id}] {msg}"
        with self.lock:
            print(line)
            with open(self.log_file, 'a') as f:
                f.write(line + "\n")


class ExactlyOnceTracker:
    """
    Tracks processed tuples for exactly-once semantics.
    Uses local file for persistence (can be synced to HyDFS separately).
    """
    
    def __init__(self, task_id: str, run_id: str, logger: TaskLogger):
        self.task_id = task_id
        self.run_id = run_id
        self.logger = logger
        
        self.processed_ids: Set[str] = set()
        self.acked_outputs: Set[str] = set()
        self.lock = threading.Lock()
        
        # Include run_id in log file name to separate between jobs
        self.local_log_file = f"eo_log_{task_id}_{run_id}.log"
        
        # Batch for efficiency
        self.pending_log_entries: List[str] = []
        self.batch_size = 10
        
        # Load existing state from local file
        self._load_state()
    
    def _load_state(self):
        """Load state from local log file."""
        try:
            if os.path.exists(self.local_log_file):
                with open(self.local_log_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith('PROCESSED:'):
                            tuple_id = line[10:]
                            self.processed_ids.add(tuple_id)
                        elif line.startswith('ACK:'):
                            output_id = line[4:]
                            self.acked_outputs.add(output_id)
                
                self.logger.log(f"EO: Loaded {len(self.processed_ids)} processed, "
                              f"{len(self.acked_outputs)} acked")
        except Exception as e:
            self.logger.log(f"EO: No existing log (new task): {e}")
    
    def is_duplicate(self, tuple_id: str) -> bool:
        """Check if a tuple has already been processed."""
        with self.lock:
            if tuple_id in self.processed_ids:
                self.logger.log(f"DUPLICATE REJECTED: {tuple_id}")
                return True
            return False
    
    def mark_processed(self, tuple_id: str):
        """Mark a tuple as processed."""
        with self.lock:
            self.processed_ids.add(tuple_id)
            self.pending_log_entries.append(f"PROCESSED:{tuple_id}")
            
            if len(self.pending_log_entries) >= self.batch_size:
                self._flush_to_file()
    
    def mark_output_acked(self, output_id: str):
        """Mark an output as acknowledged by next stage."""
        with self.lock:
            self.acked_outputs.add(output_id)
            self.pending_log_entries.append(f"ACK:{output_id}")
            
            if len(self.pending_log_entries) >= self.batch_size:
                self._flush_to_file()
    
    def is_output_acked(self, output_id: str) -> bool:
        """Check if an output has been acknowledged."""
        with self.lock:
            return output_id in self.acked_outputs
    
    def _flush_to_file(self):
        """Flush pending log entries to local file."""
        if not self.pending_log_entries:
            return
        
        try:
            with open(self.local_log_file, 'a') as f:
                for entry in self.pending_log_entries:
                    f.write(entry + "\n")
            
            self.pending_log_entries.clear()
        except Exception as e:
            self.logger.log(f"EO: Flush error: {e}")
    
    def flush(self):
        """Force flush pending entries."""
        with self.lock:
            self._flush_to_file()


class RainStormTask:
    """
    Individual RainStorm task process.
    Receives tuples, processes them with operator, and forwards to next stage.
    """
    
    def __init__(self, task_id: str, stage: int, task_idx: int, port: int,
                 op_exe: str, op_args: str, leader_host: str, run_id: str,
                 vm_id: int, exactly_once: bool):
        self.task_id = task_id
        self.stage = stage
        self.task_idx = task_idx
        self.port = port
        self.op_exe = op_exe
        self.op_args = op_args
        self.leader_host = leader_host
        self.run_id = run_id
        self.vm_id = vm_id
        self.exactly_once = exactly_once
        
        self.logger = TaskLogger(task_id, run_id, vm_id)
        
        self.running = False
        self.server_socket: Optional[socket.socket] = None
        
        # Exactly-once tracker
        self.eo_tracker: Optional[ExactlyOnceTracker] = None
        
        # Task configuration from leader
        self.config: Optional[dict] = None
        self.successor_tasks: List[dict] = []
        self.hydfs_dest: str = ''
        self.num_stages: int = 0
        
        # Rate tracking
        self.tuples_processed = 0
        self.last_rate_time = time.time()
        self.last_tuple_count = 0
        
        # Pending outputs for retry
        self.pending_outputs: Dict[str, dict] = {}  # output_id -> {tuple, target, retries}
        self.pending_lock = threading.Lock()
        
        # EOF tracking
        self.eof_received = 0
        self.expected_eof = 0
        
        # Output buffering for HyDFS writes
        self.output_buffer: List[str] = []
        self.output_buffer_lock = threading.Lock()
        self.output_batch_size = 50  # Flush to HyDFS every N tuples
    
    def start(self):
        """Start the task."""
        self.running = True
        
        self.logger.log(f"TASK START: stage={self.stage} idx={self.task_idx} "
                       f"op={self.op_exe} args={self.op_args}")
        
        # Initialize exactly-once tracker (file-based)
        if self.exactly_once:
            self.eo_tracker = ExactlyOnceTracker(self.task_id, self.run_id, self.logger)
        
        # Get configuration from leader
        self._get_config_from_leader()
        
        # Start server to receive tuples
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('', self.port))
        self.server_socket.listen(20)
        
        # Notify leader
        self._notify_leader_started()
        
        # Start rate reporting thread
        threading.Thread(target=self._rate_report_loop, daemon=True).start()
        
        # Start retry thread for exactly-once
        if self.exactly_once:
            threading.Thread(target=self._retry_loop, daemon=True).start()
        
        # Main loop
        self._server_loop()
    
    def stop(self):
        """Stop the task."""
        self.running = False
        
        # Flush any remaining output to HyDFS
        if self.stage == self.num_stages - 1:
            self._flush_output_to_hydfs()
        
        if self.eo_tracker:
            self.eo_tracker.flush()
        
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        
        self._notify_leader_completed()
        
        self.logger.log("TASK END")
    
    def _get_config_from_leader(self):
        """Get task configuration from leader."""
        try:
            msg = {'type': 'GET_TASK_CONFIG', 'task_id': self.task_id}
            response = self._send_to_leader(msg)
            
            if response and response.get('status') == 'success':
                self.config = response
                self.successor_tasks = response.get('successor_tasks', [])
                self.hydfs_dest = response.get('hydfs_dest', '')
                self.num_stages = response.get('num_stages', 1)
                
                self.logger.log(f"CONFIG: successors={len(self.successor_tasks)} "
                              f"dest={self.hydfs_dest} num_stages={self.num_stages}")
        except Exception as e:
            self.logger.log(f"CONFIG ERROR: {e}")
    
    def _notify_leader_started(self):
        """Notify leader that task has started."""
        msg = {
            'type': 'TASK_STARTED',
            'task_id': self.task_id,
            'pid': os.getpid(),
            'log_file': self.logger.log_file
        }
        self._send_to_leader(msg)
    
    def _notify_leader_completed(self):
        """Notify leader that task has completed."""
        msg = {
            'type': 'TASK_COMPLETED', 
            'task_id': self.task_id,
            'tuples_processed': self.tuples_processed
        }
        self._send_to_leader(msg)
    
    def _send_to_leader(self, msg: dict) -> Optional[dict]:
        """Send message to leader."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10.0)
            sock.connect((self.leader_host, RAINSTORM_PORT))
            sock.sendall(json.dumps(msg).encode('utf-8'))
            response = sock.recv(65536).decode('utf-8')
            sock.close()
            return json.loads(response)
        except Exception as e:
            self.logger.log(f"LEADER COMM ERROR: {e}")
            return None
    
    def _server_loop(self):
        """Main server loop to receive tuples."""
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self._handle_tuple,
                               args=(conn, addr), daemon=True).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    self.logger.log(f"SERVER ERROR: {e}")
                break
    
    def _handle_tuple(self, conn: socket.socket, addr: tuple):
        """Handle incoming tuple."""
        try:
            data = conn.recv(65536).decode('utf-8')
            if not data:
                return
            
            msg = json.loads(data)
            msg_type = msg.get('type')
            
            if msg_type == 'TUPLE':
                self._process_tuple(msg)
                # Send ACK
                conn.sendall(json.dumps({'status': 'ack'}).encode('utf-8'))
            elif msg_type == 'ACK':
                self._handle_ack(msg)
            elif msg_type == 'EOF':
                conn.sendall(json.dumps({'status': 'ack'}).encode('utf-8'))
                conn.close()
                self._handle_eof(msg)
                return
        except Exception as e:
            self.logger.log(f"TUPLE ERROR: {e}")
        finally:
            try:
                conn.close()
            except:
                pass
    
    def _process_tuple(self, msg: dict):
        """Process an incoming tuple."""
        tuple_id = msg.get('tuple_id')
        key = msg.get('key')
        value = msg.get('value')
        
        # Check for duplicates in exactly-once mode
        if self.exactly_once and self.eo_tracker:
            if self.eo_tracker.is_duplicate(tuple_id):
                return
        
        # Run operator
        output_tuples = self._run_operator(key, value)
        
        # Mark as processed
        if self.exactly_once and self.eo_tracker:
            self.eo_tracker.mark_processed(tuple_id)
        
        self.tuples_processed += 1
        
        # Forward to next stage or output
        if self.stage == self.num_stages - 1:
            # Last stage - buffer output for HyDFS
            self._buffer_output(output_tuples)
        else:
            # Forward to next stage
            for out_key, out_value in output_tuples:
                self._forward_tuple(tuple_id, out_key, out_value)
    
    def _run_operator(self, key: str, value: str) -> List[tuple]:
        """Run the operator on input tuple."""
        try:
            # Build command
            cmd = [self.op_exe]
            if self.op_args:
                cmd.extend(self.op_args.split())
            
            # Run operator with input via stdin
            input_data = f"{key}\t{value}\n"
            
            result = subprocess.run(
                cmd,
                input=input_data,
                capture_output=True,
                text=True,
                timeout=5.0
            )
            
            # Parse output
            output_tuples = []
            for line in result.stdout.strip().split('\n'):
                if line:
                    parts = line.split('\t', 1)
                    if len(parts) == 2:
                        output_tuples.append((parts[0], parts[1]))
                    elif len(parts) == 1:
                        output_tuples.append((key, parts[0]))
            
            return output_tuples
            
        except subprocess.TimeoutExpired:
            self.logger.log(f"OPERATOR TIMEOUT: {self.op_exe}")
            return []
        except FileNotFoundError:
            self.logger.log(f"OPERATOR NOT FOUND: {self.op_exe}")
            return []
        except Exception as e:
            self.logger.log(f"OPERATOR ERROR: {e}")
            return []
    
    def _forward_tuple(self, source_tuple_id: str, key: str, value: str):
        """Forward tuple to next stage."""
        if not self.successor_tasks:
            return
        
        # Hash partition
        target_idx = hash(key) % len(self.successor_tasks)
        target = self.successor_tasks[target_idx]
        
        output_id = f"{source_tuple_id}_{self.task_id}_{time.time()}"
        
        tuple_msg = {
            'type': 'TUPLE',
            'tuple_id': output_id,
            'key': key,
            'value': value,
            'source_task': self.task_id
        }
        
        # Track for retry if exactly-once
        if self.exactly_once:
            with self.pending_lock:
                self.pending_outputs[output_id] = {
                    'tuple': tuple_msg,
                    'target': target,
                    'retries': 0,
                    'time': time.time()
                }
        
        self._send_tuple_to_task(target, tuple_msg)
    
    def _send_tuple_to_task(self, target: dict, msg: dict) -> bool:
        """Send tuple to a task."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((target['hostname'], target['port']))
            sock.sendall(json.dumps(msg).encode('utf-8'))
            
            # Wait for ACK
            response = sock.recv(1024).decode('utf-8')
            sock.close()
            
            if 'ack' in response:
                # Mark as acked
                output_id = msg.get('tuple_id')
                if self.exactly_once and self.eo_tracker:
                    self.eo_tracker.mark_output_acked(output_id)
                
                with self.pending_lock:
                    if output_id in self.pending_outputs:
                        del self.pending_outputs[output_id]
                
                return True
            return False
        except Exception as e:
            return False
    
    def _handle_ack(self, msg: dict):
        """Handle ACK from next stage."""
        output_id = msg.get('output_id')
        
        if self.exactly_once and self.eo_tracker:
            self.eo_tracker.mark_output_acked(output_id)
        
        with self.pending_lock:
            if output_id in self.pending_outputs:
                del self.pending_outputs[output_id]
    
    def _handle_eof(self, msg: dict):
        """Handle EOF signal."""
        self.eof_received += 1
        self.logger.log(f"EOF received ({self.eof_received})")
        
        # Forward EOF to successor tasks if any
        if self.successor_tasks:
            for target in self.successor_tasks:
                eof_msg = {'type': 'EOF', 'source_task': self.task_id}
                self._send_tuple_to_task(target, eof_msg)
        
        # Flush exactly-once state
        if self.eo_tracker:
            self.eo_tracker.flush()
        
        # Wait briefly for any pending outputs to be acked (exactly-once mode)
        if self.exactly_once:
            max_wait = 5.0
            wait_start = time.time()
            while time.time() - wait_start < max_wait:
                with self.pending_lock:
                    if not self.pending_outputs:
                        break
                time.sleep(0.1)
        
        # Log completion and stop the task
        self.logger.log(f"EOF processed, stopping task (processed {self.tuples_processed} tuples)")
        self.stop()
    
    def _buffer_output(self, tuples: List[tuple]):
        """Buffer output tuples and flush to HyDFS when batch is full."""
        if not tuples:
            return
        
        with self.output_buffer_lock:
            for key, value in tuples:
                output_line = f"{key}\t{value}"
                self.output_buffer.append(output_line)
                
                # Print to console continuously as required by spec
                print(f"OUTPUT: {output_line}")
            
            # Flush to HyDFS if buffer is full
            if len(self.output_buffer) >= self.output_batch_size:
                self._flush_output_to_hydfs()
    
    def _flush_output_to_hydfs(self):
        """Flush buffered output to HyDFS destination file via leader."""
        with self.output_buffer_lock:
            if not self.output_buffer:
                return
            
            if not self.hydfs_dest:
                self.logger.log("WARNING: No HyDFS destination configured, skipping flush")
                self.output_buffer.clear()
                return
            
            try:
                # Send output data to leader for HyDFS append
                output_data = "\n".join(self.output_buffer) + "\n"
                
                msg = {
                    'type': 'APPEND_OUTPUT',
                    'hydfs_dest': self.hydfs_dest,
                    'data': list(output_data.encode('utf-8')),
                    'task_id': self.task_id,
                    'run_id': self.run_id
                }
                
                response = self._send_to_leader(msg)
                
                if response and response.get('status') == 'success':
                    self.logger.log(f"HYDFS FLUSH: Appended {len(self.output_buffer)} lines to {self.hydfs_dest}")
                else:
                    self.logger.log(f"HYDFS FLUSH ERROR: {response.get('message') if response else 'No response'}")
                
                self.output_buffer.clear()
                
            except Exception as e:
                self.logger.log(f"HYDFS FLUSH ERROR: {e}")
    
    def _rate_report_loop(self):
        """Report rate to leader every second."""
        while self.running:
            try:
                time.sleep(1.0)
                
                now = time.time()
                elapsed = now - self.last_rate_time
                
                if elapsed > 0:
                    rate = (self.tuples_processed - self.last_tuple_count) / elapsed
                    
                    msg = {
                        'type': 'RATE_UPDATE',
                        'task_id': self.task_id,
                        'rate': rate,
                        'tuples_processed': self.tuples_processed
                    }
                    self._send_to_leader(msg)
                    
                    self.last_rate_time = now
                    self.last_tuple_count = self.tuples_processed
                    
            except Exception as e:
                pass
    
    def _retry_loop(self):
        """Retry pending outputs for exactly-once."""
        while self.running:
            try:
                time.sleep(2.0)
                
                with self.pending_lock:
                    now = time.time()
                    for output_id, info in list(self.pending_outputs.items()):
                        # Retry if older than 5 seconds
                        if now - info['time'] > 5.0 and info['retries'] < 3:
                            info['retries'] += 1
                            info['time'] = now
                            self._send_tuple_to_task(info['target'], info['tuple'])
                            
            except Exception as e:
                pass


def main():
    parser = argparse.ArgumentParser(description='RainStorm Task')
    parser.add_argument('--task-id', required=True)
    parser.add_argument('--stage', type=int, required=True)
    parser.add_argument('--task-idx', type=int, required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--op-exe', required=True)
    parser.add_argument('--op-args', default='')
    parser.add_argument('--leader-host', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--vm-id', type=int, required=True)
    parser.add_argument('--exactly-once', action='store_true')
    
    args = parser.parse_args()
    
    task = RainStormTask(
        task_id=args.task_id,
        stage=args.stage,
        task_idx=args.task_idx,
        port=args.port,
        op_exe=args.op_exe,
        op_args=args.op_args,
        leader_host=args.leader_host,
        run_id=args.run_id,
        vm_id=args.vm_id,
        exactly_once=args.exactly_once
    )
    
    try:
        task.start()
    except KeyboardInterrupt:
        pass
    finally:
        task.stop()


if __name__ == '__main__':
    main()