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
    
    def __init__(self, task_id: str, hydfs_client, logger: TaskLogger):
        self.task_id = task_id
        self.hydfs_client = hydfs_client  # May be None
        self.logger = logger
        
        self.processed_ids: Set[str] = set()
        self.acked_outputs: Set[str] = set()
        self.lock = threading.Lock()
        
        self.local_log_file = f"eo_log_{task_id}.log"
        
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
    
    def start(self):
        """Start the task."""
        self.running = True
        
        self.logger.log(f"TASK START: stage={self.stage} idx={self.task_idx} "
                       f"op={self.op_exe} args={self.op_args}")
        
        # Initialize exactly-once tracker (file-based, no HyDFS dependency)
        if self.exactly_once:
            self.eo_tracker = ExactlyOnceTracker(self.task_id, None, self.logger)
        
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
        
        if self.eo_tracker:
            self.eo_tracker.flush()
        
        if self.server_socket:
            self.server_socket.close()
        
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
                self.expected_eof = response.get('num_predecessors', 1)

                self.logger.log(f"CONFIG: successors={len(self.successor_tasks)} "
                              f"dest={self.hydfs_dest} expected_eof={self.expected_eof}")
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
        msg = {'type': 'TASK_COMPLETED', 'task_id': self.task_id}
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
                self._handle_eof(msg)
                conn.sendall(json.dumps({'status': 'ack'}).encode('utf-8'))
        except Exception as e:
            self.logger.log(f"TUPLE ERROR: {e}")
        finally:
            conn.close()
    
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

        # Log output count (not every tuple to avoid spam)
        if self.tuples_processed % 100 == 1:
            self.logger.log(f"PROCESSED: tuple #{self.tuples_processed}, produced {len(output_tuples)} outputs, "
                          f"stage={self.stage}, num_stages={self.num_stages}, successors={len(self.successor_tasks)}")

        # Forward to next stage or output
        if self.stage == self.num_stages - 1:
            # Last stage - write to HyDFS
            self._write_output(output_tuples)
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
            self.logger.log(f"FORWARD SKIP: No successor tasks configured")
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

        success = self._send_tuple_to_task(target, tuple_msg)
        if not success:
            self.logger.log(f"FORWARD FAILED: to {target['task_id']} at {target['hostname']}:{target['port']}")
    
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
            self.logger.log(f"SEND ERROR to {target.get('hostname')}:{target.get('port')}: {e}")
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
        self.logger.log(f"EOF received ({self.eof_received}/{self.expected_eof})")

        # Check if we've received all expected EOFs
        if self.eof_received >= self.expected_eof:
            self.logger.log("All EOFs received, finishing up...")

            # Forward EOF to all successor tasks
            if self.successor_tasks:
                for target in self.successor_tasks:
                    eof_msg = {'type': 'EOF', 'source_task': self.task_id}
                    self._send_tuple_to_task(target, eof_msg)
                self.logger.log(f"Forwarded EOF to {len(self.successor_tasks)} successors")

            # Flush exactly-once tracker
            if self.eo_tracker:
                self.eo_tracker.flush()

            # Give a moment for any pending operations to complete
            time.sleep(0.5)

            # Stop the task
            self.logger.log("Task complete, shutting down")
            self.running = False
    
    def _write_output(self, tuples: List[tuple]):
        """Write output tuples to local file (can be collected to HyDFS later)."""
        if not tuples:
            return
        
        # Write to local output file
        output_file = f"output_{self.task_id}.txt"
        with open(output_file, 'a') as f:
            for key, value in tuples:
                f.write(f"{key}\t{value}\n")
        
        # Also print to console
        for key, value in tuples:
            print(f"OUTPUT: {key}\t{value}")
    
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