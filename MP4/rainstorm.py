#!/usr/bin/env python3
"""
RainStorm - Stream Processing Framework
Main entry point and Leader coordination

Usage:
  Terminal 1 (VM1): python3 rainstorm.py --vm-id 1 --leader
  Terminal 2-10:    python3 rainstorm.py --vm-id N
  Terminal on VM1:  python3 rainstorm.py --submit --nstages 2 --ntasks 3 ...
"""
import argparse
import json
import os
import socket
import sys
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

# Add parent directory to path for MP2/MP3 imports
_parent_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, _parent_dir)

# Also add MP3 directory itself so its internal imports work
_mp3_dir = os.path.join(_parent_dir, 'MP3')
sys.path.insert(0, _mp3_dir)

# Also add MP2 directory
_mp2_dir = os.path.join(_parent_dir, 'MP2')
sys.path.insert(0, _mp2_dir)

from membership import MembershipService, MemberStatus
from node import HyDFSNode
from main import HyDFSClient

# Ports
RAINSTORM_PORT = 8000
TASK_BASE_PORT = 8100

# Configuration
VM_HOSTS = {
    1: "fa25-cs425-a701.cs.illinois.edu",
    2: "fa25-cs425-a702.cs.illinois.edu",
    3: "fa25-cs425-a703.cs.illinois.edu",
    4: "fa25-cs425-a704.cs.illinois.edu",
    5: "fa25-cs425-a705.cs.illinois.edu",
    6: "fa25-cs425-a706.cs.illinois.edu",
    7: "fa25-cs425-a707.cs.illinois.edu",
    8: "fa25-cs425-a708.cs.illinois.edu",
    9: "fa25-cs425-a709.cs.illinois.edu",
    10: "fa25-cs425-a710.cs.illinois.edu"
}

def get_vm_id_from_hostname(hostname: str) -> int:
    """Get VM ID from hostname."""
    for vm_id, host in VM_HOSTS.items():
        if host == hostname or hostname.startswith(f"fa25-cs425-a7{vm_id:02d}"):
            return vm_id
    return 1

def get_timestamp() -> str:
    """Get formatted timestamp."""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


class RainStormLogger:
    """Logger for RainStorm operations."""
    
    def __init__(self, vm_id: int, run_id: str, is_leader: bool = False):
        self.vm_id = vm_id
        self.run_id = run_id
        prefix = "leader" if is_leader else "rainstorm"
        self.log_file = f"{prefix}_{vm_id}_{run_id}.log"
        self.lock = threading.Lock()
    
    def log(self, msg: str):
        """Log a message with timestamp."""
        line = f"[{get_timestamp()}] {msg}"
        with self.lock:
            print(line)
            with open(self.log_file, 'a') as f:
                f.write(line + "\n")


class TaskInfo:
    """Information about a running task."""
    
    def __init__(self, task_id: str, stage: int, task_idx: int, vm_id: int, 
                 pid: int, op_exe: str, op_args: str, log_file: str):
        self.task_id = task_id
        self.stage = stage
        self.task_idx = task_idx
        self.vm_id = vm_id
        self.pid = pid
        self.op_exe = op_exe
        self.op_args = op_args
        self.log_file = log_file
        self.start_time = time.time()
        self.tuples_processed = 0
        self.last_rate_time = time.time()
        self.last_tuple_count = 0
    
    def to_dict(self) -> dict:
        return {
            'task_id': self.task_id,
            'stage': self.stage,
            'task_idx': self.task_idx,
            'vm_id': self.vm_id,
            'pid': self.pid,
            'op_exe': self.op_exe,
            'op_args': self.op_args,
            'log_file': self.log_file
        }


class RainStormLeader:
    """
    RainStorm Leader - coordinates stream processing jobs.
    Handles scheduling, monitoring, autoscaling, and failure recovery.
    """
    
    def __init__(self, vm_id: int, hydfs_node: HyDFSNode):
        self.vm_id = vm_id
        self.hostname = socket.gethostname()
        self.hydfs_node = hydfs_node
        self.hydfs_client = HyDFSClient(hydfs_node)
        
        self.run_id = f"{int(time.time())}"
        self.logger = RainStormLogger(vm_id, self.run_id, is_leader=True)
        
        # Job state
        self.running = False
        self.job_config: Optional[dict] = None
        self.tasks: Dict[str, TaskInfo] = {}  # task_id -> TaskInfo
        self.tasks_lock = threading.Lock()
        
        # Stage configuration
        self.stages: List[dict] = []  # [{op_exe, op_args, num_tasks}]
        self.stage_tasks: Dict[int, List[str]] = {}  # stage -> [task_ids]
        
        # Rate monitoring for autoscaling
        self.task_rates: Dict[str, float] = {}  # task_id -> tuples/sec
        self.rates_lock = threading.Lock()
        
        # Autoscaling config
        self.autoscale_enabled = False
        self.input_rate = 0
        self.lw = 0  # Low watermark
        self.hw = 0  # High watermark
        
        # Exactly-once
        self.exactly_once = False
        
        # Network
        self.server_socket: Optional[socket.socket] = None
        self.server_thread: Optional[threading.Thread] = None
        
        # Monitoring threads
        self.monitor_thread: Optional[threading.Thread] = None
        self.rate_log_thread: Optional[threading.Thread] = None
    
    def start(self):
        """Start the leader server."""
        self.running = True
        
        # Start server for receiving messages
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('', RAINSTORM_PORT))
        self.server_socket.listen(20)
        
        self.server_thread = threading.Thread(target=self._server_loop, daemon=True)
        self.server_thread.start()
        
        self.logger.log(f"LEADER: Started on port {RAINSTORM_PORT}")
    
    def stop(self):
        """Stop the leader."""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        self.logger.log("LEADER: Stopped")
    
    def _server_loop(self):
        """Main server loop to accept connections."""
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self._handle_connection, 
                               args=(conn, addr), daemon=True).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    self.logger.log(f"LEADER ERROR: {e}")
    
    def _handle_connection(self, conn: socket.socket, addr: tuple):
        """Handle incoming connection."""
        try:
            data = conn.recv(65536).decode('utf-8')
            if not data:
                return
            
            msg = json.loads(data)
            msg_type = msg.get('type')
            
            response = {'status': 'error', 'message': 'Unknown message type'}
            
            if msg_type == 'RATE_UPDATE':
                response = self._handle_rate_update(msg)
            elif msg_type == 'TASK_STARTED':
                response = self._handle_task_started(msg)
            elif msg_type == 'TASK_COMPLETED':
                response = self._handle_task_completed(msg)
            elif msg_type == 'LIST_TASKS':
                response = self._handle_list_tasks()
            elif msg_type == 'KILL_TASK':
                response = self._handle_kill_task(msg)
            elif msg_type == 'GET_TASK_CONFIG':
                response = self._handle_get_task_config(msg)
            elif msg_type == 'SUBMIT_JOB':
                response = self._handle_submit_job(msg)
            elif msg_type == 'GET_WORKERS':
                response = self._handle_get_workers()
            
            conn.sendall(json.dumps(response).encode('utf-8'))
        except Exception as e:
            self.logger.log(f"LEADER ERROR handling connection: {e}")
        finally:
            conn.close()
    
    def _handle_submit_job(self, msg: dict) -> dict:
        """Handle job submission from CLI."""
        try:
            self.start_job(
                nstages=msg.get('nstages'),
                ntasks_per_stage=msg.get('ntasks'),
                operators=msg.get('operators'),
                hydfs_src=msg.get('src'),
                hydfs_dest=msg.get('dest'),
                exactly_once=msg.get('exactly_once', False),
                autoscale_enabled=msg.get('autoscale', False),
                input_rate=msg.get('input_rate', 100),
                lw=msg.get('lw', 50),
                hw=msg.get('hw', 150)
            )
            return {'status': 'success', 'message': 'Job submitted'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def _handle_get_workers(self) -> dict:
        """Return list of available workers."""
        workers = self._get_available_workers()
        return {'status': 'success', 'workers': workers}
    
    def _handle_rate_update(self, msg: dict) -> dict:
        """Handle rate update from a task."""
        task_id = msg.get('task_id')
        rate = msg.get('rate', 0)
        tuples_processed = msg.get('tuples_processed', 0)
        
        with self.rates_lock:
            self.task_rates[task_id] = rate
        
        with self.tasks_lock:
            if task_id in self.tasks:
                self.tasks[task_id].tuples_processed = tuples_processed
        
        return {'status': 'success'}
    
    def _handle_task_started(self, msg: dict) -> dict:
        """Handle task started notification."""
        task_id = msg.get('task_id')
        pid = msg.get('pid')
        log_file = msg.get('log_file')
        
        with self.tasks_lock:
            if task_id in self.tasks:
                self.tasks[task_id].pid = pid
                self.tasks[task_id].log_file = log_file
                task = self.tasks[task_id]
                self.logger.log(f"TASK START: {task_id} on VM{task.vm_id} PID={pid} "
                              f"op={task.op_exe} log={log_file}")
        
        return {'status': 'success'}
    
    def _handle_task_completed(self, msg: dict) -> dict:
        """Handle task completion notification."""
        task_id = msg.get('task_id')
        self.logger.log(f"TASK END: {task_id}")
        return {'status': 'success'}
    
    def _handle_list_tasks(self) -> dict:
        """Return list of all tasks."""
        with self.tasks_lock:
            tasks = [t.to_dict() for t in self.tasks.values()]
        return {'status': 'success', 'tasks': tasks}
    
    def _handle_kill_task(self, msg: dict) -> dict:
        """Kill a specific task."""
        vm_id = msg.get('vm_id')
        pid = msg.get('pid')
        
        # Send kill command to the worker
        hostname = VM_HOSTS.get(vm_id)
        if not hostname:
            return {'status': 'error', 'message': 'Invalid VM ID'}
        
        try:
            kill_msg = {'type': 'KILL_PROCESS', 'pid': pid}
            response = self._send_to_worker(hostname, kill_msg)
            
            if response and response.get('status') == 'success':
                self.logger.log(f"TASK KILLED: VM{vm_id} PID={pid}")
                return {'status': 'success'}
            return {'status': 'error', 'message': 'Kill failed'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def _handle_get_task_config(self, msg: dict) -> dict:
        """Return configuration for a task."""
        task_id = msg.get('task_id')
        
        with self.tasks_lock:
            if task_id not in self.tasks:
                return {'status': 'error', 'message': 'Task not found'}
            
            task = self.tasks[task_id]
            stage = task.stage
        
        # Build task configuration
        config = {
            'status': 'success',
            'task_id': task_id,
            'stage': stage,
            'task_idx': task.task_idx,
            'op_exe': task.op_exe,
            'op_args': task.op_args,
            'exactly_once': self.exactly_once,
            'hydfs_dest': self.job_config.get('hydfs_dest') if self.job_config else '',
            'num_stages': len(self.stages),
            'input_rate': self.input_rate
        }
        
        # Add successor task info
        if stage < len(self.stages) - 1:
            next_stage = stage + 1
            config['successor_tasks'] = []
            with self.tasks_lock:
                for tid in self.stage_tasks.get(next_stage, []):
                    if tid in self.tasks:
                        t = self.tasks[tid]
                        config['successor_tasks'].append({
                            'task_id': tid,
                            'vm_id': t.vm_id,
                            'hostname': VM_HOSTS[t.vm_id],
                            'port': TASK_BASE_PORT + t.task_idx
                        })
        
        return config
    
    def _send_to_worker(self, hostname: str, msg: dict, timeout: float = 10.0) -> Optional[dict]:
        """Send message to a worker node."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((hostname, RAINSTORM_PORT))
            sock.sendall(json.dumps(msg).encode('utf-8'))
            response = sock.recv(65536).decode('utf-8')
            sock.close()
            return json.loads(response)
        except Exception as e:
            self.logger.log(f"LEADER ERROR sending to {hostname}: {e}")
            return None
    
    def start_job(self, nstages: int, ntasks_per_stage: int, 
                  operators: List[Tuple[str, str]], hydfs_src: str, 
                  hydfs_dest: str, exactly_once: bool, autoscale_enabled: bool,
                  input_rate: int = 100, lw: int = 50, hw: int = 150):
        """Start a RainStorm job."""
        self.logger.log(f"RUN START: stages={nstages} tasks_per_stage={ntasks_per_stage}")
        self.logger.log(f"  src={hydfs_src} dest={hydfs_dest}")
        self.logger.log(f"  exactly_once={exactly_once} autoscale={autoscale_enabled}")
        
        self.job_config = {
            'nstages': nstages,
            'ntasks_per_stage': ntasks_per_stage,
            'operators': operators,
            'hydfs_src': hydfs_src,
            'hydfs_dest': hydfs_dest
        }
        
        self.exactly_once = exactly_once
        self.autoscale_enabled = autoscale_enabled
        self.input_rate = input_rate
        self.lw = lw
        self.hw = hw
        
        # Setup stages
        self.stages = []
        for i, (op_exe, op_args) in enumerate(operators):
            self.stages.append({
                'op_exe': op_exe,
                'op_args': op_args,
                'num_tasks': ntasks_per_stage
            })
            self.stage_tasks[i] = []
        
        # Get available workers (should already be joined)
        workers = self._get_available_workers()
        
        if len(workers) < 1:
            self.logger.log("ERROR: No workers available. Make sure workers have joined.")
            return
        
        self.logger.log(f"Available workers: {workers}")
        
        # Schedule tasks across workers
        self._schedule_tasks(workers)
        
        # Start monitoring threads
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        self.rate_log_thread = threading.Thread(target=self._rate_log_loop, daemon=True)
        self.rate_log_thread.start()
        
        # Start the source on the leader
        self._start_source(hydfs_src)
    
    def _get_available_workers(self, include_leader: bool = True) -> List[int]:
        """Get list of available worker VM IDs."""
        workers = []
        members = self.hydfs_node.membership.membership.get_members()
        
        for hostname, info in members.items():
            if info['status'] == MemberStatus.ACTIVE.value:
                vm_id = get_vm_id_from_hostname(hostname)
                workers.append(vm_id)
        
        # If no other workers found, at least include the leader
        if not workers:
            workers = [self.vm_id]
        
        return sorted(workers)
    
    def _schedule_tasks(self, workers: List[int]):
        """Schedule tasks across available workers."""
        worker_idx = 0
        num_workers = len(workers)
        
        for stage_idx, stage_config in enumerate(self.stages):
            num_tasks = stage_config['num_tasks']
            op_exe = stage_config['op_exe']
            op_args = stage_config['op_args']
            
            for task_idx in range(num_tasks):
                vm_id = workers[worker_idx % num_workers]
                worker_idx += 1
                
                task_id = f"stage{stage_idx}_task{task_idx}"
                
                task_info = TaskInfo(
                    task_id=task_id,
                    stage=stage_idx,
                    task_idx=task_idx,
                    vm_id=vm_id,
                    pid=0,
                    op_exe=op_exe,
                    op_args=op_args,
                    log_file=""
                )
                
                with self.tasks_lock:
                    self.tasks[task_id] = task_info
                    self.stage_tasks[stage_idx].append(task_id)
                
                self.logger.log(f"SCHEDULED: {task_id} on VM{vm_id}")
        
        # Send start commands to workers
        for task_id, task in self.tasks.items():
            self._start_task_on_worker(task)
    
    def _start_task_on_worker(self, task: TaskInfo):
        """Start a task on its assigned worker."""
        hostname = VM_HOSTS[task.vm_id]
        
        # If task is on leader, start it locally
        if task.vm_id == self.vm_id:
            self._start_task_locally(task)
            return
        
        msg = {
            'type': 'START_TASK',
            'task_id': task.task_id,
            'stage': task.stage,
            'task_idx': task.task_idx,
            'op_exe': task.op_exe,
            'op_args': task.op_args,
            'exactly_once': self.exactly_once,
            'leader_host': self.hostname,
            'run_id': self.run_id
        }
        
        response = self._send_to_worker(hostname, msg)
        if response and response.get('status') == 'success':
            # Update task info with PID and log_file from worker
            task.pid = response.get('pid', 0)
            task.log_file = response.get('log_file', f"task_{task.task_id}_{self.run_id}.log")
            self.logger.log(f"TASK STARTED: {task.task_id} on VM{task.vm_id} PID={task.pid}")
        else:
            self.logger.log(f"TASK START FAILED: {task.task_id} - {response}")
    
    def _start_task_locally(self, task: TaskInfo):
        """Start a task locally on the leader."""
        import subprocess
        
        task_port = TASK_BASE_PORT + task.task_idx
        log_file = f"task_{task.task_id}_{self.run_id}.log"
        
        cmd = [
            sys.executable, 'task.py',
            '--task-id', task.task_id,
            '--stage', str(task.stage),
            '--task-idx', str(task.task_idx),
            '--port', str(task_port),
            '--op-exe', task.op_exe,
            '--op-args', task.op_args,
            '--leader-host', self.hostname,
            '--run-id', self.run_id,
            '--vm-id', str(self.vm_id)
        ]
        
        if self.exactly_once:
            cmd.append('--exactly-once')
        
        process = subprocess.Popen(
            cmd,
            stdout=open(log_file, 'w'),
            stderr=subprocess.STDOUT,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        
        task.pid = process.pid
        task.log_file = log_file
        
        self.logger.log(f"TASK STARTED LOCAL: {task.task_id} PID={process.pid}")
    
    def _start_source(self, hydfs_src: str):
        """Start the source process to read from HyDFS."""
        self.logger.log(f"SOURCE START: Reading from {hydfs_src}")
        
        # Start source in a separate thread
        source_thread = threading.Thread(
            target=self._source_loop,
            args=(hydfs_src,),
            daemon=True
        )
        source_thread.start()
    
    def _source_loop(self, hydfs_src: str):
        """Source loop - reads from file and sends to stage 0 tasks."""
        try:
            # Try to read from local file first
            local_path = hydfs_src
            if not os.path.exists(local_path):
                # Try HyDFS
                temp_file = f"/tmp/rainstorm_src_{self.run_id}.csv"
                self.hydfs_client.get(hydfs_src, temp_file)
                local_path = temp_file
            
            if not os.path.exists(local_path):
                self.logger.log(f"SOURCE ERROR: Cannot find {hydfs_src}")
                return
            
            with open(local_path, 'r') as f:
                lines = f.readlines()
            
            self.logger.log(f"SOURCE: Loaded {len(lines)} lines")
            
            # Get stage 0 tasks
            stage0_tasks = []
            with self.tasks_lock:
                for task_id in self.stage_tasks.get(0, []):
                    if task_id in self.tasks:
                        t = self.tasks[task_id]
                        stage0_tasks.append({
                            'task_id': task_id,
                            'hostname': VM_HOSTS[t.vm_id],
                            'port': TASK_BASE_PORT + t.task_idx
                        })
            
            if not stage0_tasks:
                self.logger.log("SOURCE ERROR: No stage 0 tasks available")
                return
            
            # Send tuples at input_rate
            interval = 1.0 / self.input_rate if self.input_rate > 0 else 0.01
            
            for line_num, line in enumerate(lines):
                if not self.running:
                    break
                
                # Create tuple
                key = f"{hydfs_src}:{line_num}"
                value = line.strip()
                
                # Hash partition to determine target task
                task_idx = hash(key) % len(stage0_tasks)
                target = stage0_tasks[task_idx]
                
                # Send tuple
                tuple_msg = {
                    'type': 'TUPLE',
                    'tuple_id': f"{self.run_id}_{line_num}",
                    'key': key,
                    'value': value,
                    'source_task': 'source'
                }
                
                self._send_tuple(target['hostname'], target['port'], tuple_msg)
                
                time.sleep(interval)
            
            # Send EOF to all stage 0 tasks
            for target in stage0_tasks:
                eof_msg = {'type': 'EOF', 'source_task': 'source'}
                self._send_tuple(target['hostname'], target['port'], eof_msg)
            
            self.logger.log("SOURCE: Finished sending all tuples")
            
        except Exception as e:
            self.logger.log(f"SOURCE ERROR: {e}")
    
    def _send_tuple(self, hostname: str, port: int, msg: dict):
        """Send a tuple to a task."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((hostname, port))
            sock.sendall(json.dumps(msg).encode('utf-8'))
            sock.close()
        except Exception as e:
            pass  # Silently ignore tuple send failures
    
    def _monitor_loop(self):
        """Monitor tasks for failures and autoscaling."""
        while self.running:
            try:
                time.sleep(1.0)
                
                # Check for failed tasks
                self._check_task_failures()
                
                # Autoscaling
                if self.autoscale_enabled:
                    self._check_autoscaling()
                    
            except Exception as e:
                self.logger.log(f"MONITOR ERROR: {e}")
    
    def _check_task_failures(self):
        """Check for failed tasks and restart them."""
        with self.tasks_lock:
            for task_id, task in list(self.tasks.items()):
                if task.pid > 0:
                    # Check if task is still running
                    hostname = VM_HOSTS[task.vm_id]
                    msg = {'type': 'CHECK_TASK', 'pid': task.pid}
                    response = self._send_to_worker(hostname, msg, timeout=2.0)
                    
                    if response and not response.get('running', True):
                        self.logger.log(f"TASK FAILURE DETECTED: {task_id}")
                        self._restart_task(task)
    
    def _restart_task(self, task: TaskInfo):
        """Restart a failed task."""
        self.logger.log(f"TASK RESTART: {task.task_id} on VM{task.vm_id} "
                       f"op={task.op_exe} log={task.log_file}")
        
        # Re-start on the same VM
        self._start_task_on_worker(task)
    
    def _check_autoscaling(self):
        """Check if autoscaling is needed."""
        # Check each non-stateful stage
        for stage_idx in range(len(self.stages)):
            stage_config = self.stages[stage_idx]
            
            # Skip stateful stages (aggregation)
            if 'aggregate' in stage_config['op_exe'].lower():
                continue
            
            # Calculate average rate for this stage
            task_ids = self.stage_tasks.get(stage_idx, [])
            if not task_ids:
                continue
            
            with self.rates_lock:
                rates = [self.task_rates.get(tid, 0) for tid in task_ids]
            
            if not rates:
                continue
            
            avg_rate = sum(rates) / len(rates)
            
            # Check watermarks
            if avg_rate > self.hw and len(task_ids) < 10:
                self._scale_up(stage_idx, avg_rate)
            elif avg_rate < self.lw and len(task_ids) > 1:
                self._scale_down(stage_idx, avg_rate)
    
    def _scale_up(self, stage_idx: int, current_rate: float):
        """Add a task to a stage."""
        self.logger.log(f"AUTOSCALE UP: stage={stage_idx} rate={current_rate:.2f} tuples/sec")
        
        workers = self._get_available_workers()
        if not workers:
            return
        
        # Find least loaded worker
        vm_id = workers[0]
        
        stage_config = self.stages[stage_idx]
        task_idx = len(self.stage_tasks[stage_idx])
        task_id = f"stage{stage_idx}_task{task_idx}"
        
        task_info = TaskInfo(
            task_id=task_id,
            stage=stage_idx,
            task_idx=task_idx,
            vm_id=vm_id,
            pid=0,
            op_exe=stage_config['op_exe'],
            op_args=stage_config['op_args'],
            log_file=""
        )
        
        with self.tasks_lock:
            self.tasks[task_id] = task_info
            self.stage_tasks[stage_idx].append(task_id)
        
        self._start_task_on_worker(task_info)
    
    def _scale_down(self, stage_idx: int, current_rate: float):
        """Remove a task from a stage."""
        self.logger.log(f"AUTOSCALE DOWN: stage={stage_idx} rate={current_rate:.2f} tuples/sec")
        
        task_ids = self.stage_tasks[stage_idx]
        if len(task_ids) <= 1:
            return
        
        # Remove last task
        task_id = task_ids[-1]
        
        with self.tasks_lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                # Kill the task
                hostname = VM_HOSTS[task.vm_id]
                kill_msg = {'type': 'KILL_PROCESS', 'pid': task.pid}
                self._send_to_worker(hostname, kill_msg)
                
                del self.tasks[task_id]
                self.stage_tasks[stage_idx].remove(task_id)
    
    def _rate_log_loop(self):
        """Log rates every second."""
        while self.running:
            try:
                time.sleep(1.0)
                
                with self.rates_lock:
                    for task_id, rate in self.task_rates.items():
                        self.logger.log(f"RATE: {task_id} {rate:.2f} tuples/sec")
                        
            except Exception as e:
                pass


class RainStormWorker:
    """
    RainStorm Worker - runs on non-leader nodes.
    Receives commands from leader and manages local tasks.
    """
    
    def __init__(self, vm_id: int, hydfs_node: HyDFSNode):
        self.vm_id = vm_id
        self.hostname = socket.gethostname()
        self.hydfs_node = hydfs_node
        
        self.run_id = f"{int(time.time())}"
        self.logger = RainStormLogger(vm_id, self.run_id)
        
        self.running = False
        self.server_socket: Optional[socket.socket] = None
        self.task_processes: Dict[str, int] = {}  # task_id -> pid
    
    def start(self):
        """Start the worker server."""
        self.running = True
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('', RAINSTORM_PORT))
        self.server_socket.listen(20)
        
        threading.Thread(target=self._server_loop, daemon=True).start()
        
        self.logger.log(f"WORKER: Started on port {RAINSTORM_PORT}")
    
    def stop(self):
        """Stop the worker."""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
    
    def _server_loop(self):
        """Main server loop."""
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self._handle_connection,
                               args=(conn, addr), daemon=True).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    self.logger.log(f"WORKER ERROR: {e}")
    
    def _handle_connection(self, conn: socket.socket, addr: tuple):
        """Handle incoming connection."""
        try:
            data = conn.recv(65536).decode('utf-8')
            if not data:
                return
            
            msg = json.loads(data)
            msg_type = msg.get('type')
            
            response = {'status': 'error', 'message': 'Unknown message type'}
            
            if msg_type == 'START_TASK':
                response = self._handle_start_task(msg)
            elif msg_type == 'KILL_PROCESS':
                response = self._handle_kill_process(msg)
            elif msg_type == 'CHECK_TASK':
                response = self._handle_check_task(msg)
            
            conn.sendall(json.dumps(response).encode('utf-8'))
        except Exception as e:
            self.logger.log(f"WORKER ERROR: {e}")
        finally:
            conn.close()
    
    def _handle_start_task(self, msg: dict) -> dict:
        """Start a task process."""
        task_id = msg.get('task_id')
        stage = msg.get('stage')
        task_idx = msg.get('task_idx')
        op_exe = msg.get('op_exe')
        op_args = msg.get('op_args')
        exactly_once = msg.get('exactly_once', False)
        leader_host = msg.get('leader_host')
        run_id = msg.get('run_id')
        
        # Start task process
        import subprocess
        
        task_port = TASK_BASE_PORT + task_idx
        log_file = f"task_{task_id}_{run_id}.log"
        
        cmd = [
            sys.executable, 'task.py',
            '--task-id', task_id,
            '--stage', str(stage),
            '--task-idx', str(task_idx),
            '--port', str(task_port),
            '--op-exe', op_exe,
            '--op-args', op_args,
            '--leader-host', leader_host,
            '--run-id', run_id,
            '--vm-id', str(self.vm_id)
        ]
        
        if exactly_once:
            cmd.append('--exactly-once')
        
        process = subprocess.Popen(
            cmd,
            stdout=open(log_file, 'w'),
            stderr=subprocess.STDOUT,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        
        self.task_processes[task_id] = process.pid
        
        self.logger.log(f"TASK STARTED: {task_id} PID={process.pid}")
        
        return {'status': 'success', 'pid': process.pid, 'log_file': log_file}
    
    def _handle_kill_process(self, msg: dict) -> dict:
        """Kill a process by PID."""
        pid = msg.get('pid')
        
        try:
            os.kill(pid, 9)
            self.logger.log(f"KILLED: PID={pid}")
            return {'status': 'success'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def _handle_check_task(self, msg: dict) -> dict:
        """Check if a task is still running."""
        pid = msg.get('pid')
        
        try:
            os.kill(pid, 0)
            return {'status': 'success', 'running': True}
        except OSError:
            return {'status': 'success', 'running': False}


def submit_job(args):
    """Submit a job to the leader from a separate terminal."""
    # Parse operators
    operators = []
    for op in args.operators:
        parts = op.split(':')
        op_exe = parts[0]
        op_args = parts[1] if len(parts) > 1 else ''
        operators.append((op_exe, op_args))
    
    msg = {
        'type': 'SUBMIT_JOB',
        'nstages': args.nstages,
        'ntasks': args.ntasks or 3,
        'operators': operators,
        'src': args.src or '',
        'dest': args.dest or '',
        'exactly_once': args.exactly_once,
        'autoscale': args.autoscale,
        'input_rate': args.input_rate,
        'lw': args.lw,
        'hw': args.hw
    }
    
    leader_host = VM_HOSTS.get(args.leader_vm, VM_HOSTS[1])
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)
        sock.connect((leader_host, RAINSTORM_PORT))
        sock.sendall(json.dumps(msg).encode('utf-8'))
        response = sock.recv(65536).decode('utf-8')
        sock.close()
        
        result = json.loads(response)
        if result.get('status') == 'success':
            print(f"Job submitted successfully to {leader_host}")
        else:
            print(f"Job submission failed: {result.get('message')}")
    except Exception as e:
        print(f"Error submitting job: {e}")


def main():
    parser = argparse.ArgumentParser(description='RainStorm Stream Processing')
    parser.add_argument('--vm-id', type=int, help='VM ID (1-10)')
    parser.add_argument('--leader', action='store_true', help='Run as leader')
    parser.add_argument('--submit', action='store_true', help='Submit job to leader')
    parser.add_argument('--leader-vm', type=int, default=1, help='Leader VM ID for job submission')
    
    # Job parameters
    parser.add_argument('--nstages', type=int, help='Number of stages')
    parser.add_argument('--ntasks', type=int, help='Number of tasks per stage')
    parser.add_argument('--operators', nargs='+', help='op_exe:op_args pairs')
    parser.add_argument('--src', type=str, help='Source file')
    parser.add_argument('--dest', type=str, help='Destination file')
    parser.add_argument('--exactly-once', action='store_true')
    parser.add_argument('--autoscale', action='store_true')
    parser.add_argument('--input-rate', type=int, default=100)
    parser.add_argument('--lw', type=int, default=50)
    parser.add_argument('--hw', type=int, default=150)
    
    args = parser.parse_args()
    
    # Submit mode - just send job to leader and exit
    if args.submit:
        if not args.nstages or not args.operators:
            print("Error: --submit requires --nstages and --operators")
            sys.exit(1)
        submit_job(args)
        return
    
    # Node mode - requires vm-id
    if args.vm_id is None:
        print("Error: --vm-id required for leader/worker mode")
        sys.exit(1)
    
    # Start HyDFS node
    hydfs_node = HyDFSNode(args.vm_id)
    hydfs_node.start()
    
    # Join membership
    hydfs_node.membership.join_group()
    time.sleep(2)
    
    if args.leader:
        leader = RainStormLeader(args.vm_id, hydfs_node)
        leader.start()
        
        print("Leader running. Waiting for job submissions...")
        print("Submit jobs from another terminal using:")
        print("  python3 rainstorm.py --submit --nstages N --ntasks M --operators ... --src FILE --dest FILE")
        print("Press Ctrl+C to stop.")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        
        leader.stop()
    else:
        worker = RainStormWorker(args.vm_id, hydfs_node)
        worker.start()
        
        print("Worker running. Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        
        worker.stop()
    
    hydfs_node.membership.leave_group()
    hydfs_node.stop()


if __name__ == '__main__':
    main()