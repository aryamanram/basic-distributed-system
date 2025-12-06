#!/usr/bin/env python3
"""
RainStorm - Stream Processing Framework
Implements exactly-once semantics with HyDFS-backed logs, autoscaling,
failure detection/recovery, and proper integration with MP2/MP3.
"""
import argparse
import json
import os
import socket
import sys
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set

# Add parent directory to path for MP2/MP3 imports
_parent_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, _parent_dir)
sys.path.insert(0, os.path.join(_parent_dir, 'MP3'))
sys.path.insert(0, os.path.join(_parent_dir, 'MP2'))

try:
    from membership import MembershipService, MemberStatus
    from node import HyDFSNode
    from main import HyDFSClient
except ImportError:
    # Fallback for standalone testing
    print("Warning: MP2/MP3 imports not available", file=sys.stderr)
    MembershipService = None
    MemberStatus = None
    HyDFSNode = None
    HyDFSClient = None

RAINSTORM_PORT = 8000
TASK_BASE_PORT = 8100
RATE_REPORT_INTERVAL = 1.0
AUTOSCALE_CHECK_INTERVAL = 5.0
FAILURE_CHECK_INTERVAL = 2.0
MAX_TASKS_PER_STAGE = 20
MIN_TASKS_PER_STAGE = 1

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
    """Extract VM ID from hostname."""
    for vm_id, host in VM_HOSTS.items():
        if host == hostname or hostname.startswith(f"fa25-cs425-a7{vm_id:02d}"):
            return vm_id
    # Try to extract from hostname pattern
    try:
        if "a7" in hostname:
            num = int(hostname.split("a7")[1][:2])
            return num
    except:
        pass
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
                 pid: int, op_exe: str, op_args: str, log_file: str,
                 is_stateful: bool = False):
        self.task_id = task_id
        self.stage = stage
        self.task_idx = task_idx
        self.vm_id = vm_id
        self.pid = pid
        self.op_exe = op_exe
        self.op_args = op_args
        self.log_file = log_file
        self.is_stateful = is_stateful  # True for aggregation operators
        self.tuples_processed = 0
        self.input_rate = 0.0  # tuples/sec received
        self.completed = False
        self.last_heartbeat = time.time()
    
    def to_dict(self) -> dict:
        return {
            'task_id': self.task_id,
            'stage': self.stage,
            'task_idx': self.task_idx,
            'vm_id': self.vm_id,
            'pid': self.pid,
            'op_exe': self.op_exe,
            'op_args': self.op_args,
            'log_file': self.log_file,
            'is_stateful': self.is_stateful,
            'completed': self.completed
        }


class StageInfo:
    """Information about a processing stage."""
    
    def __init__(self, stage_idx: int, op_exe: str, op_args: str, 
                 num_tasks: int, is_stateful: bool = False):
        self.stage_idx = stage_idx
        self.op_exe = op_exe
        self.op_args = op_args
        self.num_tasks = num_tasks
        self.is_stateful = is_stateful
        self.task_ids: List[str] = []


class RainStormLeader:
    """
    RainStorm Leader - Coordinates the stream processing cluster.
    Handles job submission, task scheduling, failure recovery, and autoscaling.
    """
    
    def __init__(self, vm_id: int, hydfs_node: Optional['HyDFSNode']):
        self.vm_id = vm_id
        self.hostname = socket.gethostname()
        self.hydfs_node = hydfs_node
        self.hydfs_client = HyDFSClient(hydfs_node) if hydfs_node and HyDFSClient else None
        
        self.run_id = f"{int(time.time())}"
        self.logger = RainStormLogger(vm_id, self.run_id, is_leader=True)
        
        self.running = False
        self.job_running = False
        self.job_config: Optional[dict] = None
        
        # Task management
        self.tasks: Dict[str, TaskInfo] = {}
        self.tasks_lock = threading.Lock()
        
        # Stage management
        self.stages: List[StageInfo] = []
        self.stage_tasks: Dict[int, List[str]] = {}  # stage_idx -> [task_ids]
        
        # Rate tracking for autoscaling
        self.task_rates: Dict[str, float] = {}  # task_id -> input rate
        self.rates_lock = threading.Lock()
        
        # Configuration
        self.autoscale_enabled = False
        self.input_rate = 0
        self.lw = 0  # Low watermark
        self.hw = 0  # High watermark
        self.exactly_once = False
        self.ntasks_per_stage = 3
        
        # Network
        self.server_socket: Optional[socket.socket] = None
        
        # Source tracking
        self.source_completed = False
        self.hydfs_src: str = ''
        self.hydfs_dest: str = ''
        
        # Output management
        self.output_lock = threading.Lock()
        self.output_sequence = 0
        
        # Active workers (from membership)
        self.active_workers: Set[int] = set()
        self.workers_lock = threading.Lock()
        
        # Autoscale state
        self.last_autoscale_time: Dict[int, float] = {}  # stage -> last scale time
        self.autoscale_cooldown = 10.0  # seconds between scale operations
    
    def start(self):
        """Start the leader server."""
        self.running = True
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('', RAINSTORM_PORT))
        self.server_socket.listen(50)
        
        # Start server thread
        threading.Thread(target=self._server_loop, daemon=True).start()
        
        self.logger.log(f"LEADER: Started on port {RAINSTORM_PORT}")
    
    def stop(self):
        """Stop the leader."""
        self.running = False
        self.job_running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        self.logger.log("LEADER: Stopped")
    
    def _server_loop(self):
        """Main server loop for accepting connections."""
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
        """Handle an incoming connection."""
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
            elif msg_type == 'TASK_HEARTBEAT':
                response = self._handle_task_heartbeat(msg)
            elif msg_type == 'LIST_TASKS':
                response = self._handle_list_tasks()
            elif msg_type == 'KILL_TASK':
                response = self._handle_kill_task(msg)
            elif msg_type == 'GET_TASK_CONFIG':
                response = self._handle_get_task_config(msg)
            elif msg_type == 'SUBMIT_JOB':
                response = self._handle_submit_job(msg)
            elif msg_type == 'GET_WORKERS':
                response = {'status': 'success', 'workers': self._get_available_workers()}
            elif msg_type == 'APPEND_OUTPUT':
                response = self._handle_append_output(msg)
            elif msg_type == 'GET_SUCCESSOR_TASKS':
                response = self._handle_get_successor_tasks(msg)
            elif msg_type == 'GET_PREDECESSOR_COUNT':
                response = self._handle_get_predecessor_count(msg)
            elif msg_type == 'GET_EO_LOG':
                response = self._handle_get_eo_log(msg)
            elif msg_type == 'APPEND_EO_LOG':
                response = self._handle_append_eo_log(msg)
            
            conn.sendall(json.dumps(response).encode('utf-8'))
        except Exception as e:
            self.logger.log(f"LEADER ERROR handling connection: {e}")
            import traceback
            traceback.print_exc()
        finally:
            try:
                conn.close()
            except:
                pass
    
    def _handle_submit_job(self, msg: dict) -> dict:
        """Handle job submission."""
        try:
            # Parse operators - check for stateful flag
            operators = []
            for op in msg.get('operators', []):
                if isinstance(op, (list, tuple)):
                    op_exe, op_args = op[0], op[1] if len(op) > 1 else ''
                    is_stateful = op[2] if len(op) > 2 else False
                else:
                    op_exe = op.get('exe', op)
                    op_args = op.get('args', '')
                    is_stateful = op.get('stateful', False)
                
                # Auto-detect stateful operators by name
                if 'count' in op_exe.lower() or 'aggregate' in op_exe.lower() or 'sum' in op_exe.lower():
                    is_stateful = True
                
                operators.append((op_exe, op_args, is_stateful))
            
            self.start_job(
                nstages=msg.get('nstages'),
                ntasks_per_stage=msg.get('ntasks', 3),
                operators=operators,
                hydfs_src=msg.get('src', ''),
                hydfs_dest=msg.get('dest', ''),
                exactly_once=msg.get('exactly_once', False),
                autoscale_enabled=msg.get('autoscale', False),
                input_rate=msg.get('input_rate', 100),
                lw=msg.get('lw', 50),
                hw=msg.get('hw', 150)
            )
            return {'status': 'success', 'message': 'Job submitted'}
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {'status': 'error', 'message': str(e)}
    
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
                self.tasks[task_id].input_rate = rate
                self.tasks[task_id].last_heartbeat = time.time()
        
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
                self.tasks[task_id].last_heartbeat = time.time()
                task = self.tasks[task_id]
                self.logger.log(f"TASK START: {task_id} on VM{task.vm_id} PID={pid}")
        
        return {'status': 'success'}
    
    def _handle_task_heartbeat(self, msg: dict) -> dict:
        """Handle heartbeat from a task."""
        task_id = msg.get('task_id')
        
        with self.tasks_lock:
            if task_id in self.tasks:
                self.tasks[task_id].last_heartbeat = time.time()
        
        return {'status': 'success'}
    
    def _handle_task_completed(self, msg: dict) -> dict:
        """Handle task completion notification."""
        task_id = msg.get('task_id')
        tuples_processed = msg.get('tuples_processed', 0)
        
        with self.tasks_lock:
            if task_id in self.tasks:
                self.tasks[task_id].completed = True
                self.tasks[task_id].tuples_processed = tuples_processed
                task = self.tasks[task_id]
                self.logger.log(f"TASK END: {task_id} on VM{task.vm_id} "
                              f"(processed {tuples_processed} tuples)")
                del self.tasks[task_id]
        
        self._check_job_completion()
        
        return {'status': 'success'}
    
    def _handle_append_output(self, msg: dict) -> dict:
        """Handle output append request from final stage tasks."""
        hydfs_dest = msg.get('hydfs_dest')
        data = bytes(msg.get('data', []))
        task_id = msg.get('task_id')
        run_id = msg.get('run_id')
        
        if not self.hydfs_client or not hydfs_dest:
            return {'status': 'error', 'message': 'HyDFS not available or no destination'}
        
        with self.output_lock:
            try:
                self.output_sequence += 1
                
                # Write data to temp file
                temp_file = f"/tmp/rainstorm_output_{run_id}_{self.output_sequence}.tmp"
                with open(temp_file, 'wb') as f:
                    f.write(data)
                
                # Use HyDFSClient.append() for proper integration
                try:
                    self.hydfs_client.append(temp_file, hydfs_dest)
                    self.logger.log(f"OUTPUT APPEND: {len(data)} bytes from {task_id}")
                    os.remove(temp_file)
                    return {'status': 'success'}
                except Exception as e:
                    self.logger.log(f"OUTPUT APPEND ERROR: {e}")
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                    return {'status': 'error', 'message': str(e)}
                    
            except Exception as e:
                self.logger.log(f"OUTPUT APPEND ERROR: {e}")
                return {'status': 'error', 'message': str(e)}
    
    def _handle_get_successor_tasks(self, msg: dict) -> dict:
        """Get successor tasks for a given stage."""
        stage = msg.get('stage')
        
        if stage is None or stage >= len(self.stages) - 1:
            return {'status': 'success', 'successor_tasks': []}
        
        next_stage = stage + 1
        successor_tasks = []
        
        with self.tasks_lock:
            for task_id in self.stage_tasks.get(next_stage, []):
                if task_id in self.tasks:
                    t = self.tasks[task_id]
                    successor_tasks.append({
                        'task_id': task_id,
                        'vm_id': t.vm_id,
                        'hostname': VM_HOSTS[t.vm_id],
                        'port': TASK_BASE_PORT + (t.stage * 100) + t.task_idx
                    })
        
        return {'status': 'success', 'successor_tasks': successor_tasks}
    
    def _handle_get_predecessor_count(self, msg: dict) -> dict:
        """Get the number of predecessor tasks for a stage."""
        stage = msg.get('stage')
        
        if stage is None or stage == 0:
            # Stage 0 gets input from source only
            return {'status': 'success', 'predecessor_count': 1}
        
        # Other stages get input from all tasks in previous stage
        prev_stage = stage - 1
        with self.tasks_lock:
            count = len(self.stage_tasks.get(prev_stage, []))
        
        return {'status': 'success', 'predecessor_count': max(1, count)}
    
    def _handle_get_eo_log(self, msg: dict) -> dict:
        """Get exactly-once log contents from HyDFS."""
        task_id = msg.get('task_id')
        log_file = msg.get('log_file')
        
        if not self.hydfs_client or not log_file:
            return {'status': 'error', 'message': 'HyDFS not available'}
        
        try:
            # First merge the log file to ensure consistency (per spec)
            try:
                self.hydfs_client.merge(log_file)
                self.logger.log(f"EO LOG: Merged {log_file}")
            except Exception as e:
                self.logger.log(f"EO LOG: Merge skipped for {log_file}: {e}")
            
            # Get the log content using HyDFSClient.get()
            temp_file = f"/tmp/eo_get_{task_id}_{time.time()}.tmp"
            try:
                self.hydfs_client.get(log_file, temp_file)
                
                if os.path.exists(temp_file):
                    with open(temp_file, 'r') as f:
                        content = f.read()
                    os.remove(temp_file)
                    return {'status': 'success', 'content': content}
                else:
                    return {'status': 'success', 'content': ''}
            except Exception as e:
                # File may not exist yet - that's okay for new tasks
                self.logger.log(f"EO LOG: File not found (new task): {e}")
                return {'status': 'success', 'content': ''}
                
        except Exception as e:
            self.logger.log(f"EO LOG GET ERROR: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def _handle_append_eo_log(self, msg: dict) -> dict:
        """Append to exactly-once log in HyDFS."""
        task_id = msg.get('task_id')
        log_file = msg.get('log_file')
        content = msg.get('content', '')
        
        if not self.hydfs_client or not log_file or not content:
            return {'status': 'error', 'message': 'Invalid parameters'}
        
        try:
            # Write content to temp file
            temp_file = f"/tmp/eo_append_{task_id}_{time.time()}.tmp"
            with open(temp_file, 'w') as f:
                f.write(content)
            
            # Try to append using HyDFSClient.append()
            try:
                self.hydfs_client.append(temp_file, log_file)
            except Exception as e:
                # File may not exist yet, create it first
                self.logger.log(f"EO LOG: Creating new log file {log_file}")
                self.hydfs_client.create(temp_file, log_file)
            
            os.remove(temp_file)
            return {'status': 'success'}
            
        except Exception as e:
            self.logger.log(f"EO LOG APPEND ERROR: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def _check_job_completion(self):
        """Check if the job has completed."""
        with self.tasks_lock:
            active_tasks = len(self.tasks)
        
        if self.source_completed and active_tasks == 0 and self.job_running:
            self.logger.log(f"RUN END: All tasks completed. Output: {self.hydfs_dest}")
            self.job_running = False
            
            # Cleanup
            self.stages.clear()
            self.stage_tasks.clear()
            with self.rates_lock:
                self.task_rates.clear()
            with self.output_lock:
                self.output_sequence = 0
    
    def _handle_list_tasks(self) -> dict:
        """List all running tasks."""
        with self.tasks_lock:
            tasks = [t.to_dict() for t in self.tasks.values()]
        return {'status': 'success', 'tasks': tasks}
    
    def _handle_kill_task(self, msg: dict) -> dict:
        """Kill a specific task."""
        vm_id = msg.get('vm_id')
        pid = msg.get('pid')
        
        hostname = VM_HOSTS.get(vm_id)
        if not hostname:
            return {'status': 'error', 'message': 'Invalid VM ID'}
        
        try:
            response = self._send_to_worker(hostname, {'type': 'KILL_PROCESS', 'pid': pid})
            if response and response.get('status') == 'success':
                self.logger.log(f"TASK KILLED: VM{vm_id} PID={pid}")
                return {'status': 'success'}
            return {'status': 'error', 'message': 'Kill failed'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def _handle_get_task_config(self, msg: dict) -> dict:
        """Get configuration for a task."""
        task_id = msg.get('task_id')
        
        with self.tasks_lock:
            if task_id not in self.tasks:
                return {'status': 'error', 'message': 'Task not found'}
            
            task = self.tasks[task_id]
            stage = task.stage
        
        # Get predecessor count for EOF tracking
        if stage == 0:
            predecessor_count = 1  # Source only
        else:
            predecessor_count = len(self.stage_tasks.get(stage - 1, []))
        
        config = {
            'status': 'success',
            'task_id': task_id,
            'stage': stage,
            'task_idx': task.task_idx,
            'op_exe': task.op_exe,
            'op_args': task.op_args,
            'exactly_once': self.exactly_once,
            'hydfs_dest': self.hydfs_dest,
            'num_stages': len(self.stages),
            'input_rate': self.input_rate,
            'is_stateful': task.is_stateful,
            'predecessor_count': predecessor_count,
            'run_id': self.run_id
        }
        
        # Get successor tasks
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
                            'port': TASK_BASE_PORT + (t.stage * 100) + t.task_idx
                        })
        
        return config
    
    def _send_to_worker(self, hostname: str, msg: dict, timeout: float = 10.0) -> Optional[dict]:
        """Send a message to a worker."""
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
    
    def _create_output_file(self, hydfs_dest: str):
        """Create or reset the output file in HyDFS."""
        self.logger.log(f"OUTPUT: Creating {hydfs_dest}")
        
        if not self.hydfs_client:
            self.logger.log("OUTPUT ERROR: HyDFS client not available")
            return False
        
        try:
            # Create an empty temp file
            temp_file = f"/tmp/rainstorm_init_{self.run_id}.tmp"
            with open(temp_file, 'w') as f:
                f.write("")  # Empty file
            
            # Use HyDFSClient.create() to create the output file
            try:
                self.hydfs_client.create(temp_file, hydfs_dest)
                self.logger.log(f"OUTPUT: Created {hydfs_dest}")
            except Exception as e:
                # File might already exist, which is okay
                self.logger.log(f"OUTPUT: Create returned: {e} (file may already exist)")
            
            os.remove(temp_file)
            return True
            
        except Exception as e:
            self.logger.log(f"OUTPUT ERROR: {e}")
            return False
    
    def _get_available_workers(self) -> List[int]:
        """Get list of available worker VMs from MP2 membership service."""
        workers = []
        
        if self.hydfs_node:
            try:
                # HyDFSNode has a 'membership' attribute which is a MembershipService
                # MembershipService has a 'membership' attribute which is a MembershipList
                # MembershipList has get_members() returning dict of hostname -> member info
                
                membership_service = None
                
                # Try to get MembershipService from HyDFSNode
                if hasattr(self.hydfs_node, 'membership'):
                    membership_service = self.hydfs_node.membership
                
                if membership_service is None:
                    self.logger.log("Warning: No membership service found in HyDFS node")
                else:
                    # MembershipService has .membership (MembershipList)
                    membership_list = None
                    if hasattr(membership_service, 'membership'):
                        membership_list = membership_service.membership
                    
                    if membership_list and hasattr(membership_list, 'get_members'):
                        members = membership_list.get_members()
                        
                        for hostname, info in members.items():
                            # Check member status - info contains 'status' field
                            # Status is MemberStatus enum value or string
                            status = info.get('status', '')
                            
                            # Handle both enum and string representations
                            is_active = False
                            if MemberStatus:
                                if status == MemberStatus.ACTIVE or status == MemberStatus.ACTIVE.value:
                                    is_active = True
                            elif status == 'ACTIVE':
                                is_active = True
                            
                            if is_active:
                                vm_id = get_vm_id_from_hostname(hostname)
                                if vm_id != self.vm_id:  # Exclude leader from workers
                                    workers.append(vm_id)
                        
                        self.logger.log(f"Found {len(workers)} active workers from membership")
                    else:
                        self.logger.log("Warning: Could not access membership list")
                        
            except Exception as e:
                self.logger.log(f"Error getting workers from membership: {e}")
                import traceback
                traceback.print_exc()
        
        # Fallback: if no workers found, assume all VMs except leader are available
        if not workers:
            self.logger.log("Using fallback worker list (all VMs except leader)")
            workers = [i for i in range(1, 11) if i != self.vm_id]
        
        with self.workers_lock:
            self.active_workers = set(workers)
        
        return sorted(workers)
    
    def start_job(self, nstages: int, ntasks_per_stage: int,
                  operators: List[Tuple[str, str, bool]], hydfs_src: str,
                  hydfs_dest: str, exactly_once: bool, autoscale_enabled: bool,
                  input_rate: int = 100, lw: int = 50, hw: int = 150):
        """Start a new streaming job."""
        
        self.run_id = f"{int(time.time())}"
        self.logger = RainStormLogger(self.vm_id, self.run_id, is_leader=True)
        
        self.logger.log(f"RUN START: stages={nstages} tasks/stage={ntasks_per_stage}")
        self.logger.log(f"  src={hydfs_src} dest={hydfs_dest}")
        self.logger.log(f"  exactly_once={exactly_once} autoscale={autoscale_enabled}")
        if autoscale_enabled:
            self.logger.log(f"  input_rate={input_rate} lw={lw} hw={hw}")
        
        # Store configuration
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
        self.ntasks_per_stage = ntasks_per_stage
        self.job_running = True
        self.source_completed = False
        self.hydfs_src = hydfs_src
        self.hydfs_dest = hydfs_dest
        
        with self.output_lock:
            self.output_sequence = 0
        
        # Create output file
        self._create_output_file(hydfs_dest)
        
        # Initialize stages
        self.stages = []
        self.stage_tasks = {}
        for i, (op_exe, op_args, is_stateful) in enumerate(operators):
            stage = StageInfo(i, op_exe, op_args, ntasks_per_stage, is_stateful)
            self.stages.append(stage)
            self.stage_tasks[i] = []
        
        # Get available workers
        workers = self._get_available_workers()
        if not workers:
            self.logger.log("ERROR: No workers available")
            return
        
        self.logger.log(f"Workers available: {workers}")
        
        # Schedule tasks
        self._schedule_tasks(workers)
        
        # Wait for tasks to start
        time.sleep(2)
        
        # Start monitoring threads
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        threading.Thread(target=self._failure_detection_loop, daemon=True).start()
        
        if autoscale_enabled:
            threading.Thread(target=self._autoscale_loop, daemon=True).start()
        
        # Start source
        threading.Thread(target=self._source_loop, args=(hydfs_src,), daemon=True).start()
    
    def _schedule_tasks(self, workers: List[int]):
        """Schedule tasks across workers."""
        worker_idx = 0
        num_workers = len(workers)
        
        for stage_idx, stage in enumerate(self.stages):
            num_tasks = stage.num_tasks
            
            for task_idx in range(num_tasks):
                vm_id = workers[worker_idx % num_workers]
                worker_idx += 1
                
                task_id = f"stage{stage_idx}_task{task_idx}_{self.run_id}"
                
                task_info = TaskInfo(
                    task_id=task_id,
                    stage=stage_idx,
                    task_idx=task_idx,
                    vm_id=vm_id,
                    pid=0,
                    op_exe=stage.op_exe,
                    op_args=stage.op_args,
                    log_file="",
                    is_stateful=stage.is_stateful
                )
                
                with self.tasks_lock:
                    self.tasks[task_id] = task_info
                    self.stage_tasks[stage_idx].append(task_id)
                    stage.task_ids.append(task_id)
                
                self.logger.log(f"SCHEDULED: {task_id} on VM{vm_id}")
        
        # Start all tasks
        for task_id, task in list(self.tasks.items()):
            self._start_task_on_worker(task)
    
    def _start_task_on_worker(self, task: TaskInfo):
        """Start a task on a worker VM."""
        hostname = VM_HOSTS[task.vm_id]
        
        # Check if task should run locally (on leader VM)
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
            'run_id': self.run_id,
            'is_stateful': task.is_stateful
        }
        
        response = self._send_to_worker(hostname, msg)
        if response and response.get('status') == 'success':
            task.pid = response.get('pid', 0)
            task.log_file = response.get('log_file', '')
            task.last_heartbeat = time.time()
            self.logger.log(f"TASK STARTED: {task.task_id} VM{task.vm_id} PID={task.pid}")
        else:
            self.logger.log(f"TASK START FAILED: {task.task_id} on VM{task.vm_id}")
    
    def _start_task_locally(self, task: TaskInfo):
        """Start a task locally on the leader VM."""
        import subprocess
        
        task_port = TASK_BASE_PORT + (stage * 100) + task_idx
        log_file = f"task_{task.task_id}.log"
        
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
        
        if task.is_stateful:
            cmd.append('--stateful')
        
        process = subprocess.Popen(
            cmd,
            stdout=open(log_file, 'w'),
            stderr=subprocess.STDOUT,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        
        task.pid = process.pid
        task.log_file = log_file
        task.last_heartbeat = time.time()
        
        self.logger.log(f"TASK STARTED LOCAL: {task.task_id} PID={process.pid}")
    
    def _source_loop(self, hydfs_src: str):
        """Source loop - reads from HyDFS and sends tuples to stage 0."""
        try:
            # Get all files from source directory/file
            source_files = self._get_source_files(hydfs_src)
            
            if not source_files:
                self.logger.log(f"SOURCE ERROR: No files found in {hydfs_src}")
                self.source_completed = True
                self._check_job_completion()
                return
            
            self.logger.log(f"SOURCE: Found {len(source_files)} files to process")
            
            # Get stage 0 tasks
            stage0_tasks = self._get_stage_tasks(0)
            
            if not stage0_tasks:
                self.logger.log("SOURCE ERROR: No stage 0 tasks")
                self.source_completed = True
                self._check_job_completion()
                return
            
            # Calculate send interval based on input rate
            interval = 1.0 / self.input_rate if self.input_rate > 0 else 0.01
            
            # Process each file
            total_lines = 0
            for filename, lines in source_files:
                for line_num, line in enumerate(lines):
                    if not self.running or not self.job_running:
                        break
                    
                    # Key is filename:linenumber as per spec
                    key = f"{filename}:{line_num}"
                    value = line.strip()
                    
                    # Hash partition to select target task
                    task_idx = hash(key) % len(stage0_tasks)
                    target = stage0_tasks[task_idx]
                    
                    tuple_msg = {
                        'type': 'TUPLE',
                        'tuple_id': f"{self.run_id}_{filename}_{line_num}",
                        'key': key,
                        'value': value,
                        'source_task': 'source'
                    }
                    
                    self._send_tuple(target['hostname'], target['port'], tuple_msg)
                    total_lines += 1
                    
                    time.sleep(interval)
            
            self.logger.log(f"SOURCE: Sent {total_lines} lines, sending EOF")
            
            # Send EOF to all stage 0 tasks
            for target in stage0_tasks:
                self._send_eof(target['hostname'], target['port'])
            
            self.source_completed = True
            self.logger.log("SOURCE: Completed")
            
            self._check_job_completion()
            
        except Exception as e:
            self.logger.log(f"SOURCE ERROR: {e}")
            import traceback
            traceback.print_exc()
            self.source_completed = True
            self._check_job_completion()
    
    def _get_source_files(self, hydfs_src: str) -> List[Tuple[str, List[str]]]:
        """Get source files from HyDFS (handles both files and directories)."""
        files = []
        
        # First, try to treat it as a local file
        if os.path.exists(hydfs_src):
            if os.path.isdir(hydfs_src):
                for fname in os.listdir(hydfs_src):
                    fpath = os.path.join(hydfs_src, fname)
                    if os.path.isfile(fpath):
                        with open(fpath, 'r') as f:
                            files.append((fname, f.readlines()))
            else:
                with open(hydfs_src, 'r') as f:
                    files.append((os.path.basename(hydfs_src), f.readlines()))
            return files
        
        # Try to get from HyDFS
        if self.hydfs_client:
            try:
                # Try as single file first
                temp_file = f"/tmp/rainstorm_src_{self.run_id}.tmp"
                self.hydfs_client.get(hydfs_src, temp_file)
                
                if os.path.exists(temp_file):
                    with open(temp_file, 'r') as f:
                        files.append((hydfs_src, f.readlines()))
                    os.remove(temp_file)
                    return files
            except Exception as e:
                self.logger.log(f"SOURCE: Could not get {hydfs_src} as file: {e}")
            
            # Try as directory - list files and get each one
            try:
                # This would require implementing directory listing in HyDFS
                # For now, assume single file
                pass
            except:
                pass
        
        return files
    
    def _get_stage_tasks(self, stage: int) -> List[dict]:
        """Get the current tasks for a stage."""
        tasks = []
        with self.tasks_lock:
            for task_id in self.stage_tasks.get(stage, []):
                if task_id in self.tasks:
                    t = self.tasks[task_id]
                    tasks.append({
                        'task_id': task_id,
                        'hostname': VM_HOSTS[t.vm_id],
                        'port': TASK_BASE_PORT + (t.stage * 100) + t.task_idx
                    })
        return tasks
    
    def _send_tuple(self, hostname: str, port: int, msg: dict):
        """Send a tuple to a task."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((hostname, port))
            sock.sendall(json.dumps(msg).encode('utf-8'))
            sock.recv(1024)  # Wait for ACK
            sock.close()
        except Exception as e:
            pass  # Silently ignore send failures
    
    def _send_eof(self, hostname: str, port: int):
        """Send EOF signal to a task."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((hostname, port))
            sock.sendall(json.dumps({'type': 'EOF', 'source_task': 'source'}).encode('utf-8'))
            sock.recv(1024)
            sock.close()
        except Exception as e:
            self.logger.log(f"EOF SEND ERROR: {e}")
    
    def _monitor_loop(self):
        """Monitor loop - logs rates and checks job status."""
        while self.running and self.job_running:
            try:
                time.sleep(RATE_REPORT_INTERVAL)
                
                if not self.job_running:
                    break
                
                # Log rates per stage
                with self.rates_lock:
                    stage_rates: Dict[int, List[float]] = {}
                    for task_id, rate in list(self.task_rates.items()):
                        # Extract stage from task_id
                        try:
                            stage = int(task_id.split('_')[0].replace('stage', ''))
                            if stage not in stage_rates:
                                stage_rates[stage] = []
                            stage_rates[stage].append(rate)
                        except:
                            pass
                    
                    for stage, rates in sorted(stage_rates.items()):
                        if rates:
                            avg_rate = sum(rates) / len(rates)
                            total_rate = sum(rates)
                            self.logger.log(f"RATE: stage{stage} avg={avg_rate:.2f} "
                                          f"total={total_rate:.2f} tuples/sec")
                
            except Exception as e:
                self.logger.log(f"MONITOR ERROR: {e}")
    
    def _failure_detection_loop(self):
        """Detect failed tasks and restart them."""
        while self.running and self.job_running:
            try:
                time.sleep(FAILURE_CHECK_INTERVAL)
                
                if not self.job_running or not self.exactly_once:
                    continue
                
                current_time = time.time()
                failed_tasks = []
                
                # Get current failed/suspected nodes from membership
                failed_vms = self._get_failed_vms()
                
                with self.tasks_lock:
                    for task_id, task in list(self.tasks.items()):
                        # Skip stateful tasks - we don't restart them per spec
                        if task.is_stateful:
                            continue
                        
                        should_restart = False
                        
                        # Check if task's VM has failed according to membership
                        if task.vm_id in failed_vms:
                            self.logger.log(f"FAILURE: {task_id} on VM{task.vm_id} (VM failed per membership)")
                            should_restart = True
                        
                        # Check if task has timed out (no heartbeat)
                        elif current_time - task.last_heartbeat > 10.0:
                            # Verify task is actually dead
                            if not self._check_task_alive(task):
                                self.logger.log(f"FAILURE: {task_id} on VM{task.vm_id} (no heartbeat)")
                                should_restart = True
                        
                        if should_restart:
                            failed_tasks.append(task)
                
                # Restart failed tasks
                for task in failed_tasks:
                    self._restart_task(task)
                
            except Exception as e:
                self.logger.log(f"FAILURE DETECTION ERROR: {e}")
    
    def _get_failed_vms(self) -> Set[int]:
        """Get set of VM IDs that have failed according to membership service."""
        failed_vms = set()
        
        if not self.hydfs_node:
            return failed_vms
        
        try:
            membership_service = getattr(self.hydfs_node, 'membership', None)
            if membership_service is None:
                return failed_vms
            
            membership_list = getattr(membership_service, 'membership', None)
            if membership_list is None or not hasattr(membership_list, 'get_members'):
                return failed_vms
            
            members = membership_list.get_members()
            
            for hostname, info in members.items():
                status = info.get('status', '')
                
                # Check for FAILED or SUSPECTED status
                is_failed = False
                if MemberStatus:
                    if status in (MemberStatus.FAILED, MemberStatus.FAILED.value,
                                  MemberStatus.SUSPECTED, MemberStatus.SUSPECTED.value,
                                  MemberStatus.LEFT, MemberStatus.LEFT.value):
                        is_failed = True
                elif status in ('FAILED', 'SUSPECTED', 'LEFT'):
                    is_failed = True
                
                if is_failed:
                    vm_id = get_vm_id_from_hostname(hostname)
                    failed_vms.add(vm_id)
            
        except Exception as e:
            self.logger.log(f"Error checking failed VMs: {e}")
        
        return failed_vms
    
    def _check_task_alive(self, task: TaskInfo) -> bool:
        """Check if a task is still running."""
        hostname = VM_HOSTS[task.vm_id]
        
        try:
            response = self._send_to_worker(hostname, {
                'type': 'CHECK_TASK',
                'pid': task.pid
            }, timeout=5.0)
            
            return response and response.get('running', False)
        except:
            return False
    
    def _restart_task(self, task: TaskInfo):
        """Restart a failed task with the same identity."""
        self.logger.log(f"TASK RESTART: {task.task_id} (was on VM{task.vm_id})")
        
        # Find a new worker for the task
        workers = self._get_available_workers()
        if not workers:
            self.logger.log(f"TASK RESTART FAILED: No workers for {task.task_id}")
            return
        
        # Prefer a different VM if possible
        new_vm = None
        for w in workers:
            if w != task.vm_id:
                new_vm = w
                break
        if new_vm is None:
            new_vm = workers[0]
        
        # Update task info but keep the same task_id
        old_vm = task.vm_id
        task.vm_id = new_vm
        task.pid = 0
        task.last_heartbeat = time.time()
        
        self.logger.log(f"TASK RESTART: {task.task_id} moving from VM{old_vm} to VM{new_vm}")
        
        # Start task on new worker
        self._start_task_on_worker(task)
    
    def _autoscale_loop(self):
        """Autoscaling loop - adjusts task count based on load."""
        while self.running and self.job_running:
            try:
                time.sleep(AUTOSCALE_CHECK_INTERVAL)
                
                if not self.job_running or not self.autoscale_enabled:
                    continue
                
                current_time = time.time()
                
                # Check each stage (except stateful stages)
                for stage_idx, stage in enumerate(self.stages):
                    if stage.is_stateful:
                        continue
                    
                    # Check cooldown
                    last_scale = self.last_autoscale_time.get(stage_idx, 0)
                    if current_time - last_scale < self.autoscale_cooldown:
                        continue
                    
                    # Calculate average input rate per task
                    stage_rates = []
                    with self.rates_lock:
                        for task_id in self.stage_tasks.get(stage_idx, []):
                            if task_id in self.task_rates:
                                stage_rates.append(self.task_rates[task_id])
                    
                    if not stage_rates:
                        continue
                    
                    avg_rate = sum(stage_rates) / len(stage_rates)
                    current_tasks = len(stage_rates)
                    
                    # Scale up if above high watermark
                    if avg_rate > self.hw and current_tasks < MAX_TASKS_PER_STAGE:
                        self._scale_up_stage(stage_idx)
                        self.last_autoscale_time[stage_idx] = current_time
                        self.logger.log(f"AUTOSCALE UP: stage{stage_idx} "
                                      f"(avg_rate={avg_rate:.2f} > hw={self.hw})")
                    
                    # Scale down if below low watermark
                    elif avg_rate < self.lw and current_tasks > MIN_TASKS_PER_STAGE:
                        self._scale_down_stage(stage_idx)
                        self.last_autoscale_time[stage_idx] = current_time
                        self.logger.log(f"AUTOSCALE DOWN: stage{stage_idx} "
                                      f"(avg_rate={avg_rate:.2f} < lw={self.lw})")
                
            except Exception as e:
                self.logger.log(f"AUTOSCALE ERROR: {e}")
    
    def _scale_up_stage(self, stage_idx: int):
        """Add a task to a stage."""
        stage = self.stages[stage_idx]
        
        workers = self._get_available_workers()
        if not workers:
            return
        
        # Find least loaded worker
        worker_task_count: Dict[int, int] = {w: 0 for w in workers}
        with self.tasks_lock:
            for t in self.tasks.values():
                if t.vm_id in worker_task_count:
                    worker_task_count[t.vm_id] += 1
        
        target_vm = min(worker_task_count.keys(), key=lambda w: worker_task_count[w])
        
        # Create new task
        new_task_idx = len(self.stage_tasks.get(stage_idx, []))
        task_id = f"stage{stage_idx}_task{new_task_idx}_{self.run_id}"
        
        task_info = TaskInfo(
            task_id=task_id,
            stage=stage_idx,
            task_idx=new_task_idx,
            vm_id=target_vm,
            pid=0,
            op_exe=stage.op_exe,
            op_args=stage.op_args,
            log_file="",
            is_stateful=stage.is_stateful
        )
        
        with self.tasks_lock:
            self.tasks[task_id] = task_info
            self.stage_tasks[stage_idx].append(task_id)
            stage.task_ids.append(task_id)
            stage.num_tasks += 1
        
        self._start_task_on_worker(task_info)
        self.logger.log(f"AUTOSCALE: Added {task_id} on VM{target_vm}")
    
    def _scale_down_stage(self, stage_idx: int):
        """Remove a task from a stage."""
        with self.tasks_lock:
            task_ids = self.stage_tasks.get(stage_idx, [])
            if len(task_ids) <= MIN_TASKS_PER_STAGE:
                return
            
            # Remove the last task
            task_id = task_ids[-1]
            if task_id not in self.tasks:
                return
            
            task = self.tasks[task_id]
            
            # Kill the task
            hostname = VM_HOSTS[task.vm_id]
            self._send_to_worker(hostname, {'type': 'KILL_PROCESS', 'pid': task.pid})
            
            # Remove from tracking
            del self.tasks[task_id]
            self.stage_tasks[stage_idx].remove(task_id)
            self.stages[stage_idx].task_ids.remove(task_id)
            self.stages[stage_idx].num_tasks -= 1
            
            with self.rates_lock:
                if task_id in self.task_rates:
                    del self.task_rates[task_id]
        
        self.logger.log(f"AUTOSCALE: Removed {task_id}")


class RainStormWorker:
    """RainStorm Worker - Runs on non-leader VMs to execute tasks."""
    
    def __init__(self, vm_id: int, hydfs_node: Optional['HyDFSNode']):
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
        self.server_socket.listen(50)
        
        threading.Thread(target=self._server_loop, daemon=True).start()
        
        self.logger.log(f"WORKER: Started on port {RAINSTORM_PORT}")
    
    def stop(self):
        """Stop the worker."""
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
    
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
        """Handle an incoming connection."""
        try:
            data = conn.recv(65536).decode('utf-8')
            if not data:
                return
            
            msg = json.loads(data)
            msg_type = msg.get('type')
            
            response = {'status': 'error', 'message': 'Unknown'}
            
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
            try:
                conn.close()
            except:
                pass
    
    def _handle_start_task(self, msg: dict) -> dict:
        """Start a task process."""
        import subprocess
        
        task_id = msg.get('task_id')
        stage = msg.get('stage')
        task_idx = msg.get('task_idx')
        op_exe = msg.get('op_exe')
        op_args = msg.get('op_args')
        exactly_once = msg.get('exactly_once', False)
        leader_host = msg.get('leader_host')
        run_id = msg.get('run_id')
        is_stateful = msg.get('is_stateful', False)
        
        task_port = TASK_BASE_PORT + (stage * 100) + task_idx
        log_file = f"task_{task_id}.log"
        
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
        
        if is_stateful:
            cmd.append('--stateful')
        
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
        """Kill a process."""
        pid = msg.get('pid')
        try:
            os.kill(pid, 9)
            self.logger.log(f"KILLED: PID={pid}")
            return {'status': 'success'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def _handle_check_task(self, msg: dict) -> dict:
        """Check if a task is running."""
        pid = msg.get('pid')
        try:
            os.kill(pid, 0)  # Signal 0 checks if process exists
            return {'status': 'success', 'running': True}
        except OSError:
            return {'status': 'success', 'running': False}


def submit_job(args):
    """Submit a job to the leader."""
    operators = []
    for op in args.operators:
        parts = op.split(':')
        op_exe = parts[0]
        op_args = parts[1] if len(parts) > 1 else ''
        is_stateful = 'stateful' in op.lower() or 'count' in op.lower() or 'aggregate' in op.lower()
        operators.append({'exe': op_exe, 'args': op_args, 'stateful': is_stateful})
    
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
            print(f"Job submitted to {leader_host}")
        else:
            print(f"Job failed: {result.get('message')}")
    except Exception as e:
        print(f"Error: {e}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='RainStorm Stream Processing')
    parser.add_argument('--vm-id', type=int, help='VM ID (1-10)')
    parser.add_argument('--leader', action='store_true', help='Run as leader')
    parser.add_argument('--submit', action='store_true', help='Submit a job')
    parser.add_argument('--leader-vm', type=int, default=1, help='Leader VM ID')
    
    # Job parameters
    parser.add_argument('--nstages', type=int, help='Number of stages')
    parser.add_argument('--ntasks', type=int, help='Tasks per stage')
    parser.add_argument('--operators', nargs='+', help='Operators (exe:args)')
    parser.add_argument('--src', type=str, help='HyDFS source directory')
    parser.add_argument('--dest', type=str, help='HyDFS destination file')
    parser.add_argument('--exactly-once', action='store_true', help='Enable exactly-once')
    parser.add_argument('--autoscale', action='store_true', help='Enable autoscaling')
    parser.add_argument('--input-rate', type=int, default=100, help='Input rate (tuples/sec)')
    parser.add_argument('--lw', type=int, default=50, help='Low watermark')
    parser.add_argument('--hw', type=int, default=150, help='High watermark')
    
    args = parser.parse_args()
    
    # Handle job submission
    if args.submit:
        if not args.nstages or not args.operators:
            print("Error: --submit requires --nstages and --operators")
            sys.exit(1)
        submit_job(args)
        return
    
    # Need VM ID for leader/worker modes
    if args.vm_id is None:
        print("Error: --vm-id required")
        sys.exit(1)
    
    # Initialize HyDFS
    hydfs_node = None
    if HyDFSNode:
        try:
            hydfs_node = HyDFSNode(args.vm_id)
            hydfs_node.start()
            hydfs_node.membership.join_group()
            time.sleep(2)
        except Exception as e:
            print(f"Warning: Could not start HyDFS: {e}")
            hydfs_node = None
    
    # Run leader or worker
    if args.leader:
        leader = RainStormLeader(args.vm_id, hydfs_node)
        leader.start()
        
        print("Leader running. Commands:")
        print("  python3 rainstorm.py --submit --nstages N --operators op1:args op2:args --src FILE --dest FILE")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        
        leader.stop()
    else:
        worker = RainStormWorker(args.vm_id, hydfs_node)
        worker.start()
        
        print("Worker running.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        
        worker.stop()
    
    # Cleanup HyDFS
    if hydfs_node:
        try:
            hydfs_node.membership.leave_group()
            hydfs_node.stop()
        except:
            pass


if __name__ == '__main__':
    main()
