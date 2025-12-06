#!/usr/bin/env python3
"""
RainStorm - Stream Processing Framework
"""
import argparse
import json
import os
import socket
import sys
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Add parent directory to path
_parent_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, _parent_dir)
sys.path.insert(0, os.path.join(_parent_dir, 'MP3'))
sys.path.insert(0, os.path.join(_parent_dir, 'MP2'))

from membership import MembershipService, MemberStatus
from node import HyDFSNode
from main import HyDFSClient
from storage import FileBlock
from utils import get_file_id

RAINSTORM_PORT = 8000
TASK_BASE_PORT = 8100

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
    for vm_id, host in VM_HOSTS.items():
        if host == hostname or hostname.startswith(f"fa25-cs425-a7{vm_id:02d}"):
            return vm_id
    return 1

def get_timestamp() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


class RainStormLogger:
    def __init__(self, vm_id: int, run_id: str, is_leader: bool = False):
        self.vm_id = vm_id
        self.run_id = run_id
        prefix = "leader" if is_leader else "rainstorm"
        self.log_file = f"{prefix}_{vm_id}_{run_id}.log"
        self.lock = threading.Lock()
    
    def log(self, msg: str):
        line = f"[{get_timestamp()}] {msg}"
        with self.lock:
            print(line)
            with open(self.log_file, 'a') as f:
                f.write(line + "\n")


class TaskInfo:
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
        self.tuples_processed = 0
        self.completed = False
    
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
            'completed': self.completed
        }


class RainStormLeader:
    def __init__(self, vm_id: int, hydfs_node: HyDFSNode):
        self.vm_id = vm_id
        self.hostname = socket.gethostname()
        self.hydfs_node = hydfs_node
        self.hydfs_client = HyDFSClient(hydfs_node)
        
        self.run_id = f"{int(time.time())}"
        self.logger = RainStormLogger(vm_id, self.run_id, is_leader=True)
        
        self.running = False
        self.job_running = False
        self.job_config: Optional[dict] = None
        self.tasks: Dict[str, TaskInfo] = {}
        self.tasks_lock = threading.Lock()
        
        self.stages: List[dict] = []
        self.stage_tasks: Dict[int, List[str]] = {}
        
        self.task_rates: Dict[str, float] = {}
        self.rates_lock = threading.Lock()
        
        self.autoscale_enabled = False
        self.input_rate = 0
        self.lw = 0
        self.hw = 0
        self.exactly_once = False
        
        self.server_socket: Optional[socket.socket] = None
        
        self.source_completed = False
        self.hydfs_dest: str = ''
        self.output_lock = threading.Lock()
        self.output_sequence = 0
    
    def start(self):
        self.running = True
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('', RAINSTORM_PORT))
        self.server_socket.listen(20)
        
        threading.Thread(target=self._server_loop, daemon=True).start()
        
        self.logger.log(f"LEADER: Started on port {RAINSTORM_PORT}")
    
    def stop(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        self.logger.log("LEADER: Stopped")
    
    def _server_loop(self):
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
                response = {'status': 'success', 'workers': self._get_available_workers()}
            elif msg_type == 'APPEND_OUTPUT':
                response = self._handle_append_output(msg)
            
            conn.sendall(json.dumps(response).encode('utf-8'))
        except Exception as e:
            self.logger.log(f"LEADER ERROR: {e}")
        finally:
            conn.close()
    
    def _handle_submit_job(self, msg: dict) -> dict:
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
            import traceback
            traceback.print_exc()
            return {'status': 'error', 'message': str(e)}
    
    def _handle_rate_update(self, msg: dict) -> dict:
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
        task_id = msg.get('task_id')
        pid = msg.get('pid')
        log_file = msg.get('log_file')
        
        with self.tasks_lock:
            if task_id in self.tasks:
                self.tasks[task_id].pid = pid
                self.tasks[task_id].log_file = log_file
                task = self.tasks[task_id]
                self.logger.log(f"TASK START: {task_id} on VM{task.vm_id} PID={pid}")
        
        return {'status': 'success'}
    
    def _handle_task_completed(self, msg: dict) -> dict:
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
        hydfs_dest = msg.get('hydfs_dest')
        data = bytes(msg.get('data', []))
        task_id = msg.get('task_id')
        run_id = msg.get('run_id')
        
        with self.output_lock:
            try:
                self.output_sequence += 1
                
                block = FileBlock(
                    block_id=f"{get_file_id(hydfs_dest)}_{task_id}_{self.output_sequence}",
                    client_id=f"rainstorm_{run_id}",
                    sequence_num=self.output_sequence,
                    timestamp=time.time(),
                    data=data,
                    size=len(data)
                )
                
                replicas = self.hydfs_node.get_replicas_for_file(hydfs_dest)
                
                if not replicas:
                    return {'status': 'error', 'message': 'No replicas available'}
                
                hostname = replicas[0].split(':')[0]
                port = int(replicas[0].split(':')[1])
                
                append_msg = {
                    'type': 'APPEND',
                    'filename': hydfs_dest,
                    'client_id': f"rainstorm_{run_id}",
                    'block': {
                        'block_id': block.block_id,
                        'client_id': block.client_id,
                        'sequence_num': block.sequence_num,
                        'timestamp': block.timestamp,
                        'data': list(block.data),
                        'size': block.size
                    }
                }
                
                response = self.hydfs_node.network.send_message(hostname, port, append_msg)
                
                if response and response.get('status') == 'success':
                    self.logger.log(f"OUTPUT APPEND: {len(data)} bytes from {task_id}")
                    return {'status': 'success'}
                else:
                    return {'status': 'error', 'message': str(response)}
                    
            except Exception as e:
                self.logger.log(f"OUTPUT APPEND ERROR: {e}")
                return {'status': 'error', 'message': str(e)}
    
    def _check_job_completion(self):
        with self.tasks_lock:
            active_tasks = len(self.tasks)
        
        if self.source_completed and active_tasks == 0 and self.job_running:
            self.logger.log(f"RUN END: All tasks completed. Output: {self.hydfs_dest}")
            self.job_running = False
            
            self.stages.clear()
            self.stage_tasks.clear()
            with self.rates_lock:
                self.task_rates.clear()
            with self.output_lock:
                self.output_sequence = 0
    
    def _handle_list_tasks(self) -> dict:
        with self.tasks_lock:
            tasks = [t.to_dict() for t in self.tasks.values()]
        return {'status': 'success', 'tasks': tasks}
    
    def _handle_kill_task(self, msg: dict) -> dict:
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
        task_id = msg.get('task_id')
        
        with self.tasks_lock:
            if task_id not in self.tasks:
                return {'status': 'error', 'message': 'Task not found'}
            
            task = self.tasks[task_id]
            stage = task.stage
        
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
            'input_rate': self.input_rate
        }
        
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
        self.logger.log(f"OUTPUT: Creating {hydfs_dest}")
        
        try:
            replicas = self.hydfs_node.get_replicas_for_file(hydfs_dest)
            if not replicas:
                self.logger.log("OUTPUT ERROR: No replicas")
                return False
            
            for node_id in replicas:
                try:
                    hostname = node_id.split(':')[0]
                    port = int(node_id.split(':')[1])
                    
                    # Delete existing
                    self.hydfs_node.network.send_message(hostname, port, 
                        {'type': 'DELETE', 'filename': hydfs_dest}, timeout=5.0)
                    
                    # Create new
                    response = self.hydfs_node.network.send_message(hostname, port, {
                        'type': 'CREATE',
                        'filename': hydfs_dest,
                        'data': []
                    })
                    
                    if response and response.get('status') == 'success':
                        self.logger.log(f"OUTPUT: Created on {hostname}")
                except Exception as e:
                    self.logger.log(f"OUTPUT ERROR on {node_id}: {e}")
            
            return True
        except Exception as e:
            self.logger.log(f"OUTPUT ERROR: {e}")
            return False
    
    def start_job(self, nstages: int, ntasks_per_stage: int, 
                  operators: List[Tuple[str, str]], hydfs_src: str, 
                  hydfs_dest: str, exactly_once: bool, autoscale_enabled: bool,
                  input_rate: int = 100, lw: int = 50, hw: int = 150):
        
        self.run_id = f"{int(time.time())}"
        self.logger = RainStormLogger(self.vm_id, self.run_id, is_leader=True)
        
        self.logger.log(f"RUN START: stages={nstages} tasks={ntasks_per_stage}")
        self.logger.log(f"  src={hydfs_src} dest={hydfs_dest}")
        
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
        self.job_running = True
        self.source_completed = False
        self.hydfs_dest = hydfs_dest
        
        with self.output_lock:
            self.output_sequence = 0
        
        self._create_output_file(hydfs_dest)
        
        self.stages = []
        self.stage_tasks = {}
        for i, (op_exe, op_args) in enumerate(operators):
            self.stages.append({'op_exe': op_exe, 'op_args': op_args, 'num_tasks': ntasks_per_stage})
            self.stage_tasks[i] = []
        
        workers = self._get_available_workers()
        if not workers:
            self.logger.log("ERROR: No workers")
            return
        
        self.logger.log(f"Workers: {workers}")
        
        self._schedule_tasks(workers)
        
        # Wait for tasks to start
        time.sleep(2)
        
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        threading.Thread(target=self._source_loop, args=(hydfs_src,), daemon=True).start()
    
    def _get_available_workers(self) -> List[int]:
        workers = []
        members = self.hydfs_node.membership.membership.get_members()
        
        for hostname, info in members.items():
            if info['status'] == MemberStatus.ACTIVE.value:
                vm_id = get_vm_id_from_hostname(hostname)
                workers.append(vm_id)
        
        if not workers:
            workers = [self.vm_id]
        
        return sorted(workers)
    
    def _schedule_tasks(self, workers: List[int]):
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
        
        for task_id, task in list(self.tasks.items()):
            self._start_task_on_worker(task)
    
    def _start_task_on_worker(self, task: TaskInfo):
        hostname = VM_HOSTS[task.vm_id]
        
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
            task.pid = response.get('pid', 0)
            task.log_file = response.get('log_file', '')
            self.logger.log(f"TASK STARTED: {task.task_id} VM{task.vm_id} PID={task.pid}")
        else:
            self.logger.log(f"TASK START FAILED: {task.task_id}")
    
    def _start_task_locally(self, task: TaskInfo):
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
    
    def _source_loop(self, hydfs_src: str):
        try:
            local_path = hydfs_src
            if not os.path.exists(local_path):
                temp_file = f"/tmp/rainstorm_src_{self.run_id}.csv"
                self.hydfs_client.get(hydfs_src, temp_file)
                local_path = temp_file
            
            if not os.path.exists(local_path):
                self.logger.log(f"SOURCE ERROR: Cannot find {hydfs_src}")
                self.source_completed = True
                self._check_job_completion()
                return
            
            with open(local_path, 'r') as f:
                lines = f.readlines()
            
            self.logger.log(f"SOURCE: Loaded {len(lines)} lines")
            
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
                self.logger.log("SOURCE ERROR: No stage 0 tasks")
                self.source_completed = True
                self._check_job_completion()
                return
            
            interval = 1.0 / self.input_rate if self.input_rate > 0 else 0.01
            
            for line_num, line in enumerate(lines):
                if not self.running or not self.job_running:
                    break
                
                key = f"{hydfs_src}:{line_num}"
                value = line.strip()
                
                task_idx = hash(key) % len(stage0_tasks)
                target = stage0_tasks[task_idx]
                
                tuple_msg = {
                    'type': 'TUPLE',
                    'tuple_id': f"{self.run_id}_{line_num}",
                    'key': key,
                    'value': value,
                    'source_task': 'source'
                }
                
                self._send_tuple(target['hostname'], target['port'], tuple_msg)
                
                time.sleep(interval)
            
            self.logger.log("SOURCE: Sending EOF")
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
    
    def _send_tuple(self, hostname: str, port: int, msg: dict):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((hostname, port))
            sock.sendall(json.dumps(msg).encode('utf-8'))
            sock.recv(1024)
            sock.close()
        except:
            pass
    
    def _send_eof(self, hostname: str, port: int):
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
        while self.running and self.job_running:
            try:
                time.sleep(2.0)
                
                if not self.job_running:
                    break
                
                # Log rates
                with self.rates_lock:
                    for task_id, rate in list(self.task_rates.items()):
                        if rate > 0:
                            self.logger.log(f"RATE: {task_id} {rate:.2f} tuples/sec")
                
            except Exception as e:
                self.logger.log(f"MONITOR ERROR: {e}")


class RainStormWorker:
    def __init__(self, vm_id: int, hydfs_node: HyDFSNode):
        self.vm_id = vm_id
        self.hostname = socket.gethostname()
        self.hydfs_node = hydfs_node
        
        self.run_id = f"{int(time.time())}"
        self.logger = RainStormLogger(vm_id, self.run_id)
        
        self.running = False
        self.server_socket: Optional[socket.socket] = None
        self.task_processes: Dict[str, int] = {}
    
    def start(self):
        self.running = True
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('', RAINSTORM_PORT))
        self.server_socket.listen(20)
        
        threading.Thread(target=self._server_loop, daemon=True).start()
        
        self.logger.log(f"WORKER: Started on port {RAINSTORM_PORT}")
    
    def stop(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()
    
    def _server_loop(self):
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
            conn.close()
    
    def _handle_start_task(self, msg: dict) -> dict:
        import subprocess
        
        task_id = msg.get('task_id')
        stage = msg.get('stage')
        task_idx = msg.get('task_idx')
        op_exe = msg.get('op_exe')
        op_args = msg.get('op_args')
        exactly_once = msg.get('exactly_once', False)
        leader_host = msg.get('leader_host')
        run_id = msg.get('run_id')
        
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
        pid = msg.get('pid')
        try:
            os.kill(pid, 9)
            self.logger.log(f"KILLED: PID={pid}")
            return {'status': 'success'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def _handle_check_task(self, msg: dict) -> dict:
        pid = msg.get('pid')
        try:
            os.kill(pid, 0)
            return {'status': 'success', 'running': True}
        except OSError:
            return {'status': 'success', 'running': False}


def submit_job(args):
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
            print(f"Job submitted to {leader_host}")
        else:
            print(f"Job failed: {result.get('message')}")
    except Exception as e:
        print(f"Error: {e}")


def main():
    parser = argparse.ArgumentParser(description='RainStorm')
    parser.add_argument('--vm-id', type=int)
    parser.add_argument('--leader', action='store_true')
    parser.add_argument('--submit', action='store_true')
    parser.add_argument('--leader-vm', type=int, default=1)
    
    parser.add_argument('--nstages', type=int)
    parser.add_argument('--ntasks', type=int)
    parser.add_argument('--operators', nargs='+')
    parser.add_argument('--src', type=str)
    parser.add_argument('--dest', type=str)
    parser.add_argument('--exactly-once', action='store_true')
    parser.add_argument('--autoscale', action='store_true')
    parser.add_argument('--input-rate', type=int, default=100)
    parser.add_argument('--lw', type=int, default=50)
    parser.add_argument('--hw', type=int, default=150)
    
    args = parser.parse_args()
    
    if args.submit:
        if not args.nstages or not args.operators:
            print("Error: --submit requires --nstages and --operators")
            sys.exit(1)
        submit_job(args)
        return
    
    if args.vm_id is None:
        print("Error: --vm-id required")
        sys.exit(1)
    
    hydfs_node = HyDFSNode(args.vm_id)
    hydfs_node.start()
    hydfs_node.membership.join_group()
    time.sleep(2)
    
    if args.leader:
        leader = RainStormLeader(args.vm_id, hydfs_node)
        leader.start()
        
        print("Leader running. Submit jobs with:")
        print("  python3 rainstorm.py --submit --nstages N --ntasks M --operators ... --src FILE --dest FILE")
        
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
    
    hydfs_node.membership.leave_group()
    hydfs_node.stop()


if __name__ == '__main__':
    main()