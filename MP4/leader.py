    """
RainStorm Leader Node
- Job coordination & scheduling
- Task -> VM/PID tracking
- Failure handling & task restarts
- list_tasks, kill_task commands
- Autoscaling decisions (Resource Manager)
- Leader logging (job, task, rate events)
"""
import os
import sys
import time
import threading
import json
import socket
from typing import Dict, List, Optional, Tuple, Set
from collections import defaultdict
from utils import (
    RainStormLogger, load_config, get_vm_by_id, get_current_vm_id,
    make_task_id, get_task_port, hash_partition
)
from network import RPCServer, RPCClient
from autoscale import ResourceManager, PartitionManager

# Add parent for MP2
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# ============================================================================
# Task Info
# ============================================================================

class TaskInfo:
    """Information about a scheduled task."""
    def __init__(self, task_id: str, stage_num: int, task_num: int,
                 vm_id: int, hostname: str, port: int,
                 op_exe: str, op_args: str):
        self.task_id = task_id
        self.stage_num = stage_num
        self.task_num = task_num
        self.vm_id = vm_id
        self.hostname = hostname
        self.port = port
        self.pid: Optional[int] = None
        self.op_exe = op_exe
        self.op_args = op_args
        self.log_file: Optional[str] = None
        self.status = 'pending'  # pending, running, failed, completed
        self.start_time: Optional[float] = None
        self.current_rate = 0.0
    
    def to_dict(self) -> Dict:
        return {
            'task_id': self.task_id,
            'stage_num': self.stage_num,
            'task_num': self.task_num,
            'vm_id': self.vm_id,
            'hostname': self.hostname,
            'port': self.port,
            'pid': self.pid,
            'op_exe': self.op_exe,
            'log_file': self.log_file,
            'status': self.status,
            'rate': self.current_rate
        }

# ============================================================================
# Job Info
# ============================================================================

class JobInfo:
    """Information about a RainStorm job."""
    def __init__(self, job_id: str, num_stages: int, tasks_per_stage: int,
                 operators: List[Tuple[str, str]], src_file: str, dest_file: str,
                 exactly_once: bool, autoscale: bool,
                 input_rate: float, lw: float, hw: float):
        self.job_id = job_id
        self.num_stages = num_stages
        self.tasks_per_stage = tasks_per_stage
        self.operators = operators  # [(op_exe, op_args), ...]
        self.src_file = src_file
        self.dest_file = dest_file
        self.exactly_once = exactly_once
        self.autoscale = autoscale
        self.input_rate = input_rate
        self.low_watermark = lw
        self.high_watermark = hw
        self.start_time = time.time()
        self.end_time: Optional[float] = None
        self.status = 'running'

# ============================================================================
# Leader Node
# ============================================================================

class Leader:
    """
    Leader node that coordinates RainStorm jobs.
    """
    def __init__(self, vm_id: int, config: Dict):
        self.vm_id = vm_id
        self.config = config
        self.hostname = socket.gethostname()
        
        # Ports
        self.rpc_port = config.get('rainstorm_port', 8000)
        
        # Logger
        self.run_id = f"leader_{int(time.time())}"
        self.logger = RainStormLogger("leader", vm_id, self.run_id)
        
        # Job tracking
        self.current_job: Optional[JobInfo] = None
        self.job_lock = threading.Lock()
        
        # Task tracking: task_id -> TaskInfo
        self.tasks: Dict[str, TaskInfo] = {}
        self.tasks_lock = threading.Lock()
        
        # Stage tracking: stage_num -> list of task_ids
        self.stages: Dict[int, List[str]] = defaultdict(list)
        
        # Worker tracking: vm_id -> WorkerInfo
        self.workers: Dict[int, Dict] = {}
        self.workers_lock = threading.Lock()
        
        # RPC
        self.rpc_server = RPCServer(self.rpc_port, self.logger)
        self.rpc_client = RPCClient(self.logger)
        
        # Partition manager
        self.partition_manager = PartitionManager(self.logger)
        
        # Resource manager (for autoscaling)
        self.resource_manager: Optional[ResourceManager] = None
        
        # Control
        self.running = False
        
        # Rate logging interval
        self.rate_log_interval = 1.0
        
        # Register handlers
        self._register_handlers()
    
    def _register_handlers(self):
        """Register RPC handlers."""
        # Worker management
        self.rpc_server.register_handler('WORKER_REGISTER', self._handle_worker_register)
        self.rpc_server.register_handler('TASK_STARTED', self._handle_task_started)
        self.rpc_server.register_handler('TASK_FAILED', self._handle_task_failed)
        self.rpc_server.register_handler('RATE_REPORT', self._handle_rate_report)
        
        # Job management
        self.rpc_server.register_handler('SUBMIT_JOB', self._handle_submit_job)
        self.rpc_server.register_handler('JOB_STATUS', self._handle_job_status)
        self.rpc_server.register_handler('STOP_JOB', self._handle_stop_job)
        
        # Commands
        self.rpc_server.register_handler('LIST_TASKS', self._handle_list_tasks)
        self.rpc_server.register_handler('KILL_TASK', self._handle_kill_task)
    
    # ==================== Worker Management ====================
    
    def _handle_worker_register(self, message: Dict, addr: Tuple) -> Dict:
        """Handle worker registration."""
        vm_id = message.get('vm_id')
        hostname = message.get('hostname')
        rpc_port = message.get('rpc_port')
        
        with self.workers_lock:
            self.workers[vm_id] = {
                'vm_id': vm_id,
                'hostname': hostname,
                'rpc_port': rpc_port,
                'active': True,
                'tasks': []
            }
        
        self.logger.log_event("WORKER_REGISTERED", {
            'vm_id': vm_id,
            'hostname': hostname
        })
        
        return {'status': 'ok'}
    
    def _handle_task_started(self, message: Dict, addr: Tuple) -> Dict:
        """Handle task started notification."""
        vm_id = message.get('vm_id')
        task_info = message.get('task')
        task_id = task_info.get('task_id')
        
        with self.tasks_lock:
            if task_id in self.tasks:
                self.tasks[task_id].pid = task_info.get('pid')
                self.tasks[task_id].log_file = task_info.get('log_file')
                self.tasks[task_id].status = 'running'
                self.tasks[task_id].start_time = time.time()
        
        self.logger.log_event("TASK_STARTED", {
            'task_id': task_id,
            'vm_id': vm_id,
            'pid': task_info.get('pid'),
            'op_exe': task_info.get('op_exe'),
            'log_file': task_info.get('log_file')
        })
        
        return {'status': 'ok'}
    
    def _handle_task_failed(self, message: Dict, addr: Tuple) -> Dict:
        """Handle task failure notification."""
        vm_id = message.get('vm_id')
        task_id = message.get('task_id')
        pid = message.get('pid')
        
        self.logger.log_event("TASK_FAILED", {
            'task_id': task_id,
            'vm_id': vm_id,
            'pid': pid
        })
        
        with self.tasks_lock:
            if task_id in self.tasks:
                self.tasks[task_id].status = 'failed'
        
        # Restart task if job is still running
        if self.current_job and self.current_job.status == 'running':
            self._restart_task(task_id)
        
        return {'status': 'ok'}
    
    def _handle_rate_report(self, message: Dict, addr: Tuple) -> Dict:
        """Handle rate report from a task."""
        task_id = message.get('task_id')
        rate = message.get('rate', 0.0)
        
        with self.tasks_lock:
            if task_id in self.tasks:
                self.tasks[task_id].current_rate = rate
        
        # Forward to resource manager if autoscaling
        if self.resource_manager:
            self.resource_manager.report_rate(task_id, rate)
        
        return {'status': 'ok'}
    
    # ==================== Job Management ====================
    
    def _handle_submit_job(self, message: Dict, addr: Tuple) -> Dict:
        """Handle job submission."""
        job_config = message.get('job_config', {})
        
        try:
            job = self._create_job(job_config)
            self._schedule_job(job)
            
            return {
                'status': 'ok',
                'job_id': job.job_id,
                'message': 'Job submitted successfully'
            }
        except Exception as e:
            self.logger.log(f"Job submission error: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def _handle_job_status(self, message: Dict, addr: Tuple) -> Dict:
        """Handle job status query."""
        with self.job_lock:
            if not self.current_job:
                return {'status': 'error', 'message': 'No job running'}
            
            return {
                'status': 'ok',
                'job_id': self.current_job.job_id,
                'job_status': self.current_job.status,
                'elapsed': time.time() - self.current_job.start_time,
                'tasks': len(self.tasks)
            }
    
    def _handle_stop_job(self, message: Dict, addr: Tuple) -> Dict:
        """Handle job stop request."""
        self._stop_current_job()
        return {'status': 'ok'}
    
    # ==================== Commands ====================
    
    def _handle_list_tasks(self, message: Dict, addr: Tuple) -> Dict:
        """
        Handle list_tasks command.
        Returns VM, PID, op_exe, and local log file for each task.
        """
        with self.tasks_lock:
            task_list = []
            for task in self.tasks.values():
                task_list.append({
                    'task_id': task.task_id,
                    'vm': task.vm_id,
                    'pid': task.pid,
                    'op_exe': task.op_exe,
                    'log_file': task.log_file,
                    'status': task.status
                })
        
        return {
            'status': 'ok',
            'tasks': task_list
        }
    
    def _handle_kill_task(self, message: Dict, addr: Tuple) -> Dict:
        """
        Handle kill_task command.
        Sends kill -9 to specified task.
        """
        vm_id = message.get('vm_id')
        pid = message.get('pid')
        task_id = message.get('task_id')
        
        self.logger.log(f"Kill task request: vm={vm_id}, pid={pid}, task={task_id}")
        
        # Find worker
        with self.workers_lock:
            if vm_id not in self.workers:
                return {'status': 'error', 'message': f'Worker {vm_id} not found'}
            
            worker = self.workers[vm_id]
        
        # Send kill command to worker
        response = self.rpc_client.send(
            worker['hostname'], worker['rpc_port'],
            {'type': 'KILL_TASK', 'task_id': task_id, 'pid': pid}
        )
        
        return response or {'status': 'error', 'message': 'Worker not responding'}
    
    # ==================== Job Execution ====================
    
    def _create_job(self, config: Dict) -> JobInfo:
        """Create a job from configuration."""
        job_id = f"job_{int(time.time())}"
        
        # Parse operators
        operators = []
        for i in range(config['num_stages']):
            op_exe = config.get(f'op{i+1}_exe', 'identity')
            op_args = config.get(f'op{i+1}_args', '')
            operators.append((op_exe, op_args))
        
        job = JobInfo(
            job_id=job_id,
            num_stages=config['num_stages'],
            tasks_per_stage=config['tasks_per_stage'],
            operators=operators,
            src_file=config['src_file'],
            dest_file=config['dest_file'],
            exactly_once=config.get('exactly_once', True),
            autoscale=config.get('autoscale', False),
            input_rate=config.get('input_rate', 100),
            lw=config.get('lw', 10),
            hw=config.get('hw', 100)
        )
        
        with self.job_lock:
            self.current_job = job
        
        self.logger.log_event("JOB_CREATED", {
            'job_id': job_id,
            'stages': job.num_stages,
            'tasks_per_stage': job.tasks_per_stage,
            'exactly_once': job.exactly_once,
            'autoscale': job.autoscale
        })
        
        return job
    
    def _schedule_job(self, job: JobInfo):
        """Schedule all tasks for a job."""
        self.logger.log_event("JOB_START", {'job_id': job.job_id})
        
        # Get available workers
        with self.workers_lock:
            available_workers = list(self.workers.values())
        
        if not available_workers:
            raise RuntimeError("No workers available")
        
        # Clear previous tasks
        with self.tasks_lock:
            self.tasks.clear()
            self.stages.clear()
        
        # Schedule source task
        self._schedule_source_task(job, available_workers)
        
        # Schedule processing tasks for each stage
        for stage in range(1, job.num_stages + 1):
            op_exe, op_args = job.operators[stage - 1]
            is_final = (stage == job.num_stages)
            
            for task_num in range(job.tasks_per_stage):
                self._schedule_processing_task(
                    job, stage, task_num, op_exe, op_args,
                    available_workers, is_final
                )
        
        # Setup routing
        self._setup_routing(job)
        
        # Start tasks
        self._start_all_tasks(job)
        
        # Setup autoscaling if enabled
        if job.autoscale:
            self._setup_autoscaling(job)
    
    def _schedule_source_task(self, job: JobInfo, workers: List[Dict]):
        """Schedule the source task."""
        # Source runs on first available worker
        worker = workers[0]
        task_id = make_task_id(0, 0, job.job_id)
        
        task = TaskInfo(
            task_id=task_id,
            stage_num=0,
            task_num=0,
            vm_id=worker['vm_id'],
            hostname=worker['hostname'],
            port=get_task_port(self.config['task_base_port'], 0, 0),
            op_exe='source',
            op_args=''
        )
        
        with self.tasks_lock:
            self.tasks[task_id] = task
            self.stages[0].append(task_id)
    
    def _schedule_processing_task(self, job: JobInfo, stage: int, task_num: int,
                                   op_exe: str, op_args: str, workers: List[Dict],
                                   is_final: bool):
        """Schedule a processing task."""
        # Distribute tasks across workers
        worker_idx = (stage * job.tasks_per_stage + task_num) % len(workers)
        worker = workers[worker_idx]
        
        task_id = make_task_id(stage, task_num, job.job_id)
        
        task = TaskInfo(
            task_id=task_id,
            stage_num=stage,
            task_num=task_num,
            vm_id=worker['vm_id'],
            hostname=worker['hostname'],
            port=get_task_port(self.config['task_base_port'], stage, task_num),
            op_exe=op_exe,
            op_args=op_args
        )
        
        with self.tasks_lock:
            self.tasks[task_id] = task
            self.stages[stage].append(task_id)
    
    def _setup_routing(self, job: JobInfo):
        """Setup routing between stages."""
        for stage in range(job.num_stages + 1):
            task_ids = self.stages.get(stage, [])
            routing = []
            
            for task_id in task_ids:
                task = self.tasks[task_id]
                routing.append((task_id, task.hostname, task.port + 1000))
            
            self.partition_manager.set_stage_routing(stage, routing)
    
    def _start_all_tasks(self, job: JobInfo):
        """Start all scheduled tasks."""
        with self.tasks_lock:
            for task_id, task in self.tasks.items():
                self._start_task(task, job)
    
    def _start_task(self, task: TaskInfo, job: JobInfo):
        """Start a single task on its assigned worker."""
        # Determine downstream tasks
        next_stage = task.stage_num + 1
        downstream = []
        
        if next_stage <= job.num_stages:
            for tid in self.stages.get(next_stage, []):
                t = self.tasks[tid]
                downstream.append({
                    'task_id': tid,
                    'host': t.hostname,
                    'port': t.port + 1000  # Tuple port
                })
        
        # Build task config
        if task.stage_num == 0:
            task_type = 'source'
            params = {
                'input_file': job.src_file,
                'input_rate': job.input_rate,
                'use_local_file': True
            }
        elif task.stage_num == job.num_stages:
            task_type = 'sink'
            params = {
                'op_exe': task.op_exe,
                'op_args': task.op_args,
                'output_file': job.dest_file
            }
        else:
            task_type = 'processing'
            params = {
                'op_exe': task.op_exe,
                'op_args': task.op_args
            }
        
        task_config = {
            'type': task_type,
            'task_id': task.task_id,
            'stage_num': task.stage_num,
            'vm_id': task.vm_id,
            'port': task.port,
            'run_id': job.job_id,
            'exactly_once': job.exactly_once,
            'params': params,
            'downstream': downstream
        }
        
        # Send spawn command to worker
        with self.workers_lock:
            if task.vm_id not in self.workers:
                self.logger.log(f"Worker {task.vm_id} not available")
                return
            
            worker = self.workers[task.vm_id]
        
        response = self.rpc_client.send(
            worker['hostname'], worker['rpc_port'],
            {'type': 'SPAWN_TASK', 'task_config': task_config}
        )
        
        if response and response.get('status') == 'ok':
            task.pid = response.get('pid')
            task.log_file = response.get('log_file')
            task.status = 'running'
            self.logger.log(f"Task {task.task_id} started on VM{task.vm_id}")
        else:
            self.logger.log(f"Failed to start task {task.task_id}")
    
    def _restart_task(self, task_id: str):
        """Restart a failed task."""
        with self.tasks_lock:
            if task_id not in self.tasks:
                return
            
            task = self.tasks[task_id]
        
        self.logger.log_event("TASK_RESTART", {
            'task_id': task_id,
            'vm_id': task.vm_id,
            'stage': task.stage_num
        })
        
        # Re-start the task
        with self.job_lock:
            if self.current_job:
                self._start_task(task, self.current_job)
    
    def _stop_current_job(self):
        """Stop the current job."""
        with self.job_lock:
            if not self.current_job:
                return
            
            job = self.current_job
            job.status = 'stopped'
            job.end_time = time.time()
        
        # Stop all tasks
        with self.workers_lock:
            for worker in self.workers.values():
                self.rpc_client.send(
                    worker['hostname'], worker['rpc_port'],
                    {'type': 'STOP_ALL'}
                )
        
        # Stop autoscaling
        if self.resource_manager:
            self.resource_manager.stop()
            self.resource_manager = None
        
        self.logger.log_event("JOB_END", {
            'job_id': job.job_id,
            'elapsed': job.end_time - job.start_time
        })
    
    # ==================== Autoscaling ====================
    
    def _setup_autoscaling(self, job: JobInfo):
        """Setup autoscaling for the job."""
        self.resource_manager = ResourceManager(
            self.logger,
            low_watermark=job.low_watermark,
            high_watermark=job.high_watermark
        )
        
        # Register stages (excluding source and stateful stages)
        for stage in range(1, job.num_stages + 1):
            task_ids = self.stages.get(stage, [])
            self.resource_manager.register_stage(stage, task_ids)
        
        # Set scale callbacks
        self.resource_manager.set_scale_callbacks(
            scale_up=self._scale_up_stage,
            scale_down=self._scale_down_stage
        )
        
        self.resource_manager.start()
        self.logger.log("Autoscaling enabled")
    
    def _scale_up_stage(self, stage_num: int) -> Optional[str]:
        """Add a task to a stage."""
        with self.job_lock:
            if not self.current_job:
                return None
            job = self.current_job
        
        # Find available worker
        with self.workers_lock:
            workers = list(self.workers.values())
        
        if not workers:
            return None
        
        # Create new task
        task_num = len(self.stages.get(stage_num, []))
        task_id = make_task_id(stage_num, task_num, job.job_id)
        
        # Use worker with fewest tasks
        worker = min(workers, key=lambda w: len(w.get('tasks', [])))
        
        op_exe, op_args = job.operators[stage_num - 1]
        
        task = TaskInfo(
            task_id=task_id,
            stage_num=stage_num,
            task_num=task_num,
            vm_id=worker['vm_id'],
            hostname=worker['hostname'],
            port=get_task_port(self.config['task_base_port'], stage_num, task_num),
            op_exe=op_exe,
            op_args=op_args
        )
        
        with self.tasks_lock:
            self.tasks[task_id] = task
            self.stages[stage_num].append(task_id)
        
        # Update routing
        self._setup_routing(job)
        
        # Start the task
        self._start_task(task, job)
        
        # Update resource manager
        self.resource_manager.update_stage_tasks(
            stage_num, self.stages[stage_num]
        )
        
        self.logger.log_event("AUTOSCALE_TASK_ADDED", {
            'task_id': task_id,
            'stage': stage_num,
            'total_tasks': len(self.stages[stage_num])
        })
        
        return task_id
    
    def _scale_down_stage(self, stage_num: int) -> Optional[str]:
        """Remove a task from a stage."""
        task_ids = self.stages.get(stage_num, [])
        
        if len(task_ids) <= 1:
            return None  # Keep at least one task
        
        # Remove last task
        task_id = task_ids[-1]
        
        with self.tasks_lock:
            if task_id not in self.tasks:
                return None
            
            task = self.tasks[task_id]
        
        # Kill the task
        with self.workers_lock:
            if task.vm_id in self.workers:
                worker = self.workers[task.vm_id]
                self.rpc_client.send(
                    worker['hostname'], worker['rpc_port'],
                    {'type': 'KILL_TASK', 'task_id': task_id, 'pid': task.pid}
                )
        
        # Remove from tracking
        with self.tasks_lock:
            del self.tasks[task_id]
            self.stages[stage_num].remove(task_id)
        
        # Update routing
        with self.job_lock:
            if self.current_job:
                self._setup_routing(self.current_job)
        
        # Update resource manager
        self.resource_manager.update_stage_tasks(
            stage_num, self.stages[stage_num]
        )
        
        self.logger.log_event("AUTOSCALE_TASK_REMOVED", {
            'task_id': task_id,
            'stage': stage_num,
            'total_tasks': len(self.stages[stage_num])
        })
        
        return task_id
    
    # ==================== Rate Logging ====================
    
    def _rate_logging_loop(self):
        """Log rates for all tasks periodically."""
        while self.running:
            time.sleep(self.rate_log_interval)
            
            with self.tasks_lock:
                for task_id, task in self.tasks.items():
                    if task.status == 'running':
                        self.logger.log_rate(task_id, task.current_rate)
    
    # ==================== Lifecycle ====================
    
    def start(self):
        """Start the leader node."""
        self.running = True
        
        # Start RPC server
        self.rpc_server.start()
        
        # Start rate logging
        rate_thread = threading.Thread(target=self._rate_logging_loop, daemon=True)
        rate_thread.start()
        
        self.logger.log_event("LEADER_START", {
            'vm_id': self.vm_id,
            'hostname': self.hostname,
            'rpc_port': self.rpc_port
        })
    
    def stop(self):
        """Stop the leader node."""
        self.running = False
        
        # Stop current job
        self._stop_current_job()
        
        # Stop RPC server
        self.rpc_server.stop()
        
        self.logger.log_event("LEADER_STOP", {'vm_id': self.vm_id})
    
    def run(self):
        """Run the leader (blocking)."""
        self.start()
        
        try:
            while self.running:
                time.sleep(1.0)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

# ============================================================================
# Leader Entry Point
# ============================================================================

def run_leader(vm_id: int, config_path: str = "config.json"):
    """Run leader node."""
    config = load_config(config_path)
    leader = Leader(vm_id, config)
    leader.run()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python leader.py <vm_id>")
        sys.exit(1)
    
    vm_id = int(sys.argv[1])
    run_leader(vm_id)