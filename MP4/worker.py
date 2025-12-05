"""
RainStorm Worker Node
- Receives task assignments from leader
- Spawns/kills task processes
- Reports task status to leader
- Handles kill_task commands
"""
import os
import sys
import time
import signal
import subprocess
import threading
import json
import socket
from typing import Dict, List, Optional, Tuple
from utils import (
    RainStormLogger, load_config, get_vm_by_id, get_current_vm_id,
    get_leader_hostname, serialize_message, deserialize_message,
    get_task_port
)
from network import RPCServer, RPCClient

# ============================================================================
# Task Process Info
# ============================================================================

class TaskProcess:
    """
    Information about a running task process.
    """
    def __init__(self, task_id: str, stage_num: int, pid: int,
                 port: int, op_exe: str, log_file: str):
        self.task_id = task_id
        self.stage_num = stage_num
        self.pid = pid
        self.port = port
        self.op_exe = op_exe
        self.log_file = log_file
        self.start_time = time.time()
        self.process: Optional[subprocess.Popen] = None
    
    def is_alive(self) -> bool:
        """Check if process is still running."""
        if self.process:
            return self.process.poll() is None
        try:
            os.kill(self.pid, 0)
            return True
        except OSError:
            return False
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for reporting."""
        return {
            'task_id': self.task_id,
            'stage_num': self.stage_num,
            'pid': self.pid,
            'port': self.port,
            'op_exe': self.op_exe,
            'log_file': self.log_file,
            'alive': self.is_alive(),
            'uptime': time.time() - self.start_time
        }

# ============================================================================
# Worker Node
# ============================================================================

class Worker:
    """
    Worker node that manages task processes.
    """
    def __init__(self, vm_id: int, config: Dict):
        self.vm_id = vm_id
        self.config = config
        self.hostname = socket.gethostname()
        
        # Ports
        self.rpc_port = config.get('rainstorm_port', 8000)
        self.task_base_port = config.get('task_base_port', 8100)
        
        # Logger
        self.run_id = f"worker_{int(time.time())}"
        self.logger = RainStormLogger("worker", vm_id, self.run_id)
        
        # Task management
        self.tasks: Dict[str, TaskProcess] = {}  # task_id -> TaskProcess
        self.tasks_lock = threading.Lock()
        self.next_task_port = self.task_base_port
        
        # RPC
        self.rpc_server = RPCServer(self.rpc_port, self.logger)
        self.rpc_client = RPCClient(self.logger)
        
        # Leader info
        self.leader_host = get_leader_hostname(config)
        self.leader_port = self.rpc_port
        
        # Control
        self.running = False
        
        # Register handlers
        self._register_handlers()
    
    def _register_handlers(self):
        """Register RPC message handlers."""
        self.rpc_server.register_handler('SPAWN_TASK', self._handle_spawn_task)
        self.rpc_server.register_handler('KILL_TASK', self._handle_kill_task)
        self.rpc_server.register_handler('LIST_TASKS', self._handle_list_tasks)
        self.rpc_server.register_handler('TASK_STATUS', self._handle_task_status)
        self.rpc_server.register_handler('UPDATE_ROUTING', self._handle_update_routing)
        self.rpc_server.register_handler('STOP_ALL', self._handle_stop_all)
    
    def _handle_spawn_task(self, message: Dict, addr: Tuple) -> Dict:
        """Handle request to spawn a new task."""
        task_config = message.get('task_config', {})
        task_id = task_config.get('task_id')
        
        if not task_id:
            return {'status': 'error', 'message': 'Missing task_id'}
        
        self.logger.log(f"Spawning task: {task_id}")
        
        try:
            task_process = self._spawn_task(task_config)
            
            return {
                'status': 'ok',
                'task_id': task_id,
                'pid': task_process.pid,
                'port': task_process.port,
                'log_file': task_process.log_file
            }
        except Exception as e:
            self.logger.log(f"Spawn error: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def _handle_kill_task(self, message: Dict, addr: Tuple) -> Dict:
        """Handle request to kill a task (kill -9)."""
        task_id = message.get('task_id')
        pid = message.get('pid')
        
        self.logger.log(f"Kill request: task={task_id}, pid={pid}")
        
        success = self._kill_task(task_id, pid)
        
        return {
            'status': 'ok' if success else 'error',
            'task_id': task_id
        }
    
    def _handle_list_tasks(self, message: Dict, addr: Tuple) -> Dict:
        """Handle request to list all tasks."""
        with self.tasks_lock:
            tasks = [t.to_dict() for t in self.tasks.values()]
        
        return {
            'status': 'ok',
            'vm_id': self.vm_id,
            'tasks': tasks
        }
    
    def _handle_task_status(self, message: Dict, addr: Tuple) -> Dict:
        """Handle request for specific task status."""
        task_id = message.get('task_id')
        
        with self.tasks_lock:
            if task_id in self.tasks:
                return {
                    'status': 'ok',
                    'task': self.tasks[task_id].to_dict()
                }
        
        return {'status': 'error', 'message': 'Task not found'}
    
    def _handle_update_routing(self, message: Dict, addr: Tuple) -> Dict:
        """Handle routing update for a task."""
        task_id = message.get('task_id')
        downstream = message.get('downstream', [])
        
        # Forward to task's RPC
        with self.tasks_lock:
            if task_id not in self.tasks:
                return {'status': 'error', 'message': 'Task not found'}
            
            task = self.tasks[task_id]
        
        # Send routing update to task
        response = self.rpc_client.send(
            self.hostname, task.port,
            {'type': 'ROUTING_UPDATE', 'downstream': downstream}
        )
        
        return response or {'status': 'error', 'message': 'Task not responding'}
    
    def _handle_stop_all(self, message: Dict, addr: Tuple) -> Dict:
        """Handle request to stop all tasks."""
        self.logger.log("Stopping all tasks")
        
        with self.tasks_lock:
            for task_id in list(self.tasks.keys()):
                self._kill_task(task_id)
        
        return {'status': 'ok'}
    
    def _spawn_task(self, task_config: Dict) -> TaskProcess:
        """Spawn a new task process."""
        task_id = task_config['task_id']
        stage_num = task_config.get('stage_num', 0)
        op_exe = task_config.get('params', {}).get('op_exe', 'identity')
        
        # Allocate port
        port = self._allocate_port()
        task_config['port'] = port
        task_config['vm_id'] = self.vm_id
        
        # Create task config file
        config_file = f"/tmp/task_config_{task_id}.json"
        with open(config_file, 'w') as f:
            json.dump(task_config, f)
        
        # Spawn process
        cmd = [sys.executable, 'task.py', config_file]
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True  # New process group for kill -9
        )
        
        # Create log file path
        run_id = task_config.get('run_id', 'unknown')
        log_file = f"logs/task_{task_id}_vm{self.vm_id}_{run_id}.log"
        
        # Record task
        task_process = TaskProcess(
            task_id=task_id,
            stage_num=stage_num,
            pid=process.pid,
            port=port,
            op_exe=op_exe,
            log_file=log_file
        )
        task_process.process = process
        
        with self.tasks_lock:
            self.tasks[task_id] = task_process
        
        self.logger.log_event("TASK_SPAWNED", {
            'task_id': task_id,
            'pid': process.pid,
            'port': port,
            'op_exe': op_exe,
            'log_file': log_file
        })
        
        # Report to leader
        self._report_task_start(task_process)
        
        return task_process
    
    def _kill_task(self, task_id: str = None, pid: int = None) -> bool:
        """Kill a task process with kill -9."""
        target_pid = pid
        
        with self.tasks_lock:
            if task_id and task_id in self.tasks:
                target_pid = self.tasks[task_id].pid
        
        if not target_pid:
            self.logger.log(f"Cannot find task to kill: {task_id}")
            return False
        
        try:
            # Kill -9 (SIGKILL)
            os.kill(target_pid, signal.SIGKILL)
            self.logger.log_event("TASK_KILLED", {
                'task_id': task_id,
                'pid': target_pid
            })
            
            # Remove from tracking
            with self.tasks_lock:
                if task_id and task_id in self.tasks:
                    del self.tasks[task_id]
                else:
                    # Find by PID
                    for tid, task in list(self.tasks.items()):
                        if task.pid == target_pid:
                            del self.tasks[tid]
                            break
            
            # Report to leader
            self._report_task_failure(task_id or str(target_pid), target_pid)
            
            return True
        except OSError as e:
            self.logger.log(f"Kill error: {e}")
            return False
    
    def _allocate_port(self) -> int:
        """Allocate a port for a new task."""
        port = self.next_task_port
        self.next_task_port += 1
        return port
    
    def _report_task_start(self, task: TaskProcess):
        """Report task start to leader."""
        message = {
            'type': 'TASK_STARTED',
            'vm_id': self.vm_id,
            'task': task.to_dict()
        }
        
        self.rpc_client.send_async(
            self.leader_host, self.leader_port, message
        )
    
    def _report_task_failure(self, task_id: str, pid: int):
        """Report task failure to leader."""
        message = {
            'type': 'TASK_FAILED',
            'vm_id': self.vm_id,
            'task_id': task_id,
            'pid': pid
        }
        
        self.rpc_client.send_async(
            self.leader_host, self.leader_port, message
        )
    
    def _monitor_tasks(self):
        """Monitor running tasks and detect failures."""
        while self.running:
            time.sleep(1.0)
            
            with self.tasks_lock:
                for task_id, task in list(self.tasks.items()):
                    if not task.is_alive():
                        self.logger.log(f"Task {task_id} died unexpectedly")
                        self._report_task_failure(task_id, task.pid)
                        del self.tasks[task_id]
    
    def start(self):
        """Start the worker node."""
        self.running = True
        
        # Start RPC server
        self.rpc_server.start()
        
        # Start task monitor
        monitor_thread = threading.Thread(target=self._monitor_tasks, daemon=True)
        monitor_thread.start()
        
        self.logger.log_event("WORKER_START", {
            'vm_id': self.vm_id,
            'hostname': self.hostname,
            'rpc_port': self.rpc_port
        })
        
        # Register with leader
        self._register_with_leader()
    
    def stop(self):
        """Stop the worker node."""
        self.running = False
        
        # Stop all tasks
        with self.tasks_lock:
            for task_id in list(self.tasks.keys()):
                self._kill_task(task_id)
        
        # Stop RPC server
        self.rpc_server.stop()
        
        self.logger.log_event("WORKER_STOP", {'vm_id': self.vm_id})
    
    def _register_with_leader(self):
        """Register this worker with the leader."""
        message = {
            'type': 'WORKER_REGISTER',
            'vm_id': self.vm_id,
            'hostname': self.hostname,
            'rpc_port': self.rpc_port
        }
        
        response = self.rpc_client.send(
            self.leader_host, self.leader_port, message
        )
        
        if response and response.get('status') == 'ok':
            self.logger.log("Registered with leader")
        else:
            self.logger.log("Failed to register with leader")
    
    def run(self):
        """Run the worker (blocking)."""
        self.start()
        
        try:
            while self.running:
                time.sleep(1.0)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

# ============================================================================
# Worker Entry Point
# ============================================================================

def run_worker(vm_id: int, config_path: str = "config.json"):
    """Run worker node."""
    config = load_config(config_path)
    worker = Worker(vm_id, config)
    worker.run()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python worker.py <vm_id>")
        sys.exit(1)
    
    vm_id = int(sys.argv[1])
    run_worker(vm_id)