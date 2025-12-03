"""
Worker Node - Executes tasks assigned by the leader
"""
import socket
import threading
import time
from typing import Dict, List, Optional
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task import Task
from network_manager import NetworkManager
from MP2.membership import MembershipService

WORKER_PORT = 8501

class Worker:
    """
    Worker node that executes tasks.
    Responsibilities:
    - Receive task assignments from leader
    - Execute tasks
    - Forward tuples between stages
    - Report metrics to leader
    - Handle task failures and restarts
    """
    
    def __init__(self, vm_id: int, config: dict, logger):
        self.vm_id = vm_id
        self.config = config
        self.logger = logger
        self.hostname = socket.gethostname()
        
        # Components
        self.membership = MembershipService(vm_id)
        self.network = NetworkManager(WORKER_PORT, logger)
        
        # Task management
        self.tasks: Dict[str, Task] = {}  # task_id -> Task
        self.running = False
        
        # Leader connection
        self.leader_hostname = None
        
        # Register handlers
        self._register_handlers()
    
    def _register_handlers(self):
        """Register message handlers."""
        self.network.register_handler('START_TASK', self._handle_start_task)
        self.network.register_handler('STOP_TASK', self._handle_stop_task)
        self.network.register_handler('START_SOURCE', self._handle_start_source)
        self.network.register_handler('TUPLE', self._handle_tuple)
        self.network.register_handler('ACK', self._handle_ack)
    
    def start(self):
        """Start the worker node."""
        self.logger.log(f"Worker VM{self.vm_id} starting...")
        self.running = True
        
        # Start membership
        self.membership.start()
        self.membership.join_group()
        time.sleep(2)
        
        # Find leader
        self._find_leader()
        
        # Start network
        self.network.start()
        
        # Notify leader
        self._notify_leader_ready()
        
        # Start monitoring
        threading.Thread(target=self._monitor_tasks, daemon=True).start()
        threading.Thread(target=self._send_heartbeat, daemon=True).start()
        
        self.logger.log(f"Worker VM{self.vm_id} started successfully")
    
    def stop(self):
        """Stop the worker node."""
        self.logger.log(f"Worker VM{self.vm_id} stopping...")
        self.running = False
        
        # Stop all tasks
        for task in self.tasks.values():
            task.stop()
        
        self.membership.leave_group()
        self.membership.stop()
        self.network.stop()
        
        self.logger.log(f"Worker VM{self.vm_id} stopped")
    
    def _find_leader(self):
        """Find the leader node from membership."""
        # Leader is always on VM1 (fa25-cs425-a701)
        self.leader_hostname = "fa25-cs425-a701.cs.illinois.edu"
        self.logger.log(f"Leader found: {self.leader_hostname}")
    
    def _notify_leader_ready(self):
        """Notify leader that worker is ready."""
        message = {
            'type': 'WORKER_READY',
            'worker_id': self.vm_id,
            'hostname': self.hostname
        }
        
        try:
            self.network.send_message(self.leader_hostname, 8500, message)
        except Exception as e:
            self.logger.log(f"Error notifying leader: {e}")
    
    def _send_heartbeat(self):
        """Send periodic heartbeat to leader."""
        while self.running:
            try:
                time.sleep(5.0)
                
                message = {
                    'type': 'HEARTBEAT',
                    'worker_id': self.vm_id,
                    'hostname': self.hostname,
                    'ntasks': len(self.tasks)
                }
                
                self.network.send_message(self.leader_hostname, 8500, message)
            
            except Exception as e:
                self.logger.log(f"Error sending heartbeat: {e}")
    
    def _monitor_tasks(self):
        """Monitor task health and report to leader."""
        while self.running:
            try:
                time.sleep(10.0)
                
                for task_id, task in self.tasks.items():
                    if not task.is_running():
                        self.logger.log(f"Task {task_id} stopped unexpectedly")
                        
                        # Report to leader
                        message = {
                            'type': 'TASK_STATUS',
                            'task_id': task_id,
                            'status': 'failed',
                            'worker_id': self.vm_id
                        }
                        
                        self.network.send_message(self.leader_hostname, 8500, message)
            
            except Exception as e:
                self.logger.log(f"Error monitoring tasks: {e}")
    
    def _handle_start_task(self, message: Dict, addr: tuple) -> Dict:
        """Handle START_TASK command from leader."""
        task_config = message.get('task_config')
        task_id = task_config['task_id']
        
        self.logger.log(f"Starting task {task_id}")
        
        try:
            # Create and start task
            task = Task(task_config, self, self.logger)
            task.start()
            
            self.tasks[task_id] = task
            
            return {'status': 'success', 'task_id': task_id}
        
        except Exception as e:
            self.logger.log(f"Error starting task {task_id}: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def _handle_stop_task(self, message: Dict, addr: tuple) -> Dict:
        """Handle STOP_TASK command from leader."""
        task_id = message.get('task_id')
        
        self.logger.log(f"Stopping task {task_id}")
        
        if task_id in self.tasks:
            self.tasks[task_id].stop()
            del self.tasks[task_id]
            return {'status': 'success'}
        else:
            return {'status': 'error', 'message': 'Task not found'}
    
    def _handle_start_source(self, message: Dict, addr: tuple) -> Dict:
        """Handle START_SOURCE command to start source process."""
        from source import SourceProcess
        
        self.logger.log("Starting source process")
        
        source = SourceProcess(
            vm_id=self.vm_id,
            config=self.config,
            logger=self.logger,
            hydfs_directory=message['hydfs_directory'],
            input_rate=message['input_rate']
        )
        
        # Store source as special task
        self.tasks['source'] = source
        source.start()
        
        return {'status': 'success'}
    
    def _handle_tuple(self, message: Dict, addr: tuple) -> Dict:
        """Handle incoming tuple from another task."""
        target_task_id = message.get('target_task_id')
        
        if target_task_id in self.tasks:
            self.tasks[target_task_id].receive_tuple(message)
            return {'status': 'success'}
        else:
            return {'status': 'error', 'message': 'Task not found'}
    
    def _handle_ack(self, message: Dict, addr: tuple) -> Dict:
        """Handle ACK from downstream task."""
        task_id = message.get('task_id')
        
        if task_id in self.tasks:
            self.tasks[task_id].receive_ack(message)
            return {'status': 'success'}
        else:
            return {'status': 'error', 'message': 'Task not found'}