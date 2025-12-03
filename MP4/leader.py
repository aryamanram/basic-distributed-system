"""
Leader Node - Coordinates task scheduling, failure handling, and resource management
"""
import socket
import threading
import time
from typing import Dict, List, Optional, Set
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from MP2.membership import MembershipService
from scheduler import Scheduler
from resource_manager import ResourceManager
from network_manager import NetworkManager

LEADER_PORT = 8500

class Leader:
    """
    Leader node that coordinates the RainStorm cluster.
    Responsibilities:
    - Receive job submissions
    - Schedule tasks on workers
    - Handle worker failures
    - Coordinate autoscaling (via ResourceManager)
    - Maintain topology information
    """
    
    def __init__(self, config: dict, logger):
        self.config = config
        self.logger = logger
        self.hostname = socket.gethostname()
        
        # Initialize components
        self.membership = MembershipService(vm_id=1)  # Leader is always VM1
        self.scheduler = Scheduler(self, logger)
        self.resource_manager = ResourceManager(self, logger)
        self.network = NetworkManager(LEADER_PORT, logger)
        
        # Job state
        self.current_job: Optional[Dict] = None
        self.task_assignments: Dict[str, str] = {}  # task_id -> worker_hostname
        self.running = False
        
        # Register network handlers
        self._register_handlers()
    
    def _register_handlers(self):
        """Register message handlers."""
        self.network.register_handler('HEARTBEAT', self._handle_heartbeat)
        self.network.register_handler('TASK_STATUS', self._handle_task_status)
        self.network.register_handler('TASK_METRICS', self._handle_task_metrics)
        self.network.register_handler('WORKER_READY', self._handle_worker_ready)
    
    def start(self):
        """Start the leader node."""
        self.logger.log("Leader starting...")
        self.running = True
        
        # Start membership
        self.membership.start()
        self.membership.join_group()
        time.sleep(2)  # Wait for membership to stabilize
        
        # Start network
        self.network.start()
        
        # Start monitoring threads
        threading.Thread(target=self._monitor_workers, daemon=True).start()
        threading.Thread(target=self._monitor_tasks, daemon=True).start()
        
        self.logger.log("Leader started successfully")
    
    def stop(self):
        """Stop the leader node."""
        self.logger.log("Leader stopping...")
        self.running = False
        self.membership.leave_group()
        self.membership.stop()
        self.network.stop()
        self.logger.log("Leader stopped")
    
    def submit_job(self, job_config: Dict):
        """
        Submit a new job to the cluster.
        
        job_config contains:
        - nstages: number of stages
        - ntasks_per_stage: tasks per stage
        - operators: list of operator configs
        - hydfs_src_directory: input directory
        - hydfs_dest_filename: output file
        - exactly_once: enable exactly-once semantics
        - autoscale_enabled: enable autoscaling
        - input_rate, lw, hw: autoscaling parameters
        """
        self.logger.log(f"Submitting job: {job_config['nstages']} stages, "
                       f"{job_config['ntasks_per_stage']} tasks/stage")
        
        self.current_job = job_config
        
        # Create topology
        topology = self._create_topology(job_config)
        
        # Schedule tasks
        self.scheduler.schedule_topology(topology)
        
        # Start source process
        self._start_source_process(job_config)
        
        # Start resource manager if autoscaling enabled
        if job_config.get('autoscale_enabled'):
            self.resource_manager.start(job_config)
        
        self.logger.log("Job submitted successfully")
    
    def _create_topology(self, job_config: Dict) -> Dict:
        """
        Create topology from job configuration.
        
        Returns topology dict with:
        - stages: list of stage configs
        - partitioning: hash function for each stage
        """
        topology = {
            'nstages': job_config['nstages'],
            'stages': []
        }
        
        for stage_idx in range(job_config['nstages']):
            stage = {
                'stage_id': stage_idx,
                'ntasks': job_config['ntasks_per_stage'],
                'operator': job_config['operators'][stage_idx],
                'tasks': []
            }
            
            # Create task IDs
            for task_idx in range(job_config['ntasks_per_stage']):
                task_id = f"stage{stage_idx}_task{task_idx}"
                stage['tasks'].append({
                    'task_id': task_id,
                    'stage_id': stage_idx,
                    'task_index': task_idx
                })
            
            topology['stages'].append(stage)
        
        return topology
    
    def _start_source_process(self, job_config: Dict):
        """Start the source process on a worker."""
        # For simplicity, start source on VM2 (first worker)
        source_vm = 2
        
        message = {
            'type': 'START_SOURCE',
            'hydfs_directory': job_config['hydfs_src_directory'],
            'input_rate': job_config.get('input_rate', 100.0),
            'target_stage': 0,  # Source feeds into stage 0
            'ntasks': job_config['ntasks_per_stage']
        }
        
        self.network.send_to_worker(source_vm, message)
        self.logger.log(f"Started source process on VM{source_vm}")
    
    def _monitor_workers(self):
        """Monitor worker health using MP2 membership."""
        known_workers = set()
        
        while self.running:
            try:
                time.sleep(2.0)
                
                # Get current membership
                members = self.membership.membership.get_members()
                current_workers = set()
                
                for hostname, info in members.items():
                    if info['status'] == 'ACTIVE' and hostname != self.hostname:
                        current_workers.add(hostname)
                
                # Detect failed workers
                failed = known_workers - current_workers
                for worker in failed:
                    self.logger.log(f"Worker {worker} failed, rescheduling tasks")
                    self._handle_worker_failure(worker)
                
                # Detect new workers
                new_workers = current_workers - known_workers
                for worker in new_workers:
                    self.logger.log(f"New worker {worker} joined")
                
                known_workers = current_workers
            
            except Exception as e:
                self.logger.log(f"Error in worker monitoring: {e}")
    
    def _monitor_tasks(self):
        """Monitor task health and restart failed tasks."""
        while self.running:
            try:
                time.sleep(5.0)
                
                # Check task health (implemented by scheduler)
                self.scheduler.check_task_health()
            
            except Exception as e:
                self.logger.log(f"Error in task monitoring: {e}")
    
    def _handle_worker_failure(self, worker_hostname: str):
        """Handle worker failure by rescheduling its tasks."""
        failed_tasks = [task_id for task_id, hostname in self.task_assignments.items()
                       if hostname == worker_hostname]
        
        for task_id in failed_tasks:
            self.logger.log(f"Rescheduling task {task_id} after worker failure")
            self.scheduler.reschedule_task(task_id)
    
    def _handle_heartbeat(self, message: Dict, addr: tuple) -> Dict:
        """Handle heartbeat from worker."""
        return {'status': 'ok'}
    
    def _handle_task_status(self, message: Dict, addr: tuple) -> Dict:
        """Handle task status update."""
        task_id = message.get('task_id')
        status = message.get('status')
        
        self.logger.log(f"Task {task_id} status: {status}")
        
        return {'status': 'ok'}
    
    def _handle_task_metrics(self, message: Dict, addr: tuple) -> Dict:
        """Handle task metrics (for autoscaling)."""
        if self.current_job and self.current_job.get('autoscale_enabled'):
            self.resource_manager.update_metrics(message)
        
        return {'status': 'ok'}
    
    def _handle_worker_ready(self, message: Dict, addr: tuple) -> Dict:
        """Handle worker ready notification."""
        worker_id = message.get('worker_id')
        self.logger.log(f"Worker {worker_id} ready")
        
        return {'status': 'ok'}
    
    def get_available_workers(self) -> List[str]:
        """Get list of available worker hostnames."""
        members = self.membership.membership.get_members()
        workers = []
        
        for hostname, info in members.items():
            if info['status'] == 'ACTIVE' and hostname != self.hostname:
                workers.append(hostname)
        
        return workers