"""
Scheduler - Assigns tasks to workers and handles rescheduling
"""
import time
from typing import Dict, List, Optional, Set
import random

class Scheduler:
    """
    Schedules tasks across available workers.
    Uses simple round-robin scheduling with load balancing.
    """
    
    def __init__(self, leader, logger):
        self.leader = leader
        self.logger = logger
        
        # Tracking
        self.task_assignments: Dict[str, str] = {}  # task_id -> worker_hostname
        self.worker_loads: Dict[str, int] = {}  # worker_hostname -> task_count
        self.task_status: Dict[str, str] = {}  # task_id -> status
        self.failed_tasks: Set[str] = set()
    
    def schedule_topology(self, topology: Dict):
        """
        Schedule all tasks in the topology across workers.
        """
        self.logger.log("Scheduling topology...")
        
        workers = self.leader.get_available_workers()
        if not workers:
            self.logger.log("ERROR: No workers available")
            return
        
        # Initialize worker loads
        for worker in workers:
            self.worker_loads[worker] = 0
        
        # Schedule tasks stage by stage
        for stage in topology['stages']:
            stage_id = stage['stage_id']
            tasks = stage['tasks']
            operator = stage['operator']
            
            self.logger.log(f"Scheduling stage {stage_id} with {len(tasks)} tasks")
            
            for task_info in tasks:
                task_id = task_info['task_id']
                
                # Find least loaded worker
                worker = self._get_least_loaded_worker(workers)
                
                # Get downstream tasks for this task
                downstream_tasks = self._get_downstream_tasks(
                    topology, stage_id, task_info['task_index']
                )
                
                # Create task config
                task_config = {
                    'task_id': task_id,
                    'stage_id': stage_id,
                    'task_index': task_info['task_index'],
                    'operator': operator,
                    'downstream_tasks': downstream_tasks,
                    'exactly_once': topology.get('exactly_once', False)
                }
                
                # Send task to worker
                self._assign_task_to_worker(worker, task_config)
                
                # Update tracking
                self.task_assignments[task_id] = worker
                self.worker_loads[worker] += 1
                self.task_status[task_id] = 'running'
        
        self.logger.log(f"Scheduled {len(self.task_assignments)} tasks across {len(workers)} workers")
    
    def _get_least_loaded_worker(self, workers: List[str]) -> str:
        """Get the worker with the least number of tasks."""
        return min(workers, key=lambda w: self.worker_loads.get(w, 0))
    
    def _get_downstream_tasks(self, topology: Dict, stage_id: int, task_index: int) -> List[Dict]:
        """
        Get downstream tasks for a given task.
        Returns list of task configs with worker hostnames.
        """
        if stage_id >= topology['nstages'] - 1:
            # Last stage has no downstream
            return []
        
        next_stage = topology['stages'][stage_id + 1]
        downstream = []
        
        for task in next_stage['tasks']:
            task_id = task['task_id']
            worker = self.task_assignments.get(task_id)
            
            if worker:
                downstream.append({
                    'task_id': task_id,
                    'worker_hostname': worker
                })
        
        return downstream
    
    def _assign_task_to_worker(self, worker: str, task_config: Dict):
        """Send task assignment to worker."""
        message = {
            'type': 'START_TASK',
            'task_config': task_config
        }
        
        try:
            response = self.leader.network.send_message(worker, 8501, message)
            
            if response and response.get('status') == 'success':
                self.logger.log(f"Assigned task {task_config['task_id']} to {worker}")
            else:
                self.logger.log(f"Failed to assign task {task_config['task_id']} to {worker}")
        
        except Exception as e:
            self.logger.log(f"Error assigning task to {worker}: {e}")
    
    def reschedule_task(self, task_id: str):
        """Reschedule a failed task to a different worker."""
        self.logger.log(f"Rescheduling task {task_id}")
        
        if task_id in self.failed_tasks:
            self.logger.log(f"Task {task_id} already marked as failed")
            return
        
        self.failed_tasks.add(task_id)
        
        # Get available workers
        workers = self.leader.get_available_workers()
        old_worker = self.task_assignments.get(task_id)
        
        # Remove old worker from list
        if old_worker in workers:
            workers.remove(old_worker)
        
        if not workers:
            self.logger.log(f"No workers available to reschedule {task_id}")
            return
        
        # Find new worker
        new_worker = self._get_least_loaded_worker(workers)
        
        # Update tracking (task config would need to be reconstructed)
        # For now, simplified
        self.logger.log(f"Rescheduled task {task_id} from {old_worker} to {new_worker}")
        self.task_assignments[task_id] = new_worker
        self.worker_loads[old_worker] -= 1
        self.worker_loads[new_worker] += 1
    
    def check_task_health(self):
        """Check health of all tasks."""
        for task_id, status in self.task_status.items():
            if status == 'failed' and task_id not in self.failed_tasks:
                self.reschedule_task(task_id)
    
    def add_task_to_stage(self, stage_id: int, operator: Dict, downstream_tasks: List[Dict]) -> str:
        """
        Add a new task to a stage (for autoscaling).
        Returns the new task_id.
        """
        # Generate new task ID
        existing_tasks = [tid for tid in self.task_assignments.keys() 
                         if tid.startswith(f"stage{stage_id}_")]
        task_index = len(existing_tasks)
        task_id = f"stage{stage_id}_task{task_index}"
        
        # Find worker
        workers = self.leader.get_available_workers()
        worker = self._get_least_loaded_worker(workers)
        
        # Create task config
        task_config = {
            'task_id': task_id,
            'stage_id': stage_id,
            'task_index': task_index,
            'operator': operator,
            'downstream_tasks': downstream_tasks,
            'exactly_once': False  # Disabled for autoscaling
        }
        
        # Assign to worker
        self._assign_task_to_worker(worker, task_config)
        
        # Update tracking
        self.task_assignments[task_id] = worker
        self.worker_loads[worker] += 1
        self.task_status[task_id] = 'running'
        
        self.logger.log(f"Added task {task_id} to stage {stage_id} on {worker}")
        
        return task_id
    
    def remove_task(self, task_id: str):
        """Remove a task (for autoscaling)."""
        worker = self.task_assignments.get(task_id)
        
        if not worker:
            return
        
        # Send stop command
        message = {
            'type': 'STOP_TASK',
            'task_id': task_id
        }
        
        try:
            self.leader.network.send_message(worker, 8501, message)
            
            # Update tracking
            del self.task_assignments[task_id]
            del self.task_status[task_id]
            self.worker_loads[worker] -= 1
            
            self.logger.log(f"Removed task {task_id} from {worker}")
        
        except Exception as e:
            self.logger.log(f"Error removing task {task_id}: {e}")