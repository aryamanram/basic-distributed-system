"""
Resource Manager - Manages autoscaling based on load metrics
"""
import time
import threading
from typing import Dict, List, Optional

class ResourceManager:
    """
    Monitors task loads and triggers autoscaling decisions.
    """
    
    def __init__(self, leader, logger):
        self.leader = leader
        self.logger = logger
        
        # Autoscaling config
        self.enabled = False
        self.input_rate = 0.0
        self.low_watermark = 0.0  # tuples/sec per task
        self.high_watermark = 0.0  # tuples/sec per task
        self.check_interval = 5.0  # seconds
        
        # Metrics tracking
        self.stage_metrics: Dict[int, List[float]] = {}  # stage_id -> [throughputs]
        
        # State
        self.running = False
        self.monitor_thread: Optional[threading.Thread] = None
        
        # Cooldown to prevent thrashing
        self.last_scale_time: Dict[int, float] = {}  # stage_id -> timestamp
        self.scale_cooldown = 10.0  # seconds
    
    def start(self, job_config: Dict):
        """Start the resource manager with job config."""
        if not job_config.get('autoscale_enabled'):
            self.logger.log("Autoscaling disabled")
            return
        
        self.enabled = True
        self.input_rate = job_config.get('input_rate', 100.0)
        self.low_watermark = job_config.get('lw', 50.0)
        self.high_watermark = job_config.get('hw', 150.0)
        
        self.logger.log(f"Resource Manager starting: INPUT_RATE={self.input_rate}, "
                       f"LW={self.low_watermark}, HW={self.high_watermark}")
        
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        self.logger.log("Resource Manager started")
    
    def stop(self):
        """Stop the resource manager."""
        if not self.enabled:
            return
        
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2.0)
        
        self.logger.log("Resource Manager stopped")
    
    def update_metrics(self, message: Dict):
        """
        Update metrics from a task.
        
        message contains:
        - task_id
        - stage_id
        - throughput (tuples/sec)
        - tuples_processed
        """
        stage_id = message.get('stage_id')
        throughput = message.get('throughput', 0.0)
        
        if stage_id not in self.stage_metrics:
            self.stage_metrics[stage_id] = []
        
        self.stage_metrics[stage_id].append(throughput)
        
        # Keep only recent metrics (last 10)
        if len(self.stage_metrics[stage_id]) > 10:
            self.stage_metrics[stage_id] = self.stage_metrics[stage_id][-10:]
    
    def _monitor_loop(self):
        """Monitor metrics and trigger scaling decisions."""
        while self.running:
            try:
                time.sleep(self.check_interval)
                
                # Check each stage
                for stage_id in self.stage_metrics.keys():
                    self._check_stage_scaling(stage_id)
            
            except Exception as e:
                self.logger.log(f"Error in resource manager monitor: {e}")
    
    def _check_stage_scaling(self, stage_id: int):
        """Check if a stage needs scaling."""
        metrics = self.stage_metrics.get(stage_id, [])
        
        if not metrics:
            return
        
        # Calculate average throughput per task
        avg_throughput = sum(metrics) / len(metrics)
        
        # Get number of tasks in stage
        ntasks = self._get_stage_task_count(stage_id)
        
        if ntasks == 0:
            return
        
        # Calculate per-task throughput
        per_task_throughput = avg_throughput / ntasks
        
        self.logger.log(f"Stage {stage_id}: avg_throughput={avg_throughput:.2f}, "
                       f"ntasks={ntasks}, per_task={per_task_throughput:.2f}")
        
        # Check cooldown
        now = time.time()
        last_scale = self.last_scale_time.get(stage_id, 0)
        if now - last_scale < self.scale_cooldown:
            return
        
        # Scale up if above high watermark
        if per_task_throughput > self.high_watermark:
            self.logger.log(f"Stage {stage_id} above HW, scaling UP")
            self._scale_up_stage(stage_id)
            self.last_scale_time[stage_id] = now
        
        # Scale down if below low watermark (and more than 1 task)
        elif per_task_throughput < self.low_watermark and ntasks > 1:
            self.logger.log(f"Stage {stage_id} below LW, scaling DOWN")
            self._scale_down_stage(stage_id)
            self.last_scale_time[stage_id] = now
    
    def _get_stage_task_count(self, stage_id: int) -> int:
        """Get number of tasks in a stage."""
        count = 0
        for task_id in self.leader.scheduler.task_assignments.keys():
            if task_id.startswith(f"stage{stage_id}_"):
                count += 1
        return count
    
    def _scale_up_stage(self, stage_id: int):
        """Scale up a stage by adding one task."""
        # Get operator config for stage
        job = self.leader.current_job
        if not job or stage_id >= len(job['operators']):
            return
        
        operator = job['operators'][stage_id]
        
        # Get downstream tasks
        downstream_tasks = self._get_downstream_tasks_for_stage(stage_id)
        
        # Add task
        new_task_id = self.leader.scheduler.add_task_to_stage(
            stage_id, operator, downstream_tasks
        )
        
        self.logger.log(f"Scaled UP stage {stage_id}: added task {new_task_id}")
    
    def _scale_down_stage(self, stage_id: int):
        """Scale down a stage by removing one task."""
        # Find a task to remove
        tasks = [tid for tid in self.leader.scheduler.task_assignments.keys()
                if tid.startswith(f"stage{stage_id}_")]
        
        if not tasks:
            return
        
        # Remove last task
        task_to_remove = tasks[-1]
        self.leader.scheduler.remove_task(task_to_remove)
        
        self.logger.log(f"Scaled DOWN stage {stage_id}: removed task {task_to_remove}")
    
    def _get_downstream_tasks_for_stage(self, stage_id: int) -> List[Dict]:
        """Get downstream tasks for a stage."""
        job = self.leader.current_job
        if not job or stage_id >= job['nstages'] - 1:
            return []
        
        # Get tasks from next stage
        next_stage_id = stage_id + 1
        tasks = []
        
        for task_id, worker in self.leader.scheduler.task_assignments.items():
            if task_id.startswith(f"stage{next_stage_id}_"):
                tasks.append({
                    'task_id': task_id,
                    'worker_hostname': worker
                })
        
        return tasks