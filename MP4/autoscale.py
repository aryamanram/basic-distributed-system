"""
Autoscaling Coordinator - High-level autoscaling logic
"""

class AutoscaleCoordinator:
    """
    High-level coordinator for autoscaling decisions.
    Works with ResourceManager to implement scaling policies.
    """
    
    def __init__(self, leader, logger):
        self.leader = leader
        self.logger = logger
    
    def should_scale_up(self, stage_id: int, metrics: dict) -> bool:
        """
        Determine if a stage should scale up.
        
        metrics contains:
        - average_throughput
        - per_task_throughput
        - high_watermark
        """
        per_task = metrics.get('per_task_throughput', 0)
        hw = metrics.get('high_watermark', float('inf'))
        
        return per_task > hw
    
    def should_scale_down(self, stage_id: int, metrics: dict) -> bool:
        """
        Determine if a stage should scale down.
        
        metrics contains:
        - average_throughput
        - per_task_throughput
        - low_watermark
        - current_task_count
        """
        per_task = metrics.get('per_task_throughput', 0)
        lw = metrics.get('low_watermark', 0)
        current_tasks = metrics.get('current_task_count', 1)
        
        # Don't scale below 1 task
        if current_tasks <= 1:
            return False
        
        return per_task < lw
    
    def get_scale_delta(self, stage_id: int, metrics: dict) -> int:
        """
        Get the number of tasks to add/remove.
        Positive for scale up, negative for scale down.
        """
        # For now, always scale by 1
        if self.should_scale_up(stage_id, metrics):
            return 1
        elif self.should_scale_down(stage_id, metrics):
            return -1
        else:
            return 0