"""
RainStorm Autoscaling
- Rate monitoring per task
- LW/HW threshold comparison
- Scale up/down execution
- Partition remapping
"""
import time
import threading
from typing import Dict, List, Optional, Tuple, Callable
from collections import deque
from utils import RainStormLogger, hash_partition

# ============================================================================
# Rate Monitor
# ============================================================================

class RateMonitor:
    """
    Monitors tuple processing rate for a task.
    Reports rates to the Resource Manager.
    """
    def __init__(self, task_id: str, logger: RainStormLogger):
        self.task_id = task_id
        self.logger = logger
        
        # Sliding window for rate calculation
        self.window_size = 5  # seconds
        self.tuple_counts: deque = deque()  # (timestamp, count)
        self.lock = threading.Lock()
        
        # Current count in this second
        self.current_count = 0
        self.current_second = int(time.time())
        
        # Reporting
        self.running = False
        self.report_callback: Optional[Callable] = None
    
    def record_tuple(self):
        """Record that a tuple was processed."""
        now = int(time.time())
        
        with self.lock:
            if now == self.current_second:
                self.current_count += 1
            else:
                # New second - save old count and reset
                if self.current_count > 0:
                    self.tuple_counts.append((self.current_second, self.current_count))
                    # Keep only recent data
                    while self.tuple_counts and self.tuple_counts[0][0] < now - self.window_size:
                        self.tuple_counts.popleft()
                
                self.current_second = now
                self.current_count = 1
    
    def get_rate(self) -> float:
        """Get current tuples/second rate (averaged over window)."""
        now = int(time.time())
        
        with self.lock:
            # Add current count to window
            total = self.current_count
            count_seconds = 1
            
            for ts, cnt in self.tuple_counts:
                if ts >= now - self.window_size:
                    total += cnt
                    count_seconds += 1
            
            if count_seconds > 0:
                return total / count_seconds
            return 0.0
    
    def start_reporting(self, callback: Callable, interval: float = 1.0):
        """Start periodic rate reporting."""
        self.report_callback = callback
        self.running = True
        
        def report_loop():
            while self.running:
                try:
                    rate = self.get_rate()
                    if self.report_callback:
                        self.report_callback(self.task_id, rate)
                    self.logger.log_rate(self.task_id, rate)
                    time.sleep(interval)
                except Exception as e:
                    self.logger.log(f"Rate report error: {e}")
        
        thread = threading.Thread(target=report_loop, daemon=True)
        thread.start()
    
    def stop_reporting(self):
        """Stop periodic rate reporting."""
        self.running = False

# ============================================================================
# Resource Manager (runs on leader)
# ============================================================================

class ResourceManager:
    """
    Resource Manager that monitors rates and makes scaling decisions.
    Runs on the leader node.
    """
    def __init__(self, logger: RainStormLogger, 
                 low_watermark: float, high_watermark: float,
                 decision_interval: float = 5.0):
        self.logger = logger
        self.low_watermark = low_watermark  # LW tuples/sec per task
        self.high_watermark = high_watermark  # HW tuples/sec per task
        self.decision_interval = decision_interval  # seconds between decisions
        
        # Stage info: stage_num -> list of task_ids
        self.stages: Dict[int, List[str]] = {}
        
        # Task rates: task_id -> current rate
        self.task_rates: Dict[str, float] = {}
        self.rates_lock = threading.Lock()
        
        # Stage rate history for smoothing
        self.stage_rate_history: Dict[int, deque] = {}
        self.history_window = 3  # average over last N readings
        
        # Scaling callbacks
        self.scale_up_callback: Optional[Callable] = None
        self.scale_down_callback: Optional[Callable] = None
        
        # Control
        self.running = False
        self.enabled = True  # Can be disabled
        
        # Last scale time per stage (prevent rapid scaling)
        self.last_scale_time: Dict[int, float] = {}
        self.min_scale_interval = 5.0  # Minimum seconds between scale events
    
    def register_stage(self, stage_num: int, task_ids: List[str]):
        """Register a stage and its tasks."""
        self.stages[stage_num] = task_ids.copy()
        self.stage_rate_history[stage_num] = deque(maxlen=self.history_window)
        self.logger.log(f"Registered stage {stage_num} with {len(task_ids)} tasks")
    
    def update_stage_tasks(self, stage_num: int, task_ids: List[str]):
        """Update task list for a stage after scaling."""
        self.stages[stage_num] = task_ids.copy()
    
    def report_rate(self, task_id: str, rate: float):
        """Receive rate report from a task."""
        with self.rates_lock:
            self.task_rates[task_id] = rate
    
    def set_scale_callbacks(self, scale_up: Callable, scale_down: Callable):
        """
        Set callbacks for scaling events.
        scale_up(stage_num) -> new_task_id or None
        scale_down(stage_num) -> removed_task_id or None
        """
        self.scale_up_callback = scale_up
        self.scale_down_callback = scale_down
    
    def start(self):
        """Start the resource manager monitoring loop."""
        self.running = True
        thread = threading.Thread(target=self._monitor_loop, daemon=True)
        thread.start()
        self.logger.log("Resource Manager started")
    
    def stop(self):
        """Stop the resource manager."""
        self.running = False
        self.logger.log("Resource Manager stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop."""
        while self.running:
            try:
                time.sleep(self.decision_interval)
                
                if not self.enabled:
                    continue
                
                self._check_and_scale()
            except Exception as e:
                self.logger.log(f"Resource Manager error: {e}")
    
    def _check_and_scale(self):
        """Check rates and make scaling decisions."""
        for stage_num, task_ids in self.stages.items():
            if not task_ids:
                continue
            
            # Skip stateful stages (aggregation) - don't autoscale them
            # This would be indicated by a flag, but for simplicity we assume
            # stages can be scaled unless explicitly marked
            
            # Calculate average rate per task for this stage
            avg_rate = self._get_stage_avg_rate(stage_num, task_ids)
            
            # Add to history for smoothing
            self.stage_rate_history[stage_num].append(avg_rate)
            
            # Get smoothed rate
            smoothed_rate = self._get_smoothed_rate(stage_num)
            
            self.logger.log(f"Stage {stage_num}: avg_rate={avg_rate:.2f}, "
                          f"smoothed={smoothed_rate:.2f}, "
                          f"LW={self.low_watermark}, HW={self.high_watermark}")
            
            # Check if we should scale
            now = time.time()
            last_scale = self.last_scale_time.get(stage_num, 0)
            
            if now - last_scale < self.min_scale_interval:
                continue  # Too soon since last scale
            
            if smoothed_rate > self.high_watermark:
                # Scale up
                self.logger.log_event("AUTOSCALE_UP", {
                    'stage': stage_num,
                    'rate': smoothed_rate,
                    'threshold': self.high_watermark
                })
                if self.scale_up_callback:
                    new_task = self.scale_up_callback(stage_num)
                    if new_task:
                        self.last_scale_time[stage_num] = now
            
            elif smoothed_rate < self.low_watermark and len(task_ids) > 1:
                # Scale down (but keep at least 1 task)
                self.logger.log_event("AUTOSCALE_DOWN", {
                    'stage': stage_num,
                    'rate': smoothed_rate,
                    'threshold': self.low_watermark
                })
                if self.scale_down_callback:
                    removed_task = self.scale_down_callback(stage_num)
                    if removed_task:
                        self.last_scale_time[stage_num] = now
    
    def _get_stage_avg_rate(self, stage_num: int, task_ids: List[str]) -> float:
        """Calculate average rate per task for a stage."""
        with self.rates_lock:
            rates = [self.task_rates.get(tid, 0.0) for tid in task_ids]
        
        if not rates:
            return 0.0
        
        return sum(rates) / len(rates)
    
    def _get_smoothed_rate(self, stage_num: int) -> float:
        """Get smoothed rate from history."""
        history = self.stage_rate_history.get(stage_num, deque())
        if not history:
            return 0.0
        return sum(history) / len(history)
    
    def get_stats(self) -> Dict:
        """Get current autoscaling statistics."""
        stats = {
            'enabled': self.enabled,
            'low_watermark': self.low_watermark,
            'high_watermark': self.high_watermark,
            'stages': {}
        }
        
        for stage_num, task_ids in self.stages.items():
            with self.rates_lock:
                rates = {tid: self.task_rates.get(tid, 0.0) for tid in task_ids}
            
            stats['stages'][stage_num] = {
                'num_tasks': len(task_ids),
                'task_rates': rates,
                'avg_rate': sum(rates.values()) / len(rates) if rates else 0.0
            }
        
        return stats

# ============================================================================
# Partition Manager
# ============================================================================

class PartitionManager:
    """
    Manages stream partitioning between stages.
    Updates routing when tasks are added/removed.
    """
    def __init__(self, logger: RainStormLogger):
        self.logger = logger
        
        # Stage routing: stage_num -> list of (task_id, host, port)
        self.stage_routing: Dict[int, List[Tuple[str, str, int]]] = {}
        self.routing_lock = threading.Lock()
        
        # Partition function per stage (default: hash on key)
        self.partition_funcs: Dict[int, Callable] = {}
    
    def set_stage_routing(self, stage_num: int, 
                         tasks: List[Tuple[str, str, int]]):
        """Set routing for a stage."""
        with self.routing_lock:
            self.stage_routing[stage_num] = tasks.copy()
        self.logger.log(f"Set routing for stage {stage_num}: {len(tasks)} tasks")
    
    def add_task_to_stage(self, stage_num: int, task_id: str, 
                         host: str, port: int):
        """Add a task to a stage's routing."""
        with self.routing_lock:
            if stage_num not in self.stage_routing:
                self.stage_routing[stage_num] = []
            self.stage_routing[stage_num].append((task_id, host, port))
        self.logger.log(f"Added task {task_id} to stage {stage_num}")
    
    def remove_task_from_stage(self, stage_num: int, task_id: str):
        """Remove a task from a stage's routing."""
        with self.routing_lock:
            if stage_num in self.stage_routing:
                self.stage_routing[stage_num] = [
                    t for t in self.stage_routing[stage_num] 
                    if t[0] != task_id
                ]
        self.logger.log(f"Removed task {task_id} from stage {stage_num}")
    
    def get_target_for_tuple(self, stage_num: int, key: str) -> Optional[Tuple[str, str, int]]:
        """
        Get the target task for a tuple based on its key.
        Returns (task_id, host, port) or None.
        """
        with self.routing_lock:
            tasks = self.stage_routing.get(stage_num, [])
            if not tasks:
                return None
            
            # Use partition function if set, otherwise hash
            if stage_num in self.partition_funcs:
                partition = self.partition_funcs[stage_num](key, len(tasks))
            else:
                partition = hash_partition(key, len(tasks))
            
            return tasks[partition]
    
    def get_all_targets(self, stage_num: int) -> List[Tuple[str, str, int]]:
        """Get all tasks for a stage."""
        with self.routing_lock:
            return self.stage_routing.get(stage_num, []).copy()
    
    def set_partition_function(self, stage_num: int, func: Callable):
        """
        Set custom partition function for a stage.
        func(key: str, num_partitions: int) -> int
        """
        self.partition_funcs[stage_num] = func
    
    def get_num_tasks(self, stage_num: int) -> int:
        """Get number of tasks in a stage."""
        with self.routing_lock:
            return len(self.stage_routing.get(stage_num, []))

# ============================================================================
# Input Rate Controller (for source task)
# ============================================================================

class InputRateController:
    """
    Controls the input rate at the source.
    Ensures tuples are emitted at the specified INPUT_RATE.
    """
    def __init__(self, target_rate: float, logger: RainStormLogger):
        self.target_rate = target_rate  # tuples per second
        self.logger = logger
        
        # Timing
        self.interval = 1.0 / target_rate if target_rate > 0 else 0
        self.last_emit_time = 0
        
        # Stats
        self.emitted_count = 0
        self.start_time = time.time()
    
    def wait_for_next(self):
        """Wait until it's time to emit the next tuple."""
        if self.interval <= 0:
            return
        
        now = time.time()
        elapsed = now - self.last_emit_time
        
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        
        self.last_emit_time = time.time()
    
    def record_emit(self):
        """Record that a tuple was emitted."""
        self.emitted_count += 1
    
    def get_actual_rate(self) -> float:
        """Get actual emission rate."""
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            return self.emitted_count / elapsed
        return 0.0
    
    def set_rate(self, new_rate: float):
        """Update target rate."""
        self.target_rate = new_rate
        self.interval = 1.0 / new_rate if new_rate > 0 else 0
        self.logger.log(f"Input rate updated to {new_rate} tuples/sec")