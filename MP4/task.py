"""
RainStorm Task Execution
- Source task (read from HyDFS/local, throttle INPUT_RATE)
- Processing task (invoke op_exe via subprocess)
- Sink behavior (write to HyDFS + console)
- Tuple partitioning to downstream tasks
- Task logging (tuples, duplicates)
"""
import os
import sys
import time
import subprocess
import threading
import signal
import json
from typing import Dict, List, Optional, Callable, Tuple
from utils import (
    StreamTuple, RainStormLogger, load_config, get_vm_by_id,
    make_task_id, get_task_port, parse_csv_line, serialize_message,
    deserialize_message, send_sized_message, recv_sized_message
)
from network import TupleChannel, RPCServer, RPCClient
from exactly_once import ExactlyOnceManager
from autoscale import RateMonitor, InputRateController, PartitionManager

# Add parent for MP3
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# ============================================================================
# Base Task
# ============================================================================

class Task:
    """
    Base class for RainStorm tasks.
    """
    def __init__(self, task_id: str, stage_num: int, vm_id: int,
                 port: int, exactly_once: bool, run_id: str):
        self.task_id = task_id
        self.stage_num = stage_num
        self.vm_id = vm_id
        self.port = port
        self.run_id = run_id
        
        # Logger (new file per invocation)
        self.logger = RainStormLogger(f"task_{task_id}", vm_id, run_id)
        
        # Network
        self.tuple_channel = TupleChannel(task_id, self.logger)
        self.rpc_server = RPCServer(port, self.logger)
        self.rpc_client = RPCClient(self.logger)
        
        # Exactly-once
        hydfs_state_file = f"rainstorm_state_{task_id}.log"
        self.eo_manager = ExactlyOnceManager(
            task_id, hydfs_state_file, self.logger, enabled=exactly_once
        )
        
        # Rate monitoring
        self.rate_monitor = RateMonitor(task_id, self.logger)
        
        # Downstream routing
        self.downstream_tasks: List[Tuple[str, str, int]] = []  # (task_id, host, port)
        
        # Control
        self.running = False
        self.processed_count = 0
        self.output_count = 0
        
        # Register RPC handlers
        self._register_handlers()
    
    def _register_handlers(self):
        """Register RPC handlers."""
        self.rpc_server.register_handler('ROUTING_UPDATE', self._handle_routing_update)
        self.rpc_server.register_handler('STOP', self._handle_stop)
        self.rpc_server.register_handler('STATUS', self._handle_status)
    
    def _handle_routing_update(self, message: Dict, addr: Tuple) -> Dict:
        """Handle routing table update from leader."""
        downstream = message.get('downstream', [])
        self.downstream_tasks = [(t['task_id'], t['host'], t['port']) for t in downstream]
        
        # Update tuple channel
        for task_id, host, port in self.downstream_tasks:
            self.tuple_channel.add_downstream(task_id, host, port)
        
        self.logger.log(f"Routing updated: {len(self.downstream_tasks)} downstream tasks")
        return {'status': 'ok'}
    
    def _handle_stop(self, message: Dict, addr: Tuple) -> Dict:
        """Handle stop command."""
        self.stop()
        return {'status': 'ok'}
    
    def _handle_status(self, message: Dict, addr: Tuple) -> Dict:
        """Handle status query."""
        return {
            'status': 'ok',
            'task_id': self.task_id,
            'stage': self.stage_num,
            'running': self.running,
            'processed': self.processed_count,
            'output': self.output_count,
            'rate': self.rate_monitor.get_rate()
        }
    
    def set_downstream(self, tasks: List[Tuple[str, str, int]]):
        """Set downstream tasks."""
        self.downstream_tasks = tasks
        for task_id, host, port in tasks:
            self.tuple_channel.add_downstream(task_id, host, port)
    
    def send_downstream(self, tuple_data: StreamTuple) -> bool:
        """Send tuple to appropriate downstream task based on key."""
        if not self.downstream_tasks:
            return False
        
        # Partition by key
        from utils import hash_partition
        partition = hash_partition(tuple_data.key, len(self.downstream_tasks))
        target_task_id, target_host, target_port = self.downstream_tasks[partition]
        
        # Assign output tuple ID
        tuple_data.tuple_id = self.eo_manager.generate_output_id()
        tuple_data.source_task = self.task_id
        
        # Send
        success = self.tuple_channel.send_tuple(
            tuple_data, target_task_id, 
            wait_ack=self.eo_manager.enabled
        )
        
        if success:
            self.eo_manager.on_output_sent(tuple_data)
            self.output_count += 1
            self.logger.log_tuple(tuple_data, "OUT")
        
        return success
    
    def start(self):
        """Start the task."""
        self.running = True
        
        # Start RPC server
        self.rpc_server.start()
        
        # Start tuple receiver
        tuple_port = self.port + 1000  # Separate port for tuples
        self.tuple_channel.start_receiver(tuple_port)
        
        # Start rate reporting
        self.rate_monitor.start_reporting(self._report_rate)
        
        # Recover state if exactly-once
        if self.eo_manager.enabled:
            unacked = self.eo_manager.recover_state()
            if unacked:
                self.logger.log(f"Resending {len(unacked)} unacked tuples")
                for t in unacked:
                    self.send_downstream(t)
        
        self.logger.log_event("TASK_START", {
            'task_id': self.task_id,
            'stage': self.stage_num,
            'vm': self.vm_id,
            'pid': os.getpid(),
            'port': self.port
        })
    
    def stop(self):
        """Stop the task."""
        self.running = False
        
        # Flush state
        self.eo_manager.flush_state()
        
        # Stop components
        self.rate_monitor.stop_reporting()
        self.tuple_channel.stop_receiver()
        self.rpc_server.stop()
        
        self.logger.log_event("TASK_STOP", {
            'task_id': self.task_id,
            'processed': self.processed_count,
            'output': self.output_count
        })
    
    def _report_rate(self, task_id: str, rate: float):
        """Report rate to leader (placeholder - actual impl in subclass)."""
        pass
    
    def run(self):
        """Main task loop - override in subclasses."""
        raise NotImplementedError

# ============================================================================
# Source Task
# ============================================================================

class SourceTask(Task):
    """
    Source task that reads from input file and produces tuples.
    """
    def __init__(self, task_id: str, vm_id: int, port: int,
                 input_file: str, input_rate: float, 
                 exactly_once: bool, run_id: str,
                 use_local_file: bool = True):
        super().__init__(task_id, 0, vm_id, port, exactly_once, run_id)
        
        self.input_file = input_file
        self.input_rate = input_rate
        self.use_local_file = use_local_file
        
        # Rate controller
        self.rate_controller = InputRateController(input_rate, self.logger)
        
        # Line tracking
        self.current_line = 0
        self.total_lines = 0
    
    def run(self):
        """Read input file and emit tuples."""
        self.start()
        self.logger.log(f"Source starting: file={self.input_file}, rate={self.input_rate}")
        
        try:
            # Open input file
            if self.use_local_file:
                file_handle = open(self.input_file, 'r')
            else:
                # Read from HyDFS - simplified for now
                file_handle = self._open_hydfs_file()
            
            if not file_handle:
                self.logger.log("Failed to open input file")
                return
            
            # Read and emit lines
            for line in file_handle:
                if not self.running:
                    break
                
                self.current_line += 1
                line = line.strip()
                
                if not line:
                    continue
                
                # Create tuple: key = filename:linenum, value = line
                key = f"{self.input_file}:{self.current_line}"
                tuple_data = StreamTuple(key=key, value=line)
                
                # Rate control
                self.rate_controller.wait_for_next()
                
                # Send to stage 1
                success = self.send_downstream(tuple_data)
                if success:
                    self.rate_controller.record_emit()
                    self.processed_count += 1
                    self.rate_monitor.record_tuple()
            
            file_handle.close()
            self.total_lines = self.current_line
            
            self.logger.log(f"Source completed: {self.total_lines} lines processed")
            
            # Wait for all acks if exactly-once
            if self.eo_manager.enabled:
                self._wait_for_acks()
        
        except Exception as e:
            self.logger.log(f"Source error: {e}")
        finally:
            self.stop()
    
    def _open_hydfs_file(self):
        """Open file from HyDFS."""
        try:
            # Would use HyDFS client here
            # For now, fall back to local
            return open(self.input_file, 'r')
        except:
            return None
    
    def _wait_for_acks(self, timeout: float = 30.0):
        """Wait for all output acks."""
        start = time.time()
        while time.time() - start < timeout:
            if not self.tuple_channel.has_pending_acks():
                break
            time.sleep(0.5)

# ============================================================================
# Processing Task
# ============================================================================

class ProcessingTask(Task):
    """
    Processing task that applies an operator to tuples.
    """
    def __init__(self, task_id: str, stage_num: int, vm_id: int,
                 port: int, op_exe: str, op_args: str,
                 exactly_once: bool, run_id: str,
                 is_stateful: bool = False):
        super().__init__(task_id, stage_num, vm_id, port, exactly_once, run_id)
        
        self.op_exe = op_exe
        self.op_args = op_args
        self.is_stateful = is_stateful
        
        # Operator state (for aggregation)
        self.state: Dict = {}
    
    def run(self):
        """Process incoming tuples."""
        self.start()
        self.logger.log(f"Processing task starting: op={self.op_exe}, args={self.op_args}")
        
        try:
            while self.running:
                # Receive tuple
                tuple_data = self.tuple_channel.receive_tuple(timeout=1.0)
                
                if tuple_data is None:
                    continue
                
                # Check for duplicate
                if not self.eo_manager.should_process(tuple_data):
                    continue
                
                # Process tuple
                output_tuples = self._process_tuple(tuple_data)
                
                # Mark as processed
                self.eo_manager.on_tuple_processed(tuple_data)
                self.processed_count += 1
                self.rate_monitor.record_tuple()
                
                # Send outputs downstream
                for out_tuple in output_tuples:
                    self.send_downstream(out_tuple)
        
        except Exception as e:
            self.logger.log(f"Processing error: {e}")
        finally:
            self.stop()
    
    def _process_tuple(self, tuple_data: StreamTuple) -> List[StreamTuple]:
        """
        Process a tuple using the operator.
        Returns list of output tuples (may be empty for filter, or multiple).
        """
        try:
            # Run operator as subprocess
            result = subprocess.run(
                [self.op_exe] + self.op_args.split(),
                input=f"{tuple_data.key}\t{tuple_data.value}\n",
                capture_output=True,
                text=True,
                timeout=10.0
            )
            
            output_tuples = []
            
            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    if not line:
                        continue
                    
                    # Parse output: key\tvalue
                    parts = line.split('\t', 1)
                    if len(parts) == 2:
                        out_tuple = StreamTuple(key=parts[0], value=parts[1])
                        output_tuples.append(out_tuple)
                    elif len(parts) == 1:
                        # Just value, use original key
                        out_tuple = StreamTuple(key=tuple_data.key, value=parts[0])
                        output_tuples.append(out_tuple)
            
            return output_tuples
        
        except subprocess.TimeoutExpired:
            self.logger.log(f"Operator timeout for tuple {tuple_data.tuple_id}")
            return []
        except Exception as e:
            self.logger.log(f"Operator error: {e}")
            return []

# ============================================================================
# Sink Task (Final Stage)
# ============================================================================

class SinkTask(Task):
    """
    Sink task that writes output to HyDFS and console.
    """
    def __init__(self, task_id: str, stage_num: int, vm_id: int,
                 port: int, op_exe: str, op_args: str,
                 output_file: str, exactly_once: bool, run_id: str,
                 is_stateful: bool = False):
        super().__init__(task_id, stage_num, vm_id, port, exactly_once, run_id)
        
        self.op_exe = op_exe
        self.op_args = op_args
        self.output_file = output_file
        self.is_stateful = is_stateful
        
        # State for aggregation
        self.state: Dict = {}
        
        # Local output buffer
        self.output_buffer: List[str] = []
        self.buffer_lock = threading.Lock()
        self.flush_interval = 1.0  # seconds
    
    def run(self):
        """Process tuples and write to output."""
        self.start()
        self.logger.log(f"Sink starting: op={self.op_exe}, output={self.output_file}")
        
        # Start periodic flush thread
        flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        flush_thread.start()
        
        try:
            while self.running:
                # Receive tuple
                tuple_data = self.tuple_channel.receive_tuple(timeout=1.0)
                
                if tuple_data is None:
                    continue
                
                # Check for duplicate
                if not self.eo_manager.should_process(tuple_data):
                    continue
                
                # Process tuple
                output_tuples = self._process_tuple(tuple_data)
                
                # Mark as processed
                self.eo_manager.on_tuple_processed(tuple_data)
                self.processed_count += 1
                self.rate_monitor.record_tuple()
                
                # Output results
                for out_tuple in output_tuples:
                    self._output_tuple(out_tuple)
        
        except Exception as e:
            self.logger.log(f"Sink error: {e}")
        finally:
            # Final flush
            self._flush_output()
            self.stop()
    
    def _process_tuple(self, tuple_data: StreamTuple) -> List[StreamTuple]:
        """Process tuple using operator."""
        try:
            result = subprocess.run(
                [self.op_exe] + self.op_args.split(),
                input=f"{tuple_data.key}\t{tuple_data.value}\n",
                capture_output=True,
                text=True,
                timeout=10.0
            )
            
            output_tuples = []
            
            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    if not line:
                        continue
                    
                    parts = line.split('\t', 1)
                    if len(parts) == 2:
                        out_tuple = StreamTuple(key=parts[0], value=parts[1])
                        output_tuples.append(out_tuple)
                    elif len(parts) == 1:
                        out_tuple = StreamTuple(key=tuple_data.key, value=parts[0])
                        output_tuples.append(out_tuple)
            
            return output_tuples
        
        except Exception as e:
            self.logger.log(f"Operator error: {e}")
            return []
    
    def _output_tuple(self, tuple_data: StreamTuple):
        """Output a tuple to console and buffer for HyDFS."""
        output_line = f"{tuple_data.key}\t{tuple_data.value}"
        
        # Console output
        print(f"[OUTPUT] {output_line}")
        
        # Buffer for file
        with self.buffer_lock:
            self.output_buffer.append(output_line)
        
        # Log
        self.logger.log_tuple(tuple_data, "FINAL")
        self.output_count += 1
    
    def _flush_loop(self):
        """Periodically flush output buffer."""
        while self.running:
            time.sleep(self.flush_interval)
            self._flush_output()
    
    def _flush_output(self):
        """Flush output buffer to HyDFS/local file."""
        with self.buffer_lock:
            if not self.output_buffer:
                return
            
            lines = self.output_buffer.copy()
            self.output_buffer.clear()
        
        # Write to file
        try:
            # For now, write to local file
            # TODO: Append to HyDFS
            os.makedirs("output", exist_ok=True)
            output_path = f"output/{self.output_file}"
            
            with open(output_path, 'a') as f:
                for line in lines:
                    f.write(line + '\n')
            
            self.logger.log(f"Flushed {len(lines)} lines to {output_path}")
        
        except Exception as e:
            self.logger.log(f"Flush error: {e}")

# ============================================================================
# Aggregation Task (Stateful)
# ============================================================================

class AggregationTask(ProcessingTask):
    """
    Stateful aggregation task that maintains running aggregates.
    """
    def __init__(self, task_id: str, stage_num: int, vm_id: int,
                 port: int, op_exe: str, op_args: str,
                 exactly_once: bool, run_id: str,
                 group_by_column: int = None):
        super().__init__(task_id, stage_num, vm_id, port, 
                        op_exe, op_args, exactly_once, run_id, is_stateful=True)
        
        self.group_by_column = group_by_column
        
        # Aggregation state: key -> count
        self.aggregates: Dict[str, int] = {}
        self.agg_lock = threading.Lock()
    
    def _process_tuple(self, tuple_data: StreamTuple) -> List[StreamTuple]:
        """
        Process tuple for aggregation.
        Updates running count and emits updated aggregate.
        """
        # Extract group key from value
        if self.group_by_column is not None:
            fields = parse_csv_line(tuple_data.value)
            if 0 < self.group_by_column <= len(fields):
                group_key = fields[self.group_by_column - 1]
            else:
                group_key = ""  # Empty string for missing data
        else:
            group_key = tuple_data.key
        
        # Update aggregate
        with self.agg_lock:
            if group_key not in self.aggregates:
                self.aggregates[group_key] = 0
            self.aggregates[group_key] += 1
            count = self.aggregates[group_key]
        
        # Emit updated aggregate
        out_tuple = StreamTuple(key=group_key, value=str(count))
        return [out_tuple]

# ============================================================================
# Task Factory
# ============================================================================

def create_task(task_type: str, task_id: str, stage_num: int,
                vm_id: int, port: int, run_id: str,
                exactly_once: bool = True, **kwargs) -> Task:
    """
    Factory function to create tasks.
    """
    if task_type == 'source':
        return SourceTask(
            task_id=task_id,
            vm_id=vm_id,
            port=port,
            input_file=kwargs.get('input_file', ''),
            input_rate=kwargs.get('input_rate', 100),
            exactly_once=exactly_once,
            run_id=run_id,
            use_local_file=kwargs.get('use_local_file', True)
        )
    
    elif task_type == 'processing':
        return ProcessingTask(
            task_id=task_id,
            stage_num=stage_num,
            vm_id=vm_id,
            port=port,
            op_exe=kwargs.get('op_exe', ''),
            op_args=kwargs.get('op_args', ''),
            exactly_once=exactly_once,
            run_id=run_id
        )
    
    elif task_type == 'sink':
        return SinkTask(
            task_id=task_id,
            stage_num=stage_num,
            vm_id=vm_id,
            port=port,
            op_exe=kwargs.get('op_exe', ''),
            op_args=kwargs.get('op_args', ''),
            output_file=kwargs.get('output_file', 'output.txt'),
            exactly_once=exactly_once,
            run_id=run_id
        )
    
    elif task_type == 'aggregation':
        return AggregationTask(
            task_id=task_id,
            stage_num=stage_num,
            vm_id=vm_id,
            port=port,
            op_exe=kwargs.get('op_exe', ''),
            op_args=kwargs.get('op_args', ''),
            exactly_once=exactly_once,
            run_id=run_id,
            group_by_column=kwargs.get('group_by_column')
        )
    
    else:
        raise ValueError(f"Unknown task type: {task_type}")

# ============================================================================
# Task Runner (standalone execution)
# ============================================================================

def run_task_standalone(task_config: Dict):
    """
    Run a task as a standalone process.
    Called when worker spawns a task process.
    """
    task_type = task_config['type']
    task = create_task(
        task_type=task_type,
        task_id=task_config['task_id'],
        stage_num=task_config.get('stage_num', 0),
        vm_id=task_config['vm_id'],
        port=task_config['port'],
        run_id=task_config['run_id'],
        exactly_once=task_config.get('exactly_once', True),
        **task_config.get('params', {})
    )
    
    # Set downstream tasks
    if 'downstream' in task_config:
        task.set_downstream(task_config['downstream'])
    
    # Handle signals
    def signal_handler(sig, frame):
        task.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Run
    task.run()


if __name__ == '__main__':
    # Allow running task directly for testing
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
        with open(config_file, 'r') as f:
            task_config = json.load(f)
        run_task_standalone(task_config)