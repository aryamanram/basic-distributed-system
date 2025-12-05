"""
RainStorm Utilities
- Configuration loading
- Logging (timestamped, new file per run)
- Tuple serialization
- Hash partitioning
"""
import json
import os
import socket
import time
import hashlib
import threading
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

# ============================================================================
# Configuration
# ============================================================================

def load_config(config_path: str = "config.json") -> Dict:
    """Load configuration from JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)

def get_vm_by_id(config: Dict, vm_id: int) -> Optional[Dict]:
    """Get VM info by ID."""
    for vm in config['vms']:
        if vm['id'] == vm_id:
            return vm
    return None

def get_vm_by_hostname(config: Dict, hostname: str) -> Optional[Dict]:
    """Get VM info by hostname."""
    for vm in config['vms']:
        if vm['hostname'] == hostname:
            return vm
    return None

def get_current_vm_id(config: Dict) -> int:
    """Get the VM ID for the current machine."""
    hostname = socket.gethostname()
    for vm in config['vms']:
        if vm['hostname'] == hostname:
            return vm['id']
    # Fallback: try by IP
    try:
        local_ip = socket.gethostbyname(hostname)
        for vm in config['vms']:
            if vm['ip'] == local_ip:
                return vm['id']
    except:
        pass
    return -1

def get_leader_hostname(config: Dict) -> str:
    """Get the leader VM hostname."""
    leader_id = config.get('leader_vm', 1)
    vm = get_vm_by_id(config, leader_id)
    return vm['hostname'] if vm else config['vms'][0]['hostname']

# ============================================================================
# Logging
# ============================================================================

class RainStormLogger:
    """
    Logger for RainStorm with timestamped entries.
    Creates new log file per invocation.
    """
    def __init__(self, component: str, vm_id: int, run_id: str = None):
        self.component = component
        self.vm_id = vm_id
        self.run_id = run_id or f"{int(time.time() * 1000)}"
        self.lock = threading.Lock()
        
        # Create log directory if needed
        os.makedirs("logs", exist_ok=True)
        
        # Log file path: logs/<component>_vm<id>_<run_id>.log
        self.log_file = f"logs/{component}_vm{vm_id}_{self.run_id}.log"
        
        # Write header
        self._write(f"=== RainStorm {component} Log ===")
        self._write(f"VM: {vm_id}, Run ID: {run_id}")
        self._write(f"Started: {self._timestamp()}")
        self._write("=" * 50)
    
    def _timestamp(self) -> str:
        """Get formatted timestamp."""
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    
    def _write(self, message: str):
        """Write to log file."""
        with self.lock:
            line = f"[{self._timestamp()}] {message}"
            print(line)
            try:
                with open(self.log_file, 'a') as f:
                    f.write(line + '\n')
            except Exception as e:
                print(f"Log write error: {e}")
    
    def log(self, message: str):
        """Log a general message."""
        self._write(message)
    
    def log_event(self, event_type: str, details: Dict = None):
        """Log a structured event."""
        msg = f"[{event_type}]"
        if details:
            msg += " " + " ".join(f"{k}={v}" for k, v in details.items())
        self._write(msg)
    
    def log_tuple(self, tuple_data: 'StreamTuple', direction: str = "OUT"):
        """Log a tuple (for task logging)."""
        self._write(f"[TUPLE_{direction}] key={tuple_data.key} value={tuple_data.value[:50]}...")
    
    def log_duplicate(self, tuple_id: str):
        """Log a rejected duplicate tuple."""
        self._write(f"[DUPLICATE_REJECTED] tuple_id={tuple_id}")
    
    def log_rate(self, task_id: str, tuples_per_sec: float):
        """Log processing rate."""
        self._write(f"[RATE] task={task_id} rate={tuples_per_sec:.2f} tuples/sec")
    
    def get_log_file(self) -> str:
        """Return the log file path."""
        return self.log_file

# ============================================================================
# Tuple Representation
# ============================================================================

class StreamTuple:
    """
    Represents a <key, value> tuple in RainStorm.
    """
    def __init__(self, key: str, value: str, tuple_id: str = None, 
                 source_task: str = None, sequence_num: int = 0):
        self.key = key
        self.value = value
        self.tuple_id = tuple_id or f"{int(time.time()*1000000)}_{hash(key+value) % 100000}"
        self.source_task = source_task
        self.sequence_num = sequence_num
        self.timestamp = time.time()
    
    def to_dict(self) -> Dict:
        """Serialize to dictionary."""
        return {
            'key': self.key,
            'value': self.value,
            'tuple_id': self.tuple_id,
            'source_task': self.source_task,
            'sequence_num': self.sequence_num,
            'timestamp': self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'StreamTuple':
        """Deserialize from dictionary."""
        t = cls(
            key=data['key'],
            value=data['value'],
            tuple_id=data.get('tuple_id'),
            source_task=data.get('source_task'),
            sequence_num=data.get('sequence_num', 0)
        )
        t.timestamp = data.get('timestamp', time.time())
        return t
    
    def __repr__(self):
        return f"StreamTuple(key={self.key}, value={self.value[:30]}..., id={self.tuple_id})"

# ============================================================================
# Serialization
# ============================================================================

def serialize_message(msg: Dict) -> bytes:
    """Serialize a message to bytes."""
    return json.dumps(msg).encode('utf-8')

def deserialize_message(data: bytes) -> Dict:
    """Deserialize bytes to message."""
    return json.loads(data.decode('utf-8'))

def send_sized_message(sock: socket.socket, data: bytes):
    """Send message with size header."""
    size_header = f"{len(data):010d}".encode('utf-8')
    sock.sendall(size_header + data)

def recv_sized_message(sock: socket.socket, timeout: float = 30.0) -> Optional[bytes]:
    """Receive message with size header."""
    sock.settimeout(timeout)
    try:
        size_header = sock.recv(10)
        if not size_header:
            return None
        size = int(size_header.decode('utf-8'))
        data = b''
        while len(data) < size:
            chunk = sock.recv(min(4096, size - len(data)))
            if not chunk:
                return None
            data += chunk
        return data
    except socket.timeout:
        return None

# ============================================================================
# Hash Partitioning
# ============================================================================

def hash_partition(key: str, num_partitions: int) -> int:
    """
    Hash a key to a partition number.
    Uses consistent hashing for even distribution.
    """
    if num_partitions <= 0:
        return 0
    hash_val = int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)
    return hash_val % num_partitions

def get_partition_for_tuple(tuple_data: StreamTuple, num_tasks: int) -> int:
    """Get the task partition for a tuple based on its key."""
    return hash_partition(tuple_data.key, num_tasks)

# ============================================================================
# Task Identification
# ============================================================================

def make_task_id(stage: int, task_num: int, run_id: str) -> str:
    """Create a unique task ID."""
    return f"stage{stage}_task{task_num}_{run_id}"

def parse_task_id(task_id: str) -> Tuple[int, int, str]:
    """Parse task ID into (stage, task_num, run_id)."""
    parts = task_id.split('_')
    stage = int(parts[0].replace('stage', ''))
    task_num = int(parts[1].replace('task', ''))
    run_id = '_'.join(parts[2:])
    return stage, task_num, run_id

def get_task_port(base_port: int, stage: int, task_num: int) -> int:
    """Calculate port for a task."""
    return base_port + (stage * 100) + task_num

# ============================================================================
# Misc Utilities
# ============================================================================

def get_active_vms(config: Dict) -> List[Dict]:
    """Get list of all configured VMs."""
    return config['vms']

def format_elapsed(start_time: float) -> str:
    """Format elapsed time since start."""
    elapsed = time.time() - start_time
    return f"{elapsed:.2f}s"

def parse_csv_line(line: str) -> List[str]:
    """Parse a CSV line into fields."""
    # Simple CSV parsing - handles basic cases
    fields = []
    current = ""
    in_quotes = False
    for char in line:
        if char == '"':
            in_quotes = not in_quotes
        elif char == ',' and not in_quotes:
            fields.append(current.strip())
            current = ""
        else:
            current += char
    fields.append(current.strip())
    return fields

def get_field(line: str, field_num: int) -> str:
    """Get a specific field from a CSV line (1-indexed)."""
    fields = parse_csv_line(line)
    if 0 < field_num <= len(fields):
        return fields[field_num - 1]
    return ""