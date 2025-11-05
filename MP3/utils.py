"""
Utility functions for HyDFS
"""
import hashlib
import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

def hash_to_ring(key: str) -> int:
    """
    Hash a key (node ID or filename) to a position on the ring.
    Uses SHA-1 and returns an integer in the range [0, 2^160).
    """
    hash_obj = hashlib.sha1(key.encode('utf-8'))
    return int(hash_obj.hexdigest(), 16)

def get_node_id(hostname: str, port: int, timestamp: Optional[float] = None) -> str:
    """
    Generate a unique node ID based on hostname, port, and timestamp.
    """
    if timestamp is None:
        timestamp = time.time()
    return f"{hostname}:{port}:{timestamp}"

def serialize_message(msg: Dict[str, Any]) -> bytes:
    """
    Serialize a message dictionary to bytes for network transmission.
    """
    return json.dumps(msg).encode('utf-8')

def deserialize_message(data: bytes) -> Dict[str, Any]:
    """
    Deserialize bytes to a message dictionary.
    """
    return json.loads(data.decode('utf-8'))

def format_timestamp() -> str:
    """
    Format current timestamp for logging.
    """
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

class Logger:
    """
    Logger for HyDFS operations.
    """
    def __init__(self, vm_id: int, log_file: Optional[str] = None):
        self.vm_id = vm_id
        self.log_file = log_file or f"machine.{vm_id}.log"
    
    def log(self, message: str):
        """
        Log a message with timestamp.
        """
        timestamp = format_timestamp()
        log_line = f"[{timestamp}] [VM{self.vm_id}] {message}"
        print(log_line)
        try:
            with open(self.log_file, 'a') as f:
                f.write(log_line + '\n')
        except Exception as e:
            print(f"Error writing to log: {e}")

def send_sized_message(sock, data: bytes):
    """
    Send a message with a size header (10 bytes).
    """
    size_header = f"{len(data):010d}".encode('utf-8')
    sock.sendall(size_header + data)

def recv_sized_message(sock) -> Optional[bytes]:
    """
    Receive a message with a size header.
    """
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

def get_file_id(filename: str) -> str:
    """
    Generate a unique file ID for a HyDFS filename.
    """
    return hashlib.sha1(filename.encode('utf-8')).hexdigest()[:16]