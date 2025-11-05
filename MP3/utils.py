"""
Utilities and helpers
"""

import logging
import time
from datetime import datetime

# Constants
REPLICATION_FACTOR = 3
RING_SIZE = 2**32

def setup_logging(vm_id):
    """Setup logging for node"""
    log_filename = f"machine.{vm_id}.log"
    
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    return logging.getLogger(f"VM{vm_id}")

def log_operation(logger, operation, details):
    """Log a file operation"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    logger.info(f"[{operation}] {details}")

def format_file_size(size_bytes):
    """Format file size in human-readable format"""
    for unit in ['B', 'KiB', 'MiB', 'GiB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TiB"

def calculate_bandwidth(bytes_transferred, time_seconds):
    """Calculate bandwidth in Mbps"""
    if time_seconds == 0:
        return 0
    bits_per_second = (bytes_transferred * 8) / time_seconds
    return bits_per_second / (1024 * 1024)  # Convert to Mbps

def generate_node_id(ip, port, timestamp=None):
    """Generate unique node ID"""
    if timestamp is None:
        timestamp = int(time.time() * 1000)
    return f"{ip}:{port}:{timestamp}"

def print_help():
    """Print help message"""
    print("""
Available Commands:
  create <localfile> <hydfsfile>        - Create file in HyDFS
  get <hydfsfile> <localfile>           - Get file from HyDFS
  append <localfile> <hydfsfile>        - Append to file in HyDFS
  merge <hydfsfile>                     - Merge file replicas
  ls <hydfsfile>                        - List replicas for file
  liststore                             - List files stored locally
  getfromreplica <vm> <hydfs> <local>   - Get from specific replica
  list_mem_ids                          - List membership with ring IDs
  multiappend ...                       - Concurrent appends
  help                                  - Show this help
  exit                                  - Exit
    """)