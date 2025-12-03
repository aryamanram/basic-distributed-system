"""
Logger utility for RainStorm
"""
import os
from datetime import datetime
import threading

class Logger:
    """
    Logger for RainStorm operations.
    """
    
    def __init__(self, vm_id: int, prefix: str = "", log_file: str = None):
        self.vm_id = vm_id
        self.prefix = prefix
        self.log_file = log_file or f"../MP2/machine.{vm_id}.log"
        self.lock = threading.Lock()
    
    def log(self, message: str):
        """Log a message with timestamp."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        prefix_str = f"[{self.prefix}] " if self.prefix else ""
        log_line = f"[{timestamp}] {prefix_str}{message}"
        
        with self.lock:
            print(log_line)
            
            try:
                with open(self.log_file, 'a') as f:
                    f.write(log_line + '\n')
            except Exception as e:
                print(f"Error writing to log: {e}")