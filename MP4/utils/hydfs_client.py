"""
HyDFS Client wrapper for RainStorm
"""
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

class HyDFSClient:
    """
    Wrapper for HyDFS operations.
    In production, this would use the HyDFS API.
    For testing, we'll use simple file operations.
    """
    
    def __init__(self, logger):
        self.logger = logger
    
    def get(self, hydfs_filename: str) -> str:
        """Get a file from HyDFS."""
        try:
            # For testing, read from local file
            with open(hydfs_filename, 'r') as f:
                return f.read()
        except Exception as e:
            self.logger.log(f"Error reading file {hydfs_filename}: {e}")
            return ""
    
    def append(self, data: str, hydfs_filename: str):
        """Append data to a file in HyDFS."""
        try:
            # For testing, append to local file
            with open(hydfs_filename, 'a') as f:
                f.write(data)
        except Exception as e:
            self.logger.log(f"Error appending to file {hydfs_filename}: {e}")
    
    def list_files(self, directory: str) -> list:
        """List files in a HyDFS directory."""
        try:
            import glob
            pattern = os.path.join(directory, "*.txt")
            return glob.glob(pattern)
        except Exception as e:
            self.logger.log(f"Error listing files in {directory}: {e}")
            return []