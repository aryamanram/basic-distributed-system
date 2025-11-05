"""
File and Block Storage Manager
Manages local file replicas with block-based storage
"""

import os
import json
import pickle

class Block:
    """Represents a single append block"""
    def __init__(self, data, client_id, sequence, timestamp):
        self.data = data
        self.client_id = client_id
        self.sequence = sequence
        self.timestamp = timestamp
        self.size = len(data)
    
    def to_dict(self):
        return {
            'data': self.data,
            'client_id': self.client_id,
            'sequence': self.sequence,
            'timestamp': self.timestamp,
            'size': self.size
        }
    
    @staticmethod
    def from_dict(d):
        return Block(d['data'], d['client_id'], d['sequence'], d['timestamp'])

class FileStore:
    """Manages local file storage"""
    
    def __init__(self, base_path):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
    
    def _get_file_path(self, filename):
        """Get full path for a file"""
        # Use hash to avoid filename conflicts
        safe_name = filename.replace('/', '_')
        return os.path.join(self.base_path, safe_name + '.blocks')
    
    def _get_metadata_path(self, filename):
        """Get metadata path for a file"""
        safe_name = filename.replace('/', '_')
        return os.path.join(self.base_path, safe_name + '.meta')
    
    def store_file(self, filename, blocks):
        """Store a file with its blocks"""
        file_path = self._get_file_path(filename)
        
        # Serialize blocks
        block_dicts = [block.to_dict() for block in blocks]
        
        with open(file_path, 'wb') as f:
            pickle.dump(block_dicts, f)
        
        # Store metadata
        metadata = {
            'filename': filename,
            'num_blocks': len(blocks),
            'total_size': sum(b.size for b in blocks)
        }
        with open(self._get_metadata_path(filename), 'w') as f:
            json.dump(metadata, f)
    
    def get_file(self, filename):
        """Get all blocks of a file"""
        file_path = self._get_file_path(filename)
        
        if not os.path.exists(file_path):
            return None
        
        with open(file_path, 'rb') as f:
            block_dicts = pickle.load(f)
        
        return [Block.from_dict(d) for d in block_dicts]
    
    def append_block(self, filename, block):
        """Append a block to a file"""
        blocks = self.get_file(filename)
        if blocks is None:
            return False
        
        blocks.append(block)
        self.store_file(filename, blocks)
        return True
    
    def delete_file(self, filename):
        """Delete a file"""
        file_path = self._get_file_path(filename)
        meta_path = self._get_metadata_path(filename)
        
        if os.path.exists(file_path):
            os.remove(file_path)
        if os.path.exists(meta_path):
            os.remove(meta_path)
    
    def file_exists(self, filename):
        """Check if file exists"""
        return os.path.exists(self._get_file_path(filename))
    
    def list_files(self):
        """List all stored files"""
        files = []
        for fname in os.listdir(self.base_path):
            if fname.endswith('.blocks'):
                # Extract original filename
                files.append(fname[:-7].replace('_', '/'))
        return files
    
    def get_file_metadata(self, filename):
        """Get file metadata"""
        meta_path = self._get_metadata_path(filename)
        if not os.path.exists(meta_path):
            return None
        
        with open(meta_path, 'r') as f:
            return json.load(f)
    
    def clear_all_files(self):
        """Clear all files (for rejoin)"""
        for fname in os.listdir(self.base_path):
            os.remove(os.path.join(self.base_path, fname))