"""
Block-based file storage for HyDFS
"""
import os
import json
import threading
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from utils import get_file_id

@dataclass
class FileBlock:
    """
    Represents a single block (append operation) in a file.
    """
    block_id: str  # unique block ID
    client_id: str  # client that created this block
    sequence_num: int  # sequence number from that client
    timestamp: float  # when the block was created
    data: bytes  # actual data
    size: int  # size of data
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization (excluding data)."""
        return {
            'block_id': self.block_id,
            'client_id': self.client_id,
            'sequence_num': self.sequence_num,
            'timestamp': self.timestamp,
            'size': self.size
        }

class FileStorage:
    """
    Manages file storage with block-based structure.
    Each file is stored as a collection of blocks (one per append).
    """
    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        self.lock = threading.RLock()
        
        # file_id -> List[FileBlock]
        self.files: Dict[str, List[FileBlock]] = {}
        
        # file_id -> filename mapping
        self.file_names: Dict[str, str] = {}
        
        # Ensure storage directory exists
        os.makedirs(storage_dir, exist_ok=True)
        
        # Load existing files
        self._load_files()
    
    def _get_file_dir(self, file_id: str) -> str:
        """Get the directory for a specific file."""
        return os.path.join(self.storage_dir, file_id)
    
    def _get_metadata_path(self, file_id: str) -> str:
        """Get the path to the metadata file."""
        return os.path.join(self._get_file_dir(file_id), "metadata.json")
    
    def _get_block_path(self, file_id: str, block_id: str) -> str:
        """Get the path to a block file."""
        return os.path.join(self._get_file_dir(file_id), f"{block_id}.block")
    
    def _load_files(self):
        """Load existing files from disk."""
        with self.lock:
            if not os.path.exists(self.storage_dir):
                return
            
            for file_id in os.listdir(self.storage_dir):
                file_dir = self._get_file_dir(file_id)
                if not os.path.isdir(file_dir):
                    continue
                
                metadata_path = self._get_metadata_path(file_id)
                if not os.path.exists(metadata_path):
                    continue
                
                try:
                    with open(metadata_path, 'r') as f:
                        metadata = json.load(f)
                    
                    self.file_names[file_id] = metadata['filename']
                    blocks = []
                    
                    for block_meta in metadata['blocks']:
                        block_path = self._get_block_path(file_id, block_meta['block_id'])
                        if os.path.exists(block_path):
                            with open(block_path, 'rb') as bf:
                                data = bf.read()
                            
                            block = FileBlock(
                                block_id=block_meta['block_id'],
                                client_id=block_meta['client_id'],
                                sequence_num=block_meta['sequence_num'],
                                timestamp=block_meta['timestamp'],
                                data=data,
                                size=block_meta['size']
                            )
                            blocks.append(block)
                    
                    self.files[file_id] = blocks
                except Exception as e:
                    print(f"Error loading file {file_id}: {e}")
    
    def _save_metadata(self, file_id: str):
        """Save file metadata to disk."""
        with self.lock:
            if file_id not in self.files:
                return
            
            file_dir = self._get_file_dir(file_id)
            os.makedirs(file_dir, exist_ok=True)
            
            metadata = {
                'filename': self.file_names.get(file_id, ''),
                'blocks': [block.to_dict() for block in self.files[file_id]]
            }
            
            metadata_path = self._get_metadata_path(file_id)
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
    
    def create_file(self, filename: str, initial_data: bytes) -> Tuple[bool, str]:
        """
        Create a new file with initial data.
        Returns (success, message).
        """
        file_id = get_file_id(filename)
        
        with self.lock:
            if file_id in self.files:
                return False, f"File {filename} already exists"
            
            # Create initial block
            block = FileBlock(
                block_id=f"{file_id}_0",
                client_id="create",
                sequence_num=0,
                timestamp=0,
                data=initial_data,
                size=len(initial_data)
            )
            
            self.files[file_id] = [block]
            self.file_names[file_id] = filename
            
            # Save to disk
            file_dir = self._get_file_dir(file_id)
            os.makedirs(file_dir, exist_ok=True)
            
            block_path = self._get_block_path(file_id, block.block_id)
            with open(block_path, 'wb') as f:
                f.write(initial_data)
            
            self._save_metadata(file_id)
            
            return True, f"File {filename} created"
    
    def append_block(self, filename: str, block: FileBlock) -> Tuple[bool, str]:
        """
        Append a block to a file.
        Returns (success, message).
        """
        file_id = get_file_id(filename)
        
        with self.lock:
            if file_id not in self.files:
                return False, f"File {filename} does not exist"
            
            # Add block
            self.files[file_id].append(block)
            
            # Save block to disk
            block_path = self._get_block_path(file_id, block.block_id)
            with open(block_path, 'wb') as f:
                f.write(block.data)
            
            self._save_metadata(file_id)
            
            return True, f"Block appended to {filename}"
    
    def get_file_data(self, filename: str) -> Optional[bytes]:
        """
        Get the complete file data by concatenating all blocks.
        """
        file_id = get_file_id(filename)
        
        with self.lock:
            if file_id not in self.files:
                return None
            
            # Concatenate all blocks in order
            data = b''
            for block in self.files[file_id]:
                data += block.data
            
            return data
    
    def get_file_blocks(self, filename: str) -> Optional[List[FileBlock]]:
        """
        Get all blocks for a file.
        """
        file_id = get_file_id(filename)
        
        with self.lock:
            if file_id not in self.files:
                return None
            return self.files[file_id].copy()
    
    def replace_file_blocks(self, filename: str, blocks: List[FileBlock]) -> bool:
        """
        Replace all blocks of a file (used during merge).
        """
        file_id = get_file_id(filename)
        
        with self.lock:
            if file_id not in self.files:
                return False
            
            # Remove old blocks from disk
            file_dir = self._get_file_dir(file_id)
            if os.path.exists(file_dir):
                for old_block in self.files[file_id]:
                    old_path = self._get_block_path(file_id, old_block.block_id)
                    if os.path.exists(old_path):
                        os.remove(old_path)
            
            # Set new blocks
            self.files[file_id] = blocks
            
            # Save new blocks to disk
            os.makedirs(file_dir, exist_ok=True)
            for block in blocks:
                block_path = self._get_block_path(file_id, block.block_id)
                with open(block_path, 'wb') as f:
                    f.write(block.data)
            
            self._save_metadata(file_id)
            return True
    
    def file_exists(self, filename: str) -> bool:
        """Check if a file exists."""
        file_id = get_file_id(filename)
        with self.lock:
            return file_id in self.files
    
    def list_files(self) -> List[Tuple[str, str]]:
        """
        List all files as (filename, file_id) tuples.
        """
        with self.lock:
            return [(self.file_names.get(fid, 'unknown'), fid) 
                    for fid in self.files.keys()]
    
    def delete_file(self, filename: str) -> bool:
        """
        Delete a file and all its blocks.
        """
        file_id = get_file_id(filename)
        
        with self.lock:
            if file_id not in self.files:
                return False
            
            # Remove from disk
            file_dir = self._get_file_dir(file_id)
            if os.path.exists(file_dir):
                import shutil
                shutil.rmtree(file_dir)
            
            # Remove from memory
            del self.files[file_id]
            if file_id in self.file_names:
                del self.file_names[file_id]
            
            return True
    
    def clear_all_files(self):
        """
        Delete all files (used when a node rejoins).
        """
        with self.lock:
            # Remove all files from disk
            if os.path.exists(self.storage_dir):
                import shutil
                shutil.rmtree(self.storage_dir)
                os.makedirs(self.storage_dir, exist_ok=True)
            
            # Clear memory
            self.files.clear()
            self.file_names.clear()