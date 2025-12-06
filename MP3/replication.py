"""
Replication management for HyDFS
"""
import threading
import time
from typing import List, Dict, Set, Optional
from MP3.storage import FileBlock

# Replication factor: 3 replicas to tolerate 2 failures
REPLICATION_FACTOR = 3

class ReplicationManager:
    """
    Manages file replication across nodes.
    """
    def __init__(self, node_manager, logger):
        self.node_manager = node_manager
        self.logger = logger
        
        # Track which files should be stored on this node
        # filename -> Set[replica_nodes]
        self.file_replicas: Dict[str, Set[str]] = {}
        self.replica_lock = threading.Lock()
        
        # Monitor thread for re-replication
        self.monitor_running = False
        self.monitor_thread: Optional[threading.Thread] = None
    
    def start_monitoring(self):
        """
        Start monitoring for failures and trigger re-replication.
        """
        if self.monitor_running:
            return
        
        self.monitor_running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        self.logger.log("REPLICATION: Monitoring started")
    
    def stop_monitoring(self):
        """
        Stop monitoring.
        """
        self.monitor_running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2.0)
    
    def _monitor_loop(self):
        """
        Periodically check for under-replicated files.
        """
        while self.monitor_running:
            try:
                time.sleep(5.0)  # Check every 5 seconds
                self._check_and_rereplicate()
            except Exception as e:
                self.logger.log(f"REPLICATION ERROR: Monitor loop: {e}")
    
    def _check_and_rereplicate(self):
        """
        Check all files and re-replicate if needed.
        """
        files = self.node_manager.storage.list_files()
        
        for filename, file_id in files:
            try:
                # Get current replicas from ring
                current_replicas = self.node_manager.get_replicas_for_file(filename)
                
                # Check if this node should have this file
                should_have = self.node_manager.node_id in current_replicas
                has_file = self.node_manager.storage.file_exists(filename)
                
                if should_have and not has_file:
                    # This node should have the file but doesn't - fetch it
                    self.logger.log(f"REPLICATION: Node should have {filename}, fetching...")
                    self._fetch_file_from_replica(filename, current_replicas)
                
                elif not should_have and has_file:
                    # This node has the file but shouldn't - delete it
                    self.logger.log(f"REPLICATION: Node shouldn't have {filename}, deleting...")
                    self.node_manager.storage.delete_file(filename)
                
                # Check if file is under-replicated
                actual_count = sum(1 for r in current_replicas 
                                 if self._check_replica_has_file(r, filename))
                
                if actual_count < REPLICATION_FACTOR:
                    self.logger.log(
                        f"REPLICATION: {filename} under-replicated "
                        f"({actual_count}/{REPLICATION_FACTOR}), fixing..."
                    )
                    self._replicate_file(filename, current_replicas)
            
            except Exception as e:
                self.logger.log(f"REPLICATION ERROR: Check file {filename}: {e}")
    
    def _check_replica_has_file(self, node_id: str, filename: str) -> bool:
        """
        Check if a replica node has a file.
        """
        if node_id == self.node_manager.node_id:
            return self.node_manager.storage.file_exists(filename)
        
        try:
            hostname = node_id.split(':')[0]
            port = int(node_id.split(':')[1])
            
            message = {
                'type': 'CHECK_FILE',
                'filename': filename
            }
            
            response = self.node_manager.network.send_message(hostname, port, message, timeout=5.0)
            return response and response.get('exists', False)
        except:
            return False
    
    def _fetch_file_from_replica(self, filename: str, replica_nodes: List[str]):
        """
        Fetch a file from one of the replica nodes.
        """
        for node_id in replica_nodes:
            if node_id == self.node_manager.node_id:
                continue
            
            try:
                hostname = node_id.split(':')[0]
                port = int(node_id.split(':')[1])
                
                message = {
                    'type': 'GET_FILE_BLOCKS',
                    'filename': filename
                }
                
                response = self.node_manager.network.send_message(hostname, port, message)
                
                if response and response.get('status') == 'success':
                    blocks_data = response.get('blocks', [])
                    
                    # Reconstruct blocks
                    blocks = []
                    for bd in blocks_data:
                        block = FileBlock(
                            block_id=bd['block_id'],
                            client_id=bd['client_id'],
                            sequence_num=bd['sequence_num'],
                            timestamp=bd['timestamp'],
                            data=bytes(bd['data']),
                            size=bd['size']
                        )
                        blocks.append(block)
                    
                    # Create file with fetched blocks
                    if blocks:
                        # Create file with first block
                        self.node_manager.storage.create_file(filename, blocks[0].data)
                        
                        # Append remaining blocks
                        for block in blocks[1:]:
                            self.node_manager.storage.append_block(filename, block)
                        
                        self.logger.log(f"REPLICATION: Fetched {filename} from {hostname}")
                        return True
            
            except Exception as e:
                self.logger.log(f"REPLICATION ERROR: Fetch from {node_id}: {e}")
                continue
        
        return False
    
    def _replicate_file(self, filename: str, target_nodes: List[str]):
        """
        Replicate a file to target nodes.
        """
        # Get file blocks from local storage
        blocks = self.node_manager.storage.get_file_blocks(filename)
        if not blocks:
            return
        
        # Prepare blocks data for transmission
        blocks_data = []
        for block in blocks:
            blocks_data.append({
                'block_id': block.block_id,
                'client_id': block.client_id,
                'sequence_num': block.sequence_num,
                'timestamp': block.timestamp,
                'data': list(block.data),  # Convert bytes to list for JSON
                'size': block.size
            })
        
        # Send to each target node
        for node_id in target_nodes:
            if node_id == self.node_manager.node_id:
                continue
            
            try:
                hostname = node_id.split(':')[0]
                port = int(node_id.split(':')[1])
                
                message = {
                    'type': 'REPLICATE_FILE',
                    'filename': filename,
                    'blocks': blocks_data
                }
                
                response = self.node_manager.network.send_message(hostname, port, message)
                
                if response and response.get('status') == 'success':
                    self.logger.log(f"REPLICATION: Sent {filename} to {hostname}")
            
            except Exception as e:
                self.logger.log(f"REPLICATION ERROR: Send to {node_id}: {e}")
    
    def handle_node_join(self, new_node_id: str):
        """
        Handle a new node joining - rebalance files.
        """
        self.logger.log(f"REPLICATION: Node {new_node_id} joined, rebalancing...")
        
        # Get all files
        files = self.node_manager.storage.list_files()
        
        for filename, file_id in files:
            try:
                # Get new replica set
                new_replicas = self.node_manager.get_replicas_for_file(filename)
                
                # Check if this node should still have the file
                if self.node_manager.node_id not in new_replicas:
                    self.logger.log(f"REPLICATION: Removing {filename} (not in new replica set)")
                    self.node_manager.storage.delete_file(filename)
                
                # Check if new node should have the file
                elif new_node_id in new_replicas:
                    self.logger.log(f"REPLICATION: Sending {filename} to new node")
                    self._replicate_file(filename, [new_node_id])
            
            except Exception as e:
                self.logger.log(f"REPLICATION ERROR: Rebalance {filename}: {e}")
    
    def handle_node_failure(self, failed_node_id: str):
        """
        Handle a node failure - trigger re-replication.
        """
        self.logger.log(f"REPLICATION: Node {failed_node_id} failed, re-replicating...")
        
        # The monitor loop will detect under-replicated files and fix them
        # We can also trigger an immediate check
        self._check_and_rereplicate()