"""
HyDFS Node - Core implementation
"""
import os
import sys
import socket
import time
import threading
from typing import List, Optional, Dict, Any

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from MP2.membership import MembershipService
from MP3.ring import ConsistentHashRing
from MP3.storage import FileStorage, FileBlock
from MP3.consistency import ConsistencyManager, MergeCoordinator
from MP3.network import NetworkManager
from MP3.replication import ReplicationManager, REPLICATION_FACTOR
from MP3.utils import Logger, get_node_id, get_file_id

HYDFS_PORT = 7000

class HyDFSNode:
    """
    Main HyDFS node that integrates all components.
    """
    def __init__(self, vm_id: int):
        self.vm_id = vm_id
        self.hostname = socket.gethostname()
        self.port = HYDFS_PORT
        
        # Initialize logger
        self.logger = Logger(vm_id)
        
        # Initialize MP2 membership FIRST to get consistent member_id
        self.membership = MembershipService(vm_id)
        
        # Use MP2's integer timestamp for node_id (matches _monitor_membership format)
        mp2_timestamp = self.membership.member_id.split('_')[0]
        self.node_id = f"{self.hostname}:{self.port}:{mp2_timestamp}"
        
        # Initialize storage
        storage_dir = f"hydfs_storage_{vm_id}"
        self.storage = FileStorage(storage_dir)
        
        # Initialize ring with consistent node_id
        self.ring = ConsistentHashRing()
        self.ring.add_node(self.node_id)
        
        # Initialize consistency manager
        self.consistency = ConsistencyManager(self.logger)
        self.merge_coordinator = MergeCoordinator(self.logger)
        
        # Initialize network
        self.network = NetworkManager(self.hostname, self.port, self.logger)
        self._register_handlers()
        
        # Initialize replication manager
        self.replication = ReplicationManager(self, self.logger)
        
        # Track active nodes
        self.active_nodes_lock = threading.Lock()
        
        self.logger.log(f"INIT: HyDFS Node {vm_id} initialized")
        self.logger.log(f"INIT: Node ID: {self.node_id}")
        self.logger.log(f"INIT: Ring position: {self.ring.get_node_position(self.node_id)}")
    
    def start(self):
        """
        Start the HyDFS node.
        """
        # Start network server
        self.network.start()
        
        # Start membership service
        self.membership.start()
        
        # Start replication monitoring
        self.replication.start_monitoring()
        
        # Monitor membership changes
        threading.Thread(target=self._monitor_membership, daemon=True).start()
        
        self.logger.log("NODE: Started")
    
    def stop(self):
        """
        Stop the HyDFS node.
        """
        self.replication.stop_monitoring()
        self.network.stop()
        self.membership.stop()
        self.logger.log("NODE: Stopped")
    
    def _monitor_membership(self):
        """
        Monitor membership list and update ring accordingly.
        """
        known_members = set()
        
        while True:
            try:
                time.sleep(2.0)
                
                # Get current membership
                members = self.membership.membership.get_members()
                current_members = set()
                
                for hostname, info in members.items():
                    if info['status'] == 'ACTIVE':
                        # Create node ID for this member
                        member_node_id = f"{hostname}:{HYDFS_PORT}:{info['id'].split('_')[0]}"
                        current_members.add(member_node_id)
                
                # Detect new members
                new_members = current_members - known_members
                for node_id in new_members:
                    if node_id != self.node_id:
                        self.ring.add_node(node_id)
                        self.logger.log(f"MEMBERSHIP: Added node {node_id} to ring")
                        self.replication.handle_node_join(node_id)
                
                # Detect failed members
                failed_members = known_members - current_members
                for node_id in failed_members:
                    if node_id != self.node_id:
                        self.ring.remove_node(node_id)
                        self.logger.log(f"MEMBERSHIP: Removed node {node_id} from ring")
                        self.replication.handle_node_failure(node_id)
                
                known_members = current_members
            
            except Exception as e:
                self.logger.log(f"MEMBERSHIP ERROR: {e}")
    
    def get_replicas_for_file(self, filename: str) -> List[str]:
        """
        Get the list of nodes that should store replicas of a file.
        """
        return self.ring.get_successors(filename, REPLICATION_FACTOR)
    
    def _register_handlers(self):
        """
        Register network message handlers.
        """
        self.network.register_handler('CREATE', self._handle_create)
        self.network.register_handler('GET', self._handle_get)
        self.network.register_handler('APPEND', self._handle_append)
        self.network.register_handler('MERGE', self._handle_merge)
        self.network.register_handler('CHECK_FILE', self._handle_check_file)
        self.network.register_handler('GET_FILE_BLOCKS', self._handle_get_file_blocks)
        self.network.register_handler('REPLICATE_FILE', self._handle_replicate_file)
        self.network.register_handler('LS', self._handle_ls)
        self.network.register_handler('LISTSTORE', self._handle_liststore)
        self.network.register_handler('GET_FROM_REPLICA', self._handle_get_from_replica)
        self.network.register_handler('LIST_MEM_IDS', self._handle_list_mem_ids)
    
    # ==================== File Operation Handlers ====================
    
    def _handle_create(self, message: dict, addr: tuple) -> dict:
        """
        Handle CREATE request.
        """
        self.logger.log(f"OPERATION: Received CREATE request")
        
        filename = message.get('filename')
        data = bytes(message.get('data', []))
        
        # Create file locally
        success, msg = self.storage.create_file(filename, data)
        
        self.logger.log(f"OPERATION: CREATE {filename} completed - {msg}")
        
        return {'status': 'success' if success else 'error', 'message': msg}
    
    def _handle_get(self, message: dict, addr: tuple) -> dict:
        """
        Handle GET request.
        """
        self.logger.log(f"OPERATION: Received GET request")
        
        filename = message.get('filename')
        client_id = message.get('client_id', 'unknown')
        
        # Check for pending writes from this client
        if self.consistency.has_pending_writes(filename, client_id):
            return {
                'status': 'error',
                'message': 'Pending writes for this file from this client'
            }
        
        # Get file data
        data = self.storage.get_file_data(filename)
        
        if data is None:
            self.logger.log(f"OPERATION: GET {filename} completed - File not found")
            return {'status': 'error', 'message': 'File not found'}
        
        self.logger.log(f"OPERATION: GET {filename} completed - {len(data)} bytes")
        
        return {
            'status': 'success',
            'data': list(data),
            'size': len(data)
        }
    
    def _handle_append(self, message: dict, addr: tuple) -> dict:
        """
        Handle APPEND request.
        """
        self.logger.log(f"OPERATION: Received APPEND request")
        
        filename = message.get('filename')
        client_id = message.get('client_id')
        block_data = message.get('block')
        
        # Reconstruct block
        block = FileBlock(
            block_id=block_data['block_id'],
            client_id=block_data['client_id'],
            sequence_num=block_data['sequence_num'],
            timestamp=block_data['timestamp'],
            data=bytes(block_data['data']),
            size=block_data['size']
        )
        
        # Append block
        success, msg = self.storage.append_block(filename, block)
        
        self.logger.log(f"OPERATION: APPEND {filename} completed - {msg}")
        
        return {'status': 'success' if success else 'error', 'message': msg}
    
    def _handle_merge(self, message: dict, addr: tuple) -> dict:
        """
        Handle MERGE request.
        """
        self.logger.log(f"OPERATION: Received MERGE request")
        
        filename = message.get('filename')
        
        # Check if merge already in progress
        if not self.merge_coordinator.start_merge(filename):
            return {'status': 'error', 'message': 'Merge already in progress'}
        
        try:
            # Get all replicas
            replicas = self.get_replicas_for_file(filename)
            
            # Collect blocks from all replicas
            all_blocks = []
            
            for node_id in replicas:
                try:
                    if node_id == self.node_id:
                        # Local blocks
                        blocks = self.storage.get_file_blocks(filename)
                        if blocks:
                            all_blocks.append(blocks)
                    else:
                        # Remote blocks
                        hostname = node_id.split(':')[0]
                        port = int(node_id.split(':')[1])
                        
                        msg = {'type': 'GET_FILE_BLOCKS', 'filename': filename}
                        response = self.network.send_message(hostname, port, msg)
                        
                        if response and response.get('status') == 'success':
                            blocks_data = response.get('blocks', [])
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
                            if blocks:
                                all_blocks.append(blocks)
                except Exception as e:
                    self.logger.log(f"MERGE ERROR: Collect from {node_id}: {e}")
            
            # Merge blocks
            merged_blocks = self.consistency.merge_blocks(all_blocks)
            
            # Validate ordering
            if not self.consistency.validate_block_ordering(merged_blocks):
                return {'status': 'error', 'message': 'Block ordering validation failed'}
            
            # Update all replicas with merged blocks
            blocks_data = []
            for block in merged_blocks:
                blocks_data.append({
                    'block_id': block.block_id,
                    'client_id': block.client_id,
                    'sequence_num': block.sequence_num,
                    'timestamp': block.timestamp,
                    'data': list(block.data),
                    'size': block.size
                })
            
            for node_id in replicas:
                try:
                    if node_id == self.node_id:
                        # Update local storage
                        self.storage.replace_file_blocks(filename, merged_blocks)
                    else:
                        # Send to remote replica
                        hostname = node_id.split(':')[0]
                        port = int(node_id.split(':')[1])
                        
                        msg = {
                            'type': 'REPLICATE_FILE',
                            'filename': filename,
                            'blocks': blocks_data,
                            'replace': True
                        }
                        self.network.send_message(hostname, port, msg)
                except Exception as e:
                    self.logger.log(f"MERGE ERROR: Update {node_id}: {e}")
            
            self.logger.log(f"OPERATION: MERGE {filename} completed")
            return {'status': 'success', 'message': 'Merge completed'}
        
        finally:
            self.merge_coordinator.finish_merge(filename)
    
    def _handle_check_file(self, message: dict, addr: tuple) -> dict:
        """
        Handle CHECK_FILE request.
        """
        filename = message.get('filename')
        exists = self.storage.file_exists(filename)
        return {'exists': exists}
    
    def _handle_get_file_blocks(self, message: dict, addr: tuple) -> dict:
        """
        Handle GET_FILE_BLOCKS request.
        """
        filename = message.get('filename')
        blocks = self.storage.get_file_blocks(filename)
        
        if blocks is None:
            return {'status': 'error', 'message': 'File not found'}
        
        blocks_data = []
        for block in blocks:
            blocks_data.append({
                'block_id': block.block_id,
                'client_id': block.client_id,
                'sequence_num': block.sequence_num,
                'timestamp': block.timestamp,
                'data': list(block.data),
                'size': block.size
            })
        
        return {'status': 'success', 'blocks': blocks_data}
    
    def _handle_replicate_file(self, message: dict, addr: tuple) -> dict:
        """
        Handle REPLICATE_FILE request.
        """
        filename = message.get('filename')
        blocks_data = message.get('blocks', [])
        replace = message.get('replace', False)
        
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
        
        if replace:
            # Replace existing blocks
            self.storage.replace_file_blocks(filename, blocks)
        else:
            # Create new file
            if blocks:
                self.storage.create_file(filename, blocks[0].data)
                for block in blocks[1:]:
                    self.storage.append_block(filename, block)
        
        return {'status': 'success'}
    
    def _handle_ls(self, message: dict, addr: tuple) -> dict:
        """
        Handle LS request.
        """
        filename = message.get('filename')
        replicas = self.get_replicas_for_file(filename)
        file_id = get_file_id(filename)
        
        replica_info = []
        for node_id in replicas:
            hostname = node_id.split(':')[0]
            ring_pos = self.ring.get_node_position(node_id)
            replica_info.append({'hostname': hostname, 'node_id': node_id, 'ring_position': ring_pos})
        
        return {
            'status': 'success',
            'file_id': file_id,
            'replicas': replica_info
        }
    
    def _handle_liststore(self, message: dict, addr: tuple) -> dict:
        """
        Handle LISTSTORE request.
        """
        files = self.storage.list_files()
        ring_pos = self.ring.get_node_position(self.node_id)
        
        return {
            'status': 'success',
            'node_id': self.node_id,
            'ring_position': ring_pos,
            'files': [{'filename': f, 'file_id': fid} for f, fid in files]
        }
    
    def _handle_get_from_replica(self, message: dict, addr: tuple) -> dict:
        """
        Handle GET_FROM_REPLICA request.
        """
        return self._handle_get(message, addr)
    
    def _handle_list_mem_ids(self, message: dict, addr: tuple) -> dict:
        """
        Handle LIST_MEM_IDS request.
        """
        members = self.membership.membership.get_members()
        member_info = []
        
        for hostname, info in sorted(members.items()):
            if info['status'] in ['ACTIVE', 'JOINING']:
                node_id = f"{hostname}:{HYDFS_PORT}:{info['id'].split('_')[0]}"
                ring_pos = self.ring.get_node_position(node_id)
                member_info.append({
                    'hostname': hostname,
                    'node_id': node_id,
                    'ring_position': ring_pos,
                    'status': info['status']
                })
        
        # Sort by ring position
        member_info.sort(key=lambda x: x['ring_position'] if x['ring_position'] else 0)
        
        return {'status': 'success', 'members': member_info}