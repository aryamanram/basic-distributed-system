"""
HyDFS Node - Core implementation with Control Server
"""
import os
import sys
import socket
import time
import threading
import json
from typing import List, Optional, Dict, Any

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from MP2.membership import MembershipService
from ring import ConsistentHashRing
from storage import FileStorage, FileBlock
from consistency import ConsistencyManager, MergeCoordinator
from network import NetworkManager
from replication import ReplicationManager, REPLICATION_FACTOR
from utils import Logger, get_node_id, get_file_id

HYDFS_PORT = 7000
CONTROL_PORT = 9090

class HyDFSNode:
    """
    Main HyDFS node that integrates all components with control server.
    """
    def __init__(self, vm_id: int):
        self.vm_id = vm_id
        self.hostname = socket.gethostname()
        self.port = HYDFS_PORT
        self.control_port = CONTROL_PORT
        
        # Generate node ID with timestamp
        self.node_id = get_node_id(self.hostname, self.port)
        
        # Initialize logger
        self.logger = Logger(vm_id)
        
        # Initialize storage
        storage_dir = f"hydfs_storage_{vm_id}"
        self.storage = FileStorage(storage_dir)
        
        # Initialize ring
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
        
        # Initialize MP2 membership (for failure detection)
        self.membership = MembershipService(vm_id)
        
        # Track active nodes
        self.active_nodes_lock = threading.Lock()
        
        # Running flag
        self.running = False
        
        # Control thread
        self.control_thread: Optional[threading.Thread] = None
        
        self.logger.log(f"INIT: HyDFS Node {vm_id} initialized")
        self.logger.log(f"INIT: Node ID: {self.node_id}")
        self.logger.log(f"INIT: Ring position: {self.ring.get_node_position(self.node_id)}")
    
    def start(self):
        """
        Start the HyDFS node.
        """
        self.running = True
        
        # Start network server
        self.network.start()
        
        # Start membership service
        self.membership.start()
        
        # Start replication monitoring
        self.replication.start_monitoring()
        
        # Start control server
        self.start_control_server()
        
        # Monitor membership changes
        threading.Thread(target=self._monitor_membership, daemon=True).start()
        
        self.logger.log("NODE: Started")
    
    def stop(self):
        """
        Stop the HyDFS node.
        """
        self.running = False
        self.replication.stop_monitoring()
        self.network.stop()
        self.membership.stop()
        self.logger.log("NODE: Stopped")
    
    def start_control_server(self):
        """
        Start the control server for receiving commands from controller.
        """
        self.control_thread = threading.Thread(target=self._control_loop, daemon=True)
        self.control_thread.start()
        self.logger.log(f"CONTROL: Listening on port {self.control_port}")
    
    def _control_loop(self):
        """
        Control server loop for receiving commands from controller.
        """
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("", self.control_port))
        srv.listen(8)
        
        while self.running:
            conn = None
            try:
                conn, addr = srv.accept()
                
                # Receive command
                size_header = conn.recv(10)
                if not size_header:
                    conn.close()
                    continue
                
                size = int(size_header.decode('utf-8'))
                command_data = b''
                while len(command_data) < size:
                    chunk = conn.recv(min(4096, size - len(command_data)))
                    if not chunk:
                        break
                    command_data += chunk
                
                command = json.loads(command_data.decode('utf-8'))
                
                # Handle command (don't log to avoid clutter)
                response = self._handle_control_command(command)
                
                # Send response
                response_data = json.dumps(response).encode('utf-8')
                size_header = f"{len(response_data):010d}".encode('utf-8')
                conn.sendall(size_header + response_data)
                
            except Exception as e:
                if self.running:
                    self.logger.log(f"CONTROL ERROR: {e}")
                    import traceback
                    self.logger.log(traceback.format_exc())
                    
                    # Try to send error response
                    if conn:
                        try:
                            error_response = {'status': 'error', 'message': str(e)}
                            response_data = json.dumps(error_response).encode('utf-8')
                            size_header = f"{len(response_data):010d}".encode('utf-8')
                            conn.sendall(size_header + response_data)
                        except:
                            pass
            finally:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
    
    def _handle_control_command(self, command: dict) -> dict:
        """
        Handle a command from the controller.
        """
        try:
            cmd_type = command.get('type')
            
            if not cmd_type:
                return {'status': 'error', 'message': 'No command type specified'}
            
            # Handle special controller commands
            if cmd_type == 'APPEND':
                # Controller-based append with client generation
                filename = command.get('filename')
                if not filename:
                    return {'status': 'error', 'message': 'No filename specified'}
                
                data = bytes(command.get('data', []))
                
                # Generate client ID
                client_id = f"{self.hostname}_{os.getpid()}_{time.time()}"
                
                # Get sequence number
                seq_num = self.consistency.get_next_sequence(client_id)
                
                # Create block
                block_id = f"{get_file_id(filename)}_{client_id}_{seq_num}"
                block = FileBlock(
                    block_id=block_id,
                    client_id=client_id,
                    sequence_num=seq_num,
                    timestamp=time.time(),
                    data=data,
                    size=len(data)
                )
                
                # Get replicas
                replicas = self.get_replicas_for_file(filename)
                
                if not replicas:
                    return {'status': 'error', 'message': 'No replicas available'}
                
                # Send to all replicas
                success_count = 0
                for node_id in replicas:
                    try:
                        hostname = node_id.split(':')[0]
                        port = int(node_id.split(':')[1])
                        
                        msg = {
                            'type': 'APPEND',
                            'filename': filename,
                            'client_id': client_id,
                            'block': {
                                'block_id': block.block_id,
                                'client_id': block.client_id,
                                'sequence_num': block.sequence_num,
                                'timestamp': block.timestamp,
                                'data': list(block.data),
                                'size': block.size
                            }
                        }
                        
                        response = self.network.send_message(hostname, port, msg)
                        if response and response.get('status') == 'success':
                            success_count += 1
                    except:
                        pass
                
                return {
                    'status': 'success',
                    'message': f'Appended to {success_count}/{len(replicas)} replicas'
                }
            
            elif cmd_type == 'CREATE':
                # Controller-based create
                filename = command.get('filename')
                if not filename:
                    return {'status': 'error', 'message': 'No filename specified'}
                
                data = bytes(command.get('data', []))
                
                # Get replicas
                replicas = self.get_replicas_for_file(filename)
                
                if not replicas:
                    return {'status': 'error', 'message': 'No replicas available'}
                
                # Send to all replicas
                success_count = 0
                msg = {
                    'type': 'CREATE',
                    'filename': filename,
                    'data': list(data)
                }
                
                for node_id in replicas:
                    try:
                        hostname = node_id.split(':')[0]
                        port = int(node_id.split(':')[1])
                        
                        response = self.network.send_message(hostname, port, msg)
                        if response and response.get('status') == 'success':
                            success_count += 1
                    except:
                        pass
                
                return {
                    'status': 'success',
                    'message': f'Created on {success_count}/{len(replicas)} replicas'
                }
            
            # Delegate to existing handlers
            elif cmd_type in self.network.handlers:
                try:
                    # Create a fake address tuple
                    response = self.network.handlers[cmd_type](command, ('controller', 0))
                    
                    # Ensure response is a dict
                    if not isinstance(response, dict):
                        return {'status': 'error', 'message': f'Handler returned invalid response type: {type(response)}'}
                    
                    return response
                except Exception as handler_error:
                    import traceback
                    error_msg = f'Handler error: {str(handler_error)}\n{traceback.format_exc()}'
                    self.logger.log(f"CONTROL HANDLER ERROR for {cmd_type}: {error_msg}")
                    return {'status': 'error', 'message': error_msg}
            
            return {'status': 'error', 'message': f'Unknown command type: {cmd_type}'}
        
        except Exception as e:
            import traceback
            error_msg = f'{str(e)}\n{traceback.format_exc()}'
            self.logger.log(f"CONTROL ERROR: {error_msg}")
            return {'status': 'error', 'message': error_msg}
    
    def _monitor_membership(self):
        """
        Monitor membership list and update ring accordingly.
        """
        known_members = set()
        
        while self.running:
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
        filename = message.get('filename')
        data = bytes(message.get('data', []))
        
        # Create file locally
        success, msg = self.storage.create_file(filename, data)
        
        return {'status': 'success' if success else 'error', 'message': msg}
    
    def _handle_get(self, message: dict, addr: tuple) -> dict:
        """
        Handle GET request.
        """
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
            return {'status': 'error', 'message': 'File not found'}
        
        return {
            'status': 'success',
            'data': list(data),
            'size': len(data)
        }
    
    def _handle_append(self, message: dict, addr: tuple) -> dict:
        """
        Handle APPEND request.
        """
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
        
        return {'status': 'success' if success else 'error', 'message': msg}
    
    def _handle_merge(self, message: dict, addr: tuple) -> dict:
        """
        Handle MERGE request.
        """
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