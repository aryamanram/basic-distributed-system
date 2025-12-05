"""
RainStorm Network Layer
- Leader <-> Worker RPC
- Task <-> Task tuple streaming
- Acknowledgment transmission
"""
import socket
import threading
import time
import json
from typing import Dict, Any, Optional, Callable, List, Tuple
from utils import (
    serialize_message, deserialize_message, 
    send_sized_message, recv_sized_message,
    StreamTuple, RainStormLogger
)

# ============================================================================
# RPC Server
# ============================================================================

class RPCServer:
    """
    Generic RPC server for handling incoming requests.
    Used by Leader and Worker nodes.
    """
    def __init__(self, port: int, logger: RainStormLogger):
        self.port = port
        self.logger = logger
        self.handlers: Dict[str, Callable] = {}
        self.running = False
        self.server_socket: Optional[socket.socket] = None
        self.server_thread: Optional[threading.Thread] = None
    
    def register_handler(self, message_type: str, handler: Callable):
        """
        Register a handler for a message type.
        Handler signature: handler(message: Dict, addr: Tuple) -> Dict
        """
        self.handlers[message_type] = handler
    
    def start(self):
        """Start the RPC server."""
        if self.running:
            return
        
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('', self.port))
        self.server_socket.listen(50)
        self.server_socket.settimeout(1.0)
        
        self.server_thread = threading.Thread(target=self._server_loop, daemon=True)
        self.server_thread.start()
        
        self.logger.log(f"RPC Server started on port {self.port}")
    
    def stop(self):
        """Stop the RPC server."""
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        if self.server_thread:
            self.server_thread.join(timeout=2.0)
        self.logger.log("RPC Server stopped")
    
    def _server_loop(self):
        """Main server loop."""
        while self.running:
            try:
                conn, addr = self.server_socket.accept()
                thread = threading.Thread(
                    target=self._handle_connection,
                    args=(conn, addr),
                    daemon=True
                )
                thread.start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    self.logger.log(f"RPC Server error: {e}")
    
    def _handle_connection(self, conn: socket.socket, addr: Tuple):
        """Handle incoming connection."""
        try:
            data = recv_sized_message(conn, timeout=30.0)
            if not data:
                return
            
            message = deserialize_message(data)
            msg_type = message.get('type', 'UNKNOWN')
            
            if msg_type in self.handlers:
                response = self.handlers[msg_type](message, addr)
                if response:
                    send_sized_message(conn, serialize_message(response))
            else:
                self.logger.log(f"Unknown message type: {msg_type}")
                response = {'status': 'error', 'message': f'Unknown type: {msg_type}'}
                send_sized_message(conn, serialize_message(response))
        except Exception as e:
            self.logger.log(f"RPC handle error: {e}")
        finally:
            try:
                conn.close()
            except:
                pass

# ============================================================================
# RPC Client
# ============================================================================

class RPCClient:
    """
    RPC client for sending requests to other nodes.
    """
    def __init__(self, logger: RainStormLogger):
        self.logger = logger
    
    def send(self, host: str, port: int, message: Dict, 
             timeout: float = 10.0) -> Optional[Dict]:
        """Send a message and wait for response."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((host, port))
            
            send_sized_message(sock, serialize_message(message))
            
            response_data = recv_sized_message(sock, timeout)
            sock.close()
            
            if response_data:
                return deserialize_message(response_data)
            return None
        except socket.timeout:
            self.logger.log(f"RPC timeout to {host}:{port}")
            return None
        except Exception as e:
            self.logger.log(f"RPC error to {host}:{port}: {e}")
            return None
    
    def send_async(self, host: str, port: int, message: Dict,
                   callback: Callable = None):
        """Send message asynchronously."""
        def _send():
            response = self.send(host, port, message)
            if callback:
                callback(response)
        
        thread = threading.Thread(target=_send, daemon=True)
        thread.start()
    
    def broadcast(self, targets: List[Tuple[str, int]], message: Dict,
                  timeout: float = 10.0) -> Dict[str, Any]:
        """Send to multiple targets and collect responses."""
        responses = {}
        threads = []
        lock = threading.Lock()
        
        def _send_to(host, port):
            response = self.send(host, port, message, timeout)
            with lock:
                responses[f"{host}:{port}"] = response
        
        for host, port in targets:
            t = threading.Thread(target=_send_to, args=(host, port), daemon=True)
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join(timeout=timeout + 1)
        
        return responses

# ============================================================================
# Tuple Channel - For streaming tuples between tasks
# ============================================================================

class TupleChannel:
    """
    Channel for streaming tuples between tasks.
    Handles buffering, reliability, and acknowledgments.
    """
    def __init__(self, task_id: str, logger: RainStormLogger):
        self.task_id = task_id
        self.logger = logger
        
        # Outgoing connections: target_task_id -> (host, port)
        self.downstream_targets: Dict[str, Tuple[str, int]] = {}
        
        # Incoming buffer
        self.incoming_buffer: List[StreamTuple] = []
        self.buffer_lock = threading.Lock()
        self.buffer_event = threading.Event()
        
        # Pending acks: tuple_id -> (tuple, send_time, retries)
        self.pending_acks: Dict[str, Tuple[StreamTuple, float, int]] = {}
        self.ack_lock = threading.Lock()
        
        # ACK timeout and retry settings
        self.ack_timeout = 5.0
        self.max_retries = 3
        
        # Server for receiving tuples
        self.server_socket: Optional[socket.socket] = None
        self.running = False
    
    def add_downstream(self, task_id: str, host: str, port: int):
        """Add a downstream task to send tuples to."""
        self.downstream_targets[task_id] = (host, port)
    
    def start_receiver(self, port: int):
        """Start receiving tuples on given port."""
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('', port))
        self.server_socket.listen(50)
        self.server_socket.settimeout(1.0)
        
        thread = threading.Thread(target=self._receive_loop, daemon=True)
        thread.start()
        
        # Start ack checker thread
        ack_thread = threading.Thread(target=self._ack_checker_loop, daemon=True)
        ack_thread.start()
    
    def stop_receiver(self):
        """Stop receiving tuples."""
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
    
    def _receive_loop(self):
        """Loop to receive incoming tuples."""
        while self.running:
            try:
                conn, addr = self.server_socket.accept()
                thread = threading.Thread(
                    target=self._handle_incoming,
                    args=(conn, addr),
                    daemon=True
                )
                thread.start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    self.logger.log(f"Tuple receive error: {e}")
    
    def _handle_incoming(self, conn: socket.socket, addr: Tuple):
        """Handle incoming tuple or ack."""
        try:
            data = recv_sized_message(conn, timeout=10.0)
            if not data:
                return
            
            message = deserialize_message(data)
            msg_type = message.get('type')
            
            if msg_type == 'TUPLE':
                # Received a tuple
                tuple_data = StreamTuple.from_dict(message['tuple'])
                
                with self.buffer_lock:
                    self.incoming_buffer.append(tuple_data)
                    self.buffer_event.set()
                
                # Send ack back
                ack = {'type': 'ACK', 'tuple_id': tuple_data.tuple_id}
                send_sized_message(conn, serialize_message(ack))
            
            elif msg_type == 'ACK':
                # Received an ack
                tuple_id = message.get('tuple_id')
                with self.ack_lock:
                    if tuple_id in self.pending_acks:
                        del self.pending_acks[tuple_id]
            
        except Exception as e:
            self.logger.log(f"Handle incoming error: {e}")
        finally:
            try:
                conn.close()
            except:
                pass
    
    def send_tuple(self, tuple_data: StreamTuple, target_task_id: str,
                   wait_ack: bool = True) -> bool:
        """Send a tuple to a downstream task."""
        if target_task_id not in self.downstream_targets:
            self.logger.log(f"Unknown target task: {target_task_id}")
            return False
        
        host, port = self.downstream_targets[target_task_id]
        
        message = {
            'type': 'TUPLE',
            'tuple': tuple_data.to_dict()
        }
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.ack_timeout)
            sock.connect((host, port))
            
            send_sized_message(sock, serialize_message(message))
            
            if wait_ack:
                # Wait for ack
                ack_data = recv_sized_message(sock, timeout=self.ack_timeout)
                sock.close()
                
                if ack_data:
                    ack = deserialize_message(ack_data)
                    if ack.get('tuple_id') == tuple_data.tuple_id:
                        return True
                
                # No ack received - add to pending
                with self.ack_lock:
                    self.pending_acks[tuple_data.tuple_id] = (
                        tuple_data, time.time(), 0
                    )
                return False
            else:
                sock.close()
                return True
        
        except Exception as e:
            self.logger.log(f"Send tuple error to {host}:{port}: {e}")
            if wait_ack:
                with self.ack_lock:
                    self.pending_acks[tuple_data.tuple_id] = (
                        tuple_data, time.time(), 0
                    )
            return False
    
    def _ack_checker_loop(self):
        """Check for unacked tuples and retry."""
        while self.running:
            try:
                time.sleep(1.0)
                self._retry_unacked()
            except Exception as e:
                self.logger.log(f"Ack checker error: {e}")
    
    def _retry_unacked(self):
        """Retry sending unacked tuples."""
        now = time.time()
        to_retry = []
        
        with self.ack_lock:
            for tuple_id, (tuple_data, send_time, retries) in list(self.pending_acks.items()):
                if now - send_time > self.ack_timeout:
                    if retries < self.max_retries:
                        to_retry.append((tuple_id, tuple_data, retries + 1))
                    else:
                        self.logger.log(f"Max retries reached for tuple {tuple_id}")
                        del self.pending_acks[tuple_id]
        
        for tuple_id, tuple_data, retries in to_retry:
            # Find target and retry
            for target_id in self.downstream_targets:
                success = self.send_tuple(tuple_data, target_id, wait_ack=True)
                if success:
                    with self.ack_lock:
                        if tuple_id in self.pending_acks:
                            del self.pending_acks[tuple_id]
                    break
                else:
                    with self.ack_lock:
                        self.pending_acks[tuple_id] = (tuple_data, time.time(), retries)
    
    def receive_tuple(self, timeout: float = None) -> Optional[StreamTuple]:
        """Receive a tuple from the buffer."""
        if timeout:
            self.buffer_event.wait(timeout)
        
        with self.buffer_lock:
            if self.incoming_buffer:
                tuple_data = self.incoming_buffer.pop(0)
                if not self.incoming_buffer:
                    self.buffer_event.clear()
                return tuple_data
        return None
    
    def has_pending_acks(self) -> bool:
        """Check if there are pending acks."""
        with self.ack_lock:
            return len(self.pending_acks) > 0

# ============================================================================
# Connection Pool (for efficiency)
# ============================================================================

class ConnectionPool:
    """
    Pool of connections for reuse.
    """
    def __init__(self, max_connections: int = 10):
        self.max_connections = max_connections
        self.connections: Dict[str, List[socket.socket]] = {}
        self.lock = threading.Lock()
    
    def get_connection(self, host: str, port: int) -> socket.socket:
        """Get a connection from pool or create new."""
        key = f"{host}:{port}"
        
        with self.lock:
            if key in self.connections and self.connections[key]:
                return self.connections[key].pop()
        
        # Create new connection
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        return sock
    
    def return_connection(self, host: str, port: int, sock: socket.socket):
        """Return a connection to the pool."""
        key = f"{host}:{port}"
        
        with self.lock:
            if key not in self.connections:
                self.connections[key] = []
            
            if len(self.connections[key]) < self.max_connections:
                self.connections[key].append(sock)
            else:
                try:
                    sock.close()
                except:
                    pass
    
    def close_all(self):
        """Close all pooled connections."""
        with self.lock:
            for conns in self.connections.values():
                for sock in conns:
                    try:
                        sock.close()
                    except:
                        pass
            self.connections.clear()