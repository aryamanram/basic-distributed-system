"""
Network communication for HyDFS
"""
import socket
import threading
import time
from typing import Dict, Any, Optional, Callable
from utils import serialize_message, deserialize_message, send_sized_message, recv_sized_message

class NetworkManager:
    """
    Manages network communication for HyDFS operations.
    """
    def __init__(self, hostname: str, port: int, logger):
        self.hostname = hostname
        self.port = port
        self.logger = logger
        
        self.running = False
        self.server_socket: Optional[socket.socket] = None
        self.server_thread: Optional[threading.Thread] = None
        
        # Message handlers: message_type -> handler_function
        self.handlers: Dict[str, Callable] = {}
        
    def register_handler(self, message_type: str, handler: Callable):
        """
        Register a handler for a specific message type.
        Handler signature: handler(message: dict, addr: tuple) -> dict
        """
        self.handlers[message_type] = handler
    
    def start(self):
        """
        Start the network server.
        """
        if self.running:
            return
        
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('', self.port))
        self.server_socket.listen(20)
        
        self.server_thread = threading.Thread(target=self._server_loop, daemon=True)
        self.server_thread.start()
        
        self.logger.log(f"NETWORK: Server started on port {self.port}")
    
    def stop(self):
        """
        Stop the network server.
        """
        if not self.running:
            return
        
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        
        self.logger.log("NETWORK: Server stopped")
    
    def _server_loop(self):
        """
        Main server loop to accept connections.
        """
        while self.running:
            try:
                conn, addr = self.server_socket.accept()
                thread = threading.Thread(
                    target=self._handle_connection,
                    args=(conn, addr),
                    daemon=True
                )
                thread.start()
            except Exception as e:
                if self.running:
                    self.logger.log(f"NETWORK ERROR: Accept failed: {e}")
    
    def _handle_connection(self, conn: socket.socket, addr: tuple):
        """
        Handle a single client connection.
        """
        try:
            # Receive message
            data = recv_sized_message(conn)
            if not data:
                return
            
            message = deserialize_message(data)
            msg_type = message.get('type', 'UNKNOWN')
            
            # Log received message
            self.logger.log(f"NETWORK: Received {msg_type} from {addr[0]}")
            
            # Handle message
            if msg_type in self.handlers:
                response = self.handlers[msg_type](message, addr)
                
                # Send response
                if response:
                    response_data = serialize_message(response)
                    send_sized_message(conn, response_data)
                    self.logger.log(f"NETWORK: Sent response for {msg_type} to {addr[0]}")
            else:
                self.logger.log(f"NETWORK WARNING: No handler for message type {msg_type}")
                response = {'status': 'error', 'message': f'Unknown message type: {msg_type}'}
                response_data = serialize_message(response)
                send_sized_message(conn, response_data)
        
        except Exception as e:
            self.logger.log(f"NETWORK ERROR: Handle connection: {e}")
        finally:
            try:
                conn.close()
            except:
                pass
    
    def send_message(self, target_host: str, target_port: int, 
                     message: Dict[str, Any], timeout: float = 10.0) -> Optional[Dict[str, Any]]:
        """
        Send a message to a target node and wait for response.
        Returns the response or None on failure.
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            
            sock.connect((target_host, target_port))
            
            # Send message
            message_data = serialize_message(message)
            send_sized_message(sock, message_data)
            
            # Receive response
            response_data = recv_sized_message(sock)
            if not response_data:
                return None
            
            response = deserialize_message(response_data)
            
            sock.close()
            return response
        
        except socket.timeout:
            self.logger.log(f"NETWORK ERROR: Timeout sending to {target_host}:{target_port}")
            return None
        except Exception as e:
            self.logger.log(f"NETWORK ERROR: Send to {target_host}:{target_port}: {e}")
            return None
    
    def send_message_async(self, target_host: str, target_port: int, 
                          message: Dict[str, Any], callback: Optional[Callable] = None):
        """
        Send a message asynchronously without waiting for response.
        """
        def _send():
            response = self.send_message(target_host, target_port, message)
            if callback:
                callback(response)
        
        thread = threading.Thread(target=_send, daemon=True)
        thread.start()
    
    def broadcast_message(self, targets: list, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Broadcast a message to multiple targets and collect responses.
        Returns a dict of {target_host: response}.
        """
        responses = {}
        threads = []
        lock = threading.Lock()
        
        def _send_to_target(target_host, target_port):
            response = self.send_message(target_host, target_port, message)
            with lock:
                responses[target_host] = response
        
        for target_host, target_port in targets:
            thread = threading.Thread(
                target=_send_to_target,
                args=(target_host, target_port),
                daemon=True
            )
            thread.start()
            threads.append(thread)
        
        # Wait for all threads with timeout
        for thread in threads:
            thread.join(timeout=15.0)
        
        return responses