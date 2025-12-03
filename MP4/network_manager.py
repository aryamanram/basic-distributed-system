"""
Network Manager - Handles network communication
"""
import socket
import threading
import json
from typing import Dict, Callable, Optional, Any

class NetworkManager:
    """
    Manages network communication for RainStorm.
    """
    
    def __init__(self, port: int, logger):
        self.port = port
        self.logger = logger
        
        self.running = False
        self.server_socket: Optional[socket.socket] = None
        self.server_thread: Optional[threading.Thread] = None
        
        # Message handlers
        self.handlers: Dict[str, Callable] = {}
    
    def register_handler(self, message_type: str, handler: Callable):
        """Register a handler for a message type."""
        self.handlers[message_type] = handler
    
    def start(self):
        """Start the network server."""
        if self.running:
            return
        
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('', self.port))
        self.server_socket.listen(20)
        
        self.server_thread = threading.Thread(target=self._server_loop, daemon=True)
        self.server_thread.start()
        
        self.logger.log(f"Network server started on port {self.port}")
    
    def stop(self):
        """Stop the network server."""
        if not self.running:
            return
        
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        
        self.logger.log("Network server stopped")
    
    def _server_loop(self):
        """Main server loop."""
        while self.running:
            try:
                conn, addr = self.server_socket.accept()
                threading.Thread(
                    target=self._handle_connection,
                    args=(conn, addr),
                    daemon=True
                ).start()
            except Exception as e:
                if self.running:
                    self.logger.log(f"Error accepting connection: {e}")
    
    def _handle_connection(self, conn: socket.socket, addr: tuple):
        """Handle a single connection."""
        try:
            # Receive size header
            size_header = conn.recv(10)
            if not size_header:
                return
            
            size = int(size_header.decode('utf-8'))
            
            # Receive message
            data = b''
            while len(data) < size:
                chunk = conn.recv(min(4096, size - len(data)))
                if not chunk:
                    return
                data += chunk
            
            # Parse message
            message = json.loads(data.decode('utf-8'))
            msg_type = message.get('type', 'UNKNOWN')
            
            # Handle message
            response = {'status': 'error', 'message': 'Unknown message type'}
            
            if msg_type in self.handlers:
                response = self.handlers[msg_type](message, addr)
            
            # Send response
            response_data = json.dumps(response).encode('utf-8')
            size_header = f"{len(response_data):010d}".encode('utf-8')
            conn.sendall(size_header + response_data)
        
        except Exception as e:
            self.logger.log(f"Error handling connection: {e}")
        finally:
            try:
                conn.close()
            except:
                pass
    
    def send_message(self, target_host: str, target_port: int, 
                     message: Dict[str, Any], timeout: float = 10.0) -> Optional[Dict]:
        """Send a message and wait for response."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((target_host, target_port))
            
            # Send message
            message_data = json.dumps(message).encode('utf-8')
            size_header = f"{len(message_data):010d}".encode('utf-8')
            sock.sendall(size_header + message_data)
            
            # Receive response
            size_header = sock.recv(10)
            if not size_header:
                return None
            
            size = int(size_header.decode('utf-8'))
            
            data = b''
            while len(data) < size:
                chunk = sock.recv(min(4096, size - len(data)))
                if not chunk:
                    return None
                data += chunk
            
            response = json.loads(data.decode('utf-8'))
            sock.close()
            
            return response
        
        except socket.timeout:
            self.logger.log(f"Timeout sending to {target_host}:{target_port}")
            return None
        except Exception as e:
            self.logger.log(f"Error sending to {target_host}:{target_port}: {e}")
            return None
    
    def send_to_worker(self, vm_id: int, message: Dict):
        """Send message to a worker by VM ID."""
        # Get hostname from config
        vm_hosts = {
            1: "fa25-cs425-a701.cs.illinois.edu",
            2: "fa25-cs425-a702.cs.illinois.edu",
            3: "fa25-cs425-a703.cs.illinois.edu",
            4: "fa25-cs425-a704.cs.illinois.edu",
            5: "fa25-cs425-a705.cs.illinois.edu",
            6: "fa25-cs425-a706.cs.illinois.edu",
            7: "fa25-cs425-a707.cs.illinois.edu",
            8: "fa25-cs425-a708.cs.illinois.edu",
            9: "fa25-cs425-a709.cs.illinois.edu",
            10: "fa25-cs425-a710.cs.illinois.edu"
        }
        
        hostname = vm_hosts.get(vm_id)
        if hostname:
            return self.send_message(hostname, 8501, message)
        else:
            self.logger.log(f"Invalid VM ID: {vm_id}")
            return None