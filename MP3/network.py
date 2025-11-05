"""
Network Manager
Handles client/server communication
"""

import socket
import json
import threading

class NetworkManager:
    def __init__(self, node_address, port):
        self.address = node_address
        self.port = port
        self.server_socket = None
        self.server_thread = None
        self.running = False
        
        # Bandwidth tracking
        self.bytes_sent = 0
        self.bytes_received = 0
    
    def start_server(self):
        """Start server to receive requests"""
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('0.0.0.0', self.port))
        self.server_socket.listen(5)
        
        self.server_thread = threading.Thread(target=self._server_loop)
        self.server_thread.daemon = True
        self.server_thread.start()
        
        print(f"Server started on port {self.port}")
    
    def stop_server(self):
        """Stop server"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        if self.server_thread:
            self.server_thread.join()
    
    def _server_loop(self):
        """Server loop"""
        while self.running:
            try:
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self._handle_connection, args=(conn, addr)).start()
            except Exception as e:
                if self.running:
                    print(f"Server error: {e}")
    
    def _handle_connection(self, conn, addr):
        """Handle incoming connection"""
        try:
            # Receive message
            data = self._receive_message(conn)
            self.bytes_received += len(data)
            
            message = json.loads(data.decode())
            
            # Handle message
            response = self.handle_request(message)
            
            # Send response
            response_data = json.dumps(response).encode()
            self._send_message(conn, response_data)
            self.bytes_sent += len(response_data)
        
        except Exception as e:
            print(f"Connection error: {e}")
        finally:
            conn.close()
    
    def send_request(self, address, message):
        """Send request to another node"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((address, self.port))
            
            # Send message
            message_data = json.dumps(message).encode()
            self._send_message(sock, message_data)
            self.bytes_sent += len(message_data)
            
            # Receive response
            response_data = self._receive_message(sock)
            self.bytes_received += len(response_data)
            
            response = json.loads(response_data.decode())
            
            sock.close()
            return response
        
        except Exception as e:
            print(f"Send error to {address}: {e}")
            return None
    
    def handle_request(self, message):
        """Handle incoming request"""
        msg_type = message.get('type')
        
        # Dispatch to appropriate handler
        if msg_type == 'CREATE_REQUEST':
            return self.node.replication.handle_create_request(message)
        elif msg_type == 'GET_REQUEST':
            return self.node.replication.handle_get_request(message)
        elif msg_type == 'APPEND_REQUEST':
            return self.node.replication.handle_append_request(message)
        elif msg_type == 'MERGE_REQUEST':
            return self.node.consistency.handle_merge_request(message)
        else:
            return {'success': False, 'error': f'Unknown message type: {msg_type}'}
    
    def _send_message(self, sock, data):
        """Send message with length prefix"""
        # Send length prefix (4 bytes)
        length = len(data)
        sock.sendall(length.to_bytes(4, byteorder='big'))
        
        # Send data
        sock.sendall(data)
    
    def _receive_message(self, sock):
        """Receive message with length prefix"""
        # Receive length prefix
        length_bytes = sock.recv(4)
        if not length_bytes:
            return b''
        
        length = int.from_bytes(length_bytes, byteorder='big')
        
        # Receive data
        data = b''
        while len(data) < length:
            chunk = sock.recv(min(4096, length - len(data)))
            if not chunk:
                break
            data += chunk
        
        return data
    
    def measure_bandwidth(self):
        """Get bandwidth statistics"""
        return {
            'bytes_sent': self.bytes_sent,
            'bytes_received': self.bytes_received,
            'total': self.bytes_sent + self.bytes_received
        }