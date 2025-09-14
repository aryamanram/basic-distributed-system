#!/usr/bin/env python3
"""
Distributed Grep Server
Enhanced version that automatically serves the correct machine.i.log file
"""

import socket
import sys
import signal
import os
import subprocess
import json
from threading import Thread

PORT = 3490
BACKLOG = 10
CONFIG_FILE = 'config.json'

def get_vm_id():
    """
    Determine VM ID from hostname or config file.
    Returns the VM number (1-10) or None if cannot determine.
    """
    hostname = socket.gethostname()
    
    # Try to extract VM number from hostname (e.g., fa25-cs425-a707 -> 7)
    if 'a70' in hostname or 'a71' in hostname:
        # Extract the last digit(s) after 'a70' or 'a71'
        try:
            if 'a70' in hostname:
                vm_num = hostname.split('a70')[1].split('.')[0]
            else:
                vm_num = hostname.split('a71')[1].split('.')[0]
            return int(vm_num)
        except:
            pass
    
    # Fallback: load config and match by hostname
    try:
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
            for vm in config['vms']:
                if vm['hostname'] in hostname or hostname in vm['hostname']:
                    return vm['id']
    except:
        pass
    
    return None

def get_log_filename():
    """Get the appropriate log filename for this VM."""
    vm_id = get_vm_id()
    if vm_id:
        return f"machine.{vm_id}.log"
    else:
        # Default fallback
        return "machine.1.log"

def execute_grep(pattern, filename, grep_options=None):
    """Execute grep command on the specified file."""
    try:
        # Check if file exists
        if not os.path.exists(filename):
            return {
                'success': False,
                'results': [],
                'line_count': 0,
                'filename': filename,
                'error': f'File not found: {filename}'
            }
        
        # Build grep command
        cmd = ['grep']
        
        if grep_options:
            cmd.extend(grep_options)
        
        cmd.extend(['-e', pattern])
        cmd.append(filename)
        
        print(f"Executing: {' '.join(cmd)}")
        
        # Execute grep
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        # Process output
        if result.returncode == 0:
            # Matches found
            output_lines = result.stdout.strip().split('\n') if result.stdout.strip() else []
            line_count = len(output_lines) if output_lines != [''] else 0
        elif result.returncode == 1:
            # No matches found (this is not an error)
            output_lines = []
            line_count = 0
        else:
            # Actual error
            return {
                'success': False,
                'results': [],
                'line_count': 0,
                'filename': filename,
                'error': f'Grep error: {result.stderr}'
            }
        
        return {
            'success': True,
            'results': output_lines,
            'line_count': line_count,
            'filename': filename,
            'error': None
        }
        
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'results': [],
            'line_count': 0,
            'filename': filename,
            'error': 'Grep command timed out'
        }
    except Exception as e:
        return {
            'success': False,
            'results': [],
            'line_count': 0,
            'filename': filename,
            'error': f'Error executing grep: {str(e)}'
        }

def parse_grep_request(request_data):
    """Parse the JSON request from the client."""
    try:
        request = json.loads(request_data)
        
        if 'pattern' not in request:
            raise ValueError("Missing 'pattern' field in request")
        
        pattern = request['pattern']
        
        # Use the filename from request, or default to this VM's log file
        filename = request.get('filename', get_log_filename())
        options = request.get('options', [])
        
        return {
            'pattern': pattern,
            'filename': filename,
            'options': options
        }
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON format")
    except Exception as e:
        raise ValueError(f"Error parsing request: {str(e)}")

def send_response(client_socket, response_data):
    """Send a JSON response back to the client."""
    try:
        response_json = json.dumps(response_data)
        message_length = len(response_json.encode('utf-8'))
        length_header = f"{message_length:010d}".encode('utf-8')
        
        client_socket.send(length_header)
        client_socket.send(response_json.encode('utf-8'))
    except Exception as e:
        print(f"Error sending response: {e}")

def receive_request(client_socket):
    """Receive a complete request from the client."""
    try:
        # Receive length header
        length_data = b''
        client_socket.settimeout(10)  # 10 second timeout for receiving
        
        while len(length_data) < 10:
            chunk = client_socket.recv(10 - len(length_data))
            if not chunk:
                raise ConnectionError("Client disconnected")
            length_data += chunk
        
        message_length = int(length_data.decode('utf-8'))
        
        # Receive actual message
        message_data = b''
        while len(message_data) < message_length:
            chunk = client_socket.recv(min(4096, message_length - len(message_data)))
            if not chunk:
                raise ConnectionError("Client disconnected")
            message_data += chunk
        
        return message_data.decode('utf-8')
    except socket.timeout:
        raise ConnectionError("Request timeout")
    except Exception as e:
        raise ConnectionError(f"Error receiving request: {e}")

def handle_client(client_socket, client_address):
    """Handle individual client connection."""
    try:
        print(f"Connection from {client_address[0]}:{client_address[1]}")
        
        # Receive request
        request_data = receive_request(client_socket)
        print(f"Received request: {request_data[:100]}...")  # Print first 100 chars
        
        # Parse request
        try:
            request = parse_grep_request(request_data)
            
            # Execute grep
            result = execute_grep(
                request['pattern'],
                request['filename'],
                request['options']
            )
            
            # Send response
            send_response(client_socket, result)
            
            if result['success']:
                print(f"Grep completed: {result['line_count']} matches in {result['filename']}")
            else:
                print(f"Grep failed: {result['error']}")
                
        except ValueError as e:
            error_response = {
                'success': False,
                'results': [],
                'line_count': 0,
                'filename': '',
                'error': str(e)
            }
            send_response(client_socket, error_response)
            print(f"Invalid request: {e}")
        
    except Exception as e:
        print(f"Error handling client: {e}")
        try:
            error_response = {
                'success': False,
                'results': [],
                'line_count': 0,
                'filename': '',
                'error': f'Server error: {str(e)}'
            }
            send_response(client_socket, error_response)
        except:
            pass
    finally:
        client_socket.close()
        print(f"Connection closed: {client_address[0]}:{client_address[1]}")

def signal_handler(sig, frame):
    """Handle SIGINT (Ctrl+C) gracefully."""
    print("\nShutting down server...")
    sys.exit(0)

def main():
    """Main server function."""
    signal.signal(signal.SIGINT, signal_handler)
    
    # Determine VM ID and log file
    vm_id = get_vm_id()
    log_file = get_log_filename()
    
    print(f"{'='*60}")
    print(f"Distributed Grep Server")
    print(f"VM ID: {vm_id if vm_id else 'Unknown'}")
    print(f"Default log file: {log_file}")
    print(f"Port: {PORT}")
    print(f"{'='*60}\n")
    
    # Check if log file exists
    if not os.path.exists(log_file):
        print(f"Warning: Log file '{log_file}' not found in current directory")
        print("Server will report errors if clients request this file\n")
    
    try:
        # Create and configure socket
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Bind to all interfaces
        server_socket.bind(('', PORT))
        server_socket.listen(BACKLOG)
        
        print(f"Server listening on all interfaces, port {PORT}")
        print("Waiting for grep requests...\n")
        
        while True:
            try:
                client_socket, client_address = server_socket.accept()
                
                # Handle each client in a separate thread
                client_thread = Thread(
                    target=handle_client,
                    args=(client_socket, client_address)
                )
                client_thread.daemon = True
                client_thread.start()
                
            except socket.error as e:
                print(f"Accept error: {e}")
                continue
                
    except socket.error as e:
        print(f"Socket error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        if 'server_socket' in locals():
            server_socket.close()

if __name__ == "__main__":
    main()