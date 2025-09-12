#!/usr/bin/env python3
"""
Distributed Grep Server
Receives grep commands from clients and executes them on local log files
"""

import socket
import sys
import signal
import os
import subprocess
import json
from threading import Thread

PORT = 3490  # the port users will be connecting to
BACKLOG = 10  # how many pending connections queue will hold

def execute_grep(pattern, filename, grep_options=None):
    """
    Execute grep command on the specified file with the given pattern.
    
    Args:
        pattern (str): The regex pattern to search for
        filename (str): The log file to search in
        grep_options (list): Additional grep options (e.g., ['-i', '-n'])
    
    Returns:
        dict: Contains 'success', 'results', 'line_count', 'filename', and 'error' fields
    """
    try:
        # Build the grep command
        cmd = ['grep']
        
        # Add options if provided
        if grep_options:
            cmd.extend(grep_options)
        
        # Add the pattern (using -e flag for safety with special characters)
        cmd.extend(['-e', pattern])
        
        # Add filename
        cmd.append(filename)
        
        print(f"Executing: {' '.join(cmd)}")
        
        # Execute the grep command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30  # 30 second timeout
        )
        
        # Get the output
        output_lines = result.stdout.strip().split('\n') if result.stdout.strip() else []
        line_count = len(output_lines) if output_lines != [''] else 0
        
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
    except FileNotFoundError:
        return {
            'success': False,
            'results': [],
            'line_count': 0,
            'filename': filename,
            'error': f'File not found: {filename}'
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
    """
    Parse the JSON request from the client.
    
    Args:
        request_data (str): JSON string containing grep request
    
    Returns:
        dict: Parsed request with 'pattern', 'filename', and 'options' keys
    """
    try:
        request = json.loads(request_data)
        
        # Validate required fields
        if 'pattern' not in request:
            raise ValueError("Missing 'pattern' field in request")
        
        # Set defaults
        pattern = request['pattern']
        filename = request.get('filename', 'machine.1.log')  # Default to machine.1.log
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
    """
    Send a JSON response back to the client.
    
    Args:
        client_socket: The client socket to send data to
        response_data (dict): The response data to send
    """
    try:
        # Convert response to JSON and encode
        response_json = json.dumps(response_data)
        
        # Send the length of the message first (for proper message framing)
        message_length = len(response_json.encode('utf-8'))
        length_header = f"{message_length:010d}".encode('utf-8')  # 10-digit length
        
        # Send length header followed by the actual message
        client_socket.send(length_header)
        client_socket.send(response_json.encode('utf-8'))
        
    except Exception as e:
        print(f"Error sending response: {e}")

def receive_request(client_socket):
    """
    Receive a complete request from the client.
    
    Args:
        client_socket: The client socket to receive data from
    
    Returns:
        str: The received request data
    """
    try:
        # First, receive the 10-byte length header
        length_data = b''
        while len(length_data) < 10:
            chunk = client_socket.recv(10 - len(length_data))
            if not chunk:
                raise ConnectionError("Client disconnected")
            length_data += chunk
        
        # Parse the message length
        message_length = int(length_data.decode('utf-8'))
        
        # Now receive the actual message
        message_data = b''
        while len(message_data) < message_length:
            chunk = client_socket.recv(message_length - len(message_data))
            if not chunk:
                raise ConnectionError("Client disconnected")
            message_data += chunk
        
        return message_data.decode('utf-8')
        
    except Exception as e:
        raise ConnectionError(f"Error receiving request: {e}")

def handle_client(client_socket, client_address):
    """
    Handle individual client connection.
    Receives grep requests and sends back results.
    
    Args:
        client_socket: The socket connection to the client
        client_address: The client's address (IP, port)
    """
    try:
        print(f"server: got connection from {client_address[0]}")
        
        # Receive the request from client
        request_data = receive_request(client_socket)
        print(f"Received request: {request_data}")
        
        # Parse the grep request
        try:
            request = parse_grep_request(request_data)
            
            # Execute the grep command
            result = execute_grep(
                request['pattern'], 
                request['filename'], 
                request['options']
            )
            
            # Send the result back to client
            send_response(client_socket, result)
            
            # Log the result
            if result['success']:
                print(f"Grep completed: found {result['line_count']} matches")
            else:
                print(f"Grep failed: {result['error']}")
                
        except ValueError as e:
            # Send error response for invalid requests
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
            # Try to send an error response
            error_response = {
                'success': False,
                'results': [],
                'line_count': 0,
                'filename': '',
                'error': f'Server error: {str(e)}'
            }
            send_response(client_socket, error_response)
        except:
            pass  # If we can't send error response, just close connection
    finally:
        client_socket.close()
        print(f"Connection with {client_address[0]} closed")

def signal_handler(sig, frame):
    """
    Handle SIGINT (Ctrl+C) gracefully.
    
    Args:
        sig: Signal number
        frame: Current stack frame
    """
    print("\nShutting down server...")
    sys.exit(0)

def main():
    """
    Main server function. Sets up the socket server and handles connections.
    """
    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    # Create socket
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Allow socket reuse (prevents "Address already in use" errors)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Bind to address and port
        server_socket.bind(('', PORT))
        
        # Listen for connections
        server_socket.listen(BACKLOG)
        
        print(f"Distributed Grep Server listening on port {PORT}")
        print("Waiting for grep requests...")
        
        while True:
            try:
                # Accept connection
                client_socket, client_address = server_socket.accept()
                
                # Handle client in a separate thread
                client_thread = Thread(
                    target=handle_client, 
                    args=(client_socket, client_address)
                )
                client_thread.daemon = True  # Thread will exit when main program exits
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