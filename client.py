#!/usr/bin/env python3
"""
Distributed Grep Client
Sends grep commands to remote servers and displays results
"""

import socket
import sys
import json
import argparse

PORT = 3490  # the port client will be connecting to
MAXDATASIZE = 4096  # max number of bytes we can get at once

def send_request(client_socket, request_data):
    """
    Send a JSON request to the server.
    
    Args:
        client_socket: The socket connection to the server
        request_data (dict): The request data to send
    """
    try:
        # Convert request to JSON and encode
        request_json = json.dumps(request_data)
        
        # Send the length of the message first (for proper message framing)
        message_length = len(request_json.encode('utf-8'))
        length_header = f"{message_length:010d}".encode('utf-8')  # 10-digit length
        
        # Send length header followed by the actual message
        client_socket.send(length_header)
        client_socket.send(request_json.encode('utf-8'))
        
    except Exception as e:
        raise ConnectionError(f"Error sending request: {e}")

def receive_response(client_socket):
    """
    Receive a complete response from the server.
    
    Args:
        client_socket: The socket connection to the server
    
    Returns:
        dict: The parsed JSON response
    """
    try:
        # First, receive the 10-byte length header
        length_data = b''
        while len(length_data) < 10:
            chunk = client_socket.recv(10 - len(length_data))
            if not chunk:
                raise ConnectionError("Server disconnected")
            length_data += chunk
        
        # Parse the message length
        message_length = int(length_data.decode('utf-8'))
        
        # Now receive the actual message
        message_data = b''
        while len(message_data) < message_length:
            chunk = client_socket.recv(message_length - len(message_data))
            if not chunk:
                raise ConnectionError("Server disconnected")
            message_data += chunk
        
        # Parse JSON response
        response_json = message_data.decode('utf-8')
        return json.loads(response_json)
        
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON response from server")
    except Exception as e:
        raise ConnectionError(f"Error receiving response: {e}")

def display_results(response):
    """
    Display the grep results in a formatted way.
    
    Args:
        response (dict): The response from the server containing grep results
    """
    if not response['success']:
        print(f"Error: {response['error']}")
        return
    
    filename = response['filename']
    line_count = response['line_count']
    results = response['results']
    
    print(f"\n=== Results from {filename} ===")
    
    if line_count == 0:
        print("No matches found.")
    else:
        print(f"Found {line_count} matching line(s):\n")
        
        for i, line in enumerate(results, 1):
            # Add filename prefix to each line (like grep does when searching multiple files)
            print(f"{filename}:{line}")
        
        print(f"\n--- Total: {line_count} matches ---")

def parse_arguments():
    """
    Parse command line arguments for the grep client.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Distributed Grep Client - Search for patterns in remote log files',
        epilog='Example: python3 client.py server1.example.com "Hamlet" -i -n'
    )
    
    parser.add_argument(
        'hostname',
        help='Hostname or IP address of the grep server'
    )
    
    parser.add_argument(
        'pattern',
        help='The search pattern (regex) to grep for'
    )
    
    parser.add_argument(
        '-f', '--filename',
        default='machine.1.log',
        help='Log file to search (default: machine.1.log)'
    )
    
    parser.add_argument(
        '-i', '--ignore-case',
        action='store_true',
        help='Ignore case distinctions in pattern and input'
    )
    
    parser.add_argument(
        '-n', '--line-number',
        action='store_true',
        help='Show line numbers with output lines'
    )
    
    parser.add_argument(
        '-v', '--invert-match',
        action='store_true',
        help='Invert the sense of matching, to select non-matching lines'
    )
    
    parser.add_argument(
        '-c', '--count',
        action='store_true',
        help='Only print count of matching lines'
    )
    
    parser.add_argument(
        '--color',
        choices=['never', 'always', 'auto'],
        default='auto',
        help='When to use color for highlighting matches'
    )
    
    return parser.parse_args()

def build_grep_options(args):
    """
    Build the list of grep options based on command line arguments.
    
    Args:
        args: Parsed command line arguments
    
    Returns:
        list: List of grep option flags
    """
    options = []
    
    if args.ignore_case:
        options.append('-i')
    
    if args.line_number:
        options.append('-n')
    
    if args.invert_match:
        options.append('-v')
    
    if args.count:
        options.append('-c')
    
    if args.color != 'auto':
        options.append(f'--color={args.color}')
    
    return options

def main():
    """
    Main client function. Connects to server and sends grep request.
    """
    # Parse command line arguments
    args = parse_arguments()
    
    hostname = args.hostname
    pattern = args.pattern
    filename = args.filename
    grep_options = build_grep_options(args)
    
    try:
        # Create socket
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        print(f"client: attempting connection to {hostname}")
        
        # Connect to server
        client_socket.connect((hostname, PORT))
        
        print(f"client: connected to {hostname}")
        
        # Build the request
        request = {
            'pattern': pattern,
            'filename': filename,
            'options': grep_options
        }
        
        print(f"Searching for pattern '{pattern}' in {filename}")
        if grep_options:
            print(f"Using options: {' '.join(grep_options)}")
        
        # Send the grep request
        send_request(client_socket, request)
        
        # Receive the response
        response = receive_response(client_socket)
        
        # Display the results
        display_results(response)
        
    except socket.gaierror as e:
        print(f"getaddrinfo error: {e}")
        sys.exit(1)
    except ConnectionError as e:
        print(f"Connection error: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"Data error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        if 'client_socket' in locals():
            client_socket.close()

if __name__ == "__main__":
    main()