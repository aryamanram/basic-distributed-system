#!/usr/bin/env python3
"""
Distributed Grep Client
Queries all VMs in parallel and aggregates results
"""

import socket
import sys
import json
import argparse
import time
from threading import Thread, Lock
from collections import defaultdict

# Configuration file path
CONFIG_FILE = 'config.json'
MAXDATASIZE = 4096

# Global variables for result aggregation
results_lock = Lock()
all_results = {}
total_matches = 0
failed_vms = []

def load_config():
    """Load VM configuration from JSON file."""
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{CONFIG_FILE}' not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in '{CONFIG_FILE}'.")
        sys.exit(1)

def send_request(client_socket, request_data):
    """Send a JSON request to the server."""
    try:
        request_json = json.dumps(request_data)
        message_length = len(request_json.encode('utf-8'))
        length_header = f"{message_length:010d}".encode('utf-8')
        
        client_socket.send(length_header)
        client_socket.send(request_json.encode('utf-8'))
    except Exception as e:
        raise ConnectionError(f"Error sending request: {e}")

def receive_response(client_socket):
    """Receive a complete response from the server."""
    try:
        # Receive length header
        length_data = b''
        while len(length_data) < 10:
            chunk = client_socket.recv(10 - len(length_data))
            if not chunk:
                raise ConnectionError("Server disconnected")
            length_data += chunk
        
        message_length = int(length_data.decode('utf-8'))
        
        # Receive actual message
        message_data = b''
        while len(message_data) < message_length:
            remaining = min(MAXDATASIZE, message_length - len(message_data))
            chunk = client_socket.recv(remaining)
            if not chunk:
                raise ConnectionError("Server disconnected")
            message_data += chunk
        
        response_json = message_data.decode('utf-8')
        return json.loads(response_json)
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON response from server")
    except Exception as e:
        raise ConnectionError(f"Error receiving response: {e}")

def query_single_vm(vm_info, pattern, grep_options, timeout):
    """
    Query a single VM for grep results.
    
    Args:
        vm_info (dict): VM configuration (id, hostname, port, log_file)
        pattern (str): The grep pattern
        grep_options (list): Grep options
        timeout (int): Connection timeout in seconds
    """
    global all_results, total_matches, failed_vms
    
    vm_id = vm_info['id']
    hostname = vm_info['hostname']
    port = vm_info['port']
    log_file = vm_info['log_file']
    
    try:
        # Create socket with timeout
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(timeout)
        
        # Try hostname first, fall back to IP if needed
        try:
            client_socket.connect((hostname, port))
        except socket.gaierror:
            # If hostname fails, try IP
            client_socket.connect((vm_info['ip'], port))
        
        # Build and send request
        request = {
            'pattern': pattern,
            'filename': log_file,
            'options': grep_options
        }
        
        send_request(client_socket, request)
        
        # Receive response
        response = receive_response(client_socket)
        
        # Store results
        with results_lock:
            if response['success']:
                all_results[vm_id] = {
                    'hostname': hostname,
                    'results': response['results'],
                    'line_count': response['line_count'],
                    'filename': response['filename']
                }
                total_matches += response['line_count']
            else:
                failed_vms.append({
                    'vm_id': vm_id,
                    'hostname': hostname,
                    'error': response.get('error', 'Unknown error')
                })
        
        client_socket.close()
        
    except (socket.timeout, socket.error, ConnectionError) as e:
        with results_lock:
            failed_vms.append({
                'vm_id': vm_id,
                'hostname': hostname,
                'error': f"Connection failed: {str(e)}"
            })
    except Exception as e:
        with results_lock:
            failed_vms.append({
                'vm_id': vm_id,
                'hostname': hostname,
                'error': f"Unexpected error: {str(e)}"
            })

def display_results(pattern, show_line_numbers=False):
    """
    Display aggregated results from all VMs.
    
    Args:
        pattern (str): The search pattern (for display)
        show_line_numbers (bool): Whether line numbers are included
    """
    print(f"\n{'='*70}")
    print(f"Distributed Grep Results for pattern: '{pattern}'")
    print(f"{'='*70}\n")
    
    # Display results from successful VMs
    if all_results:
        for vm_id in sorted(all_results.keys()):
            result = all_results[vm_id]
            hostname = result['hostname']
            line_count = result['line_count']
            filename = result['filename']
            
            if line_count > 0:
                print(f"VM{vm_id} ({hostname}) - {filename}: {line_count} match(es)")
                print("-" * 50)
                for line in result['results']:
                    # Add VM identifier to each line
                    print(f"VM{vm_id}:{filename}:{line}")
                print()
    
    # Display failed VMs
    if failed_vms:
        print(f"\n{'='*70}")
        print("Failed to query the following VMs:")
        print("-" * 50)
        for failed in failed_vms:
            print(f"VM{failed['vm_id']} ({failed['hostname']}): Unable to retrieve data")
    
    # Summary
    print(f"\n{'='*70}")
    print(f"Summary:")
    print(f"  Total VMs queried: {len(all_results) + len(failed_vms)}")
    print(f"  Successful queries: {len(all_results)}")
    print(f"  Failed queries: {len(failed_vms)}")
    print(f"  Total matches found: {total_matches}")
    print(f"{'='*70}\n")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Distributed Grep Client - Search all VMs for patterns',
        epilog='Example: python3 distributed_client.py "error" -i'
    )
    
    parser.add_argument(
        'pattern',
        help='The search pattern (regex) to grep for'
    )
    
    parser.add_argument(
        '-i', '--ignore-case',
        action='store_true',
        help='Ignore case distinctions'
    )
    
    parser.add_argument(
        '-n', '--line-number',
        action='store_true',
        help='Show line numbers with output'
    )
    
    parser.add_argument(
        '-v', '--invert-match',
        action='store_true',
        help='Select non-matching lines'
    )
    
    parser.add_argument(
        '-c', '--count',
        action='store_true',
        help='Only print count of matching lines'
    )
    
    parser.add_argument(
        '--sequential',
        action='store_true',
        help='Query VMs sequentially instead of in parallel'
    )
    
    return parser.parse_args()

def build_grep_options(args):
    """Build grep options from command line arguments."""
    options = []
    
    if args.ignore_case:
        options.append('-i')
    if args.line_number:
        options.append('-n')
    if args.invert_match:
        options.append('-v')
    if args.count:
        options.append('-c')
    
    return options

def main():
    """Main function - coordinates distributed grep across all VMs."""
    global all_results, total_matches, failed_vms
    
    # Parse arguments
    args = parse_arguments()
    pattern = args.pattern
    grep_options = build_grep_options(args)
    
    # Load configuration
    config = load_config()
    vms = config['vms']
    timeout = config.get('timeout', 5)
    parallel = not args.sequential and config.get('parallel', True)
    
    # Reset global variables
    all_results = {}
    total_matches = 0
    failed_vms = []
    
    print(f"\nQuerying {len(vms)} VMs for pattern '{pattern}'...")
    print(f"Mode: {'Parallel' if parallel else 'Sequential'}")
    print(f"Timeout: {timeout} seconds per VM\n")
    
    start_time = time.time()
    
    if parallel:
        # Query all VMs in parallel using threads
        threads = []
        for vm_info in vms:
            thread = Thread(
                target=query_single_vm,
                args=(vm_info, pattern, grep_options, timeout)
            )
            thread.start()
            threads.append(thread)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
    else:
        # Query VMs sequentially
        for vm_info in vms:
            query_single_vm(vm_info, pattern, grep_options, timeout)
    
    end_time = time.time()
    query_time = end_time - start_time
    
    # Display results
    display_results(pattern, args.line_number)
    
    print(f"Total query time: {query_time:.2f} seconds")
    
    # Exit with error code if all queries failed
    if len(failed_vms) == len(vms):
        sys.exit(1)

if __name__ == "__main__":
    main()