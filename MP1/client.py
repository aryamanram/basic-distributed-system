## Adapted from Beej's guide

import socket
import json
import sys
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

PORT = 3490
TIMEOUT = 720
VM_COUNT = 10

# sends grep pattern and requirements to server, receives response
def query_server(vm_info, pattern, options):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(TIMEOUT)
        
        s.connect((vm_info['ip'], PORT))
        
        request = {
            'pattern': pattern,
            'options': options
        }
        s.send(json.dumps(request).encode('utf-8'))
        
        size_header = s.recv(10)
        if not size_header:
            return None
        
        response_size = int(size_header.decode('utf-8'))
        
        response_data = b''
        while len(response_data) < response_size:
            chunk = s.recv(min(4096, response_size - len(response_data)))
            if not chunk:
                break
            response_data += chunk
        
        s.close()
        
        response = json.loads(response_data.decode('utf-8'))
        return response
        
    except socket.timeout:
        return {
            'vm_id': vm_info['id'],
            'count': 0,
            'lines': [],
            'error': 'Connection timeout'
        }
    except Exception as e: # catchall for any exceptions/errors
        return {
            'vm_id': vm_info['id'],
            'count': 0,
            'lines': [],
            'error': str(e)
        }

# run the grep command on each VM, display results
def run_grep(pattern, options='', verbose=False):
    # config = load_config()
    with open('config.json', 'r') as f:
        config = json.load(f)
    vms = config['vms']
    # timeout = config.get('timeout', 5000) / 1000 
    
    start_time = time.time()
    
    print(f"\nExecuting grep: pattern='{pattern}' options='{options}'")
    print("-" * 60)
    
    results = []
    total_count = 0
    failed_vms = []
    vm_counts = {}  # Store count per VM
    
    with ThreadPoolExecutor(max_workers=VM_COUNT) as executor:
        future_to_vm = {
            executor.submit(query_server, vm, pattern, options): vm 
            for vm in vms
        }
        
        for future in as_completed(future_to_vm):
            vm = future_to_vm[future]
            result = future.result()
            
            if result:
                if result.get('error'):
                    failed_vms.append((vm['id'], result['error']))
                    vm_counts[vm['id']] = {'count': 0, 'status': 'FAILED', 'error': result['error']}
                    if verbose:
                        print(f"VM{vm['id']}: ERROR - {result['error']}")
                else:
                    results.append(result)
                    total_count += result['count']
                    vm_counts[vm['id']] = {'count': result['count'], 'status': 'SUCCESS'}
                    if verbose:
                        print(f"VM{vm['id']}: {result['count']} matches found")
            else:
                failed_vms.append((vm['id'], 'No response'))
                vm_counts[vm['id']] = {'count': 0, 'status': 'FAILED', 'error': 'No response'}
                if verbose:
                    print(f"VM{vm['id']}: No response")
    
    print("\n\n")
    
    for result in sorted(results, key=lambda x: x['vm_id']):
        if result['lines']:
            for line in result['lines']:
                print(line)
    
    print("\n\n")
    
    print("Line counts:")
    for vm_id in sorted(vm_counts.keys()):
        vm_info = vm_counts[vm_id]
        log_file = f"machine.{vm_id}.log"
        
        if vm_info['status'] == 'SUCCESS':
            print(f"{log_file}: {vm_info['count']:6d}")
        else:
            print(f"{log_file}: FAILED - {vm_info['error']}")
    
    print(f"\nSum of line counts: {total_count}")
    print(f"Successful VMs: {len(results)}/{len(vms)}")
    
    if failed_vms:
        print(f"Failed VMs: {len(failed_vms)}")
    
    elapsed_time = time.time() - start_time
    print(f"\nQuery completed in {elapsed_time:.3f} seconds")
    
    return total_count, results, failed_vms

# main, also parse flags
def main():
    parser = argparse.ArgumentParser(description='Distributed Log Query Client')
    parser.add_argument('pattern', help='Grep pattern to search for')
    parser.add_argument('-e', '--regexp', action='store_true', 
                       help='Use extended regular expressions')
    parser.add_argument('-i', '--ignore-case', action='store_true',
                       help='Ignore case distinctions')
    parser.add_argument('-c', '--count', action='store_true',
                       help='Only print count of matching lines')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Verbose output')
    parser.add_argument('-n', '--line-number', action='store_true',
                       help='Output with line numbers')
    parser.add_argument('--options', default='',
                       help='Additional grep options')
    
    args = parser.parse_args()
    
    options = args.options
    if args.regexp:
        options += ' -E'
    if args.ignore_case:
        options += ' -i'
    if args.count:
        options += ' -c'
    if args.line_number:
        options += ' -n'
    
    run_grep(args.pattern, options.strip(), args.verbose)


if __name__ == "__main__":
    main()