## Adapted from Beej's guide

import socket
import subprocess
import json
import threading
import os
import sys
import signal

PORT = 3490

# load info form config file
def load_config():
    with open('config.json', 'r') as f:
        config = json.load(f)
    
    hostname = socket.gethostname()
    vm_config = None
    
    for vm in config['vms']:
        if vm['hostname'] == hostname:
            vm_config = vm
            break
    
    if not vm_config:
        local_ip = socket.gethostbyname(hostname)
        for vm in config['vms']:
            if vm['ip'] == local_ip:
                vm_config = vm
                break
    
    if not vm_config:
        print(f"Error: Could not find {hostname}")
        sys.exit(1)
    
    return vm_config

# receive grep specs, run grep, send back results
def run_client_grep(c, addr, vm_config):
    try:
        data = c.recv(4096).decode('utf-8')
        if not data:
            return
        
        request = json.loads(data)
        grep_pattern = request.get('pattern', '')
        grep_options = request.get('options', '')
        
        vm_id = vm_config['id']
        log_file = vm_config['log_file']
        
        try:
            if grep_options:
                cmd = f"grep {grep_options} '{grep_pattern}' {log_file}"
            else:
                cmd = f"grep '{grep_pattern}' {log_file}"
            
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            lines = []
            if result.stdout:
                lines = result.stdout.strip().split('\n')
                lines = [f"{log_file}:{line}" for line in lines if line]
            
            response = {
                'vm_id': vm_id,
                'count': len(lines),
                'lines': lines[:1000],
                'error': None
            }
        except Exception as e:
            response = {
                'vm_id': vm_id,
                'count': 0,
                'lines': [],
                'error': str(e)
            }
        
        response_data = json.dumps(response).encode('utf-8')
        size_header = f"{len(response_data):010d}".encode('utf-8')
        c.send(size_header)
        c.send(response_data)
        
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        c.close()

# main function, socket connection
def main():
    vm_config = load_config()
    port = PORT
    vm_id = vm_config['id']
    log_file = vm_config['log_file']
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    s.bind(('0.0.0.0', port))
    s.listen(10)
    
    print(f"Server VM{vm_id}, port {port}")
    print(f"Log file: {log_file}")
    
    def signal_handler(sig, frame):
        print("\nShutting down server...")
        s.close()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    while True:
        try:
            c, addr = s.accept()
            thread = threading.Thread(
                target=run_client_grep,
                args=(c, addr, vm_config)
            )
            thread.daemon = True
            thread.start()
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error accepting connection: {e}")
    
    s.close()

if __name__ == "__main__":
    main()