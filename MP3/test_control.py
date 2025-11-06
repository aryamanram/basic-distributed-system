#!/usr/bin/env python3
"""
Simple test script to verify HyDFS control server is working
"""
import socket
import json
import sys

CONTROL_PORT = 9090

def test_control_server(hostname, command):
    """
    Send a command to the control server and print the response.
    """
    try:
        print(f"Connecting to {hostname}:{CONTROL_PORT}...")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)
        sock.connect((hostname, CONTROL_PORT))
        print("✓ Connected")
        
        # Send command
        command_data = json.dumps(command).encode('utf-8')
        size_header = f"{len(command_data):010d}".encode('utf-8')
        sock.sendall(size_header + command_data)
        print(f"✓ Sent command: {command}")
        
        # Receive response
        print("Waiting for response...")
        size_header = sock.recv(10)
        if not size_header:
            print("✗ No response received")
            return False
        
        print(f"✓ Received size header: {size_header}")
        
        try:
            size = int(size_header.decode('utf-8'))
            print(f"✓ Response size: {size} bytes")
        except ValueError as e:
            print(f"✗ Invalid size header: '{size_header.decode('utf-8', errors='replace')}'")
            # Try to read more to see what was sent
            extra = sock.recv(100)
            print(f"  Additional data received: '{extra.decode('utf-8', errors='replace')}'")
            return False
        
        # Read full response
        response_data = b''
        while len(response_data) < size:
            chunk = sock.recv(min(4096, size - len(response_data)))
            if not chunk:
                break
            response_data += chunk
        
        print(f"✓ Received {len(response_data)} bytes of response data")
        
        # Parse response
        try:
            response = json.loads(response_data.decode('utf-8'))
            print(f"✓ Parsed JSON response")
            print(f"\nResponse:")
            print(json.dumps(response, indent=2))
            return True
        except json.JSONDecodeError as e:
            print(f"✗ Invalid JSON response: {e}")
            print(f"  Raw data: {response_data[:200]}")
            return False
        
    except socket.timeout:
        print("✗ Connection timeout")
        return False
    except ConnectionRefusedError:
        print("✗ Connection refused - is the node running?")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        try:
            sock.close()
        except:
            pass

def main():
    if len(sys.argv) < 2:
        print("Usage: python test_control.py <hostname> [command_type]")
        print("\nExample:")
        print("  python test_control.py fa25-cs425-a701.cs.illinois.edu")
        print("  python test_control.py fa25-cs425-a701.cs.illinois.edu LISTSTORE")
        print("  python test_control.py localhost LIST_MEM_IDS")
        sys.exit(1)
    
    hostname = sys.argv[1]
    command_type = sys.argv[2] if len(sys.argv) > 2 else 'LISTSTORE'
    
    print("="*60)
    print("HyDFS Control Server Test")
    print("="*60)
    print()
    
    command = {'type': command_type}
    
    success = test_control_server(hostname, command)
    
    print()
    print("="*60)
    if success:
        print("✓ Test PASSED")
    else:
        print("✗ Test FAILED")
    print("="*60)
    
    return 0 if success else 1

if __name__ == '__main__':
    sys.exit(main())