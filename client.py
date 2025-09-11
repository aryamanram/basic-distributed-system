#!/usr/bin/env python3
"""
client.py -- a stream socket client demo (Python translation)
"""

import socket
import sys

PORT = 3490  # the port client will be connecting to
MAXDATASIZE = 100  # max number of bytes we can get at once

def main():
    if len(sys.argv) != 2:
        print("usage: python3 client.py hostname")
        sys.exit(1)
    
    hostname = sys.argv[1]
    
    try:
        # Create socket
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        print(f"client: attempting connection to {hostname}")
        
        # Connect to server
        client_socket.connect((hostname, PORT))
        
        print(f"client: connected to {hostname}")
        
        # Receive data from server
        data = client_socket.recv(MAXDATASIZE)
        
        if data:
            received_message = data.decode('utf-8')
            print(f"client: received '{received_message}'")
        else:
            print("client: no data received")
            
    except socket.gaierror as e:
        print(f"getaddrinfo error: {e}")
        sys.exit(1)
    except socket.error as e:
        print(f"socket error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"error: {e}")
        sys.exit(1)
    finally:
        if 'client_socket' in locals():
            client_socket.close()

if __name__ == "__main__":
    main()