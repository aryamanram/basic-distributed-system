#!/usr/bin/env python3
"""
server.py -- a stream socket server demo (Python translation)
"""

import socket
import sys
import signal
import os
from threading import Thread

PORT = 3490  # the port users will be connecting to
BACKLOG = 10  # how many pending connections queue will hold

def handle_client(client_socket, client_address):
    """Handle individual client connection"""
    try:
        print(f"server: got connection from {client_address[0]}")
        
        # Send message to client
        message = "Hello, world!"
        client_socket.send(message.encode('utf-8'))
        
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        client_socket.close()

def signal_handler(sig, frame):
    """Handle SIGINT (Ctrl+C) gracefully"""
    print("\nShutting down server...")
    sys.exit(0)

def main():
    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    # Create socket
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Allow socket reuse
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Bind to address and port
        server_socket.bind(('', PORT))
        
        # Listen for connections
        server_socket.listen(BACKLOG)
        
        print("server: waiting for connections...")
        
        while True:
            try:
                # Accept connection
                client_socket, client_address = server_socket.accept()
                
                # Handle client in a separate thread (similar to fork() behavior)
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