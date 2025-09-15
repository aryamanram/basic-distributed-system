import socket
import sys

def test():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_connection:
        try:
            server_connection.connect(("127.0.0.1", 8080))
        except Exception:
            print(f"Error connecting to the server with host: ...")
            exit()
        print("This should not be printed")

test()

#need test that generates log files at every machine w/ 1) known lines & 2) random lines
print("Final Print")