# Client.py

import time # For stopwatch
import sys # For Accepting arguments from terminal
import threading # Threading for Server Connections
import socket # Socket programming for Connection between client-server
import json # JSON to decode and send TCP packets between client-server

VMS = [] # List for VMs Addresses
PORT = 8080 # Arbitrary Port for communication with server
resultlogs = {} # Final Dictionary to save Response from servers

with open('serverslist.txt','r') as serverslist:
    # Reading Servers List from the text file
    servers = serverslist.readlines()
    serverslist.close()
    for server in servers:
        serverip = server.split("\n")[0]
        VMS.append(serverip)

def querythreads(host, port, command_parameters, starttime):
    # Initializing each server connection as a Thread
    Error = False # Flag in case server is not alive

    query_dict = {"command_parameters":command_parameters}
    queryvar = json.dumps(query_dict).encode('utf-8') # Query to be sent through the TCP Connection
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_connection:
        try:
            server_connection.connect((host, port)) # Conncting to a server using the given Host and Port Combination
        except Exception:
            print(f"Error connecting to the server with host: {host}...") # Server offline
            Error = True
        
        if Error == False:
            # If server is alive and waiting for connection
            server_connection.sendall(queryvar)
            print(f"Successful Connection to... {host}")
            res = "" # Final formatted Data received via the connection
            while True: 
                responsestream = b'' # Byte String variable to store the response
                while True:
                    resdata = server_connection.recv(12288) # Receiving 4kb data packet
                    if resdata:
                        responsestream += resdata # Keep appending data to the variable
                    else:
                        break
                if responsestream:
                    res = json.loads(responsestream.decode('utf-8')) # Converting received json response to a python readable dictionary
                else:
                    break
            if res != "":
                resultlogs[host] = {"data": res, "time":time.time()-starttime} # "data" is the final result of the grep command from this VM, "time" is the execution time of this grep command on this machine
                print(f"Finished receiving data from {host}...")
            else:
                print(f"Couldn't find the pattern in {host} machine logs!")

def search_query():
    # Search Query method to maintain multiple Threads
    input_message = input(" -> ")
    input_message_list = input_message.split(" ")
    startwatch = time.time()
    threads=[] # List to maintain all threads of the machines

    # Main Code
    for vm in VMS:
        threads.append(threading.Thread(target=querythreads, args=(vm, PORT, input_message_list, time.time())))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    stopwatch=time.time() # Final time taken to execute on all machines
    print(f"Final Result Logs: \n")

    for log in resultlogs.keys():
        print(f"{log}: {resultlogs[log]} \n")
    print("Total Execution Time : ",stopwatch-startwatch)

if __name__ == '__main__':
    print(f"Executing {sys.argv[0]}")
    search_query()