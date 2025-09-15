# Server.py

import socket
import json
from subprocess import check_output

HOST = socket.gethostname()
PORT = 8080
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:

    s.bind((HOST, PORT))
    print(f"Socket bound to {HOST} to port {PORT}")
    s.listen()
    print("Listening for connections...")

    while True: # To keep listening for further queries
        conn, addr = s.accept()
        with conn:
            print(f"Connected by {addr}") # Which machine is making the connection
            try:

                data = conn.recv(1024) # Receiving data packet
                decodeddata = data.decode('utf-8') # Decoding data
                load = json.loads(decodeddata) # loading JSON data to python readable dictionary
                
                command = load["command_parameters"][0] # Command passed, in this case it is grep
                input_command_list = load["command_parameters"] # All parameters passed to the terminal
                pattern = ""
                option = "-n" # Default -n parameter to get the line number for the output
                optione = False # Flag for the -E parameter
                for parameter in input_command_list:
                    if parameter.lower()!="grep" and parameter[0]!="-":
                        pattern = pattern + parameter + " "
                    if parameter == "-E":
                        optione = True
                    if parameter == "-c":
                        option = "-c"
                    if parameter == "-Ec":
                        option = "-Ec"
                pattern = pattern[0:len(pattern)-1]
                if pattern[0] == "\"":
                    pattern = pattern[1:]
                if pattern[len(pattern)-1] == "\"":
                    pattern = pattern[:len(pattern)-1]

                command_list = []
                command_list.append(command)
                command_list.append(option)
                if optione:
                    command_list.append("-E")
                command_list.append(pattern)
                filename = HOST.split("-")[2][2:4]
                command_list.append(f"machine.{filename}.log")        
                #assume log file is already generated on machine
                #run command: grep <options> <pattern> and store return value
                print(f"Final Command to be run: {command_list}") 
                log_string = check_output(command_list).decode("utf-8")
                print(f"Output: {log_string}")
                tempdict = {}
                if not log_string[:-1].isdigit():
                    for line in log_string.split("\r\n"):
                        key = line.split(":")[0]
                        value = "".join(line.split(":")[1:])
                        if key != "" and value != "":
                            tempdict[key] = value
                    count = len(tempdict.keys())
                else:
                    tempdict[0] = "Passed -c as a parameter"
                    count = log_string[:-1]
                
                #out_dict sends dictionary back. key: inputted log file, value: grep output from inputted log file w/ Line numbers
                out_dict = {}
                filename_string = f"machine.{filename}.log"
                out_dict[filename_string] = tempdict
                out_dict["count"]=count
                print(out_dict) # Final Output from the Command
                conn.sendall(json.dumps(out_dict).encode('utf-8'))
            except Exception as e:
                print(e)