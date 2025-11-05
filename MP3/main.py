"""
HyDFS Main Entry Point
Handles CLI commands and coordinates with node.py
"""

import argparse
import sys
from node import HyDFSNode
from MP2.membership import MembershipService

def parse_command(command_str):
    """Parse user command and extract operation and arguments"""
    parts = command_str.strip().split()
    if not parts:
        return None, []
    return parts[0], parts[1:]

def main():
    parser = argparse.ArgumentParser(description='HyDFS Node')
    parser.add_argument('--vm-id', type=int, required=True, help='VM ID (1-10)')
    args = parser.parse_args()
    
    # Initialize MP2 membership
    membership = MembershipService(args.vm_id)
    membership.start()
    
    # Initialize HyDFS node
    node = HyDFSNode(args.vm_id, membership)
    node.start()
    
    print(f"HyDFS Node {args.vm_id} started. Type 'help' for commands.")
    
    # Command loop
    try:
        while True:
            command_str = input(f"HyDFS[VM{args.vm_id}]> ")
            cmd, args_list = parse_command(command_str)
            
            if cmd == "create":
                if len(args_list) != 2:
                    print("Usage: create <localfilename> <HyDFSfilename>")
                    continue
                node.create_file(args_list[0], args_list[1])
            
            elif cmd == "get":
                if len(args_list) != 2:
                    print("Usage: get <HyDFSfilename> <localfilename>")
                    continue
                node.get_file(args_list[0], args_list[1])
            
            elif cmd == "append":
                if len(args_list) != 2:
                    print("Usage: append <localfilename> <HyDFSfilename>")
                    continue
                node.append_file(args_list[0], args_list[1])
            
            elif cmd == "merge":
                if len(args_list) != 1:
                    print("Usage: merge <HyDFSfilename>")
                    continue
                node.merge_file(args_list[0])
            
            elif cmd == "ls":
                if len(args_list) != 1:
                    print("Usage: ls <HyDFSfilename>")
                    continue
                node.ls_file(args_list[0])
            
            elif cmd == "liststore":
                node.liststore()
            
            elif cmd == "getfromreplica":
                if len(args_list) != 3:
                    print("Usage: getfromreplica <VMaddress> <HyDFSfilename> <localfilename>")
                    continue
                node.getfromreplica(args_list[0], args_list[1], args_list[2])
            
            elif cmd == "list_mem_ids":
                node.list_mem_ids()
            
            elif cmd == "multiappend":
                # Parse: multiappend(filename, VM1, VM2, ..., local1, local2, ...)
                # TODO: Implement parsing
                pass
            
            elif cmd == "exit":
                break
            
            elif cmd == "help":
                print_help()
            
            else:
                print(f"Unknown command: {cmd}")
    
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        node.stop()
        membership.stop()

if __name__ == "__main__":
    main()