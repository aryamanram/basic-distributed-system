#!/usr/bin/env python3
"""
RainStorm CLI - Command line interface for interacting with RainStorm
Supports: list_tasks, kill_task, status
"""
import argparse
import json
import socket
import sys

RAINSTORM_PORT = 8000

VM_HOSTS = {
    1: "fa25-cs425-a701.cs.illinois.edu",
    2: "fa25-cs425-a702.cs.illinois.edu",
    3: "fa25-cs425-a703.cs.illinois.edu",
    4: "fa25-cs425-a704.cs.illinois.edu",
    5: "fa25-cs425-a705.cs.illinois.edu",
    6: "fa25-cs425-a706.cs.illinois.edu",
    7: "fa25-cs425-a707.cs.illinois.edu",
    8: "fa25-cs425-a708.cs.illinois.edu",
    9: "fa25-cs425-a709.cs.illinois.edu",
    10: "fa25-cs425-a710.cs.illinois.edu"
}


def send_to_leader(leader_vm: int, msg: dict) -> dict:
    """Send message to leader and get response."""
    hostname = VM_HOSTS.get(leader_vm)
    if not hostname:
        return {'status': 'error', 'message': 'Invalid VM ID'}
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)
        sock.connect((hostname, RAINSTORM_PORT))
        sock.sendall(json.dumps(msg).encode('utf-8'))
        response = sock.recv(65536).decode('utf-8')
        sock.close()
        return json.loads(response)
    except Exception as e:
        return {'status': 'error', 'message': str(e)}


def list_tasks(leader_vm: int):
    """List all running tasks."""
    msg = {'type': 'LIST_TASKS'}
    response = send_to_leader(leader_vm, msg)
    
    if response.get('status') == 'success':
        tasks = response.get('tasks', [])
        
        if not tasks:
            print("No tasks running.")
            return
        
        print(f"\n{'Task ID':<20} {'VM':<5} {'PID':<10} {'Op':<20} {'Log File':<30}")
        print("-" * 85)
        
        for task in tasks:
            print(f"{task['task_id']:<20} "
                  f"VM{task['vm_id']:<3} "
                  f"{task['pid']:<10} "
                  f"{task['op_exe']:<20} "
                  f"{task['log_file']:<30}")
        
        print(f"\nTotal: {len(tasks)} tasks")
    else:
        print(f"Error: {response.get('message')}")


def kill_task(leader_vm: int, vm_id: int, pid: int):
    """Kill a specific task."""
    msg = {
        'type': 'KILL_TASK',
        'vm_id': vm_id,
        'pid': pid
    }
    response = send_to_leader(leader_vm, msg)
    
    if response.get('status') == 'success':
        print(f"Task killed: VM{vm_id} PID={pid}")
    else:
        print(f"Error: {response.get('message')}")


def main():
    parser = argparse.ArgumentParser(description='RainStorm CLI')
    parser.add_argument('--leader', type=int, default=1, help='Leader VM ID')
    
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # list_tasks
    subparsers.add_parser('list_tasks', help='List all running tasks')
    
    # kill_task
    kill_parser = subparsers.add_parser('kill_task', help='Kill a task')
    kill_parser.add_argument('vm_id', type=int, help='VM ID')
    kill_parser.add_argument('pid', type=int, help='Process ID')
    
    args = parser.parse_args()
    
    if args.command == 'list_tasks':
        list_tasks(args.leader)
    elif args.command == 'kill_task':
        kill_task(args.leader, args.vm_id, args.pid)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()