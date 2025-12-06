#!/usr/bin/env python3
"""
RainStorm CLI - Command line interface for interacting with RainStorm
Supports: list_tasks, kill_task, submit, status

Usage examples:
    # List all running tasks
    python3 cli.py --leader 1 list_tasks
    
    # Kill a specific task
    python3 cli.py --leader 1 kill_task 2 12345
    
    # Submit a job (spec-compliant format)
    python3 cli.py --leader 1 submit 2 3 ./filter_op.py CS425 ./transform_op.py 3 /input /output true false
    
    # Submit with autoscaling
    python3 cli.py --leader 1 submit 2 3 ./filter_op.py CS425 ./count_op.py 0 /input /output false true 100 50 150
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


def send_to_leader(leader_vm: int, msg: dict, timeout: float = 30.0) -> dict:
    """Send message to leader and get response."""
    hostname = VM_HOSTS.get(leader_vm)
    if not hostname:
        return {'status': 'error', 'message': f'Invalid VM ID: {leader_vm}'}
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
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
        
        print(f"\n{'Task ID':<35} {'Stage':<6} {'VM':<5} {'PID':<10} {'Operator':<20}")
        print("-" * 80)
        
        for task in sorted(tasks, key=lambda t: (t['stage'], t['task_idx'])):
            print(f"{task['task_id']:<35} "
                  f"{task['stage']:<6} "
                  f"VM{task['vm_id']:<3} "
                  f"{task['pid']:<10} "
                  f"{task['op_exe']:<20}")
        
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


def submit_job(leader_vm: int, args: list):
    """
    Submit a job using the spec-compliant command line format:
    
    RainStorm <Nstages> <Ntasks_per_stage> <op1_exe> <op1_args> ... <opN_exe> <opN_args>
              <hydfs_src_directory> <hydfs_dest_filename> <exactly_once>
              <autoscale_enabled> [INPUT_RATE] [LW] [HW]
    """
    if len(args) < 6:
        print("Error: Not enough arguments for submit")
        print("Usage: submit <nstages> <ntasks> <op1_exe> <op1_args> ... <src> <dest> <exactly_once> <autoscale> [INPUT_RATE LW HW]")
        return
    
    try:
        nstages = int(args[0])
        ntasks = int(args[1])
        
        # Parse operators (exe + args pairs)
        operators = []
        idx = 2
        for _ in range(nstages):
            if idx >= len(args):
                print(f"Error: Missing operator {len(operators) + 1}")
                return
            op_exe = args[idx]
            idx += 1
            
            # Get args (could be empty or a value)
            op_args = ""
            if idx < len(args) and not args[idx].startswith('/') and args[idx] not in ('true', 'false'):
                op_args = args[idx]
                idx += 1
            
            operators.append({'exe': op_exe, 'args': op_args})
        
        # Parse remaining arguments
        if idx >= len(args):
            print("Error: Missing source path")
            return
        hydfs_src = args[idx]
        idx += 1
        
        if idx >= len(args):
            print("Error: Missing destination path")
            return
        hydfs_dest = args[idx]
        idx += 1
        
        if idx >= len(args):
            print("Error: Missing exactly_once flag")
            return
        exactly_once = args[idx].lower() in ('true', '1', 'yes')
        idx += 1
        
        if idx >= len(args):
            print("Error: Missing autoscale_enabled flag")
            return
        autoscale = args[idx].lower() in ('true', '1', 'yes')
        idx += 1
        
        # Autoscale parameters (optional)
        input_rate = 100
        lw = 50
        hw = 150
        
        if autoscale:
            if idx < len(args):
                input_rate = int(args[idx])
                idx += 1
            if idx < len(args):
                lw = int(args[idx])
                idx += 1
            if idx < len(args):
                hw = int(args[idx])
                idx += 1
        
        # Build and send message
        msg = {
            'type': 'SUBMIT_JOB',
            'nstages': nstages,
            'ntasks': ntasks,
            'operators': operators,
            'src': hydfs_src,
            'dest': hydfs_dest,
            'exactly_once': exactly_once,
            'autoscale': autoscale,
            'input_rate': input_rate,
            'lw': lw,
            'hw': hw
        }
        
        print(f"Submitting job to leader VM{leader_vm}:")
        print(f"  Stages: {nstages}, Tasks/stage: {ntasks}")
        print(f"  Operators: {operators}")
        print(f"  Source: {hydfs_src}")
        print(f"  Destination: {hydfs_dest}")
        print(f"  Exactly-once: {exactly_once}")
        print(f"  Autoscale: {autoscale}")
        if autoscale:
            print(f"  Input rate: {input_rate}, LW: {lw}, HW: {hw}")
        
        response = send_to_leader(leader_vm, msg)
        
        if response.get('status') == 'success':
            print("\nJob submitted successfully!")
        else:
            print(f"\nError: {response.get('message')}")
            
    except ValueError as e:
        print(f"Error parsing arguments: {e}")
    except Exception as e:
        print(f"Error: {e}")


def get_status(leader_vm: int):
    """Get cluster status."""
    # Get workers
    msg = {'type': 'GET_WORKERS'}
    response = send_to_leader(leader_vm, msg)
    
    if response.get('status') == 'success':
        workers = response.get('workers', [])
        print(f"\nActive workers: {workers}")
    else:
        print(f"Error getting workers: {response.get('message')}")
    
    # Get tasks
    list_tasks(leader_vm)


def main():
    parser = argparse.ArgumentParser(
        description='RainStorm CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  List tasks:     cli.py --leader 1 list_tasks
  Kill task:      cli.py --leader 1 kill_task 2 12345
  Get status:     cli.py --leader 1 status
  Submit job:     cli.py --leader 1 submit 2 3 ./filter_op.py CS425 ./transform_op.py 3 /src /dest true false
        """
    )
    parser.add_argument('--leader', type=int, default=1, help='Leader VM ID (default: 1)')
    
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # list_tasks
    subparsers.add_parser('list_tasks', help='List all running tasks')
    
    # kill_task
    kill_parser = subparsers.add_parser('kill_task', help='Kill a task')
    kill_parser.add_argument('vm_id', type=int, help='VM ID')
    kill_parser.add_argument('pid', type=int, help='Process ID')
    
    # status
    subparsers.add_parser('status', help='Get cluster status')
    
    # submit - takes remaining args
    submit_parser = subparsers.add_parser('submit', help='Submit a job')
    submit_parser.add_argument('job_args', nargs='*', help='Job arguments')
    
    args = parser.parse_args()
    
    if args.command == 'list_tasks':
        list_tasks(args.leader)
    elif args.command == 'kill_task':
        kill_task(args.leader, args.vm_id, args.pid)
    elif args.command == 'status':
        get_status(args.leader)
    elif args.command == 'submit':
        submit_job(args.leader, args.job_args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
