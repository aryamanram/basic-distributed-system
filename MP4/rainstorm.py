#!/usr/bin/env python3
"""
RainStorm - Stream Processing System
Main entry point and CLI

Usage:
    # Start as leader (VM 1)
    python rainstorm.py --leader --vm-id 1
    
    # Start as worker
    python rainstorm.py --worker --vm-id 2
    
    # Submit a job
    python rainstorm.py submit <Nstages> <Ntasks_per_stage> <op1_exe> <op1_args> ... 
                               <hydfs_src> <hydfs_dest> <exactly_once> <autoscale>
                               <INPUT_RATE> <LW> <HW>
    
    # Commands
    python rainstorm.py list_tasks
    python rainstorm.py kill_task <vm_id> <pid>
    python rainstorm.py status
"""
import argparse
import sys
import os
import time
import json
import socket
from typing import Dict, List, Optional

from utils import load_config, get_leader_hostname, RainStormLogger
from network import RPCClient
from leader import Leader, run_leader
from worker import Worker, run_worker

# ============================================================================
# CLI Argument Parsing
# ============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='RainStorm Stream Processing System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start leader on VM 1
  python rainstorm.py --leader --vm-id 1
  
  # Start worker on VM 2
  python rainstorm.py --worker --vm-id 2
  
  # Submit a job (2 stages, 3 tasks per stage)
  python rainstorm.py submit 2 3 filter.py "pattern" transform.py "" \\
      dataset.csv output.txt true false 100 10 100
  
  # List all tasks
  python rainstorm.py list_tasks
  
  # Kill a task
  python rainstorm.py kill_task 2 12345
  
  # Check job status
  python rainstorm.py status
        """
    )
    
    # Mode selection
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--leader', action='store_true',
                           help='Run as leader node')
    mode_group.add_argument('--worker', action='store_true',
                           help='Run as worker node')
    
    # VM ID
    parser.add_argument('--vm-id', type=int,
                       help='VM ID (1-10)')
    
    # Config file
    parser.add_argument('--config', default='config.json',
                       help='Path to config file')
    
    # Command (for client mode)
    parser.add_argument('command', nargs='?',
                       choices=['submit', 'list_tasks', 'kill_task', 'status', 'stop'],
                       help='Command to execute')
    
    # Command arguments
    parser.add_argument('args', nargs='*',
                       help='Command arguments')
    
    return parser.parse_args()

# ============================================================================
# Job Submission
# ============================================================================

def submit_job(args: List[str], config: Dict) -> bool:
    """
    Submit a RainStorm job.
    
    Args format:
    <Nstages> <Ntasks_per_stage> <op1_exe> <op1_args> ... <opN_exe> <opN_args>
    <hydfs_src> <hydfs_dest> <exactly_once> <autoscale> <INPUT_RATE> <LW> <HW>
    """
    if len(args) < 7:
        print("Error: Not enough arguments for submit")
        print("Usage: submit <Nstages> <Ntasks_per_stage> <op1_exe> <op1_args> ... "
              "<hydfs_src> <hydfs_dest> <exactly_once> <autoscale> [INPUT_RATE] [LW] [HW]")
        return False
    
    try:
        # Parse required arguments
        num_stages = int(args[0])
        tasks_per_stage = int(args[1])
        
        # Parse operators (pairs of exe and args)
        op_idx = 2
        operators = []
        for i in range(num_stages):
            if op_idx + 1 >= len(args):
                print(f"Error: Missing operator {i+1}")
                return False
            op_exe = args[op_idx]
            op_args = args[op_idx + 1]
            operators.append((op_exe, op_args))
            op_idx += 2
        
        # Parse remaining arguments
        remaining = args[op_idx:]
        if len(remaining) < 4:
            print("Error: Missing src, dest, exactly_once, or autoscale")
            return False
        
        src_file = remaining[0]
        dest_file = remaining[1]
        exactly_once = remaining[2].lower() in ('true', '1', 'yes')
        autoscale = remaining[3].lower() in ('true', '1', 'yes')
        
        # Optional autoscale parameters
        input_rate = float(remaining[4]) if len(remaining) > 4 else 100
        lw = float(remaining[5]) if len(remaining) > 5 else 10
        hw = float(remaining[6]) if len(remaining) > 6 else 100
        
        # Build job config
        job_config = {
            'num_stages': num_stages,
            'tasks_per_stage': tasks_per_stage,
            'src_file': src_file,
            'dest_file': dest_file,
            'exactly_once': exactly_once,
            'autoscale': autoscale,
            'input_rate': input_rate,
            'lw': lw,
            'hw': hw
        }
        
        # Add operators
        for i, (op_exe, op_args) in enumerate(operators):
            job_config[f'op{i+1}_exe'] = op_exe
            job_config[f'op{i+1}_args'] = op_args
        
        # Send to leader
        logger = RainStormLogger("cli", 0, "submit")
        client = RPCClient(logger)
        
        leader_host = get_leader_hostname(config)
        leader_port = config.get('rainstorm_port', 8000)
        
        print(f"Submitting job to leader at {leader_host}:{leader_port}")
        print(f"  Stages: {num_stages}")
        print(f"  Tasks per stage: {tasks_per_stage}")
        print(f"  Source: {src_file}")
        print(f"  Dest: {dest_file}")
        print(f"  Exactly-once: {exactly_once}")
        print(f"  Autoscale: {autoscale}")
        if autoscale:
            print(f"  Input rate: {input_rate} tuples/sec")
            print(f"  Low watermark: {lw}")
            print(f"  High watermark: {hw}")
        
        response = client.send(
            leader_host, leader_port,
            {'type': 'SUBMIT_JOB', 'job_config': job_config},
            timeout=30.0
        )
        
        if response and response.get('status') == 'ok':
            print(f"\n✓ Job submitted successfully")
            print(f"  Job ID: {response.get('job_id')}")
            return True
        else:
            print(f"\n✗ Job submission failed")
            if response:
                print(f"  Error: {response.get('message')}")
            return False
    
    except Exception as e:
        print(f"Error submitting job: {e}")
        import traceback
        traceback.print_exc()
        return False

# ============================================================================
# Commands
# ============================================================================

def list_tasks(config: Dict):
    """List all running tasks."""
    logger = RainStormLogger("cli", 0, "list_tasks")
    client = RPCClient(logger)
    
    leader_host = get_leader_hostname(config)
    leader_port = config.get('rainstorm_port', 8000)
    
    response = client.send(
        leader_host, leader_port,
        {'type': 'LIST_TASKS'},
        timeout=10.0
    )
    
    if response and response.get('status') == 'ok':
        tasks = response.get('tasks', [])
        
        if not tasks:
            print("No tasks running")
            return
        
        print(f"\n{'Task ID':<30} {'VM':<5} {'PID':<8} {'Op Exe':<15} {'Status':<10} {'Log File'}")
        print("-" * 100)
        
        for task in tasks:
            print(f"{task['task_id']:<30} "
                  f"{task['vm']:<5} "
                  f"{task['pid'] or 'N/A':<8} "
                  f"{task['op_exe']:<15} "
                  f"{task['status']:<10} "
                  f"{task['log_file'] or 'N/A'}")
    else:
        print("Failed to get task list")
        if response:
            print(f"Error: {response.get('message')}")

def kill_task(args: List[str], config: Dict):
    """Kill a specific task."""
    if len(args) < 2:
        print("Usage: kill_task <vm_id> <pid>")
        return
    
    vm_id = int(args[0])
    pid = int(args[1])
    
    logger = RainStormLogger("cli", 0, "kill_task")
    client = RPCClient(logger)
    
    leader_host = get_leader_hostname(config)
    leader_port = config.get('rainstorm_port', 8000)
    
    print(f"Killing task on VM{vm_id} with PID {pid}...")
    
    response = client.send(
        leader_host, leader_port,
        {'type': 'KILL_TASK', 'vm_id': vm_id, 'pid': pid},
        timeout=10.0
    )
    
    if response and response.get('status') == 'ok':
        print("✓ Task killed successfully")
    else:
        print("✗ Failed to kill task")
        if response:
            print(f"Error: {response.get('message')}")

def get_status(config: Dict):
    """Get current job status."""
    logger = RainStormLogger("cli", 0, "status")
    client = RPCClient(logger)
    
    leader_host = get_leader_hostname(config)
    leader_port = config.get('rainstorm_port', 8000)
    
    response = client.send(
        leader_host, leader_port,
        {'type': 'JOB_STATUS'},
        timeout=10.0
    )
    
    if response and response.get('status') == 'ok':
        print(f"\nJob Status:")
        print(f"  Job ID: {response.get('job_id')}")
        print(f"  Status: {response.get('job_status')}")
        print(f"  Elapsed: {response.get('elapsed', 0):.2f} seconds")
        print(f"  Tasks: {response.get('tasks')}")
    else:
        print("No job running or failed to get status")
        if response:
            print(f"Message: {response.get('message')}")

def stop_job(config: Dict):
    """Stop the current job."""
    logger = RainStormLogger("cli", 0, "stop")
    client = RPCClient(logger)
    
    leader_host = get_leader_hostname(config)
    leader_port = config.get('rainstorm_port', 8000)
    
    print("Stopping current job...")
    
    response = client.send(
        leader_host, leader_port,
        {'type': 'STOP_JOB'},
        timeout=10.0
    )
    
    if response and response.get('status') == 'ok':
        print("✓ Job stopped")
    else:
        print("✗ Failed to stop job")

# ============================================================================
# Main
# ============================================================================

def main():
    args = parse_args()
    
    # Load config
    try:
        config = load_config(args.config)
    except FileNotFoundError:
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)
    
    # Determine VM ID
    vm_id = args.vm_id
    if vm_id is None and (args.leader or args.worker):
        # Try to auto-detect
        from utils import get_current_vm_id
        vm_id = get_current_vm_id(config)
        if vm_id < 0:
            print("Error: Could not determine VM ID. Please specify with --vm-id")
            sys.exit(1)
    
    # Run in appropriate mode
    if args.leader:
        print(f"Starting RainStorm Leader on VM{vm_id}")
        run_leader(vm_id, args.config)
    
    elif args.worker:
        print(f"Starting RainStorm Worker on VM{vm_id}")
        run_worker(vm_id, args.config)
    
    elif args.command:
        # Client mode - execute command
        if args.command == 'submit':
            submit_job(args.args, config)
        
        elif args.command == 'list_tasks':
            list_tasks(config)
        
        elif args.command == 'kill_task':
            kill_task(args.args, config)
        
        elif args.command == 'status':
            get_status(config)
        
        elif args.command == 'stop':
            stop_job(config)
    
    else:
        # Interactive mode or help
        print("RainStorm Stream Processing System")
        print("=" * 50)
        print("\nUsage:")
        print("  Start leader:  python rainstorm.py --leader --vm-id 1")
        print("  Start worker:  python rainstorm.py --worker --vm-id 2")
        print("  Submit job:    python rainstorm.py submit ...")
        print("  List tasks:    python rainstorm.py list_tasks")
        print("  Kill task:     python rainstorm.py kill_task <vm_id> <pid>")
        print("  Job status:    python rainstorm.py status")
        print("  Stop job:      python rainstorm.py stop")
        print("\nRun with -h for detailed help")


if __name__ == '__main__':
    main()