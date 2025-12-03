#!/usr/bin/env python3
"""
RainStorm - Stream Processing Framework
Main entry point for launching RainStorm jobs
"""
import argparse
import sys
import os
import json
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from leader import Leader
from worker import Worker
from source import SourceProcess
from utils.logger import Logger
from utils.hydfs_client import HyDFSClient

def parse_arguments():
    """
    Parse command line arguments for RainStorm.
    
    Format:
    RainStorm <Nstages> <Ntasks_per_stage> <op1_exe> <op1_args> ... 
              <opNstages_exe> <opNstages_args> <hydfs_src_directory> 
              <hydfs_dest_filename> <exactly_once> <autoscale_enabled> 
              <INPUT_RATE> <LW> <HW>
    """
    parser = argparse.ArgumentParser(description='RainStorm Stream Processing')
    parser.add_argument('nstages', type=int, help='Number of stages')
    parser.add_argument('ntasks_per_stage', type=int, help='Tasks per stage')
    
    # Parse operator executables and arguments
    parser.add_argument('operators', nargs='+', help='Operator executables and arguments')
    
    parser.add_argument('hydfs_src_directory', help='HyDFS source directory')
    parser.add_argument('hydfs_dest_filename', help='HyDFS destination filename')
    parser.add_argument('exactly_once', type=lambda x: x.lower() == 'true', 
                       help='Enable exactly-once semantics (true/false)')
    parser.add_argument('autoscale_enabled', type=lambda x: x.lower() == 'true',
                       help='Enable autoscaling (true/false)')
    
    # Autoscaling parameters (optional)
    parser.add_argument('input_rate', nargs='?', type=float, default=None,
                       help='Input rate (tuples/sec)')
    parser.add_argument('lw', nargs='?', type=float, default=None,
                       help='Low watermark (tuples/sec per task)')
    parser.add_argument('hw', nargs='?', type=float, default=None,
                       help='High watermark (tuples/sec per task)')
    
    # Additional flags
    parser.add_argument('--mode', choices=['leader', 'worker', 'source'], 
                       default='leader', help='Run mode')
    parser.add_argument('--vm-id', type=int, help='VM ID (1-10)')
    
    args = parser.parse_args()
    
    # Parse operators
    operators = []
    i = 0
    while i < len(args.operators) and len(operators) < args.nstages:
        if i + 1 < len(args.operators):
            operators.append({
                'executable': args.operators[i],
                'arguments': args.operators[i + 1]
            })
            i += 2
        else:
            operators.append({
                'executable': args.operators[i],
                'arguments': ''
            })
            i += 1
    
    return args, operators

def load_config():
    """Load system configuration."""
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

def run_leader(args, operators, config):
    """Run as leader node."""
    logger = Logger(vm_id=args.vm_id or 1, prefix="LEADER")
    logger.log("Starting RainStorm Leader")
    
    job_config = {
        'nstages': args.nstages,
        'ntasks_per_stage': args.ntasks_per_stage,
        'operators': operators,
        'hydfs_src_directory': args.hydfs_src_directory,
        'hydfs_dest_filename': args.hydfs_dest_filename,
        'exactly_once': args.exactly_once,
        'autoscale_enabled': args.autoscale_enabled,
        'input_rate': args.input_rate,
        'lw': args.lw,
        'hw': args.hw
    }
    
    leader = Leader(config, logger)
    leader.start()
    leader.submit_job(job_config)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.log("Stopping RainStorm Leader")
        leader.stop()

def run_worker(args, config):
    """Run as worker node."""
    if not args.vm_id:
        print("Error: --vm-id required for worker mode")
        sys.exit(1)
    
    logger = Logger(vm_id=args.vm_id, prefix="WORKER")
    logger.log(f"Starting RainStorm Worker on VM{args.vm_id}")
    
    worker = Worker(vm_id=args.vm_id, config=config, logger=logger)
    worker.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.log("Stopping RainStorm Worker")
        worker.stop()

def run_source(args, config):
    """Run as source process."""
    if not args.vm_id:
        print("Error: --vm-id required for source mode")
        sys.exit(1)
    
    logger = Logger(vm_id=args.vm_id, prefix="SOURCE")
    logger.log(f"Starting RainStorm Source on VM{args.vm_id}")
    
    source = SourceProcess(
        vm_id=args.vm_id,
        config=config,
        logger=logger,
        hydfs_directory=args.hydfs_src_directory,
        input_rate=args.input_rate or 100.0
    )
    source.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.log("Stopping RainStorm Source")
        source.stop()

def main():
    args, operators = parse_arguments()
    config = load_config()
    
    if args.mode == 'leader':
        run_leader(args, operators, config)
    elif args.mode == 'worker':
        run_worker(args, config)
    elif args.mode == 'source':
        run_source(args, config)

if __name__ == '__main__':
    main()