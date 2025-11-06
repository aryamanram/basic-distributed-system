"""
HyDFS Main - Starts the node and waits (commands handled via controller)
"""
import argparse
import time
from node import HyDFSNode

def main():
    parser = argparse.ArgumentParser(description='HyDFS - Hybrid Distributed File System')
    parser.add_argument('--vm-id', type=int, required=True, help='VM ID (1-10)')
    
    args = parser.parse_args()
    
    # Create and start node
    node = HyDFSNode(args.vm_id)
    node.start()
    
    # Join the group
    print("\nJoining group...")
    node.membership.join_group()
    time.sleep(2)  # Wait for membership to stabilize
    
    print("\n" + "="*60)
    print(f"HyDFS Node {args.vm_id} is running")
    print("="*60)
    print(f"Hostname: {node.hostname}")
    print(f"HyDFS Port: {node.port}")
    print(f"Control Port: {node.control_port}")
    print(f"Node ID: {node.node_id[:50]}...")
    print(f"Ring Position: {node.ring.get_node_position(node.node_id)}")
    print("\nUse the controller (controller.py) to send commands")
    print("Press Ctrl+C to stop")
    print("="*60 + "\n")
    
    # Keep running
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\n\nShutting down...")
    
    # Cleanup
    print("Leaving group...")
    node.membership.leave_group()
    time.sleep(1)
    node.stop()
    print("Goodbye!")

if __name__ == '__main__':
    main()