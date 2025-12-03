#!/usr/bin/env python3
"""
Grep operator - Filter lines containing pattern
"""
import sys

def main():
    # Get pattern from command line
    pattern = sys.argv[1] if len(sys.argv) > 1 else ""
    
    # Read from stdin, write to stdout
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        # Parse key\tvalue
        parts = line.split('\t', 1)
        if len(parts) != 2:
            continue
        
        key, value = parts
        
        # Filter
        if pattern in value:
            print(f"{key}\t{value}")
            sys.stdout.flush()

if __name__ == '__main__':
    main()