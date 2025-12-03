#!/usr/bin/env python3
"""
Replace operator - Replace pattern in lines
"""
import sys

def main():
    # Get patterns from command line
    old_pattern = sys.argv[1] if len(sys.argv) > 1 else ""
    new_pattern = sys.argv[2] if len(sys.argv) > 2 else ""
    
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
        
        # Replace
        new_value = value.replace(old_pattern, new_pattern)
        print(f"{key}\t{new_value}")
        sys.stdout.flush()

if __name__ == '__main__':
    main()