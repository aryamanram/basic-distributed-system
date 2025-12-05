#!/usr/bin/env python3
"""
Filter Operator - filters lines containing a pattern.
Used for Application 1 Stage 1.

Usage: python filter.py <pattern>

Input: key\tvalue (where value is a CSV line)
Output: key\tvalue (only if value contains pattern)

Case-sensitive string matching.
"""
import sys

def main():
    if len(sys.argv) < 2:
        print("Usage: filter.py <pattern>", file=sys.stderr)
        sys.exit(1)
    
    pattern = sys.argv[1]
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        # Split key and value
        parts = line.split('\t', 1)
        if len(parts) < 2:
            continue
        
        key, value = parts
        
        # Case-sensitive pattern match
        if pattern in value:
            print(f"{key}\t{value}")
            sys.stdout.flush()

if __name__ == '__main__':
    main()