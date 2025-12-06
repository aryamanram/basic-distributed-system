#!/usr/bin/env python3
"""
RainStorm Identity Operator
Passes through input unchanged. Used for testing basic RainStorm functionality.

Usage: ./identity_op.py
Input via stdin: key<TAB>value
Output to stdout: key<TAB>value (unchanged)
"""
import sys


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        # Parse key<TAB>value format
        parts = line.split('\t', 1)
        if len(parts) == 2:
            key, value = parts
        else:
            key = ""
            value = line
        
        # Pass through unchanged
        print(f"{key}\t{value}")


if __name__ == '__main__':
    main()
