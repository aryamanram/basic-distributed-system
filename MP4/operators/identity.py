#!/usr/bin/env python3
"""
Identity Operator - passes through tuples unchanged.
Used for testing basic RainStorm functionality.

Input: key\tvalue
Output: key\tvalue
"""
import sys

def main():
    for line in sys.stdin:
        line = line.strip()
        if line:
            # Pass through unchanged
            print(line)
            sys.stdout.flush()

if __name__ == '__main__':
    main()