#!/usr/bin/env python3
"""
Identity operator - Pass through (for testing)
"""
import sys

def main():
    # Read from stdin, write to stdout
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        # Just pass through
        print(line)
        sys.stdout.flush()

if __name__ == '__main__':
    main()