#!/usr/bin/env python3
"""
RainStorm Replace Operator
Replaces occurrences of a pattern with a replacement string.

Usage: ./replace_op.py <pattern> <replacement>
Input via stdin: key<TAB>value
Output to stdout: key<TAB>modified_value
"""
import sys


def main():
    if len(sys.argv) < 3:
        print("Usage: replace_op.py <pattern> <replacement>", file=sys.stderr)
        sys.exit(1)
    
    pattern = sys.argv[1]
    replacement = sys.argv[2]
    
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
        
        # Replace pattern with replacement
        modified_value = value.replace(pattern, replacement)
        
        print(f"{key}\t{modified_value}")


if __name__ == '__main__':
    main()
