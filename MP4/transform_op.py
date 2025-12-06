#!/usr/bin/env python3
"""
Transform operator - extracts first 3 fields from CSV.
Usage: ./transform_op.py
Input via stdin: key<TAB>value (CSV line)
Output to stdout: key<TAB>field1,field2,field3
"""
import sys

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split('\t', 1)
        if len(parts) == 2:
            key, value = parts
        else:
            key = ""
            value = line
        
        # Parse CSV and extract first 3 fields
        csv_parts = value.split(',')
        first_three = csv_parts[:3]
        
        # Output first 3 fields joined by comma
        output = ','.join(first_three)
        print(f"{key}\t{output}")

if __name__ == '__main__':
    main()