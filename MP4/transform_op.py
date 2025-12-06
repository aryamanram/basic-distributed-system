#!/usr/bin/env python3
"""
RainStorm Transform Operator
Extracts the first N fields from CSV data.

Usage: ./transform_op.py [num_fields]
Input via stdin: key<TAB>value (CSV line)
Output to stdout: key<TAB>field1,field2,...,fieldN
"""
import sys


def main():
    # Default to 3 fields if not specified
    num_fields = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    
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
        
        # Parse CSV and extract first N fields
        csv_parts = value.split(',')
        selected_fields = csv_parts[:num_fields]
        
        # Output selected fields joined by comma
        output = ','.join(selected_fields)
        print(f"{key}\t{output}")


if __name__ == '__main__':
    main()
