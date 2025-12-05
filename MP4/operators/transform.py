#!/usr/bin/env python3
"""
Transform Operator - extracts first 3 fields from CSV.
Used for Application 2 Stage 2.

Input: key\tvalue (where value is a CSV line)
Output: key\t<field1,field2,field3>

Extracts fields 1-3 (first three fields) from the CSV line.
"""
import sys

def parse_csv(line):
    """Parse CSV line into fields."""
    fields = []
    current = ""
    in_quotes = False
    
    for char in line:
        if char == '"':
            in_quotes = not in_quotes
        elif char == ',' and not in_quotes:
            fields.append(current)
            current = ""
        else:
            current += char
    
    fields.append(current)
    return fields

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        # Split key and value
        parts = line.split('\t', 1)
        if len(parts) < 2:
            continue
        
        key, value = parts
        
        # Parse CSV and get first 3 fields
        fields = parse_csv(value)
        
        # Get first 3 fields (or fewer if not enough)
        first_three = fields[:3]
        
        # Output
        output_value = ','.join(first_three)
        print(f"{key}\t{output_value}")
        sys.stdout.flush()

if __name__ == '__main__':
    main()