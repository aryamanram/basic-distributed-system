#!/usr/bin/env python3
"""
Aggregate Operator - counts by column N.
Used for Application 1 Stage 2.

Usage: python aggregate.py <column_number>

Input: key\tvalue (where value is a CSV line)
Output: <column_value>\t<count>

Groups by the Nth column (1-indexed) and counts occurrences.
Missing data treated as empty string.
Single space ' ' is a separate key.
"""
import sys
from collections import defaultdict

# Global state for aggregation
counts = defaultdict(int)

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
    if len(sys.argv) < 2:
        print("Usage: aggregate.py <column_number>", file=sys.stderr)
        sys.exit(1)
    
    column_num = int(sys.argv[1])  # 1-indexed
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        # Split key and value
        parts = line.split('\t', 1)
        if len(parts) < 2:
            continue
        
        key, value = parts
        
        # Parse CSV and get column value
        fields = parse_csv(value)
        
        if 0 < column_num <= len(fields):
            group_key = fields[column_num - 1]
        else:
            group_key = ""  # Missing data
        
        # Update count
        counts[group_key] += 1
        
        # Output updated count
        print(f"{group_key}\t{counts[group_key]}")
        sys.stdout.flush()

if __name__ == '__main__':
    main()