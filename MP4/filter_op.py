#!/usr/bin/env python3
"""
RainStorm Operators for Demo Applications
Application 1: Filter & Count (Filter by pattern, AggregateByKey count)
Application 2: Filter & Transform (Filter by pattern, extract first 3 fields)
"""
import sys

def filter_op():
    """
    Filter operator - accepts lines containing a pattern.
    Usage: filter_op.py <pattern>
    Input: key<TAB>value (line)
    Output: key<TAB>value (if pattern in value)
    """
    if len(sys.argv) < 2:
        sys.exit(1)
    
    pattern = sys.argv[1]
    
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
        
        # Case-sensitive string matching
        if pattern in value:
            print(f"{key}\t{value}")

def count_by_key_op():
    """
    AggregateByKey count operator - counts lines grouped by Nth column.
    Usage: count_by_key_op.py <column_index>
    Input: key<TAB>value (CSV line)
    Output: group_key<TAB>count
    
    Note: This is a stateful operator that maintains counts.
    For simplicity, we use file-based state.
    """
    if len(sys.argv) < 2:
        sys.exit(1)
    
    col_idx = int(sys.argv[1])
    state_file = f"/tmp/count_state_{sys.argv[2] if len(sys.argv) > 2 else 'default'}.json"
    
    import json
    import os
    
    # Load existing state
    counts = {}
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                counts = json.load(f)
        except:
            counts = {}
    
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
        
        # Parse CSV to get the Nth column
        csv_parts = value.split(',')
        if col_idx < len(csv_parts):
            group_key = csv_parts[col_idx].strip()
        else:
            group_key = ""  # Missing data = empty string
        
        # Increment count
        if group_key not in counts:
            counts[group_key] = 0
        counts[group_key] += 1
        
        # Output current count for this key
        print(f"{group_key}\t{counts[group_key]}")
    
    # Save state
    with open(state_file, 'w') as f:
        json.dump(counts, f)

def transform_op():
    """
    Transform operator - extracts first 3 fields from CSV.
    Usage: transform_op.py
    Input: key<TAB>value (CSV line)
    Output: key<TAB>field1,field2,field3
    """
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

def identity_op():
    """
    Identity operator - passes through input unchanged.
    Used for testing basic RainStorm functionality.
    """
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
        
        print(f"{key}\t{value}")


if __name__ == '__main__':
    # Determine which operator to run based on script name
    import os
    script_name = os.path.basename(sys.argv[0])
    
    if 'filter' in script_name:
        filter_op()
    elif 'count' in script_name:
        count_by_key_op()
    elif 'transform' in script_name:
        transform_op()
    elif 'identity' in script_name:
        identity_op()
    else:
        # Default: check first argument
        if len(sys.argv) >= 2:
            op = sys.argv[1].lower()
            sys.argv = sys.argv[1:]  # Shift arguments
            
            if op == 'filter':
                filter_op()
            elif op == 'count':
                count_by_key_op()
            elif op == 'transform':
                transform_op()
            elif op == 'identity':
                identity_op()
            else:
                print("Unknown operator", file=sys.stderr)
                sys.exit(1)
        else:
            print("Usage: ops.py <filter|count|transform|identity> [args...]", file=sys.stderr)
            sys.exit(1)