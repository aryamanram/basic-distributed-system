#!/usr/bin/env python3
"""
RainStorm Count/AggregateByKey Operator
Counts occurrences grouped by the Nth column of CSV data.

This is a STATEFUL operator that maintains counts across invocations.
State is stored in a local file for persistence.

Usage: ./count_op.py <column_index> [state_id]
Input via stdin: key<TAB>value (CSV line)
Output to stdout: group_key<TAB>count
"""
import json
import os
import sys


def main():
    if len(sys.argv) < 2:
        print("Usage: count_op.py <column_index> [state_id]", file=sys.stderr)
        sys.exit(1)
    
    col_idx = int(sys.argv[1])
    state_id = sys.argv[2] if len(sys.argv) > 2 else 'default'
    state_file = f"/tmp/count_state_{state_id}.json"
    
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
        
        # Parse key<TAB>value format
        parts = line.split('\t', 1)
        if len(parts) == 2:
            key, value = parts
        else:
            key = ""
            value = line
        
        # Parse CSV to get the Nth column as group key
        csv_parts = value.split(',')
        if col_idx < len(csv_parts):
            group_key = csv_parts[col_idx].strip()
        else:
            group_key = ""  # Missing data = empty string
        
        # Increment count for this group
        if group_key not in counts:
            counts[group_key] = 0
        counts[group_key] += 1
        
        # Output current count for this key
        print(f"{group_key}\t{counts[group_key]}")
    
    # Save state for next invocation
    try:
        with open(state_file, 'w') as f:
            json.dump(counts, f)
    except Exception as e:
        print(f"Warning: Could not save state: {e}", file=sys.stderr)


if __name__ == '__main__':
    main()
