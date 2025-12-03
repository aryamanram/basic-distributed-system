#!/usr/bin/env python3
"""
WordCount operator - Count words
"""
import sys

# State for aggregation
word_counts = {}

def main():
    # Read from stdin, write to stdout
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        # Parse key\tvalue
        parts = line.split('\t', 1)
        if len(parts) != 2:
            continue
        
        key, value = parts
        
        # Split into words and count
        words = value.split()
        for word in words:
            word = word.lower().strip('.,!?;:')
            if word:
                if word not in word_counts:
                    word_counts[word] = 0
                word_counts[word] += 1
                
                # Output updated count
                print(f"{word}\t{word_counts[word]}")
                sys.stdout.flush()

if __name__ == '__main__':
    main()