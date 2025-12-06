#!/bin/bash
# Cleanup script for RainStorm/HyDFS testing
# Run this on any VM to clean up files between test runs

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MP4_DIR="$SCRIPT_DIR"
MP3_DIR="$(dirname "$SCRIPT_DIR")/MP3"
MP2_DIR="$(dirname "$SCRIPT_DIR")/MP2"

echo "=== RainStorm/HyDFS Cleanup ==="
echo "Working directory: $SCRIPT_DIR"

# Count files before cleanup
count_files() {
    local pattern="$1"
    local dir="$2"
    find "$dir" -maxdepth 1 -name "$pattern" 2>/dev/null | wc -l | tr -d ' '
}

# --- MP4 (RainStorm) cleanup ---
echo ""
echo "--- MP4 (RainStorm) ---"

# Task logs: task_*.log
n=$(count_files "task_*.log" "$MP4_DIR")
if [ "$n" -gt 0 ]; then
    echo "Removing $n task log files (task_*.log)"
    rm -f "$MP4_DIR"/task_*.log
fi

# Leader/worker logs: leader_*.log, rainstorm_*.log
n=$(count_files "leader_*.log" "$MP4_DIR")
if [ "$n" -gt 0 ]; then
    echo "Removing $n leader log files (leader_*.log)"
    rm -f "$MP4_DIR"/leader_*.log
fi

n=$(count_files "rainstorm_*.log" "$MP4_DIR")
if [ "$n" -gt 0 ]; then
    echo "Removing $n rainstorm log files (rainstorm_*.log)"
    rm -f "$MP4_DIR"/rainstorm_*.log
fi

# Output files: output_*.txt
n=$(count_files "output_*.txt" "$MP4_DIR")
if [ "$n" -gt 0 ]; then
    echo "Removing $n output files (output_*.txt)"
    rm -f "$MP4_DIR"/output_*.txt
fi

# Exactly-once logs: eo_log_*.log
n=$(count_files "eo_log_*.log" "$MP4_DIR")
if [ "$n" -gt 0 ]; then
    echo "Removing $n exactly-once log files (eo_log_*.log)"
    rm -f "$MP4_DIR"/eo_log_*.log
fi

# --- MP3 (HyDFS) cleanup ---
echo ""
echo "--- MP3 (HyDFS) ---"

# HyDFS storage directories: hydfs_storage_*
for dir in "$MP3_DIR"/hydfs_storage_*; do
    if [ -d "$dir" ]; then
        echo "Removing HyDFS storage: $dir"
        rm -rf "$dir"
    fi
done

# Also check in MP4 directory (in case HyDFS runs from there)
for dir in "$MP4_DIR"/hydfs_storage_*; do
    if [ -d "$dir" ]; then
        echo "Removing HyDFS storage: $dir"
        rm -rf "$dir"
    fi
done

# --- MP2 (Membership) cleanup ---
echo ""
echo "--- MP2 (Membership) ---"

# Machine logs: machine.*.log
n=$(count_files "machine.*.log" "$MP2_DIR")
if [ "$n" -gt 0 ]; then
    echo "Removing $n machine log files (machine.*.log)"
    rm -f "$MP2_DIR"/machine.*.log
fi

# Also check MP3 and MP4 directories
for d in "$MP3_DIR" "$MP4_DIR"; do
    n=$(count_files "machine.*.log" "$d")
    if [ "$n" -gt 0 ]; then
        echo "Removing $n machine log files in $(basename "$d")"
        rm -f "$d"/machine.*.log
    fi
done

# --- /tmp cleanup ---
echo ""
echo "--- /tmp files ---"

# RainStorm source temp files
n=$(find /tmp -maxdepth 1 -name "rainstorm_src_*.csv" 2>/dev/null | wc -l | tr -d ' ')
if [ "$n" -gt 0 ]; then
    echo "Removing $n rainstorm source temp files"
    rm -f /tmp/rainstorm_src_*.csv
fi

# Count operator state files
n=$(find /tmp -maxdepth 1 -name "count_state_*.json" 2>/dev/null | wc -l | tr -d ' ')
if [ "$n" -gt 0 ]; then
    echo "Removing $n count operator state files"
    rm -f /tmp/count_state_*.json
fi

# --- Python cache cleanup ---
echo ""
echo "--- Python cache ---"

for d in "$MP2_DIR" "$MP3_DIR" "$MP4_DIR"; do
    if [ -d "$d/__pycache__" ]; then
        echo "Removing __pycache__ in $(basename "$d")"
        rm -rf "$d/__pycache__"
    fi
done

echo ""
echo "--- Setting permissions ---"

# Make operator scripts executable
chmod +x "$MP4_DIR"/*.py 2>/dev/null && echo "Made Python scripts executable"

echo ""
echo "=== Cleanup complete ==="
