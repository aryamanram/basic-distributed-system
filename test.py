import os
import random
import string
import time
import json
import subprocess

# generates test logs based on certain patterns
def generate_test_logs(num_lines=1000):
    print("Generating test log files...")
    
    patterns = [
        "ERROR: connection failed",
        "WARNING: high memory usage detected",
        "INFO: processed successfully",
        "CRITICAL: shutting down"
    ]
    
    with open('config.json', 'r') as f:
        config = json.load(f)
    
    for vm in config['vms']:
        log_file = vm['log_file']
        with open(log_file, 'w') as f:
            for i in range(num_lines):
                if random.random() < 0.1:
                    line = random.choice(patterns)
                else:
                    line = f"{random.choice(['INFO', 'WARNING'])}: " \
                           f"{''.join(random.choices(string.ascii_letters + string.digits, k=50))}"
                
                timestamp = f"2024-09-{random.randint(1,30):02d} " \
                           f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
                f.write(f"{timestamp} {line}\n")
        
        print(f"Created {log_file} with {num_lines} lines")

# general function to test client grep for every case
def run_grep_test(pattern, options=""):
    cmd = f"python3 client.py {options} '{pattern}'"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    for line in result.stdout.split('\n'):
        if line.startswith('Total matches:'):
            count = int(line.split(':')[1].strip())
            return count
    return 0

# test that general queries work
def test_basic_search():
    print("\nTest 1: Basic Search")
    count = run_grep_test("ERROR")
    assert count > 0, "Should find ERROR patterns"
    print(f"✓ Found {count} ERROR messages")
    return True

# test that regex queries work
def test_regex_search():
    print("\nTest 2: Regex Search  ")
    count = run_grep_test("[0-9]{2}:[0-9]{2}:[0-9]{2}", "-e")
    assert count > 0, "Should find timestamp patterns"
    print(f"✓ Found {count} timestamp patterns")
    return True

# test that -i flagged queries work
def test_case_insensitive():
    print("\nTest 3: Case Insensitive Search")
    count1 = run_grep_test("error")
    count2 = run_grep_test("error", "-i")
    assert count2 >= count1, "Case-insensitive should find more or equal matches"
    print(f"✓ Case sensitive: {count1}, Case insensitive: {count2}")
    return True

# test that rare queries work
def test_rare_pattern():
    print("\nTest 4: Rare Pattern Search")
    count = run_grep_test("CRITICAL")
    print(f"✓ Found {count} CRITICAL messages (rare pattern)")
    return True

# test case where the pattern doesn't exist
def test_nonexistent_pattern():
    print("\nTest 5: Non-existent Pattern")
    count = run_grep_test("XYZABC123NOTFOUND")
    assert count == 0, "Should find no matches for non-existent pattern"
    print("✓ Correctly returned 0 matches")
    return True

# run tests
def main():
    print("-----------------------")
    print("test.py for grep system:")
    print("-----------------------")
    generate_test_logs(1000)
    print("\nStarting servers...")
    time.sleep(2)
    
    tests = [
        test_basic_search,
        test_regex_search,
        test_case_insensitive,
        test_rare_pattern,
        test_nonexistent_pattern
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"✗ Test failed: {e}")
            failed += 1
    
    print("-----------------------")
    print(f"Results: {passed} passed, {failed} failed")
    print("-----------------------")
    
    return failed == 0

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)