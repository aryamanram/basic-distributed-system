# HyDFS - Hybrid Distributed File System (MP3)
**CS425 Fall 2025 - Group 7**  
Aditi Sadwelkar, Ary Ramchandran

## Overview

HyDFS is a hybrid distributed file system combining concepts from HDFS and Cassandra. It provides:
- **Consistent hashing** for node and file placement on a ring
- **3-way replication** to tolerate up to 2 simultaneous failures
- **Per-client append ordering** guarantee
- **Eventual consistency** across replicas
- **Read-my-writes** consistency for individual clients
- **Automatic re-replication** after failures
- **Automatic re-balancing** when nodes join

## Architecture

### Components

1. **ring.py** - Consistent hashing ring implementation
2. **storage.py** - Block-based file storage with metadata
3. **consistency.py** - Consistency guarantees and merge operations
4. **network.py** - Network communication and RPC handling
5. **replication.py** - Replication management and failure handling
6. **node.py** - Core HyDFS node implementation
7. **utils.py** - Utility functions (hashing, logging, serialization)
8. **main.py** - Command-line interface

### Design Decisions

- **Replication Factor**: 3 replicas (minimum to tolerate 2 failures)
- **Consistent Hashing**: SHA-1 hash function for ring positions
- **File Storage**: Block-based storage where each block = one append operation
- **Block Metadata**: Each block tracks `(block_id, client_id, sequence_num, timestamp, data, size)`
- **Merge Algorithm**: Collect blocks from all replicas, sort by `(client_id, sequence_num, timestamp)`
- **Node ID Format**: `hostname:port:timestamp` for unique identification
- **Replication Strategy**: Write to all replicas asynchronously

## Running the System

### Prerequisites

- Python 3.7+
- Access to CS VM Cluster (fa25-cs425-a701 through a710)
- Completed MP1 and MP2 implementations

### Setup

1. Ensure MP1 and MP2 directories are at the same level as MP3:
```
├── MP1/
│   ├── client.py
│   ├── server.py
│   └── config.json
├── MP2/
│   ├── membership.py
│   └── config.json
└── MP3/
    ├── main.py
    ├── node.py
    ├── ring.py
    ├── storage.py
    ├── consistency.py
    ├── network.py
    ├── replication.py
    └── utils.py
```

2. Start HyDFS on each VM:
```bash
cd MP3
python main.py --vm-id <1-10>
```

Example:
```bash
# On VM1 (fa25-cs425-a701)
python main.py --vm-id 1

# On VM2 (fa25-cs425-a702)
python main.py --vm-id 2

# ... etc for all 10 VMs
```

## File Operations

### 1. Create File
```bash
create <localfilename> <HyDFSfilename>
```
Creates a file on HyDFS and copies contents from local file.

Example:
```bash
create test.txt hydfs_test.txt
```

### 2. Get File
```bash
get <HyDFSfilename> <localfilename>
```
Fetches entire file from HyDFS to local file.

Example:
```bash
get hydfs_test.txt downloaded.txt
```

### 3. Append to File
```bash
append <localfilename> <HyDFSfilename>
```
Appends contents of local file to HyDFS file.

Example:
```bash
append append_data.txt hydfs_test.txt
```

### 4. Merge File
```bash
merge <HyDFSfilename>
```
Ensures all replicas of a file are identical.

Example:
```bash
merge hydfs_test.txt
```

## Demo Operations

### 1. List File Replicas
```bash
ls <HyDFSfilename>
```
Lists all machines storing the file with their ring IDs and file ID.

Example:
```bash
ls hydfs_test.txt
```

### 2. List Stored Files
```bash
liststore
```
Lists all files stored on the current VM with their file IDs and the VM's ring ID.

Example:
```bash
liststore
```

### 3. Get from Specific Replica
```bash
getfromreplica <VMaddress> <HyDFSfilename> <localfilename>
```
Gets file from a specific replica (used for testing consistency).

Example:
```bash
getfromreplica fa25-cs425-a701.cs.illinois.edu hydfs_test.txt test_from_vm1.txt
```

### 4. List Membership with Ring IDs
```bash
list_mem_ids
```
Shows all active nodes sorted by ring position.

Example:
```bash
list_mem_ids
```

### 5. Multi-Append
```bash
multiappend <HyDFSfilename> <VM1,VM2,...> <file1,file2,...>
```
Launches simultaneous appends from multiple VMs.

Example:
```bash
multiappend hydfs_test.txt vm1,vm2,vm3 append1.txt,append2.txt,append3.txt
```

## Consistency Guarantees

### 1. Per-Client Append Ordering
If a client completes append A and then issues append B to the same file:
- A must appear before B in the file
- Enforced using client-specific sequence numbers

### 2. Eventual Consistency
Once the system quiesces (no outstanding updates or failures):
- All replicas contain identical copies of each file
- Achieved through periodic background merging and explicit merge operations

### 3. Read-My-Writes
If a client issues a get:
- System returns file contents reflecting the client's most recent completed appends
- May not immediately reflect appends from other clients

## Failure Handling

### Node Failure
1. MP2 membership protocol detects failure
2. Node is removed from consistent hashing ring
3. Replication manager detects under-replicated files
4. Files are automatically re-replicated to maintain 3 replicas

### Node Join
1. Node joins via MP2 membership protocol
2. Node is added to consistent hashing ring
3. Files are automatically rebalanced
4. Node fetches files it should store based on ring position

### Node Rejoin
When a node rejoins after failure:
- Gets new node ID (with new timestamp)
- All old file blocks are deleted before rejoin
- Fetches required files from other replicas

## Logging

All operations are logged to `machine.<vm_id>.log` with timestamps:
- File operation requests (received)
- File operation completions
- Replication events
- Membership changes
- Merge operations

Query logs using MP1's grep mechanism:
```bash
cd ../MP1
python client.py "OPERATION" -n
```

## Testing

### Basic Functionality Test
```bash
# Create a file
create test.txt file1

# Append to it
append append.txt file1

# Get it back
get file1 retrieved.txt

# Check replicas
ls file1

# Merge replicas
merge file1
```

### Consistency Test
```bash
# From VM1: Append data1
append data1.txt testfile

# From VM2: Append data2 simultaneously
append data2.txt testfile

# Merge the file
merge testfile

# Verify all replicas are identical
getfromreplica fa25-cs425-a701.cs.illinois.edu testfile test_vm1.txt
getfromreplica fa25-cs425-a702.cs.illinois.edu testfile test_vm2.txt
getfromreplica fa25-cs425-a703.cs.illinois.edu testfile test_vm3.txt

# Compare files (should be identical)
diff test_vm1.txt test_vm2.txt
diff test_vm2.txt test_vm3.txt
```

### Failure Test
```bash
# Create and replicate a file across 3 VMs
create test.txt testfile

# Check replicas
ls testfile

# Kill one VM (Ctrl+C on that VM)

# Wait for failure detection (~5 seconds)

# Check that file is still accessible
get testfile retrieved.txt

# Check replicas (should still be 3)
ls testfile
```

## Performance Considerations

### Replication Overhead
- Write operations require 3 network round-trips (one per replica)
- Asynchronous replication reduces latency
- Typical create/append: 50-200ms depending on network and file size

### Merge Performance
- Merge time scales with:
  - Number of blocks
  - Number of concurrent appends
  - File size
- Typical merge: 100ms-2s for files with <100 blocks

### Re-replication Bandwidth
- Monitored every 5 seconds
- Bandwidth depends on file size and replication factor
- Large files (>1MB) may take several seconds to re-replicate

## Troubleshooting

### "No replica nodes available"
- Ensure membership protocol is working (check with `list_mem_ids`)
- Wait for membership to stabilize after starting nodes

### "File not found"
- Check if file exists with `ls <filename>`
- Verify replicas are online with `list_mem_ids`

### "Merge already in progress"
- Wait for current merge to complete
- Only one merge per file can run at a time

### "Pending writes for this file"
- A client has an in-progress append
- Wait for append to complete before reading

## Integration with MP1 and MP2

### MP1 (Distributed Grep)
- Used for querying HyDFS logs
- All file operations are logged and queryable

### MP2 (Membership Protocol)
- Provides failure detection
- Maintains active node list
- HyDFS monitors membership for ring updates

## Code Structure

### Node Lifecycle
1. **Initialization**: Create storage, ring, network, replication manager
2. **Start**: Start network server, membership service, replication monitoring
3. **Operation**: Handle file operations, monitor membership, re-replicate
4. **Stop**: Leave membership, stop services, cleanup

### Request Flow (Create)
1. Client reads local file
2. Client determines replicas from ring
3. Client sends CREATE message to all replicas
4. Each replica stores file locally
5. Client reports success/failure

### Request Flow (Append)
1. Client reads local file
2. Client gets sequence number from consistency manager
3. Client creates block with metadata
4. Client sends APPEND message to all replicas with block
5. Each replica appends block to file
6. Client marks write as complete

### Request Flow (Merge)
1. Client sends MERGE message to coordinator (first replica)
2. Coordinator collects blocks from all replicas
3. Coordinator merges blocks (sorts by client_id, sequence_num, timestamp)
4. Coordinator validates ordering
5. Coordinator sends merged blocks to all replicas
6. All replicas replace their blocks with merged version

## Notes

- **LLM Usage**: If LLMs were used in development, document this in the report
- **Citations**: Include chat session files if LLMs were used
- **Testing**: Recommended to write unit tests for basic operations
- **Git**: Use git for code management (VMs have no persistent storage)

## Demo Preparation

1. Ensure all 10 VMs are running HyDFS
2. Verify membership protocol is working
3. Create several test files of varying sizes
4. Practice all commands
5. Prepare to demonstrate:
   - File operations (create, get, append)
   - Consistency (merge, concurrent appends)
   - Failure handling (kill a node, verify re-replication)
   - Rebalancing (add a node, verify file movement)

## Contact

For questions or issues:
- Check Piazza
- Contact TAs: Aishwarya/Indy
- Review MP specification document

---

**Good luck with your demo!**