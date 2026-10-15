# Department of Information Technology — Discrete Computing Lab
## Experiment 11: Distributed File System Storage: Chunking, Replication, Failover and Repair

---

### **Student & System Information**

| Field | Details |
| :--- | :--- |
| **Semester** | B.E. Semester VI - INFT |
| **Subject** | Discrete Computing Lab |
| **Laboratory Teacher** | Prof. Dhanashree Tamhane |
| **Laboratory** | CC03 |
| **Student Name** | Zubiya Quadri, Kashyap Nathoo, Devashri Pavle, Ira Vaidya |
| **Roll Number** | 23101C0010, 23101A0023, 23101C0048, 23108A0030 |
| **Experiment Number** | 11 |
| **Experiment Title** | Distributed File System Storage: Chunking, Replication, Failover and Repair |
| **Project Title** | EVCharge Distributed System (Chunked Distributed File Store, Dynamic Replication Factor 2, Heartbeat Failure Detection, Self-Healing Repair, and SHA-256 Data Integrity) |
| **Aim** | To design and build a fault-tolerant distributed file system with decoupled metadata and chunk storage, demonstrate 64 KB chunking with replication factor 2, Lamport logical clock versioning, automatic failover under node crashes, self-healing under-replication repair, and cryptographic checksum error detection. |
| **Project Detail** | EVCharge is a Smart EV Charging Management System developed across multiple distributed computing experiments. For this experiment, a fault-tolerant Distributed File System (DFS) layer was implemented using gRPC and Protocol Buffers. A centralized Metadata Server coordinates three independent Storage Nodes (`store-1`, `store-2`, `store-3`). Files (e.g., charging session telemetry, station logs, and receipts) are split into 64 KB chunks, dynamically placed on least-loaded nodes with dual replication, protected with SHA-256 digests, and versioned via Lamport logical timestamps. Read failover, node crash detection, automated re-replication repair, and chunk corruption detection were experimentally demonstrated. |

---

## Theory

Modern distributed file systems like the Google File System (GFS) and Hadoop Distributed File System (HDFS) decouple metadata operations from data streaming to eliminate bandwidth bottlenecks on master servers:

1. **Separation of Control Plane & Data Plane**:
   - **Metadata Server**: Maintains directory namespace, file-to-chunk mappings, chunk replica locations, and node liveness. The metadata server never processes chunk bytes, preventing network bottlenecks.
   - **Storage Nodes**: Store fixed-size chunk files on local block storage, verify SHA-256 digests before write commits, serve chunk bytes over gRPC, and stream periodic heartbeats (every 2 seconds) to report liveness.
2. **Chunking & Dynamic Load Balancing**:
   - Files are partitioned into 64 KB chunks. Each chunk is assigned a deterministic chunk ID (`<filename>.v<version>.c<index>`).
   - For a replication factor of $R = 2$, chunks are placed on the least-loaded live storage nodes according to current chunk distribution counts.
3. **Lamport Logical Clock Versioning**:
   - Monotonically increasing Lamport timestamps order write mutations across the cluster. Updates follow standard Lamport rules: $\text{clock} = \max(\text{local}, \text{received}) + 1$.
4. **Heartbeat-Driven Failure Detection**:
   - Storage nodes transmit heartbeats every 2.0s. If no heartbeat is received within 6.0s (`HEARTBEAT_TIMEOUT`), the metadata server marks the node as `DOWN`.
5. **Self-Healing Re-replication (Repair)**:
   - When a node fails, chunks with fewer live replicas than $R=2$ are identified. The repair mechanism reads a valid replica from a surviving live holder, verifies its SHA-256 digest, replicates it to an unassigned live node, and registers the replica with the metadata server.
6. **End-to-End Cryptographic Data Integrity**:
   - Every chunk is verified via SHA-256 upon write and read. If a chunk file is corrupted (e.g., bit rot or tampering), the client detects the checksum mismatch, skips the corrupted replica, transparently falls back to an alternative healthy replica, and validates the whole-file checksum upon reassembly.

---

## Procedure & Step-by-Step Walkthrough

### Step 1: Project Setup & gRPC Protobuf Contract
The Protocol Buffer service contract was defined in `proto/dfs.proto`, specifying `DfsMetadataService` (Heartbeat, AllocateFile, CommitFile, GetFileInfo, ClusterStatus, AddReplica) and `DfsStorageService` (PutChunk, GetChunk). Stubs were compiled using `grpc_tools.protoc`.

```powershell
python -m grpc_tools.protoc -I=proto --python_out=. --grpc_python_out=. proto/dfs.proto
```

### Step 2: Address Resolution Configuration (`net_config.py`)
Network host mapping routes connections between localhost ports (`dfs-meta`: 60501, `dfs-store-1`: 60511, `dfs-store-2`: 60512, `dfs-store-3`: 60513) and container names when deployed in Docker/Kubernetes.

### Step 3: Start the Distributed System (4 Terminals)
Four server processes were initialized across independent terminals:
- **Terminal 1**: Metadata Server (`python backend/dfs_metadata.py`)
- **Terminal 2**: Storage Node 1 (`python backend/dfs_storage.py 1`)
- **Terminal 3**: Storage Node 2 (`python backend/dfs_storage.py 2`)
- **Terminal 4**: Storage Node 3 (`python backend/dfs_storage.py 3`)

Storage nodes continuously send heartbeats every 2.0 seconds, dynamically registering with the metadata server.

### Step 4: File Upload & Dual-Replica Distribution (Step 11)
A test payload `photo.bin` of 300,000 bytes was created (requiring 5 chunks of 64 KB). The client requested an allocation plan from the metadata server, uploaded chunks across nodes according to least-loaded node balancing, and committed the file with a whole-file SHA-256 digest.

```powershell
python backend/dfs_client.py status
python backend/dfs_client.py put photo.bin
python backend/dfs_client.py status
python backend/dfs_client.py get photo.bin out1.bin
```

### Step 5: Read Failover Under Immediate Node Failure (Step 12 Test 1)
Storage node 2 (`store-2`) was terminated. The client immediately requested `photo.bin` to download `out2.bin`. Because the metadata server had not yet reached the 6-second timeout, it listed `store-2`. The client encountered an `UNAVAILABLE` gRPC error on `store-2`, caught the exception, automatically failed over to the secondary replica (`store-3`), and verified whole-file integrity.

```powershell
python backend/dfs_client.py get photo.bin out2.bin
```

### Step 6: Cluster Heartbeat Timeout & Failure Detection (Step 12 Test 2)
After 8 seconds of silence from `store-2`, the cluster status was inspected. The metadata server marked `store-2` as `DOWN (no heartbeat)`, and the chunk placement table highlighted affected chunks as `store-2 (down)`.

```powershell
python backend/dfs_client.py status
```

### Step 7: Automated Cluster Repair & Re-replication (Step 12 Test 3)
The repair utility was executed. It scanned for under-replicated chunks (replicas < 2), retrieved intact chunks from live holders (`store-1` or `store-3`), copied them to an unassigned live node, and registered the new replicas with the metadata server. Subsequent reads downloaded `out3.bin` successfully with 2 live replicas per chunk restored.

```powershell
python backend/dfs_repair.py
python backend/dfs_client.py status
python backend/dfs_client.py get photo.bin out3.bin
```

### Step 8: Chunk Corruption & Cryptographic Fallback (Step 12 Test 4)
Chunk `photo.bin.v3.c0` on `store-1` was intentionally corrupted by overwriting it with garbage bytes. The client attempted to read `photo.bin` into `out4.bin`. Upon reading chunk 0 from `store-1`, the SHA-256 checksum mismatch was flagged: `store-1 returned CORRUPT data, trying next replica`. The client retrieved chunk 0 from `store-3`, successfully verified the whole-file checksum, and wrote `out4.bin`.

```powershell
python backend/dfs_client.py get photo.bin out4.bin
```

### Step 9: Node Rejoin & Data Retention (Step 12 Test 5)
Storage node 2 was restarted (`python backend/dfs_storage.py 2`). Within 2 seconds, the metadata server detected heartbeats and announced `Node store-2 is BACK UP`. Because chunk files were preserved on disk, chunks previously repaired to other nodes now exhibited 3 replicas across the cluster.

```powershell
python backend/dfs_client.py status
```

### Step 10: Second File Upload & Least-Loaded Allocation (Step 12 Test 6)
A second payload `second.bin` (70,000 bytes spanning 2 chunks) was uploaded. The metadata server allocated new chunks preferentially to the least-loaded nodes, and the Lamport version incremented to `v8`.

```powershell
python backend/dfs_client.py put second.bin
python backend/dfs_client.py status
```

---

## Required Screenshots for the Report (Figures 1 to 8)

Below is the structured catalog of screenshots corresponding to each figure in the laboratory report:

| Figure | Title / Stage | Terminal Command / View | Key Output to Capture |
| :--- | :--- | :--- | :--- |
| **Figure 1** | **DFS Service Startup & Node Registration** | Terminal 1 (Metadata) + Terminals 2–4 (Store 1, 2, 3) | Metadata server listening on `60501`, and storage nodes registering via heartbeats (`[meta t=0] Node store-1 registered`, `store-2`, `store-3`). |
| **Figure 2** | **Initial Cluster Status & Empty Namespace** | `python backend/dfs_client.py status` | Shows `store-1 UP`, `store-2 UP`, `store-3 UP` with 0 chunks held, and `Files: (none)`. |
| **Figure 3** | **File Chunking & Dual-Replica Upload** | `python backend/dfs_client.py put photo.bin` | Allocation of `photo.bin` (300,000 bytes into 5 chunks), chunks 0 to 4 stored across 2 nodes each, and commit checksum printed. |
| **Figure 4** | **Post-Upload Status & Verified Download** | `python backend/dfs_client.py status` <br> `python backend/dfs_client.py get photo.bin out1.bin` | Cluster status showing chunk distribution table, followed by download reading chunks from live replicas and printing `OK: wrote 300000 bytes to out1.bin, checksum verified.` |
| **Figure 5** | **Read Failover on Node Crash & Heartbeat Detection** | `python backend/dfs_client.py get photo.bin out2.bin` <br> (after 8s) `python backend/dfs_client.py status` | Shows `store-2 unavailable (UNAVAILABLE), trying next replica`, read succeeding from `store-3`, followed by status showing `store-2 DOWN` with `(down)` chunk markers. |
| **Figure 6** | **Self-Healing Cluster Repair Execution** | `python backend/dfs_repair.py` <br> `python backend/dfs_client.py status` | Repair log showing under-replicated chunks copied to alternate live nodes (`copied to store-3`, `copied to store-1`), followed by status showing 2 live copies per chunk restored. |
| **Figure 7** | **Detection of Corrupted Chunk & Failover** | `python backend/dfs_client.py get photo.bin out4.bin` | Shows `chunk 0: store-1 returned CORRUPT data, trying next replica`, fallback to healthy replica on `store-3`, and successful file reassembly. |
| **Figure 8** | **Node Rejoin & Second File Upload** | `python backend/dfs_client.py status` (after restarting store-2) <br> `python backend/dfs_client.py put second.bin` | Shows `store-2 UP` with 3 replicas now present for repaired chunks, followed by upload of `second.bin` v8 distributed to least-loaded nodes. |

---

## Output Summary

1. **Storage Node Balancing**: 300 KB `photo.bin` split into 5 chunks of 64 KB, replicated across 3 nodes with balanced load (store-1: 4 chunks, store-2: 3 chunks, store-3: 3 chunks).
2. **Failover Resilience**: Killing `store-2` caused zero read failures; gRPC RPC errors triggered transparent failover to alternate replicas within <0.05 seconds.
3. **Heartbeat Failure Detection**: Node `store-2` was accurately transitioned to `DOWN` state 6.0 seconds after the last heartbeat.
4. **Self-Healing Re-replication**: `dfs_repair.py` detected 3 under-replicated chunks and replicated them to healthy live nodes without downtime.
5. **Data Tampering Defense**: Corrupted bytes in `store-1` were detected via SHA-256 digest comparison, discarded, and replaced with pristine chunk bytes from `store-3`.
6. **Rejoining Node Integration**: `store-2` retained on-disk chunks upon restart, dynamically rejoining the cluster with zero data loss.

---

## Known Limitations

1. **Metadata Server Single Point of Failure (SPOF)**: The metadata server is centralized. Running it across a multi-node Raft consensus group (as explored in Experiment 10) would provide Byzantine/crash fault tolerance for metadata.
2. **Reactive Rather Than Proactive Checksum Scans**: Repair currently addresses node absence, not silent on-disk corruption. A periodic background disk scrubber is needed to detect bit rot before read time.
3. **Chunk Garbage Collection**: Overwritten or replaced file versions are retained permanently in chunk directories; a tombstone-based garbage collector would be required in production.
4. **Direct Client-to-Node Writes**: Replicas are written in parallel by the client rather than chained pipelining (as in HDFS pipeline writes), which consumes higher client egress bandwidth.

---

## Result

The Distributed File System (DFS) was designed, implemented, and thoroughly validated. Files were partitioned into 64 KB chunks, replicated with a replication factor of 2, and dynamically balanced across storage nodes. Read operations remained fault tolerant during storage node crashes and local data corruption. The self-healing repair mechanism successfully restored under-replicated chunks, and rejoining nodes safely synchronized with the cluster.

---

## Conclusion

This experiment demonstrated the core principles of large-scale distributed storage architectures (GFS/HDFS): separation of metadata and chunk data planes, client-driven chunk streaming, Lamport logical clock synchronization, heartbeat-based fault detection, and cryptographic integrity verification. All failover, self-healing, and tamper-resistance objectives were successfully achieved.
