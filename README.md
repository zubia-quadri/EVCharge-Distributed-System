# EVCharge ⚡

**Smart EV Charging Management** — A minimal Distributed Computing mini-project.

EVCharge demonstrates **EV charging slot management** using:
- A polished web UI (Flask)
- Two distributed Python services communicating over **gRPC**
- **Lamport Logical Clocks** for distributed event ordering

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  Web Browser                    │
│          http://localhost:5000                  │
└────────────────────┬────────────────────────────┘
                     │  HTTP (REST)
                     ▼
┌─────────────────────────────────────────────────┐
│          Reservation Service                    │
│       reservation_service.py  :5000             │
│       (Flask web server + gRPC CLIENT)          │
│       Lamport Clock ─ sends timestamps          │
└────────────────────┬────────────────────────────┘
                     │  gRPC (port 50051)
                     ▼
┌─────────────────────────────────────────────────┐
│           Charging Service                      │
│        charging_service.py  :50051              │
│        (gRPC SERVER)                            │
│        Lamport Clock ─ receives & updates       │
│        In-memory charger state                  │
└─────────────────────────────────────────────────┘
```

---

## Folder Structure

```
EVCharge/
│
├── frontend/
│   └── index.html              ← Single-page web UI
│
├── backend/
│   ├── reservation_service.py  ← Flask server + gRPC client
│   ├── charging_service.py     ← gRPC server
│   └── lamport_clock.py        ← Lamport Logical Clock (shared)
│
├── proto/
│   └── evcharge.proto          ← gRPC service definition
│
├── evcharge_pb2.py             ← Generated (do not edit)
├── evcharge_pb2_grpc.py        ← Generated (do not edit)
│
├── requirements.txt
└── README.md
```

---

## Installation

### Step 1 — Clone / open the project

```bash
# Already done if you have the folder
cd EVCharge
```

### Step 2 — Create a virtual environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python3 -m venv venv
source venv/bin/activate
```

### Step 3 — Install dependencies

```bash
pip install -r requirements.txt
```

This installs:
| Package | Purpose |
|---------|---------|
| `flask` | Web framework for the Reservation Service |
| `grpcio` | gRPC runtime |
| `grpcio-tools` | `protoc` compiler for Python |
| `protobuf` | Protocol Buffers serialization |

---

## Step 4 — Generate gRPC Files

**This step is required** before running the services.  
Run from the **project root** (`EVCharge/`):

```bash
python -m grpc_tools.protoc -I=proto --python_out=. --grpc_python_out=. proto/evcharge.proto
```

This generates two files in the project root:
- `evcharge_pb2.py` — message classes
- `evcharge_pb2_grpc.py` — service stubs and servicers

> ⚠️ Never edit these generated files manually.

---

## Running the Project

Open **three separate terminals**, all with the virtual environment activated.

### Terminal 1 — Start the Charging Service (gRPC Server)

```bash
python backend/charging_service.py
```

Expected output:
```
[Charging Service] gRPC server started on port 50051
[Charging Service] Waiting for requests ...
```

### Terminal 2 — Start the Reservation Service (Flask + gRPC Client)

```bash
python backend/reservation_service.py
```

Expected output:
```
[Reservation Service] Starting Flask server on http://localhost:5000
[Reservation Service] Connecting to Charging Service at localhost:50051
```

### Terminal 3 — (optional) Watch logs in real time

Both Terminal 1 and Terminal 2 will print detailed gRPC and Lamport Clock logs as requests flow through the system.

---

## Using the Application

1. Open your browser at **http://localhost:5000**
2. The UI loads and shows the live status of all 3 chargers via gRPC
3. Enter an **EV ID** (e.g., `EV102`)
4. Select an **available charger** from the dropdown
5. Click **Request Charging**
6. The UI shows whether the reservation succeeded or failed
7. Charger cards update automatically to reflect the new state

---

## Test Procedure — Verifying gRPC + Lamport Clocks

### Test 1: Reserve an available charger

1. Enter EV ID: `EV101`, select `Charger 01`, click **Request Charging**
2. **Expected Reservation Service terminal:**
   ```
   [Reservation Service] ── POST /reserve ──
   [Reservation Service] EV: EV101 → Charger: Charger_01
   [Reservation Service] Step 1 — Checking charger availability ...
     [Reservation Service] Lamport Clock (send): 1
   [Reservation Service] Availability confirmed: Charger_01 is AVAILABLE
   [Reservation Service] Step 2 — Sending reservation request ...
     [Reservation Service] Lamport Clock (send): 3
   [Reservation Service] Reservation successful ✓
   ```
3. **Expected Charging Service terminal:**
   ```
   [Charging Service] ── CheckChargerAvailability ──
     Received Timestamp: 1 → Lamport Clock: 2
     Charger_01: AVAILABLE ✓
     [Charging Service] Lamport Clock (send): 3
   
   [Charging Service] ── ReserveCharger ──
     Received Timestamp: 3 → Lamport Clock: 4
     Charger_01 reserved for EV101 ✓
     [Charging Service] Lamport Clock (send): 5
   ```

### Test 2: Reserve an occupied charger

1. Try to reserve `Charger 02` (pre-set as OCCUPIED)
2. Reservation Service will detect it's occupied and return an error
3. Activity log in the UI shows: `Charger is OCCUPIED → Reservation failed`

### Test 3: Double-reservation attempt

1. Reserve `Charger 01` (succeeds)
2. Try to reserve `Charger 01` again — should fail with "OCCUPIED"

---

## Where gRPC Is Used

| Component | Role | Port |
|-----------|------|------|
| `charging_service.py` | gRPC **server** — implements `ChargingService` | 50051 |
| `reservation_service.py` | gRPC **client** — calls the Charging Service | (client) |
| `proto/evcharge.proto` | Service contract — defines RPCs and messages | — |

**Three gRPC RPC methods are defined:**

| Method | Description |
|--------|-------------|
| `CheckChargerAvailability` | Ask if a charger is free |
| `ReserveCharger` | Reserve a charger for an EV |
| `UpdateChargerStatus` | Update a charger's status (e.g., free it) |

---

## Where Lamport Clocks Are Used

Each service maintains its **own independent Lamport Clock** (`lamport_clock.py`).

| Event | Clock Action |
|-------|-------------|
| Sending a gRPC request | `clock.send()` → attach timestamp to message |
| Receiving a gRPC response | `clock.receive(timestamp)` → `max(local, received) + 1` |

This gives every event in the distributed system a **logical timestamp**, enabling partial ordering of all actions without a global physical clock.

---

## Key Files Explained

| File | Description |
|------|-------------|
| `frontend/index.html` | Single-page UI — charger cards, reservation form, activity log |
| `backend/reservation_service.py` | Flask server that serves the UI and acts as the gRPC client |
| `backend/charging_service.py` | gRPC server that manages charger state |
| `backend/lamport_clock.py` | Reusable thread-safe Lamport Clock class |
| `proto/evcharge.proto` | gRPC proto definition (source of truth) |
| `evcharge_pb2.py` | Auto-generated protobuf message classes |
| `evcharge_pb2_grpc.py` | Auto-generated gRPC stubs |

---

## Distributed Computing Concepts Demonstrated

- **Distributed Services**: Two independent Python processes
- **gRPC**: High-performance RPC framework for inter-service communication
- **Protocol Buffers**: Efficient binary serialization of messages
- **Lamport Logical Clocks**: Causal ordering of events in a distributed system
- **Client-Server Pattern**: Reservation Service (client) ↔ Charging Service (server)

---

## Experiment 11: Distributed File System (DFS)

Experiment 11 demonstrates a fault-tolerant distributed file system with 1 Metadata Server, 3 Storage Nodes, 64 KB chunking, replication factor 2, SHA-256 integrity verification, and Lamport logical clock file versioning.

### Running DFS Services (Stage 2)

Open **four separate terminals**, all with the virtual environment activated:

#### Terminal 1 — Start the DFS Metadata Server
```powershell
.\venv\Scripts\python.exe backend\dfs_metadata.py
```
*Listens on `localhost:60501` (bound to `0.0.0.0:60501`).*

#### Terminal 2 — Start Storage Node 1 (`store-1`)
```powershell
.\venv\Scripts\python.exe backend\dfs_storage.py 1
```
*Listens on `localhost:60511`, stores chunks in `dfs_data\store-1\`.*

#### Terminal 3 — Start Storage Node 2 (`store-2`)
```powershell
.\venv\Scripts\python.exe backend\dfs_storage.py 2
```
*Listens on `localhost:60512`, stores chunks in `dfs_data\store-2\`.*

#### Terminal 4 — Start Storage Node 3 (`store-3`)
```powershell
.\venv\Scripts\python.exe backend\dfs_storage.py 3
```
*Listens on `localhost:60513`, stores chunks in `dfs_data\store-3\`.*

### Running DFS Client Commands (Stage 3)

In a **fifth terminal** (or any new terminal):

```powershell
# 1. Query cluster health, live nodes, and stored files
.\venv\Scripts\python.exe backend\dfs_client.py status

# 2. Upload a file (splits into 64 KB chunks, replicates x2, verifies SHA-256)
.\venv\Scripts\python.exe backend\dfs_client.py put test_sample.txt

# 3. Download and reassemble a file with checksum verification
.\venv\Scripts\python.exe backend\dfs_client.py get test_sample.txt downloaded_sample.txt

# 4. Run automated self-healing repair after node failures
.\venv\Scripts\python.exe backend\dfs_repair.py
```

### Automated Experiment 11 Test Runner
To run the complete automated test suite (upload, download, node kill failover, 6s heartbeat detection, repair re-replication, chunk corruption fallback, and node rejoin):
```powershell
.\venv\Scripts\python.exe test_exp11_complete_runner.py
```

