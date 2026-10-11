# DC Lab: Experiment 10 Walkthrough & Lab Report
## Deploy a Complete Distributed System on Kubernetes, Simulate Load & Test Failover

---

### **Student & System Information**
- **Experiment Number:** 10
- **Title:** General Purpose Distributed Computing with Kubernetes (Deployments, Self-Healing, Headless Services & gRPC Load Balancing)
- **Application:** EVCharge Distributed System
- **Replicas Configured:** **5 Replicas** (`replicas: 5` — scaling beyond standard 3 replicas)
- **Environment:** Kubernetes (k3s control-plane) on WSL2 / Windows 11, Docker container runtime, Python 3.11, gRPC v1.84, Protobuf v7.36

---

## 1. Objectives & Theoretical Framework

### 1.1 Goal
Deploy a multi-service distributed architecture (Raft consensus cluster + replicated EV charging worker pool) onto a live Kubernetes cluster, verify inter-pod gRPC networking, test consensus failover behavior under Kubernetes controller semantics, and solve the **gRPC L7 load balancing trap** by distributing concurrent traffic across **5 active replicas** via a Headless Service and client-side round-robin balancing.

### 1.2 Docker Compose vs. Kubernetes
| Feature | Docker Compose | Kubernetes |
| :--- | :--- | :--- |
| **Container Lifecycle** | Starts containers; if a container exits, it may remain stopped unless explicitly configured with restart policies. | **Self-healing Declarative Reconciliation**: Controllers (`ReplicaSet`, `Deployment`) continually reconcile actual state to desired state. |
| **Failover Mechanics** | Stopping a container keeps it dead. | Deleting a pod causes an instant replacement within milliseconds. To simulate true node failure, the deployment must be scaled to `0`. |
| **Scaling** | Manual port mapping conflicts on single host. | Native single-field scaling (`replicas: 5`) with dynamic Pod IP allocation on an overlay SDN (`10.42.0.0/16`). |
| **Service Discovery** | Static container name DNS. | Integrated CoreDNS with ClusterIP, Headless Services (`clusterIP: None`), and Endpoint slices. |

---

## 2. Distributed Architecture & Topology

```
                              ┌────────────────────────────────────────┐
                              │       Kubernetes Load-Test Job         │
                              │           (batch/v1 Job)               │
                              │    Channel: dns:///worker:60201        │
                              │   LB Policy: round_robin (gRPC L7)     │
                              └───────────────────┬────────────────────┘
                                                  │
                                                  ▼
                     ┌─────────────────────────────────────────────────────────┐
                     │            Headless Service: "worker"                   │
                     │                 (clusterIP: None)                       │
                     │          Returns all 5 A-records via DNS                │
                     └───┬─────────────┬─────────────┬─────────────┬───────────┘
                         │             │             │             │
        ┌────────────────┼─────────────┼─────────────┼─────────────┼────────────────┐
        ▼                ▼             ▼             ▼             ▼                ▼
 ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
 │ Worker Pod 1│  │ Worker Pod 2│  │ Worker Pod 3│  │ Worker Pod 4│  │ Worker Pod 5│
 │ 10.42.0.139 │  │ 10.42.0.141 │  │ 10.42.0.142 │  │ 10.42.0.144 │  │ 10.42.0.143 │
 └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘

                                ═════════════════════════════════
                                 Raft Consensus Cluster (3 Nodes)
                                ═════════════════════════════════
        ┌─────────────────────────┐   Heartbeats / Votes    ┌─────────────────────────┐
        │       Service-A         │ ◄─────────────────────► │       Service-B         │
        │      (10.42.0.140)      │                         │      (10.42.0.137)      │
        └────────────┬────────────┘                         └────────────┬────────────┘
                     │                                                   │
                     │              ┌─────────────────────────┐          │
                     └────────────► │       Service-C         │ ◄────────┘
                                    │      (10.42.0.145)      │
                                    └─────────────────────────┘
```

---

## 3. Step-by-Step Implementation & Walkthrough

### Step 1: Solving the Two Critical Networking Traps

#### Trap 1: The Localhost Bind Address Trap
When a gRPC server binds to `localhost:port`, the Linux network stack inside the container attaches the listening socket exclusively to the container's local loopback interface (`127.0.0.1`). Consequently, inbound packets routed from other pods via the Kubernetes virtual bridge are dropped.
- **Remedy**: Every server must bind to `0.0.0.0:{port}`:
  ```python
  # backend/load_worker.py & backend/raft_node.py
  server.add_insecure_port(f"0.0.0.0:{port}")
  ```

#### Trap 2: Hardcoded Peer Addresses Trap
If a pod dials `localhost:PORT` to contact a peer, it communicates with its own local loopback rather than remote microservices. Kubernetes assigns predictable DNS records matching Service names (`service-a:60401`, `service-b:60402`, `worker:60201`).
- **Remedy**: An environment-aware name resolver (`net_config.py`):
  ```python
  # net_config.py
  import os

  RUN_ENV = os.environ.get("RUN_ENV", "local")

  LOCAL_HOSTS = {
      "service-a": "localhost:60401",
      "service-b": "localhost:60402",
      "service-c": "localhost:60403",
      "worker": "localhost:60201",
  }

  DOCKER_HOSTS = {
      "service-a": "service-a:60401",
      "service-b": "service-b:60402",
      "service-c": "service-c:60403",
      "worker": "worker:60201",
  }

  def resolve(name):
      hosts = DOCKER_HOSTS if RUN_ENV == "docker" else LOCAL_HOSTS
      return hosts.get(name, f"{name}:60401")
  ```

---

### Step 2: Enabling Kubernetes & Node Verification

The Kubernetes cluster was activated with Docker container runtime support. Verification confirmed the single-node control-plane state:

```bash
kubectl version --client
kubectl get nodes -o wide
```

#### Terminal Output / Screenshot Proof:
```text
PS C:\Users\quadr\.gemini\antigravity\scratch\EVCharge> .\kubectl.bat version --client
Client Version: v1.36.5+k3s1
Kustomize Version: v5.8.1

PS C:\Users\quadr\.gemini\antigravity\scratch\EVCharge> .\kubectl.bat get nodes -o wide
NAME     STATUS   ROLES           AGE    VERSION        INTERNAL-IP      OS-IMAGE             KERNEL-VERSION
z-zown   Ready    control-plane   12m    v1.36.5+k3s1   172.27.192.227   Ubuntu 24.04.1 LTS   6.18.33.2-microsoft-standard-WSL2
```

---

### Step 3: Shared Microservice Docker Image

All microservices (Raft consensus peers, stateless EV workers, load client) share dependencies (`grpcio`, `protobuf`, `flask`, `redis`). A single Docker image was constructed with `ENV PYTHONUNBUFFERED=1` so standard output streams immediately flush to container logs without buffer stalling.

#### Dockerfile:
```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV RUN_ENV=docker
ENV PYTHONUNBUFFERED=1

CMD ["python", "peer_node.py", "A"]
```

#### Build Execution:
```bash
docker build -t myapp-backend:latest .
```
```text
Sending build context to Docker daemon  224.3kB
Step 1/8 : FROM python:3.11-slim
 ---> 0dd364ba7e10
...
Step 7/8 : ENV PYTHONUNBUFFERED=1
 ---> Running in 605935f97538
 ---> 736420b3ed44
Step 8/8 : CMD ["python", "peer_node.py", "A"]
 ---> Running in 07e6dea26eb5
 ---> ca2217e466ef
Successfully built ca2217e466ef
Successfully tagged myapp-backend:latest
```

---

### Step 4: Writing Declarative Kubernetes Manifests (`k8s/app.yaml`)

Each peer component (`service-a`, `service-b`, `service-c`) receives a Deployment and a corresponding Service.
The stateless worker pool is declared with **5 replicas** (`replicas: 5`) and backed by a **Headless Service** (`clusterIP: None`):

```yaml
# Replicated Worker Deployment with 5 replicas (> 3 replicas configured)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: worker
spec:
  replicas: 5
  selector:
    matchLabels:
      app: worker
  template:
    metadata:
      labels:
        app: worker
    spec:
      containers:
      - name: worker
        image: myapp-backend:latest
        imagePullPolicy: Never
        command: ["python", "-u", "backend/load_worker.py", "60201"]
        ports:
        - containerPort: 60201
---
# Headless Service: returns all pod IPs via DNS for client-side gRPC load balancing
apiVersion: v1
kind: Service
metadata:
  name: worker
spec:
  clusterIP: None
  selector:
    app: worker
  ports:
  - port: 60201
    targetPort: 60201
```

---

### Step 5: Deployment & Cross-Pod Coordination Verification

Applying the manifests brought all deployments and services into the active cluster:

```bash
kubectl apply -f k8s/app.yaml
kubectl get pods -o wide
```

#### Terminal Output / Screenshot Proof:
```text
PS C:\Users\quadr\.gemini\antigravity\scratch\EVCharge> .\kubectl.bat get pods -o wide
NAME                         READY   STATUS    RESTARTS   AGE   IP            NODE     NOMINATED NODE   READINESS GATES
service-a-7c6dff4f9b-ppnmc   1/1     Running   0          59s   10.42.0.140   z-zown   <none>           <none>
service-b-9f9c786db-mprfq    1/1     Running   0          59s   10.42.0.137   z-zown   <none>           <none>
service-c-65b497b47b-mvrs6   1/1     Running   0          59s   10.42.0.145   z-zown   <none>           <none>
worker-7b6fd5488f-5q8bt      1/1     Running   0          59s   10.42.0.139   z-zown   <none>           <none>
worker-7b6fd5488f-89g2j      1/1     Running   0          57s   10.42.0.141   z-zown   <none>           <none>
worker-7b6fd5488f-8wbq7      1/1     Running   0          59s   10.42.0.142   z-zown   <none>           <none>
worker-7b6fd5488f-k5ct4      1/1     Running   0          59s   10.42.0.144   z-zown   <none>           <none>
worker-7b6fd5488f-tcg87      1/1     Running   0          57s   10.42.0.143   z-zown   <none>           <none>
```

#### Cross-Pod Consensus Coordination Logs:
```text
PS C:\Users\quadr\.gemini\antigravity\scratch\EVCharge> .\kubectl.bat logs deployment/service-b
============================================================
EVCharge Raft Node B Running
============================================================
Address       : service-b:60402
Role          : FOLLOWER
Term          : 0
Peers         : {'A': 'service-a:60401', 'C': 'service-c:60403'}
Services      : RaftService + LockService (Experiment 5)
============================================================

[B term=1 CANDIDATE] Starting election
[B term=2 CANDIDATE] Starting election
[B term=2 CANDIDATE] Vote from A
[B term=2 LEADER] *** ELECTED LEADER *** (2/3 votes)
```
```text
PS C:\Users\quadr\.gemini\antigravity\scratch\EVCharge> .\kubectl.bat logs deployment/service-a
[A term=2 FOLLOWER] Leader = B
```
```text
PS C:\Users\quadr\.gemini\antigravity\scratch\EVCharge> .\kubectl.bat logs deployment/service-c
[C term=2 FOLLOWER] Leader = B
```

**Observation:** Node B solicited votes over Kubernetes internal DNS (`service-a:60401`), received quorum consent from Node A, and transitioned to `LEADER` for Term 2. Nodes A and C acknowledged heartbeats and synchronized as `FOLLOWER`s.

---

### Step 6: Testing Failover Under Kubernetes Controller Semantics

#### Why `kubectl delete pod` Behaves Differently:
In Docker Compose, killing a container leaves it offline. In Kubernetes, deleting a pod triggers immediate replacement by the ReplicaSet controller within milliseconds—often before surviving nodes exhaust their randomized election timers.
To test genuine network partition and failover, the deployment must be scaled down to 0 replicas:

```bash
kubectl scale deployment service-b --replicas=0
```

#### Election of New Leader by Surviving Quorum:
Upon Node B's failure, Nodes A and C detected missed heartbeats, timed out, and initiated a new election at **Term 3**:

```text
PS C:\Users\quadr\.gemini\antigravity\scratch\EVCharge> .\kubectl.bat logs deployment/service-a
[A term=2 FOLLOWER] Leader = B
[A term=3 CANDIDATE] Starting election
[A term=3 CANDIDATE] Vote from C
[A term=3 LEADER] *** ELECTED LEADER *** (2/3 votes)

PS C:\Users\quadr\.gemini\antigravity\scratch\EVCharge> .\kubectl.bat logs deployment/service-c
[C term=3 FOLLOWER] Leader = A
```

#### Rejoining Re-scaled Node:
```bash
kubectl scale deployment service-b --replicas=1
```
When Node B was restored, it received heartbeats from active Leader A at Term 3 and quietly transitioned to `FOLLOWER`:
```text
PS C:\Users\quadr\.gemini\antigravity\scratch\EVCharge> .\kubectl.bat logs deployment/service-b
[B term=3 FOLLOWER] Leader = A
```

---

### Step 7: Simulating Load via Kubernetes Job (`k8s/load-test-job.yaml`)

Because cluster pods reside on an internal overlay network (`10.42.0.0/16`), the load generator was executed natively inside the cluster as a Kubernetes `batch/v1` Job:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: load-test
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: load-test
        image: myapp-backend:latest
        imagePullPolicy: Never
        command: ["python", "-u", "load_test.py"]
        env:
        - name: TARGET_ADDR
          value: "worker:60201"
        - name: NUM_REQUESTS
          value: "25"
        - name: CONCURRENCY
          value: "5"
```

#### Execution:
```bash
kubectl apply -f k8s/load-test-job.yaml
kubectl wait --for=condition=complete job/load-test --timeout=90s
kubectl logs job/load-test
```

#### Terminal Output / Screenshot Proof:
```text
PS C:\Users\quadr\.gemini\antigravity\scratch\EVCharge> .\kubectl.bat logs job/load-test
=================================================================
EVCharge Kubernetes Load Test Starting
Target Service   : dns:///worker:60201
Total Requests   : 25
Concurrency      : 5
gRPC LB Policy   : round_robin
=================================================================
[LoadClient] Req 03 -> EVWorker-worker-7b6fd5488f-k5ct4 (0.13s, status=completed)
[LoadClient] Req 02 -> EVWorker-worker-7b6fd5488f-k5ct4 (0.18s, status=completed)
[LoadClient] Req 01 -> EVWorker-worker-7b6fd5488f-k5ct4 (0.22s, status=completed)
[LoadClient] Req 04 -> EVWorker-worker-7b6fd5488f-k5ct4 (0.28s, status=completed)
[LoadClient] Req 07 -> EVWorker-worker-7b6fd5488f-8wbq7 (0.13s, status=completed)
[LoadClient] Req 06 -> EVWorker-worker-7b6fd5488f-k5ct4 (0.19s, status=completed)
[LoadClient] Req 05 -> EVWorker-worker-7b6fd5488f-k5ct4 (0.34s, status=completed)
[LoadClient] Req 09 -> EVWorker-worker-7b6fd5488f-89g2j (0.11s, status=completed)
[LoadClient] Req 08 -> EVWorker-worker-7b6fd5488f-tcg87 (0.24s, status=completed)
[LoadClient] Req 12 -> EVWorker-worker-7b6fd5488f-8wbq7 (0.13s, status=completed)
[LoadClient] Req 10 -> EVWorker-worker-7b6fd5488f-5q8bt (0.36s, status=completed)
[LoadClient] Req 11 -> EVWorker-worker-7b6fd5488f-k5ct4 (0.39s, status=completed)
[LoadClient] Req 15 -> EVWorker-worker-7b6fd5488f-5q8bt (0.30s, status=completed)
[LoadClient] Req 13 -> EVWorker-worker-7b6fd5488f-tcg87 (0.40s, status=completed)
[LoadClient] Req 14 -> EVWorker-worker-7b6fd5488f-89g2j (0.35s, status=completed)
[LoadClient] Req 16 -> EVWorker-worker-7b6fd5488f-k5ct4 (0.15s, status=completed)
[LoadClient] Req 17 -> EVWorker-worker-7b6fd5488f-8wbq7 (0.18s, status=completed)
[LoadClient] Req 18 -> EVWorker-worker-7b6fd5488f-tcg87 (0.14s, status=completed)
[LoadClient] Req 21 -> EVWorker-worker-7b6fd5488f-k5ct4 (0.11s, status=completed)
[LoadClient] Req 19 -> EVWorker-worker-7b6fd5488f-89g2j (0.16s, status=completed)
[LoadClient] Req 22 -> EVWorker-worker-7b6fd5488f-8wbq7 (0.19s, status=completed)
[LoadClient] Req 20 -> EVWorker-worker-7b6fd5488f-5q8bt (0.27s, status=completed)
[LoadClient] Req 25 -> EVWorker-worker-7b6fd5488f-5q8bt (0.18s, status=completed)
[LoadClient] Req 24 -> EVWorker-worker-7b6fd5488f-89g2j (0.34s, status=completed)
[LoadClient] Req 23 -> EVWorker-worker-7b6fd5488f-tcg87 (0.39s, status=completed)

=================================================================
Load Test Summary
=================================================================
Total Completed Requests: 25/25 (100.0%)
Total Duration          : 1.32s
Throughput              : 19.01 req/s

Traffic Distribution per Pod Replica:
  • EVWorker-worker-7b6fd5488f-5q8bt   :  4 requests (16.0%)
  • EVWorker-worker-7b6fd5488f-89g2j   :  4 requests (16.0%)
  • EVWorker-worker-7b6fd5488f-8wbq7   :  4 requests (16.0%)
  • EVWorker-worker-7b6fd5488f-k5ct4   :  9 requests (36.0%)
  • EVWorker-worker-7b6fd5488f-tcg87   :  4 requests (16.0%)
=================================================================
```

---

### Step 8: Verifying Multi-Replica Traffic Distribution (Overcoming the gRPC L4 Trap)

#### The Problem:
Standard Kubernetes Services operate at **Layer 4 (TCP)** via `iptables`/IPVS. Because gRPC multiplexes all RPCs over a single persistent HTTP/2 TCP connection, traditional ClusterIP load balancers bind the entire connection to a single backend pod. As a result, 1 pod receives 100% of the traffic while other replicas sit completely idle.

#### The Triple Fix:
1. **Headless Service (`clusterIP: None`)**: CoreDNS returns an `A` record for every individual pod IP instead of a single virtual ClusterIP.
2. **DNS Resolver Prefix**: Connecting via `dns:///worker:60201` instructs the gRPC C-core resolver to perform multi-record DNS resolution.
3. **Client-Side Balancing Policy**: `options=[("grpc.lb_policy_name", "round_robin")]` instructs gRPC to maintain subchannels to all resolved IPs and rotate streams per RPC.

#### Multi-Replica Pod Log Verification:
Executing the verification command across all 5 replica pods:

```bash
for p in $(kubectl get pods -l app=worker -o name); do
  echo "$p: $(kubectl logs $p | grep -o "Handling request" | wc -l) requests"
done
```

#### Terminal Output / Screenshot Proof:
```text
pod/worker-7b6fd5488f-5q8bt: 4 requests
pod/worker-7b6fd5488f-89g2j: 4 requests
pod/worker-7b6fd5488f-8wbq7: 4 requests
pod/worker-7b6fd5488f-k5ct4: 9 requests
pod/worker-7b6fd5488f-tcg87: 4 requests
```

**Total Traffic Verified:** `4 + 4 + 4 + 9 + 4 = 25 requests (100% distribution across all 5 replicas)`.

#### Individual Replica Log Audit (e.g., `worker-7b6fd5488f-5q8bt`):
```text
[EVWorker-worker-7b6fd5488f-5q8bt] listening on 0.0.0.0:60201
[EVWorker-worker-7b6fd5488f-5q8bt] Handling request 10 (will take 0.29s)
[EVWorker-worker-7b6fd5488f-5q8bt] Finished EVCharge request 10
[EVWorker-worker-7b6fd5488f-5q8bt] Handling request 15 (will take 0.31s)
[EVWorker-worker-7b6fd5488f-5q8bt] Handling request 20 (will take 0.32s)
[EVWorker-worker-7b6fd5488f-5q8bt] Finished EVCharge request 15
[EVWorker-worker-7b6fd5488f-5q8bt] Finished EVCharge request 20
[EVWorker-worker-7b6fd5488f-5q8bt] Handling request 25 (will take 0.29s)
[EVWorker-worker-7b6fd5488f-5q8bt] Finished EVCharge request 25
```

---

### Step 9: Clean-up Operations

When decommissioning the test harness, resources are cleaned up cleanly:

```bash
kubectl delete job load-test
kubectl delete -f k8s/app.yaml
```

---

## 4. Experimental Checklist & Results Summary

| Requirement / Check | Expected Behavior | Observed Result | Status |
| :--- | :--- | :--- | :--- |
| **Replicas Configuration** | Scaled beyond 3 replicas | **5 replicas** active and ready (`worker-7b6fd5488f-*`) | **PASS** |
| **Container Status** | All pods reach `Running` with 0 initial restarts | 8/8 pods running, 1/1 ready | **PASS** |
| **Raft Coordination** | Cross-pod election and heartbeat dissemination | Elected Leader (Term 1 & 2), synchronized Followers | **PASS** |
| **Failover Re-election** | Replicas scaled to 0 triggers higher term election | Scaled `service-b` to 0; `service-a` elected at Term 3 | **PASS** |
| **Clean Rejoin** | Resumed node re-joins as follower without election | Node re-scaled to 1 accepted Leader A quietly | **PASS** |
| **Job Execution** | Kubernetes Job runs inside cluster and completes | Job `load-test` completed 25 requests in 1.32s (19 req/s) | **PASS** |
| **gRPC Load Spread** | Traffic distributed across all replicas | All 5 replicas handled shares (4, 4, 4, 9, 4 requests) | **PASS** |

---

## 5. Key Takeaways & Conclusion

1. **Kubernetes Declarative Self-Healing:** Kubernetes maintains high availability by continuously reconciling pod state. Unlike Docker Compose, simulated node failures require scaling down replicas (`--replicas=0`) rather than simple pod termination.
2. **Overlay Networking & Port Binding:** Cloud-native microservices must bind to `0.0.0.0` to permit container bridge routing, and use dynamic DNS service discovery instead of hardcoded loopback addresses.
3. **L7 Protocol Balancing in L4 Fabrics:** gRPC's multiplexed HTTP/2 long-lived connections break standard L4 round-robin proxies. Creating a **Headless Service** (`clusterIP: None`) and combining it with client-side `round_robin` DNS resolution ensures equitable load distribution across all scaled replicas.
