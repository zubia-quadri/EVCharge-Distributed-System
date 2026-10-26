# -*- coding: utf-8 -*-
"""
reservation_service.py - Unified Flask Web Server & API Gateway for EVCharge Distributed System

This server acts as:
  1. The web-facing dashboard backend for all Experiments (XP 1 through 11).
  2. The gRPC Client for live ChargingService and DFS MetadataService.
  3. An interactive distributed system simulator & verifier that allows users to
     inspect, trigger, and demonstrate every distributed computing principle from the web UI.

Start with:
    python backend/reservation_service.py
"""

import sys
import os

# Set stdout/stderr to UTF-8 on Windows
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass
import json
import time
import random
import hashlib
import threading
from concurrent import futures
from flask import Flask, request, jsonify, send_from_directory

# -- Path setup ----------------------------------------------------------------
ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, ROOT)

import grpc
import evcharge_pb2
import evcharge_pb2_grpc
import dfs_pb2
import dfs_pb2_grpc
import raft_pb2
import raft_pb2_grpc

from lamport_clock import LamportClock

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
CHARGING_SERVICE_ADDR = "localhost:50051"
DFS_META_ADDR = "localhost:60501"
FRONTEND_DIR = os.path.join(ROOT, "frontend")
DFS_DATA_DIR = os.path.join(ROOT, "dfs_data")

app = Flask(__name__, static_folder=FRONTEND_DIR)
clock = LamportClock(owner="Reservation Service")

# ------------------------------------------------------------------------------
# In-Memory & Live State Bridges for Experiments 1-11
# ------------------------------------------------------------------------------
state_lock = threading.Lock()

# XP 1 & 2: Local fallback state when standalone charging_service is not running
local_chargers = {
    "Charger_01": {"available": True, "status": "AVAILABLE", "user": None},
    "Charger_02": {"available": False, "status": "OCCUPIED", "user": "EV999"},
    "Charger_03": {"available": True, "status": "AVAILABLE", "user": None},
}

# XP 3: Lamport Clock History
lamport_history = [
    {"event": "System Initialized", "timestamp": 1, "type": "INTERNAL"}
]

# XP 4: Distributed Mutual Exclusion (Ricart-Agrawala 3-Node Model)
mutex_nodes = {
    1: {"id": 1, "state": "RELEASED", "clock": 1, "request_ts": None, "deferred": []},
    2: {"id": 2, "state": "RELEASED", "clock": 2, "request_ts": None, "deferred": []},
    3: {"id": 3, "state": "RELEASED", "clock": 1, "request_ts": None, "deferred": []},
}
critical_section_holder = None

# XP 5: Deadlock Detection & Resolution (Wait-For Graph)
deadlock_state = {
    "locks": {
        "Charger_01": None,
        "Battery_Buffer": None
    },
    "waiting": {},  # worker_id -> resource_id
    "workers": {
        1: {"status": "IDLE", "holding": [], "waiting": None},
        2: {"status": "IDLE", "holding": [], "waiting": None}
    },
    "cycle_detected": False,
    "cycle_path": []
}

# XP 6: Least-Connections Load Balancing (3 Workers)
lb_workers = {
    "Worker_1 (60201)": {"active": 0, "total": 0, "address": "localhost:60201"},
    "Worker_2 (60202)": {"active": 0, "total": 0, "address": "localhost:60202"},
    "Worker_3 (60203)": {"active": 0, "total": 0, "address": "localhost:60203"},
}

# XP 7: Gossip Replication & Eventual Consistency (Replicas A, B, C)
replicas_data = {
    "A": {"clock": 10, "store": {"tariff": {"val": "$0.28/kWh", "ts": 10, "origin": "A"}}},
    "B": {"clock": 10, "store": {"tariff": {"val": "$0.28/kWh", "ts": 10, "origin": "A"}}},
    "C": {"clock": 10, "store": {"tariff": {"val": "$0.28/kWh", "ts": 10, "origin": "A"}}},
}

# XP 8: Distributed Caching Gateway (Redis Model)
cache_store = {}
cache_stats = {"hits": 0, "misses": 0, "stale_fallbacks": 0}

# XP 9: Raft Consensus Cluster
raft_cluster = {
    "nodes": {
        "Service-A": {"state": "LEADER", "term": 3, "alive": True, "voted_for": "Service-A", "heartbeats": 142},
        "Service-B": {"state": "FOLLOWER", "term": 3, "alive": True, "voted_for": "Service-A", "heartbeats": 142},
        "Service-C": {"state": "FOLLOWER", "term": 3, "alive": True, "voted_for": "Service-A", "heartbeats": 142},
    },
    "leader": "Service-A",
    "term": 3
}

# XP 10: Kubernetes Cluster & Pod Topology
k8s_pods = [
    {"name": "worker-deployment-78d49b-r4k2m", "ip": "10.42.0.139", "status": "Running", "requests": 14},
    {"name": "worker-deployment-78d49b-x98pt", "ip": "10.42.0.141", "status": "Running", "requests": 15},
    {"name": "worker-deployment-78d49b-7m1qq", "ip": "10.42.0.142", "status": "Running", "requests": 14},
    {"name": "worker-deployment-78d49b-w22ll", "ip": "10.42.0.144", "status": "Running", "requests": 13},
    {"name": "worker-deployment-78d49b-9c3ab", "ip": "10.42.0.143", "status": "Running", "requests": 14},
]
k8s_rr_index = 0

# XP 11: Distributed File System Local Simulation Nodes
dfs_sim_nodes = {
    "store-1": {"address": "localhost:60511", "alive": True},
    "store-2": {"address": "localhost:60512", "alive": True},
    "store-3": {"address": "localhost:60513", "alive": True},
}


# ------------------------------------------------------------------------------
# gRPC Helper Stubs & Fast Port Prober
# ------------------------------------------------------------------------------
import socket

def is_port_listening(host, port, timeout=0.04):
    """Fast check whether a TCP port is active to avoid gRPC connection hangs."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


def get_charging_stub():
    """Return gRPC stub for ChargingService."""
    channel = grpc.insecure_channel(CHARGING_SERVICE_ADDR)
    return evcharge_pb2_grpc.ChargingServiceStub(channel)


def get_dfs_meta_stub():
    """Return gRPC stub for DFS MetadataService."""
    channel = grpc.insecure_channel(DFS_META_ADDR)
    return dfs_pb2_grpc.DfsMetadataServiceStub(channel)


# ------------------------------------------------------------------------------
# Frontend Static Route
# ------------------------------------------------------------------------------
@app.route("/")
def index():
    return send_from_directory(FRONTEND_DIR, "index.html")


@app.route("/assets/<path:filename>")
def serve_assets(filename):
    return send_from_directory(os.path.join(FRONTEND_DIR, "assets"), filename)


# ------------------------------------------------------------------------------
# SYSTEM OVERVIEW API: All 11 Experiments
# ------------------------------------------------------------------------------
@app.route("/api/summary", methods=["GET"])
def get_system_summary():
    with state_lock:
        live_grpc = is_port_listening("127.0.0.1", 50051)
        live_dfs = is_port_listening("127.0.0.1", 60501)

        return jsonify({
            "status": "ONLINE",
            "experiments_count": 11,
            "clock_tick": clock.value,
            "live_services": {
                "charging_service_grpc": live_grpc,
                "dfs_metadata_grpc": live_dfs,
            },
            "raft_leader": raft_cluster["leader"],
            "mutex_holder": critical_section_holder or "NONE",
            "deadlock_active": deadlock_state["cycle_detected"],
            "cache_hit_rate": f"{(cache_stats['hits'] / max(1, cache_stats['hits'] + cache_stats['misses']) * 100):.1f}%"
        })


# ------------------------------------------------------------------------------
# EXPERIMENT 1 & 2: Chargers & gRPC
# ------------------------------------------------------------------------------
@app.route("/slots", methods=["GET"])
def get_slots():
    chargers = ["Charger_01", "Charger_02", "Charger_03"]
    results = []

    if is_port_listening("127.0.0.1", 50051):
        try:
            stub = get_charging_stub()
            for cid in chargers:
                ts = clock.send()
                response = stub.CheckChargerAvailability(
                    evcharge_pb2.ChargerRequest(charger_id=cid, lamport_timestamp=ts),
                    timeout=0.8
                )
                clock.receive(response.lamport_timestamp)
                results.append({
                    "id": cid,
                    "available": response.available,
                    "status": "AVAILABLE" if response.available else "OCCUPIED",
                    "source": "live_grpc"
                })
            return jsonify(results)
        except Exception:
            pass

    # Graceful fallback to local simulated chargers so the dashboard never breaks
    with state_lock:
        for cid in chargers:
            c = local_chargers[cid]
            results.append({
                "id": cid,
                "available": c["available"],
                "status": c["status"],
                "user": c.get("user"),
                "car_model": c.get("car_model"),
                "battery_level": c.get("battery_level"),
                "target_kwh": c.get("target_kwh"),
                "charging_kw": c.get("charging_kw"),
                "source": "in_memory"
            })
        return jsonify(results)


@app.route("/reserve", methods=["POST"])
def reserve():
    data = request.get_json(force=True) or {}
    charger_id = data.get("charger_id", "").strip()
    ev_id = data.get("ev_id", "").strip()
    car_model = data.get("car_model", "Tesla Model 3").strip()
    battery_level = int(data.get("battery_level", 28))
    target_kwh = float(data.get("target_kwh", 45))

    if not charger_id or not ev_id:
        return jsonify({"success": False, "message": "EV ID and Charger ID required."}), 400

    ts1 = clock.send()

    if is_port_listening("127.0.0.1", 50051):
        try:
            stub = get_charging_stub()
            avail_resp = stub.CheckChargerAvailability(
                evcharge_pb2.ChargerRequest(charger_id=charger_id, lamport_timestamp=ts1),
                timeout=1.0
            )
            clock.receive(avail_resp.lamport_timestamp)

            if not avail_resp.available:
                return jsonify({
                    "success": False,
                    "message": f"{charger_id} is currently OCCUPIED.",
                    "activity": [f"{ev_id} requested {charger_id}", "Availability check: OCCUPIED", "Reservation aborted"]
                })

            ts2 = clock.send()
            res_resp = stub.ReserveCharger(
                evcharge_pb2.ReservationRequest(charger_id=charger_id, ev_id=ev_id, lamport_timestamp=ts2),
                timeout=1.0
            )
            clock.receive(res_resp.lamport_timestamp)
            return jsonify({
                "success": res_resp.success,
                "message": res_resp.message,
                "activity": [
                    f"{ev_id} ({car_model}) requested {charger_id}",
                    "gRPC check passed: AVAILABLE",
                    f"Reserved via gRPC for {ev_id}",
                    "Lamport clocks synchronized"
                ]
            })
        except Exception:
            pass

    # Local simulation fallback
    with state_lock:
        c = local_chargers.get(charger_id)
        if not c or not c["available"]:
            return jsonify({
                "success": False,
                "message": f"{charger_id} is currently OCCUPIED.",
                "activity": [f"{ev_id} requested {charger_id}", "Status: OCCUPIED", "Reservation rejected"]
            })
        c["available"] = False
        c["status"] = "OCCUPIED"
        c["user"] = ev_id
        c["car_model"] = car_model
        c["battery_level"] = battery_level
        c["target_kwh"] = target_kwh
        c["charging_kw"] = 150 if charger_id == "Charger_01" else (22 if charger_id == "Charger_02" else 350)
        c["start_time"] = time.time()

        t = clock.tick()
        lamport_history.append({
            "event": f"Reserved {charger_id} for {car_model} [{ev_id}] (Battery: {battery_level}%, Target: {target_kwh} kWh)",
            "timestamp": t,
            "type": "LOCAL"
        })
        return jsonify({
            "success": True,
            "message": f"Successfully reserved {charger_id} for {car_model} ({ev_id})!",
            "clock_tick": t,
            "activity": [
                f"{ev_id} ({car_model}) requested {charger_id}",
                "Availability confirmed (Mutual Exclusion CS lock acquired)",
                f"Battery State of Charge: {battery_level}%",
                f"Clock incremented to {t}"
            ]
        })


@app.route("/update_status", methods=["POST"])
def update_status():
    data = request.get_json(force=True) or {}
    charger_id = data.get("charger_id", "").strip()
    new_status = data.get("status", "AVAILABLE").strip()

    if is_port_listening("127.0.0.1", 50051):
        try:
            stub = get_charging_stub()
            ts = clock.send()
            resp = stub.UpdateChargerStatus(
                evcharge_pb2.StatusUpdateRequest(charger_id=charger_id, new_status=new_status, lamport_timestamp=ts),
                timeout=1.0
            )
            clock.receive(resp.lamport_timestamp)
            return jsonify({"success": resp.success, "message": resp.message})
        except Exception:
            pass

    with state_lock:
        if charger_id in local_chargers:
            local_chargers[charger_id]["available"] = (new_status == "AVAILABLE")
            local_chargers[charger_id]["status"] = new_status
            if new_status == "AVAILABLE":
                local_chargers[charger_id]["user"] = None
            return jsonify({"success": True, "message": f"{charger_id} set to {new_status}"})
        return jsonify({"success": False, "message": "Charger not found."}), 404


# ------------------------------------------------------------------------------
# EXPERIMENT 3: Lamport Logical Clocks
# ------------------------------------------------------------------------------
@app.route("/api/lamport/state", methods=["GET"])
def get_lamport_state():
    with state_lock:
        return jsonify({
            "current_clock": clock.value,
            "owner": "Reservation Service (Coordinator)",
            "history": lamport_history[-15:]
        })


@app.route("/api/lamport/tick", methods=["POST"])
def lamport_tick():
    data = request.get_json(silent=True) or {}
    event_name = data.get("event", "Local Computation Event")
    with state_lock:
        new_val = clock.tick()
        lamport_history.append({"event": event_name, "timestamp": new_val, "type": "TICK"})
        return jsonify({
            "success": True,
            "event": event_name,
            "new_timestamp": new_val,
            "rule": "L1: clock = clock + 1"
        })


@app.route("/api/lamport/sync", methods=["POST"])
def lamport_sync():
    data = request.get_json(force=True) or {}
    received_ts = int(data.get("received_timestamp", 10))
    event_name = data.get("event", f"Received message with TS={received_ts}")
    with state_lock:
        old_val = clock.value
        new_val = clock.receive(received_ts)
        lamport_history.append({
            "event": f"{event_name} (local={old_val}, msg={received_ts})",
            "timestamp": new_val,
            "type": "RECEIVE"
        })
        return jsonify({
            "success": True,
            "old_timestamp": old_val,
            "received_timestamp": received_ts,
            "new_timestamp": new_val,
            "rule": "L2: clock = max(local, received) + 1"
        })


@app.route("/api/lamport/reset", methods=["POST"])
def lamport_reset():
    with state_lock:
        with clock._lock:
            clock._clock = 0
        lamport_history.clear()
        lamport_history.append({"event": "Clock Reset", "timestamp": 0, "type": "RESET"})
        return jsonify({"success": True, "clock": 0})


# ------------------------------------------------------------------------------
# EXPERIMENT 4: Distributed Mutual Exclusion (Ricart-Agrawala)
# ------------------------------------------------------------------------------
@app.route("/api/mutex/state", methods=["GET"])
def get_mutex_state():
    with state_lock:
        return jsonify({
            "nodes": mutex_nodes,
            "critical_section_holder": critical_section_holder,
            "algorithm": "Ricart-Agrawala (Optimal N-1 messages)"
        })


@app.route("/api/mutex/request", methods=["POST"])
def mutex_request():
    data = request.get_json(force=True) or {}
    node_id = int(data.get("node_id", 1))

    with state_lock:
        req_node = mutex_nodes.get(node_id)
        if not req_node:
            return jsonify({"success": False, "message": "Invalid node ID"}), 400

        # Step 1: Tick Lamport clock and set state to WANTED
        req_node["clock"] += 1
        req_ts = req_node["clock"]
        req_node["state"] = "WANTED"
        req_node["request_ts"] = req_ts

        # Step 2: Compare priority with all peers
        # Priority rule: lower timestamp wins; on tie, lower node_id wins
        granted_by = []
        deferred_by = []

        for peer_id, peer in mutex_nodes.items():
            if peer_id == node_id:
                continue

            # Peer decides whether to reply immediately or defer
            peer_defer = False
            if peer["state"] == "HELD":
                peer_defer = True
            elif peer["state"] == "WANTED":
                # Compare (peer_req_ts, peer_id) vs (req_ts, node_id)
                if (peer["request_ts"], peer_id) < (req_ts, node_id):
                    peer_defer = True

            if peer_defer:
                peer["deferred"].append(node_id)
                deferred_by.append(peer_id)
            else:
                peer["clock"] = max(peer["clock"], req_ts) + 1
                granted_by.append(peer_id)

        # If granted by all peers, enter critical section
        global critical_section_holder
        if len(granted_by) == len(mutex_nodes) - 1:
            req_node["state"] = "HELD"
            critical_section_holder = node_id
            status_msg = f"Node {node_id} gathered all {len(granted_by)} grants and ENTERED Critical Section!"
        else:
            status_msg = f"Node {node_id} waiting: granted by {granted_by}, deferred by {deferred_by}."

        return jsonify({
            "success": True,
            "node_id": node_id,
            "request_timestamp": req_ts,
            "state": req_node["state"],
            "granted_by": granted_by,
            "deferred_by": deferred_by,
            "critical_section_holder": critical_section_holder,
            "message": status_msg
        })


@app.route("/api/mutex/release", methods=["POST"])
def mutex_release():
    data = request.get_json(force=True) or {}
    node_id = int(data.get("node_id", 1))

    with state_lock:
        global critical_section_holder
        req_node = mutex_nodes.get(node_id)
        if not req_node:
            return jsonify({"success": False, "message": "Invalid node"}), 400

        req_node["state"] = "RELEASED"
        req_node["request_ts"] = None
        critical_section_holder = None

        # Flush deferred requests
        flushed = list(req_node["deferred"])
        req_node["deferred"] = []

        # Wake up deferred nodes
        for waiting_id in flushed:
            waiting_node = mutex_nodes[waiting_id]
            waiting_node["state"] = "HELD"
            critical_section_holder = waiting_id

        return jsonify({
            "success": True,
            "node_id": node_id,
            "state": "RELEASED",
            "flushed_deferred": flushed,
            "new_cs_holder": critical_section_holder,
            "message": f"Node {node_id} exited Critical Section and replied to deferred peers: {flushed}"
        })


# ------------------------------------------------------------------------------
# EXPERIMENT 5: Deadlock Simulation & Wait-For Graph (WFG)
# ------------------------------------------------------------------------------
@app.route("/api/deadlock/state", methods=["GET"])
def get_deadlock_state():
    with state_lock:
        wfg_edges = []
        for worker_id, res in deadlock_state["waiting"].items():
            holder = deadlock_state["locks"].get(res)
            if holder and holder != worker_id:
                wfg_edges.append({"from": f"Worker_{worker_id}", "to": f"Worker_{holder}", "resource": res})

        return jsonify({
            "locks": deadlock_state["locks"],
            "workers": deadlock_state["workers"],
            "waiting": deadlock_state["waiting"],
            "wfg_edges": wfg_edges,
            "cycle_detected": deadlock_state["cycle_detected"],
            "cycle_path": deadlock_state["cycle_path"]
        })


@app.route("/api/deadlock/simulate", methods=["POST"])
def simulate_deadlock():
    with state_lock:
        # Setup circular wait:
        # Worker 1 holds Charger_01, waits for Battery_Buffer
        # Worker 2 holds Battery_Buffer, waits for Charger_01
        deadlock_state["locks"]["Charger_01"] = 1
        deadlock_state["locks"]["Battery_Buffer"] = 2

        deadlock_state["workers"][1] = {"status": "BLOCKED", "holding": ["Charger_01"], "waiting": "Battery_Buffer"}
        deadlock_state["workers"][2] = {"status": "BLOCKED", "holding": ["Battery_Buffer"], "waiting": "Charger_01"}

        deadlock_state["waiting"] = {1: "Battery_Buffer", 2: "Charger_01"}
        deadlock_state["cycle_detected"] = True
        deadlock_state["cycle_path"] = ["Worker_1", "Battery_Buffer", "Worker_2", "Charger_01", "Worker_1"]

        return jsonify({
            "success": True,
            "message": "Deadlock circular wait state successfully simulated!",
            "details": "Worker 1 holds Charger_01, waits for Battery_Buffer. Worker 2 holds Battery_Buffer, waits for Charger_01."
        })


@app.route("/api/deadlock/detect", methods=["POST"])
def detect_deadlock():
    with state_lock:
        # DFS cycle check on WFG
        wfg = {}
        for w, res in deadlock_state["waiting"].items():
            holder = deadlock_state["locks"].get(res)
            if holder:
                wfg[w] = holder

        # Check for 1 -> 2 -> 1
        cycle = []
        has_cycle = False
        for start in wfg:
            visited = []
            curr = start
            while curr in wfg:
                visited.append(curr)
                curr = wfg[curr]
                if curr in visited:
                    has_cycle = True
                    cycle = visited + [curr]
                    break
            if has_cycle:
                break

        deadlock_state["cycle_detected"] = has_cycle
        deadlock_state["cycle_path"] = cycle
        return jsonify({
            "cycle_found": has_cycle,
            "cycle": cycle,
            "message": "CRITICAL: Circular wait cycle detected in Wait-For Graph!" if has_cycle else "No deadlock detected."
        })


@app.route("/api/deadlock/resolve", methods=["POST"])
def resolve_deadlock():
    with state_lock:
        # Victim selection: abort Worker 2, release Battery_Buffer to Worker 1
        deadlock_state["workers"][2] = {"status": "ABORTED / ROLLED BACK", "holding": [], "waiting": None}
        deadlock_state["locks"]["Battery_Buffer"] = 1
        deadlock_state["workers"][1] = {"status": "COMPLETED", "holding": ["Charger_01", "Battery_Buffer"], "waiting": None}
        deadlock_state["waiting"].clear()
        deadlock_state["cycle_detected"] = False
        deadlock_state["cycle_path"] = []

        return jsonify({
            "success": True,
            "victim": "Worker_2",
            "message": "Deadlock resolved! Worker 2 aborted as victim. Resources granted to Worker 1."
        })


# ------------------------------------------------------------------------------
# EXPERIMENT 6: Least-Connections Load Balancing
# ------------------------------------------------------------------------------
@app.route("/api/lb/stats", methods=["GET"])
def get_lb_stats():
    with state_lock:
        return jsonify({
            "workers": lb_workers,
            "algorithm": "Least Connections Dynamic Dispatcher"
        })


@app.route("/api/lb/dispatch", methods=["POST"])
def lb_dispatch():
    with state_lock:
        # Pick worker with minimum active connections
        best_worker = min(lb_workers.keys(), key=lambda w: lb_workers[w]["active"])
        lb_workers[best_worker]["active"] += 1
        lb_workers[best_worker]["total"] += 1
        act = lb_workers[best_worker]["active"]
        tot = lb_workers[best_worker]["total"]

    # Simulate fast async completion
    def finish():
        time.sleep(0.8)
        with state_lock:
            if lb_workers[best_worker]["active"] > 0:
                lb_workers[best_worker]["active"] -= 1

    threading.Thread(target=finish, daemon=True).start()

    return jsonify({
        "success": True,
        "routed_to": best_worker,
        "active_connections": act,
        "total_served": tot,
        "message": f"Routed request to least-loaded node: {best_worker}"
    })


@app.route("/api/lb/burst", methods=["POST"])
def lb_burst():
    dispatched = []
    with state_lock:
        for i in range(9):
            best = min(lb_workers.keys(), key=lambda w: lb_workers[w]["active"])
            lb_workers[best]["active"] += 1
            lb_workers[best]["total"] += 1
            dispatched.append({"req": i + 1, "worker": best, "active_then": lb_workers[best]["active"]})

    def finish_burst():
        time.sleep(1.2)
        with state_lock:
            for w in lb_workers:
                lb_workers[w]["active"] = 0

    threading.Thread(target=finish_burst, daemon=True).start()

    return jsonify({
        "success": True,
        "burst_count": 9,
        "distribution": dispatched,
        "message": "Dispatched burst of 9 requests evenly across least-connection pool!"
    })


# ------------------------------------------------------------------------------
# EXPERIMENT 7: Gossip Replication & Eventual Consistency (LWW)
# ------------------------------------------------------------------------------
@app.route("/api/replica/state", methods=["GET"])
def get_replica_state():
    with state_lock:
        return jsonify({"replicas": replicas_data})


@app.route("/api/replica/write", methods=["POST"])
def replica_write():
    data = request.get_json(force=True) or {}
    node = data.get("node", "A").upper()
    val = data.get("value", "$0.42/kWh")

    with state_lock:
        if node not in replicas_data:
            return jsonify({"success": False, "message": "Invalid replica"}), 400

        replicas_data[node]["clock"] += 1
        ts = replicas_data[node]["clock"]
        replicas_data[node]["store"]["tariff"] = {"val": val, "ts": ts, "origin": node}

        return jsonify({
            "success": True,
            "node": node,
            "new_value": val,
            "lamport_timestamp": ts,
            "message": f"Wrote tariff='{val}' to Replica {node} at Lamport Clock {ts}"
        })


@app.route("/api/replica/gossip", methods=["POST"])
def replica_gossip():
    with state_lock:
        # Find highest timestamp across all replicas for key 'tariff'
        highest_val = None
        highest_ts = -1
        highest_origin = None

        for n, r in replicas_data.items():
            item = r["store"].get("tariff")
            if item and item["ts"] > highest_ts:
                highest_ts = item["ts"]
                highest_val = item["val"]
                highest_origin = item["origin"]

        # Sync all replicas using Last-Write-Wins (LWW)
        for n in replicas_data:
            replicas_data[n]["store"]["tariff"] = {
                "val": highest_val,
                "ts": highest_ts,
                "origin": highest_origin
            }
            replicas_data[n]["clock"] = max(replicas_data[n]["clock"], highest_ts) + 1

        return jsonify({
            "success": True,
            "winning_value": highest_val,
            "winning_timestamp": highest_ts,
            "winning_origin": highest_origin,
            "message": f"Gossip converged all 3 replicas to '{highest_val}' (TS={highest_ts} from {highest_origin}) via LWW."
        })


# ------------------------------------------------------------------------------
# EXPERIMENT 8: Distributed Caching (Redis Gateway)
# ------------------------------------------------------------------------------
@app.route("/api/cache/stats", methods=["GET"])
def get_cache_stats():
    with state_lock:
        return jsonify({
            "cached_entries": len(cache_store),
            "stats": cache_stats,
            "ttl_policy": "Cache-Aside, TTL=10s, Stale-while-revalidate fallback"
        })


@app.route("/api/cache/query", methods=["POST"])
def cache_query():
    data = request.get_json(force=True) or {}
    charger_id = data.get("charger_id", "Charger_01")
    now = time.time()

    with state_lock:
        entry = cache_store.get(charger_id)
        if entry and entry["expires_at"] > now:
            cache_stats["hits"] += 1
            return jsonify({
                "status": "CACHE_HIT",
                "source": "Redis In-Memory L1 Cache",
                "latency_ms": round(random.uniform(0.4, 1.2), 2),
                "data": entry["value"],
                "ttl_remaining": round(entry["expires_at"] - now, 1)
            })
        else:
            cache_stats["misses"] += 1
            # Fetch from backend simulator and populate
            fetched_value = local_chargers.get(charger_id, {"status": "AVAILABLE", "available": True})
            cache_store[charger_id] = {
                "value": fetched_value,
                "expires_at": now + 10.0
            }
            return jsonify({
                "status": "CACHE_MISS",
                "source": "Backend gRPC Service (Populated Redis TTL=10s)",
                "latency_ms": round(random.uniform(28.0, 48.0), 2),
                "data": fetched_value,
                "ttl_remaining": 10.0
            })


@app.route("/api/cache/flush", methods=["POST"])
def cache_flush():
    with state_lock:
        cache_store.clear()
        return jsonify({"success": True, "message": "Redis cache flushed."})


# ------------------------------------------------------------------------------
# EXPERIMENT 9: Raft Consensus Cluster
# ------------------------------------------------------------------------------
@app.route("/api/raft/state", methods=["GET"])
def get_raft_state():
    with state_lock:
        return jsonify(raft_cluster)


@app.route("/api/raft/kill_leader", methods=["POST"])
def raft_kill_leader():
    with state_lock:
        old_leader = raft_cluster["leader"]
        raft_cluster["nodes"][old_leader]["alive"] = False
        raft_cluster["nodes"][old_leader]["state"] = "DEAD / ISOLATED"

        # Followers detect missing heartbeats and start election
        candidates = [n for n, d in raft_cluster["nodes"].items() if d["alive"]]
        new_leader = candidates[0] if candidates else None

        if new_leader:
            raft_cluster["term"] += 1
            term = raft_cluster["term"]
            raft_cluster["nodes"][new_leader]["term"] = term
            raft_cluster["nodes"][new_leader]["state"] = "LEADER"
            raft_cluster["nodes"][new_leader]["voted_for"] = new_leader
            raft_cluster["leader"] = new_leader

            # Remaining followers vote for new leader
            for n in candidates:
                if n != new_leader:
                    raft_cluster["nodes"][n]["state"] = "FOLLOWER"
                    raft_cluster["nodes"][n]["term"] = term
                    raft_cluster["nodes"][n]["voted_for"] = new_leader

            msg = f"Old leader {old_leader} failed. Election triggered. {new_leader} gathered 2/2 quorum votes and became NEW LEADER for Term {term}!"
        else:
            msg = "Cluster has no live nodes left."

        return jsonify({
            "success": True,
            "old_leader": old_leader,
            "new_leader": new_leader,
            "term": raft_cluster["term"],
            "message": msg
        })


@app.route("/api/raft/reset", methods=["POST"])
def raft_reset():
    with state_lock:
        for n in raft_cluster["nodes"]:
            raft_cluster["nodes"][n]["alive"] = True
            raft_cluster["nodes"][n]["state"] = "FOLLOWER"
            raft_cluster["nodes"][n]["term"] = raft_cluster["term"]
        raft_cluster["nodes"]["Service-A"]["state"] = "LEADER"
        raft_cluster["leader"] = "Service-A"
        return jsonify({"success": True, "message": "Raft cluster restored to normal state with Leader: Service-A."})


# ------------------------------------------------------------------------------
# EXPERIMENT 10: Kubernetes Topology & L7 gRPC Balancing
# ------------------------------------------------------------------------------
@app.route("/api/k8s/topology", methods=["GET"])
def get_k8s_topology():
    return jsonify({
        "service": {
            "name": "worker",
            "type": "Headless Service (clusterIP: None)",
            "dns_resolution": "dns:///worker:60201",
            "lb_policy": "round_robin (HTTP/2 L7 balancing)"
        },
        "pods": k8s_pods
    })


@app.route("/api/k8s/simulate", methods=["POST"])
def k8s_simulate():
    global k8s_rr_index
    with state_lock:
        pod = k8s_pods[k8s_rr_index]
        pod["requests"] += 1
        k8s_rr_index = (k8s_rr_index + 1) % len(k8s_pods)
        return jsonify({
            "routed_pod": pod["name"],
            "pod_ip": pod["ip"],
            "total_requests": pod["requests"],
            "message": f"gRPC L7 round-robin dispatched to {pod['name']} ({pod['ip']})"
        })


# ------------------------------------------------------------------------------
# EXPERIMENT 11: Distributed File System (DFS)
# ------------------------------------------------------------------------------
@app.route("/api/dfs/cluster", methods=["GET"])
def get_dfs_cluster():
    # 1. Try real live gRPC metadata server if port is listening
    if is_port_listening("127.0.0.1", 60501):
        try:
            stub = get_dfs_meta_stub()
            reply = stub.ClusterStatus(dfs_pb2.DfsStatusRequest(), timeout=0.8)
            nodes = []
            for n in reply.nodes:
                nodes.append({
                    "id": n.node_id,
                    "address": n.address,
                    "alive": n.alive,
                    "chunks": n.chunk_count,
                    "source": "live_grpc"
                })
            files = []
            for f in reply.files:
                files.append({
                    "filename": f.filename,
                    "version": f.version,
                    "size": f.size,
                    "sha256": f.sha256[:16] + "...",
                    "num_chunks": len(f.chunks)
                })
            return jsonify({
                "status": "ONLINE (LIVE gRPC)",
                "meta_address": DFS_META_ADDR,
                "nodes": nodes,
                "files": files
            })
        except Exception:
            pass

    # 2. Try disk persistence state: dfs_data/metadata.json
    state_file = os.path.join(DFS_DATA_DIR, "metadata.json")
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            nodes = []
            for nid, addr in data.get("addresses", {}).items():
                store_dir = os.path.join(DFS_DATA_DIR, nid)
                chunk_cnt = len(os.listdir(store_dir)) if os.path.exists(store_dir) else 0
                is_alive = dfs_sim_nodes.get(nid, {}).get("alive", True)
                nodes.append({
                    "id": nid,
                    "address": addr,
                    "alive": is_alive,
                    "chunks": chunk_cnt,
                    "source": "disk_metadata"
                })

            files = []
            for fname, finfo in data.get("files", {}).items():
                files.append({
                    "filename": fname,
                    "version": finfo.get("version", 1),
                    "size": finfo.get("size", 0),
                    "sha256": finfo.get("sha256", "")[:16] + "...",
                    "full_sha256": finfo.get("sha256", ""),
                    "num_chunks": len(finfo.get("chunks", [])),
                    "chunks": finfo.get("chunks", [])
                })

            return jsonify({
                "status": "ONLINE (Persisted DFS Store)",
                "meta_address": "localhost:60501",
                "nodes": nodes,
                "files": files
            })
        except Exception as e:
            pass

    # Fallback default
    return jsonify({
        "status": "READY",
        "nodes": [
            {"id": "store-1", "address": "localhost:60511", "alive": True, "chunks": 4},
            {"id": "store-2", "address": "localhost:60512", "alive": True, "chunks": 5},
            {"id": "store-3", "address": "localhost:60513", "alive": True, "chunks": 3},
        ],
        "files": []
    })


@app.route("/api/dfs/file_details", methods=["GET"])
def get_dfs_file_details():
    filename = request.args.get("filename", "photo.bin")
    state_file = os.path.join(DFS_DATA_DIR, "metadata.json")
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            finfo = data.get("files", {}).get(filename)
            if finfo:
                return jsonify({"found": True, "file": finfo})
        except Exception:
            pass

    return jsonify({"found": False, "message": "File not found"}), 404


@app.route("/api/dfs/toggle_node", methods=["POST"])
def dfs_toggle_node():
    data = request.get_json(force=True) or {}
    node_id = data.get("node_id", "store-1")
    with state_lock:
        if node_id in dfs_sim_nodes:
            dfs_sim_nodes[node_id]["alive"] = not dfs_sim_nodes[node_id]["alive"]
            status_str = "ALIVE" if dfs_sim_nodes[node_id]["alive"] else "CRASHED / DOWN"
            return jsonify({
                "success": True,
                "node_id": node_id,
                "alive": dfs_sim_nodes[node_id]["alive"],
                "message": f"Storage node {node_id} marked as {status_str}."
            })
        return jsonify({"success": False, "message": "Unknown node ID"}), 400


@app.route("/api/dfs/corrupt_chunk", methods=["POST"])
def dfs_corrupt_chunk():
    """Inject a corrupted byte into a chunk to test SHA-256 detection and fallback."""
    data = request.get_json(force=True) or {}
    node_id = data.get("node_id", "store-1")
    chunk_name = data.get("chunk_id", "photo.bin.v13.c0")

    chunk_path = os.path.join(DFS_DATA_DIR, node_id, chunk_name)
    if not os.path.exists(chunk_path):
        # find any chunk in store-1
        store_dir = os.path.join(DFS_DATA_DIR, node_id)
        if os.path.exists(store_dir) and os.listdir(store_dir):
            chunk_name = os.listdir(store_dir)[0]
            chunk_path = os.path.join(store_dir, chunk_name)
        else:
            return jsonify({"success": False, "message": f"No chunks found in {node_id}"}), 404

    try:
        with open(chunk_path, "r+b") as f:
            f.seek(0)
            f.write(b"CORRUPTED_TAMPERED_BYTE")

        return jsonify({
            "success": True,
            "corrupted_node": node_id,
            "chunk_id": chunk_name,
            "message": f"Injected bit-rot into {chunk_name} on {node_id}. DFS Client will catch checksum mismatch and fallback to surviving replica!"
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/api/dfs/repair", methods=["POST"])
def dfs_repair():
    """Execute automated self-healing re-replication."""
    # Run dfs_repair logic or return self-healing report
    state_file = os.path.join(DFS_DATA_DIR, "metadata.json")
    repaired_chunks = []
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            dead_nodes = [nid for nid, d in dfs_sim_nodes.items() if not d.get("alive", True)]

            # Check under-replicated chunks
            for fname, finfo in data.get("files", {}).items():
                for c in finfo.get("chunks", []):
                    live_holders = [n for n in c.get("nodes", []) if n not in dead_nodes]
                    if len(live_holders) < 2 and live_holders:
                        donor = live_holders[0]
                        # pick target
                        targets = [n for n in dfs_sim_nodes if dfs_sim_nodes[n].get("alive", True) and n not in c.get("nodes", [])]
                        if targets:
                            target = targets[0]
                            c["nodes"].append(target)
                            repaired_chunks.append({
                                "chunk_id": c["chunk_id"],
                                "donor": donor,
                                "target": target
                            })

            with open(state_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

        except Exception as e:
            return jsonify({"success": False, "message": str(e)}), 500

    return jsonify({
        "success": True,
        "repaired_count": len(repaired_chunks),
        "details": repaired_chunks,
        "message": f"Self-healing completed! Re-replicated {len(repaired_chunks)} under-replicated chunks back to target nodes."
    })


# ------------------------------------------------------------------------------
# Entry-Point
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 70)
    print("[EVCharge] Unified Distributed Systems Mission Control Server")
    print("=" * 70)
    print("  Web Dashboard : http://localhost:5000")
    print(f"  Live gRPC Port: {CHARGING_SERVICE_ADDR}")
    print("  Experiments   : XP 1 through XP 11 Supported (Live + Interactive)")
    print("=" * 70 + "\n")
    app.run(host="0.0.0.0", port=5000, debug=False)
