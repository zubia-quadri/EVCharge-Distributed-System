# -*- coding: utf-8 -*-
"""
backend/raft_node.py - Raft-Based Leader Election and Failover for EVCharge coordination.

Experiment 9:
- Implements Raft consensus (FOLLOWER, CANDIDATE, LEADER)
- Heartbeats, randomized election timeouts, vote solicitation
- Hosts RaftService and EVCharge LockService on the same gRPC server
- Leadership gating for LockService (Experiment 5)
"""

import os
import sys
import time
import random
import threading
from concurrent import futures

# Add project root to sys.path so generated stubs and backend modules import correctly
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import grpc
import evcharge_pb2
import evcharge_pb2_grpc
import raft_pb2
import raft_pb2_grpc
from backend.lock_manager import LockManager

import net_config

# Node cluster topology
NODES = {
    "A": net_config.resolve("service-a"),
    "B": net_config.resolve("service-b"),
    "C": net_config.resolve("service-c")
}

HEARTBEAT_INTERVAL = 0.5   # Leader sends heartbeats every ~0.5s


class RaftNode:
    def __init__(self, node_id):
        self.node_id = node_id.upper()
        self.address = NODES[self.node_id]
        self.peers = {nid: addr for nid, addr in NODES.items() if nid != self.node_id}

        self.lock = threading.RLock()
        self.state = "FOLLOWER"        # "FOLLOWER", "CANDIDATE", "LEADER"
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None

        self.election_timeout = random.uniform(1.5, 3.0)
        self.last_heartbeat = time.time()
        self.last_printed_leader = None
        self.last_printed_term = None

        self.running = True
        self.server = None

        # Integrated Experiment 5 LockManager
        self.lock_manager = LockManager()

        # Reusable gRPC channels and stubs for peers
        self.channels = {
            pid: grpc.insecure_channel(paddr)
            for pid, paddr in self.peers.items()
        }
        self.raft_stubs = {
            pid: raft_pb2_grpc.RaftServiceStub(self.channels[pid])
            for pid in self.peers
        }

    def reset_election_timeout(self):
        """Reset randomized election timeout between 1.5 and 3.0 seconds."""
        self.election_timeout = random.uniform(1.5, 3.0)
        self.last_heartbeat = time.time()

    # --------------------------------------------------------------------------
    # RAFT RPC HANDLERS
    # --------------------------------------------------------------------------

    def handle_request_vote(self, request):
        with self.lock:
            # Stale term: reject vote
            if request.term < self.current_term:
                return raft_pb2.VoteReply(
                    term=self.current_term,
                    vote_granted=False
                )

            # Higher term: update term and revert to follower
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = "FOLLOWER"
                self.voted_for = None
                self.leader_id = None

            # Grant vote if not yet voted or voted for this candidate in this term
            if self.voted_for is None or self.voted_for == request.candidate_id:
                self.voted_for = request.candidate_id
                self.reset_election_timeout()
                return raft_pb2.VoteReply(
                    term=self.current_term,
                    vote_granted=True
                )

            return raft_pb2.VoteReply(
                term=self.current_term,
                vote_granted=False
            )

    def handle_append_entries(self, request):
        with self.lock:
            # Stale term: reject heartbeat
            if request.term < self.current_term:
                return raft_pb2.HeartbeatReply(
                    term=self.current_term,
                    success=False
                )

            # Higher term: update term
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = "FOLLOWER"
                self.voted_for = None

            # Candidate steps down upon receiving valid heartbeat
            if self.state == "CANDIDATE":
                self.state = "FOLLOWER"

            self.leader_id = request.leader_id
            self.reset_election_timeout()

            # Follower console output on leader or term change
            if self.last_printed_leader != self.leader_id or self.last_printed_term != self.current_term:
                self.last_printed_leader = self.leader_id
                self.last_printed_term = self.current_term
                print(f"[{self.node_id} term={self.current_term} FOLLOWER] Leader = {self.leader_id}", flush=True)

            return raft_pb2.HeartbeatReply(
                term=self.current_term,
                success=True
            )

    # --------------------------------------------------------------------------
    # ELECTION LOGIC
    # --------------------------------------------------------------------------

    def _start_election(self):
        with self.lock:
            if self.state == "LEADER":
                return
            self.state = "CANDIDATE"
            self.current_term += 1
            self.voted_for = self.node_id
            self.leader_id = None
            self.reset_election_timeout()
            term = self.current_term
            node_id = self.node_id
            peers = sorted(self.peers.keys())

        print(f"[{node_id} term={term} CANDIDATE] Starting election", flush=True)

        votes = 1  # Self vote
        majority = 2  # 2/3 votes required

        for peer_id in peers:
            with self.lock:
                if self.state != "CANDIDATE" or self.current_term != term:
                    return

            try:
                stub = self.raft_stubs[peer_id]
                reply = stub.RequestVote(
                    raft_pb2.VoteRequest(term=term, candidate_id=node_id),
                    timeout=0.4
                )

                with self.lock:
                    if reply.term > self.current_term:
                        self.current_term = reply.term
                        self.state = "FOLLOWER"
                        self.voted_for = None
                        self.leader_id = None
                        self.reset_election_timeout()
                        return

                    if self.state == "CANDIDATE" and self.current_term == term and reply.vote_granted:
                        votes += 1
                        print(f"[{node_id} term={term} CANDIDATE] Vote from {peer_id}", flush=True)

            except (grpc.RpcError, Exception):
                print(f"[{node_id} term={term} CANDIDATE] {peer_id} unavailable", flush=True)

            became_leader = False
            with self.lock:
                if self.state == "CANDIDATE" and self.current_term == term and votes >= majority:
                    self.state = "LEADER"
                    self.leader_id = node_id
                    became_leader = True
                    print(f"[{node_id} term={term} LEADER] *** ELECTED LEADER *** ({votes}/3 votes)", flush=True)

            if became_leader:
                self._send_heartbeats_to_all()
                return

    def _election_timer_loop(self):
        while self.running:
            time.sleep(0.05)
            if not self.running:
                break

            with self.lock:
                if self.state == "LEADER":
                    continue
                elapsed = time.time() - self.last_heartbeat
                timeout = self.election_timeout

            if elapsed >= timeout:
                self._start_election()

    # --------------------------------------------------------------------------
    # HEARTBEAT LOGIC
    # --------------------------------------------------------------------------

    def _send_one_heartbeat(self, peer_id, term, leader_id):
        try:
            stub = self.raft_stubs[peer_id]
            reply = stub.AppendEntries(
                raft_pb2.HeartbeatRequest(term=term, leader_id=leader_id),
                timeout=0.25
            )
            with self.lock:
                if reply.term > self.current_term:
                    self.current_term = reply.term
                    self.state = "FOLLOWER"
                    self.voted_for = None
                    self.leader_id = None
                    self.reset_election_timeout()
        except Exception:
            pass  # Peer unreachable; do not crash

    def _send_heartbeats_to_all(self):
        with self.lock:
            if self.state != "LEADER":
                return
            term = self.current_term
            leader_id = self.node_id
            peers = list(self.peers.keys())

        for peer_id in peers:
            threading.Thread(
                target=self._send_one_heartbeat,
                args=(peer_id, term, leader_id),
                daemon=True
            ).start()

    def _heartbeat_loop(self):
        while self.running:
            time.sleep(HEARTBEAT_INTERVAL)
            if not self.running:
                break
            self._send_heartbeats_to_all()

    # --------------------------------------------------------------------------
    # SERVER LIFECYCLE
    # --------------------------------------------------------------------------

    def serve(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

        # Register both RaftService and LockService on the same gRPC server
        raft_pb2_grpc.add_RaftServiceServicer_to_server(
            RaftServiceServicer(self),
            self.server
        )
        evcharge_pb2_grpc.add_LockServiceServicer_to_server(
            RaftLockServiceServicer(self),
            self.server
        )

        bind_port = self.address.split(":")[-1]
        self.server.add_insecure_port(f"0.0.0.0:{bind_port}")
        self.server.start()

        print("\n" + "=" * 60, flush=True)
        print(f"EVCharge Raft Node {self.node_id} Running", flush=True)
        print("=" * 60, flush=True)
        print(f"Address       : {self.address}", flush=True)
        print(f"Role          : {self.state}", flush=True)
        print(f"Term          : {self.current_term}", flush=True)
        print(f"Peers         : {self.peers}", flush=True)
        print("Services      : RaftService + LockService (Experiment 5)", flush=True)
        print("=" * 60 + "\n", flush=True)

        # Start background threads for election timer and heartbeat dissemination
        threading.Thread(target=self._election_timer_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

        return self.server

    def stop(self):
        self.running = False
        for ch in self.channels.values():
            try:
                ch.close()
            except Exception:
                pass


# ==============================================================================
# gRPC SERVICERS
# ==============================================================================

class RaftServiceServicer(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, raft_node):
        self.raft_node = raft_node

    def RequestVote(self, request, context):
        return self.raft_node.handle_request_vote(request)

    def AppendEntries(self, request, context):
        return self.raft_node.handle_append_entries(request)


class RaftLockServiceServicer(evcharge_pb2_grpc.LockServiceServicer):
    """
    Leadership gating for Experiment 5 LockService:
    Only the active Raft leader executes lock/release operations.
    Followers reject requests returning 'not-leader: <known leader ID>'.
    """
    def __init__(self, raft_node):
        self.raft_node = raft_node

    def AcquireLock(self, request, context):
        with self.raft_node.lock:
            is_leader = (self.raft_node.state == "LEADER")
            leader_id = self.raft_node.leader_id

        if not is_leader:
            known = leader_id if leader_id else "none"
            return evcharge_pb2.LockReply(
                granted=False,
                message=f"not-leader: {known}"
            )

        # Dynamic resource registration in LockManager
        if request.resource_id not in self.raft_node.lock_manager.locks:
            with self.raft_node.lock_manager.mutex:
                self.raft_node.lock_manager.locks[request.resource_id] = None
                self.raft_node.lock_manager.conditions[request.resource_id] = threading.Condition()

        return self.raft_node.lock_manager.AcquireLock(request, context)

    def ReleaseLock(self, request, context):
        with self.raft_node.lock:
            is_leader = (self.raft_node.state == "LEADER")
            leader_id = self.raft_node.leader_id

        if not is_leader:
            known = leader_id if leader_id else "none"
            return evcharge_pb2.LockReply(
                granted=False,
                message=f"not-leader: {known}"
            )

        return self.raft_node.lock_manager.ReleaseLock(request, context)


# ==============================================================================
# MAIN ENTRYPOINT
# ==============================================================================

def main():
    if len(sys.argv) < 2:
        print("Usage: python backend/raft_node.py <A|B|C>", flush=True)
        sys.exit(1)

    node_id = sys.argv[1].upper()
    if node_id not in NODES:
        print(f"Error: Node ID must be one of {list(NODES.keys())}", flush=True)
        sys.exit(1)

    node = RaftNode(node_id)
    server = node.serve()

    try:
        while node.running:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print(f"\n[{node_id}] Shutting down...", flush=True)
    finally:
        node.stop()
        server.stop(0)


if __name__ == "__main__":
    main()
