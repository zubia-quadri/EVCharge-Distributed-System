# -*- coding: utf-8 -*-
"""
dfs_metadata.py - Metadata server for a small distributed file store
(Distributed File System).
It never touches file bytes. It only knows:
- which storage nodes exist and whether they are alive (heartbeats)
- for every file: its version, size, checksum, and which nodes hold
each chunk
New files get a version from a Lamport clock, using the same rules as
Experiment 3 (tick before sending, max(local, received) + 1 on receive).
State is saved to disk so a restart does not forget the files.
"""
import json
import os
import sys
import threading
import time
from concurrent import futures
import grpc

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import dfs_pb2
import dfs_pb2_grpc
from backend import net_config

REPLICATION_FACTOR = 2  # every chunk is stored on this many nodes
HEARTBEAT_TIMEOUT = 6.0  # seconds without a heartbeat -> node counts as down
STATE_FILE = os.path.join(PROJECT_ROOT, "dfs_data", "metadata.json")


class MetadataServicer(dfs_pb2_grpc.DfsMetadataServiceServicer):
    def __init__(self):
        self.lock = threading.Lock()
        self.clock = 0
        self.addresses = {}   # node_id -> address (saved to disk)
        self.last_seen = {}   # node_id -> time of last heartbeat (memory only)
        self.files = {}       # filename -> record (saved to disk)
        self._load()

    def log(self, msg):
        print(f"[meta t={self.clock:>3}] {msg}", flush=True)

    # ---- Lamport clock (call while holding self.lock) ----
    def _tick(self):
        self.clock += 1
        return self.clock

    def _update(self, received):
        self.clock = max(self.clock, received) + 1
        return self.clock

    # ---- persistence ----
    def _load(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.files = data.get("files", {})
                self.addresses = data.get("addresses", {})
                self.clock = data.get("clock", 0)
                print(f"Loaded {len(self.files)} file(s) from {STATE_FILE}", flush=True)
            except Exception as e:
                print(f"Warning: Could not load {STATE_FILE}: {e}", flush=True)

    def _save(self):
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({
                "clock": self.clock,
                "addresses": self.addresses,
                "files": self.files
            }, f, indent=2)
        os.replace(tmp, STATE_FILE)

    # ---- helpers (call while holding self.lock) ----
    def _alive(self, node_id):
        seen = self.last_seen.get(node_id)
        return seen is not None and (time.time() - seen) <= HEARTBEAT_TIMEOUT

    def _chunk_counts(self):
        counts = {}
        for rec in self.files.values():
            for ch in rec["chunks"]:
                for nid in ch["nodes"]:
                    counts[nid] = counts.get(nid, 0) + 1
        return counts

    def _file_info(self, filename, rec):
        chunks = []
        for ch in rec["chunks"]:
            # live replicas first, so clients try them before dead ones
            nodes = sorted(ch["nodes"], key=lambda n: 0 if self._alive(n) else 1)
            chunks.append(dfs_pb2.DfsChunkPlan(
                index=ch["index"],
                chunk_id=ch["chunk_id"],
                sha256=ch["sha256"],
                node_ids=nodes,
                addresses=[self.addresses.get(n, "") for n in nodes],
                alive=[self._alive(n) for n in nodes]
            ))
        return dfs_pb2.DfsFileInfo(
            found=True,
            filename=filename,
            version=rec["version"],
            size=rec["size"],
            sha256=rec["sha256"],
            chunks=chunks
        )

    # ---- RPCs ----
    def Heartbeat(self, request, context):
        with self.lock:
            is_new = request.node_id not in self.last_seen
            self.last_seen[request.node_id] = time.time()
            if self.addresses.get(request.node_id) != request.address:
                self.addresses[request.node_id] = request.address
                self._save()
            if is_new:
                self.log(f"Node {request.node_id} registered at {request.address}")
            return dfs_pb2.DfsAck(ok=True, message="ok")

    def AllocateFile(self, request, context):
        with self.lock:
            self._update(request.lamport_timestamp)
            live = [n for n in self.addresses if self._alive(n)]
            if not live:
                return dfs_pb2.DfsAllocateReply(
                    ok=False,
                    message="no live storage nodes",
                    lamport_timestamp=self.clock
                )

            version = self._tick()
            r = min(REPLICATION_FACTOR, len(live))
            counts = self._chunk_counts()
            load = {n: counts.get(n, 0) for n in live}
            plan = []
            for i in range(request.num_chunks):
                # least loaded live nodes hold this chunk
                chosen = sorted(live, key=lambda n: (load[n], n))[:r]
                for n in chosen:
                    load[n] += 1
                plan.append(dfs_pb2.DfsChunkPlan(
                    index=i,
                    chunk_id=f"{request.filename}.v{version}.c{i}",
                    node_ids=chosen,
                    addresses=[self.addresses[n] for n in chosen]
                ))
            self.log(
                f"Allocated '{request.filename}' v{version}: "
                f"{request.num_chunks} chunk(s), {r} replica(s) each"
            )
            return dfs_pb2.DfsAllocateReply(
                ok=True,
                message="allocated",
                version=version,
                plan=plan,
                lamport_timestamp=self.clock
            )

    def CommitFile(self, request, context):
        with self.lock:
            if any(len(ch.node_ids) == 0 for ch in request.chunks):
                return dfs_pb2.DfsCommitReply(
                    ok=False,
                    message="a chunk has no stored replica",
                    lamport_timestamp=self.clock
                )
            self.files[request.filename] = {
                "version": request.version,
                "size": request.size,
                "sha256": request.sha256,
                "chunks": [
                    {
                        "index": ch.index,
                        "chunk_id": ch.chunk_id,
                        "sha256": ch.sha256,
                        "nodes": list(ch.node_ids)
                    }
                    for ch in sorted(request.chunks, key=lambda c: c.index)
                ],
            }
            ts = self._tick()
            self._save()
            self.log(
                f"Committed '{request.filename}' v{request.version} "
                f"({request.size} bytes)"
            )
            return dfs_pb2.DfsCommitReply(
                ok=True,
                message="committed",
                lamport_timestamp=ts
            )

    def GetFileInfo(self, request, context):
        with self.lock:
            rec = self.files.get(request.filename)
            if rec is None:
                return dfs_pb2.DfsFileInfo(found=False, filename=request.filename)
            return self._file_info(request.filename, rec)

    def ClusterStatus(self, request, context):
        with self.lock:
            counts = self._chunk_counts()
            nodes = [
                dfs_pb2.DfsNodeStatus(
                    node_id=n,
                    address=a,
                    alive=self._alive(n),
                    chunk_count=counts.get(n, 0)
                )
                for n, a in sorted(self.addresses.items())
            ]
            files = [
                self._file_info(fn, rec)
                for fn, rec in sorted(self.files.items())
            ]
            return dfs_pb2.DfsStatusReply(nodes=nodes, files=files)

    def AddReplica(self, request, context):
        with self.lock:
            rec = self.files.get(request.filename)
            if rec is None:
                return dfs_pb2.DfsAck(ok=False, message="unknown file")
            for ch in rec["chunks"]:
                if ch["index"] == request.chunk_index:
                    if request.node_id not in ch["nodes"]:
                        ch["nodes"].append(request.node_id)
                        self._save()
                        self.log(
                            f"Replica of '{request.filename}' chunk "
                            f"{request.chunk_index} added on {request.node_id}"
                        )
                        return dfs_pb2.DfsAck(ok=True, message="replica recorded")
                    return dfs_pb2.DfsAck(ok=True, message="already recorded")
            return dfs_pb2.DfsAck(ok=False, message="unknown chunk")

    # ---- background thread: report nodes going down / coming back ----
    def monitor(self):
        reported = {}
        while True:
            time.sleep(1)
            with self.lock:
                for nid in list(self.addresses):
                    alive = self._alive(nid)
                    prev = reported.get(nid)
                    reported[nid] = alive
                    if prev is not None and prev != alive:
                        state = "BACK UP" if alive else "DOWN (no heartbeat)"
                        self.log(f"Node {nid} is {state}")


def serve():
    address = net_config.resolve("dfs-meta")
    port = address.split(":")[1]
    servicer = MetadataServicer()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dfs_pb2_grpc.add_DfsMetadataServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(
        f"DFS metadata server listening on {address} "
        f"(replication factor {REPLICATION_FACTOR})",
        flush=True
    )
    threading.Thread(target=servicer.monitor, daemon=True).start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve()
