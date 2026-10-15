# -*- coding: utf-8 -*-
"""
dfs_storage.py - One storage node of a small distributed file store
(Distributed File System).
Run three copies, each in its own terminal:
python backend/dfs_storage.py 1
python backend/dfs_storage.py 2
python backend/dfs_storage.py 3
A node only stores and serves chunks. It checks every chunk's checksum
before writing it to disk, and sends a heartbeat to the metadata server
every 2 seconds so the metadata server knows it is alive.
"""
import hashlib
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

HEARTBEAT_INTERVAL = 2.0


class StorageServicer(dfs_pb2_grpc.DfsStorageServiceServicer):
    def __init__(self, node_id, directory):
        self.node_id = node_id
        self.directory = directory
        os.makedirs(directory, exist_ok=True)

    def _path(self, chunk_id):
        return os.path.join(self.directory, chunk_id.replace("/", "_"))

    def PutChunk(self, request, context):
        digest = hashlib.sha256(request.data).hexdigest()
        if digest != request.sha256:
            print(
                f"[{self.node_id}] REJECTED {request.chunk_id}: checksum mismatch",
                flush=True
            )
            return dfs_pb2.DfsAck(ok=False, message="checksum mismatch")
        path = self._path(request.chunk_id)
        tmp = path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(request.data)
        os.replace(tmp, path)
        print(
            f"[{self.node_id}] stored {request.chunk_id} "
            f"({len(request.data)} bytes)",
            flush=True
        )
        return dfs_pb2.DfsAck(ok=True, message="stored")

    def GetChunk(self, request, context):
        path = self._path(request.chunk_id)
        if not os.path.exists(path):
            context.abort(grpc.StatusCode.NOT_FOUND, "chunk not found")
        with open(path, "rb") as f:
            data = f.read()
        print(
            f"[{self.node_id}] served {request.chunk_id} ({len(data)} bytes)",
            flush=True
        )
        return dfs_pb2.DfsChunkData(
            chunk_id=request.chunk_id,
            data=data,
            sha256=hashlib.sha256(data).hexdigest()
        )


def heartbeat_loop(node_id, address):
    meta_addr = net_config.resolve("dfs-meta")
    reachable = None
    while True:
        try:
            with grpc.insecure_channel(meta_addr) as channel:
                stub = dfs_pb2_grpc.DfsMetadataServiceStub(channel)
                stub.Heartbeat(
                    dfs_pb2.DfsNodeInfo(node_id=node_id, address=address),
                    timeout=2
                )
                if reachable is not True:
                    print(
                        f"[{node_id}] connected to metadata server at {meta_addr}",
                        flush=True
                    )
                    reachable = True
        except grpc.RpcError:
            if reachable is not False:
                print(
                    f"[{node_id}] metadata server unreachable, will keep retrying",
                    flush=True
                )
                reachable = False
        time.sleep(HEARTBEAT_INTERVAL)


def serve(node_num):
    node_id = f"store-{node_num}"
    address = net_config.resolve(f"dfs-store-{node_num}")
    port = address.split(":")[1]
    directory = os.path.join(PROJECT_ROOT, "dfs_data", node_id)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dfs_pb2_grpc.add_DfsStorageServiceServicer_to_server(
        StorageServicer(node_id, directory),
        server
    )
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(
        f"DFS storage node {node_id} listening on {address}, "
        f"keeping chunks in {directory}/",
        flush=True
    )
    threading.Thread(
        target=heartbeat_loop,
        args=(node_id, address),
        daemon=True
    ).start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backend/dfs_storage.py <1|2|3>", flush=True)
        sys.exit(1)
    serve(sys.argv[1])
