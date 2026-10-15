# -*- coding: utf-8 -*-
"""
dfs_repair.py - Restores the replication factor after a storage node dies
(Distributed File System).
It asks the metadata server which chunks have fewer live replicas than the
replication factor, copies each one from a live replica to a live node that
does not hold it yet (checking the checksum on the way), and tells the
metadata server about the new copy.
"""
import hashlib
import os
import sys
import grpc

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import dfs_pb2
import dfs_pb2_grpc
try:
    from backend import net_config
except ImportError:
    import net_config

REPLICATION_FACTOR = 2


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def repair():
    try:
        with grpc.insecure_channel(net_config.resolve("dfs-meta")) as channel:
            meta = dfs_pb2_grpc.DfsMetadataServiceStub(channel)
            status = meta.ClusterStatus(dfs_pb2.DfsStatusRequest(), timeout=5)
            alive = {n.node_id: n.address for n in status.nodes if n.alive}
            print(f"Live nodes: {', '.join(sorted(alive)) or 'none'}")
            copied = 0
            problems = 0
            for f in status.files:
                for chunk in sorted(f.chunks, key=lambda c: c.index):
                    holders = [
                        (nid, addr)
                        for nid, addr, up in zip(
                            chunk.node_ids, chunk.addresses, chunk.alive
                        )
                        if up
                    ]
                    missing = REPLICATION_FACTOR - len(holders)
                    if missing <= 0:
                        continue
                    if not holders:
                        print(
                            f"  {f.filename} chunk {chunk.index}: NO live replica, cannot repair"
                        )
                        problems += 1
                        continue

                    # read a good copy from a live holder
                    data = None
                    for nid, addr in holders:
                        try:
                            with grpc.insecure_channel(addr) as sch:
                                reply = dfs_pb2_grpc.DfsStorageServiceStub(sch).GetChunk(
                                    dfs_pb2.DfsChunkId(chunk_id=chunk.chunk_id),
                                    timeout=3,
                                )
                                if sha256(reply.data) == chunk.sha256:
                                    data = reply.data
                                    break
                        except grpc.RpcError:
                            continue

                    if data is None:
                        print(
                            f"  {f.filename} chunk {chunk.index}: no healthy copy to read from"
                        )
                        problems += 1
                        continue

                    targets = [n for n in sorted(alive) if n not in chunk.node_ids]
                    for target in targets[:missing]:
                        try:
                            with grpc.insecure_channel(alive[target]) as sch:
                                ack = dfs_pb2_grpc.DfsStorageServiceStub(sch).PutChunk(
                                    dfs_pb2.DfsChunkData(
                                        chunk_id=chunk.chunk_id,
                                        data=data,
                                        sha256=chunk.sha256,
                                    ),
                                    timeout=5,
                                )
                                if not ack.ok:
                                    continue
                                meta.AddReplica(
                                    dfs_pb2.DfsReplicaUpdate(
                                        filename=f.filename,
                                        chunk_index=chunk.index,
                                        node_id=target,
                                    ),
                                    timeout=5,
                                )
                                print(
                                    f"  {f.filename} chunk {chunk.index}: copied to {target}"
                                )
                                copied += 1
                        except grpc.RpcError as e:
                            print(
                                f"  {f.filename} chunk {chunk.index}: could not copy to {target} ({e.code().name})"
                            )
                            problems += 1
            print(
                f"Repair finished: {copied} chunk copy(ies) made, {problems} problem(s)."
            )
    except grpc.RpcError as e:
        print(f"Could not reach the metadata server ({e.code().name}).")


if __name__ == "__main__":
    repair()
