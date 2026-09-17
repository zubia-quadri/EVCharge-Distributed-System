import os
import sys
import time
import threading
from concurrent import futures

import grpc

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import evcharge_pb2
import evcharge_pb2_grpc


ALL_REPLICAS = {
    "A": "localhost:60301",
    "B": "localhost:60302",
    "C": "localhost:60303"
}


class ReplicaNode(
    evcharge_pb2_grpc.ReplicaServiceServicer
):

    def __init__(self, name):
        self.name = name

        self.peers = {
            n: addr
            for n, addr in ALL_REPLICAS.items()
            if n != name
        }

        self.clock = 0
        self.lock = threading.Lock()

        # key -> (content, lamport_timestamp, origin_replica)
        self.store = {}

    def log(self, message):
        print(
            f"[Replica-{self.name} "
            f"t={self.clock:>2}] {message}",
            flush=True
        )

    def tick(self):
        with self.lock:
            self.clock += 1
            return self.clock

    def update_clock(self, received_ts):
        with self.lock:
            self.clock = max(
                self.clock,
                received_ts
            ) + 1

            return self.clock

    def apply_if_newer(
        self,
        key,
        content,
        timestamp,
        origin
    ):
        with self.lock:

            current = self.store.get(key)

            if (
                current is None
                or (timestamp, origin)
                > (current[1], current[2])
            ):

                self.store[key] = (
                    content,
                    timestamp,
                    origin
                )

                self.log(
                    f"APPLIED '{key}' = "
                    f"\"{content}\" "
                    f"(ts={timestamp}, origin={origin})"
                )

                return True

            self.log(
                f"IGNORED stale update for '{key}' "
                f"(incoming ts={timestamp}/{origin} "
                f"<= current ts={current[1]}/{current[2]})"
            )

            return False

    def gossip(
        self,
        key,
        content,
        timestamp,
        origin
    ):

        for peer_name, address in self.peers.items():

            try:

                with grpc.insecure_channel(address) as channel:

                    stub = (
                        evcharge_pb2_grpc
                        .ReplicaServiceStub(channel)
                    )

                    stub.SyncUpdate(
                        evcharge_pb2.ValueUpdate(
                            key=key,
                            content=content,
                            lamport_timestamp=timestamp,
                            origin_replica=origin
                        )
                    )

                    self.log(
                        f"Gossiped '{key}' -> "
                        f"{peer_name}"
                    )

            except grpc.RpcError as e:

                self.log(
                    f"Gossip to {peer_name} failed: "
                    f"{e.code()}"
                )

    def SaveValue(self, request, context):

        timestamp = self.tick()

        # Apply locally FIRST
        self.apply_if_newer(
            request.key,
            request.content,
            timestamp,
            self.name
        )

        # Gossip AFTER local write
        threading.Thread(
            target=self.gossip,
            args=(
                request.key,
                request.content,
                timestamp,
                self.name
            ),
            daemon=True
        ).start()

        return evcharge_pb2.SaveAck(
            accepted=True,
            replica=self.name,
            lamport_timestamp=timestamp
        )

    def SyncUpdate(self, request, context):

        self.update_clock(
            request.lamport_timestamp
        )

        self.apply_if_newer(
            request.key,
            request.content,
            request.lamport_timestamp,
            request.origin_replica
        )

        return evcharge_pb2.SaveAck(
            accepted=True,
            replica=self.name,
            lamport_timestamp=self.clock
        )

    def GetValue(self, request, context):

        with self.lock:

            entry = self.store.get(request.key)

            if entry is None:

                return evcharge_pb2.ValueState(
                    key=request.key,
                    content="",
                    lamport_timestamp=0,
                    origin_replica=""
                )

            content, timestamp, origin = entry

            return evcharge_pb2.ValueState(
                key=request.key,
                content=content,
                lamport_timestamp=timestamp,
                origin_replica=origin
            )


def serve(name, port):

    server = grpc.server(
        futures.ThreadPoolExecutor(
            max_workers=20
        )
    )

    evcharge_pb2_grpc.add_ReplicaServiceServicer_to_server(
        ReplicaNode(name),
        server
    )

    server.add_insecure_port(
        f"localhost:{port}"
    )

    server.start()

    print(
        f"Replica-{name} listening "
        f"on localhost:{port}",
        flush=True
    )

    try:

        while True:
            time.sleep(86400)

    except KeyboardInterrupt:

        server.stop(0)


if __name__ == "__main__":

    serve(
        sys.argv[1],
        int(sys.argv[2])
    )