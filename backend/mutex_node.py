import os
import sys
import time
import threading
from concurrent import futures

# Add the EVCharge project root to Python's module path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import grpc
import evcharge_pb2
import evcharge_pb2_grpc


# Three equal EVCharge peer nodes
PEERS = {
    1: "localhost:60051",
    2: "localhost:60052",
    3: "localhost:60053",
}


class MutexNode:
    def __init__(self, node_id):
        self.node_id = node_id

        # Every node communicates with the other two nodes
        self.peers = {
            peer_id: address
            for peer_id, address in PEERS.items()
            if peer_id != node_id
        }

        # Lamport logical clock
        self.clock = 0

        # RELEASED -> WANTED -> HELD
        self.state = "RELEASED"

        # Timestamp when this node requested the critical section
        self.request_timestamp = None

        # Protect shared local variables
        self.lock = threading.Lock()

        # Stores deferred peer requests
        self.deferred = []


    # -----------------------------
    # LAMPORT CLOCK
    # -----------------------------

    def tick(self):
        with self.lock:
            self.clock += 1
            return self.clock


    def update_clock(self, received_timestamp):
        with self.lock:
            self.clock = max(self.clock, received_timestamp) + 1
            return self.clock


    # -----------------------------
    # START gRPC SERVER
    # -----------------------------

    def serve(self):
        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10)
        )

        evcharge_pb2_grpc.add_MutexServiceServicer_to_server(
            MutexServicer(self),
            server
        )

        server.add_insecure_port(PEERS[self.node_id])
        server.start()

        print("\n" + "=" * 55)
        print(f"EVCHARGE PEER NODE {self.node_id} RUNNING")
        print("=" * 55)
        print(f"Port          : {PEERS[self.node_id]}")
        print(f"Lamport Clock : {self.clock}")
        print(f"Initial State : {self.state}")
        print("Algorithm     : Ricart-Agrawala Mutual Exclusion")
        print(f"Peers         : {self.peers}")
        print("=" * 55 + "\n")

        return server


    # -----------------------------
    # REQUEST ACCESS FROM PEER
    # -----------------------------

    def call_peer(self, address, request_timestamp):
        deadline = time.time() + 30

        while time.time() < deadline:
            try:
                with grpc.insecure_channel(address) as channel:
                    stub = evcharge_pb2_grpc.MutexServiceStub(channel)

                    reply = stub.RequestAccess(
                        evcharge_pb2.AccessRequest(
                            node_id=self.node_id,
                            timestamp=request_timestamp
                        )
                    )

                    return reply

            except grpc.RpcError:
                time.sleep(0.5)

        raise RuntimeError(
            f"Node-{self.node_id} could not reach peer {address}"
        )


    # -----------------------------
    # REQUEST CRITICAL SECTION
    # -----------------------------

    def request_critical_section(self):

        # 1. Increment Lamport clock
        request_timestamp = self.tick()

        # 2. Change state to WANTED
        with self.lock:
            self.state = "WANTED"
            self.request_timestamp = request_timestamp

        print(
            f"\n[NODE {self.node_id}] WANTS TO FINALIZE "
            f"THE CHARGING RESERVATION"
        )

        print(
            f"[NODE {self.node_id}] Request priority: "
            f"({request_timestamp}, {self.node_id})"
        )

        # 3. Request permission from every peer
        for peer_id, address in self.peers.items():

            print(
                f"[NODE {self.node_id}] Requesting access "
                f"from NODE {peer_id}..."
            )

            reply = self.call_peer(
                address,
                request_timestamp
            )

            # Update Lamport clock after receiving reply
            self.update_clock(reply.timestamp)

            print(
                f"[NODE {self.node_id}] Permission granted "
                f"by NODE {peer_id}"
            )

        # 4. Received all replies -> enter CS
        with self.lock:
            self.state = "HELD"

        print("\n" + "!" * 55)
        print(
            f"[NODE {self.node_id}] ENTERED CRITICAL SECTION"
        )
        print(
            f"[NODE {self.node_id}] Finalizing exclusive "
            f"EV charging reservation..."
        )
        print("!" * 55)

        # Simulate exclusive reservation operation
        time.sleep(3)

        self.release_critical_section()


    # -----------------------------
    # RELEASE CRITICAL SECTION
    # -----------------------------

    def release_critical_section(self):

        print(
            f"\n[NODE {self.node_id}] RELEASED CRITICAL SECTION"
        )

        # Change state and collect deferred requests
        with self.lock:
            self.state = "RELEASED"

            deferred_requests = self.deferred
            self.deferred = []

        # Allow deferred peers to receive their replies
        for event in deferred_requests:
            event.set()

        print(
            f"[NODE {self.node_id}] Released "
            f"{len(deferred_requests)} deferred request(s)"
        )


# ==========================================
# gRPC SERVICE
# ==========================================

class MutexServicer(
    evcharge_pb2_grpc.MutexServiceServicer
):

    def __init__(self, node):
        self.node = node


    def RequestAccess(self, request, context):

        node = self.node

        # Receive timestamp and update Lamport clock
        updated_clock = node.update_clock(request.timestamp)

        print(
            f"\n[NODE {node.node_id}] Received access request "
            f"from NODE {request.node_id}"
        )

        print(
            f"[NODE {node.node_id}] Received priority: "
            f"({request.timestamp}, {request.node_id})"
        )

        print(
            f"[NODE {node.node_id}] Current state: "
            f"{node.state}"
        )

        with node.lock:

            # Ricart-Agrawala decision rule:
            #
            # DEFER if:
            # 1. Current node is already HELD
            #
            # OR
            #
            # 2. Current node is WANTED AND
            #    its own priority is higher
            #    (smaller timestamp, node_id)

            defer = (
                node.state == "HELD"
                or (
                    node.state == "WANTED"
                    and (
                        node.request_timestamp,
                        node.node_id
                    )
                    <
                    (
                        request.timestamp,
                        request.node_id
                    )
                )
            )

            if defer:
                event = threading.Event()
                node.deferred.append(event)

                print(
                    f"[NODE {node.node_id}] DEFERS "
                    f"NODE {request.node_id}"
                )

            else:
                event = None

                print(
                    f"[NODE {node.node_id}] GRANTS "
                    f"NODE {request.node_id}"
                )

        # If deferred, block this RPC
        if event is not None:
            event.wait()

            print(
                f"[NODE {node.node_id}] Now releasing "
                f"deferred NODE {request.node_id}"
            )

        # Send reply timestamp
        reply_timestamp = node.tick()

        return evcharge_pb2.AccessReply(
            node_id=node.node_id,
            timestamp=reply_timestamp
        )


# ==========================================
# MAIN
# ==========================================

def main():

    if len(sys.argv) != 2:
        print("Usage: python backend/mutex_node.py <node_id>")
        print("Example: python backend/mutex_node.py 1")
        return

    node_id = int(sys.argv[1])

    if node_id not in PEERS:
        print("Node ID must be 1, 2, or 3")
        return

    node = MutexNode(node_id)

    # Start this node's gRPC server
    server = node.serve()

    try:
        print(f"[NODE {node_id}] Server is ready.")
        print(f"[NODE {node_id}] Start all 3 nodes first.")
        print(
            f"[NODE {node_id}] Press ENTER to request "
            "the critical section."
        )

        # Wait until YOU decide to start the request
        input()

        node.request_critical_section()

        print(f"\n[NODE {node_id}] Experiment completed.")
        print("Press CTRL+C to stop the node.")

        # Keep the gRPC server alive
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n[NODE {node_id}] Stopping...")

    finally:
        server.stop(0)


if __name__ == "__main__":
    main()