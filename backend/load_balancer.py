import os
import sys
import threading
import time
import grpc

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import evcharge_pb2
import evcharge_pb2_grpc


BACKENDS = [
    "localhost:60201",
    "localhost:60202",
    "localhost:60203"
]

NUM_REQUESTS = 9

# Number of currently active requests for each backend
active = [0] * len(BACKENDS)

lock = threading.Lock()


def pick_least_connections():
    """
    Select the backend with the fewest active connections.
    Increment the counter immediately before dispatching.
    """

    with lock:
        idx = active.index(min(active))
        active[idx] += 1

        print(
            f"[LB] Routing -> {BACKENDS[idx]} "
            f"(active connections now: {active})"
        )

        return idx


def release(idx):
    """
    Decrease the active connection count
    after the request finishes.
    """

    with lock:
        active[idx] -= 1

        print(
            f"[LB] Released {BACKENDS[idx]} "
            f"(active connections now: {active})"
        )


def handle_request(req_id):
    idx = pick_least_connections()
    address = BACKENDS[idx]

    try:
        with grpc.insecure_channel(address) as channel:

            stub = evcharge_pb2_grpc.EVWorkerServiceStub(
                channel
            )

            reply = stub.HandleRequest(
                evcharge_pb2.LoadRequest(
                    request_id=req_id
                )
            )

            print(
                f"[LB] Request {req_id} -> "
                f"handled by {reply.handled_by}"
            )

    finally:
        release(idx)


def main():

    print(
        f"[LB] EVCharge Load Balancer starting.\n"
        f"[LB] Backends: {BACKENDS}"
    )

    threads = []

    for req_id in range(1, NUM_REQUESTS + 1):

        t = threading.Thread(
            target=handle_request,
            args=(req_id,)
        )

        threads.append(t)
        t.start()

        # Simulate incoming traffic
        time.sleep(0.15)

    for t in threads:
        t.join()

    print("\n[LB] All EVCharge requests processed.")
    print(
        f"[LB] Final active connection counts: {active}"
    )


if __name__ == "__main__":
    main()