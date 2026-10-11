import os
import sys
import time
import random
from concurrent import futures

import grpc

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import evcharge_pb2
import evcharge_pb2_grpc


class EVWorkerServicer(evcharge_pb2_grpc.EVWorkerServiceServicer):

    def __init__(self, name):
        self.name = name

    def HandleRequest(self, request, context):
        # Simulate EV charging request processing times
        work_time = random.uniform(0.1, 0.4)

        print(
            f"[{self.name}] Handling request "
            f"{request.request_id} "
            f"(will take {work_time:.2f}s)",
            flush=True
        )

        time.sleep(work_time)

        print(
            f"[{self.name}] Finished EVCharge request "
            f"{request.request_id}",
            flush=True
        )

        return evcharge_pb2.LoadReply(
            request_id=request.request_id,
            handled_by=self.name,
            status="completed"
        )


def serve(port=60201):
    pod_id = os.environ.get("HOSTNAME", f"port-{port}")
    name = f"EVWorker-{pod_id}"

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=20)
    )

    evcharge_pb2_grpc.add_EVWorkerServiceServicer_to_server(
        EVWorkerServicer(name),
        server
    )

    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()

    print(f"[{name}] listening on 0.0.0.0:{port}", flush=True)

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.environ.get("WORKER_PORT", 60201))
    serve(port)