# load_test.py - EVCharge gRPC Load Test for Kubernetes
import os
import sys
import time
from concurrent import futures

import grpc

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import evcharge_pb2
import evcharge_pb2_grpc

TARGET_ADDR = os.environ.get("TARGET_ADDR", "worker:60201")
NUM_REQUESTS = int(os.environ.get("NUM_REQUESTS", "25"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "5"))


def send_request(stub, req_id):
    start = time.time()
    try:
        reply = stub.HandleRequest(
            evcharge_pb2.LoadRequest(request_id=req_id),
            timeout=10.0
        )
        elapsed = time.time() - start
        print(f"[LoadClient] Req {req_id:02d} -> {reply.handled_by} ({elapsed:.2f}s, status={reply.status})", flush=True)
        return reply.handled_by, True
    except Exception as e:
        elapsed = time.time() - start
        print(f"[LoadClient] Req {req_id:02d} FAILED ({elapsed:.2f}s): {e}", flush=True)
        return "ERROR", False


def main():
    target = TARGET_ADDR
    if not target.startswith("dns:///"):
        target = f"dns:///{target}"

    print("=" * 65, flush=True)
    print("EVCharge Kubernetes Load Test Starting", flush=True)
    print(f"Target Service   : {target}", flush=True)
    print(f"Total Requests   : {NUM_REQUESTS}", flush=True)
    print(f"Concurrency      : {CONCURRENCY}", flush=True)
    print(f"gRPC LB Policy   : round_robin", flush=True)
    print("=" * 65, flush=True)

    channel = grpc.insecure_channel(
        target,
        options=[("grpc.lb_policy_name", "round_robin")]
    )
    stub = evcharge_pb2_grpc.EVWorkerServiceStub(channel)

    results = {}
    successful = 0

    start_total = time.time()
    with futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        future_to_req = {
            executor.submit(send_request, stub, req_id): req_id
            for req_id in range(1, NUM_REQUESTS + 1)
        }
        for future in futures.as_completed(future_to_req):
            handler, ok = future.result()
            if ok:
                successful += 1
                results[handler] = results.get(handler, 0) + 1

    total_time = time.time() - start_total
    channel.close()

    print("\n" + "=" * 65, flush=True)
    print("Load Test Summary", flush=True)
    print("=" * 65, flush=True)
    print(f"Total Completed Requests: {successful}/{NUM_REQUESTS} ({successful/NUM_REQUESTS*100:.1f}%)", flush=True)
    print(f"Total Duration          : {total_time:.2f}s", flush=True)
    print(f"Throughput              : {successful/total_time:.2f} req/s", flush=True)
    print("\nTraffic Distribution per Pod Replica:", flush=True)
    for pod, count in sorted(results.items()):
        print(f"  • {pod:<35}: {count:>2} requests ({count/NUM_REQUESTS*100:.1f}%)", flush=True)
    print("=" * 65, flush=True)


if __name__ == "__main__":
    main()
