import os
import sys
import time
import threading

import grpc

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import evcharge_pb2
import evcharge_pb2_grpc


REPLICAS = {
    "A": "localhost:60301",
    "B": "localhost:60302",
    "C": "localhost:60303"
}


def save(replica_name, key, content):

    with grpc.insecure_channel(
        REPLICAS[replica_name]
    ) as channel:

        stub = (
            evcharge_pb2_grpc
            .ReplicaServiceStub(channel)
        )

        ack = stub.SaveValue(
            evcharge_pb2.ValueUpdate(
                key=key,
                content=content,
                lamport_timestamp=0,
                origin_replica=""
            )
        )

        print(
            f"[Client] Saved on Replica-{replica_name}: "
            f"\"{content}\" "
            f"-> accepted={ack.accepted}, "
            f"replica_ts={ack.lamport_timestamp}"
        )


def read(replica_name, key):

    with grpc.insecure_channel(
        REPLICAS[replica_name]
    ) as channel:

        stub = (
            evcharge_pb2_grpc
            .ReplicaServiceStub(channel)
        )

        state = stub.GetValue(
            evcharge_pb2.ValueQuery(
                key=key
            )
        )

        return (
            state.content,
            state.lamport_timestamp,
            state.origin_replica
        )


def main():

    key = "charging-status"

    print(
        "=== EVCharge Eventual Consistency Demo ===\n"
    )

    # Two writers target different replicas
    t1 = threading.Thread(
        target=save,
        args=(
            "A",
            key,
            "Charging started"
        )
    )

    t2 = threading.Thread(
        target=save,
        args=(
            "C",
            key,
            "Charging completed"
        )
    )

    t1.start()

    time.sleep(0.05)

    t2.start()

    t1.join()
    t2.join()

    print(
        "\n=== Immediately after writes "
        "(replicas may disagree) ==="
    )

    for name in REPLICAS:

        content, ts, origin = read(
            name,
            key
        )

        print(
            f"Replica-{name}: "
            f"\"{content}\" "
            f"(ts={ts}, origin={origin})"
        )

    print(
        "\nWaiting 2 seconds for gossip "
        "to propagate...\n"
    )

    time.sleep(2)

    print(
        "=== After convergence window ==="
    )

    results = {}

    for name in REPLICAS:

        content, ts, origin = read(
            name,
            key
        )

        results[name] = content

        print(
            f"Replica-{name}: "
            f"\"{content}\" "
            f"(ts={ts}, origin={origin})"
        )

    if len(set(results.values())) == 1:

        print(
            "\n*** CONVERGED: all replicas "
            f"agree on: "
            f"\"{list(results.values())[0]}\" ***"
        )

    else:

        print(
            "\n*** NOT YET CONVERGED: "
            f"{set(results.values())} ***"
        )


if __name__ == "__main__":
    main()