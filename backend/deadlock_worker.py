import os
import sys
import time
import grpc

# Allow imports from the EVCharge project root
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import evcharge_pb2
import evcharge_pb2_grpc


LOCK_MANAGER = "localhost:60100"


def acquire(stub, resource, node_id, timestamp):

    reply = stub.AcquireLock(
        evcharge_pb2.LockRequest(
            resource_id=resource,
            holder_id=node_id,
            timestamp=timestamp
        )
    )

    print(
        f"Node-{node_id}: '{resource}' -> "
        f"granted={reply.granted} "
        f"({reply.message})"
    )

    return reply.granted


def release(stub, resource, node_id):

    stub.ReleaseLock(
        evcharge_pb2.LockRequest(
            resource_id=resource,
            holder_id=node_id,
            timestamp=0
        )
    )


def run(node_id, first, second):

    timestamp = node_id
    held = []

    with grpc.insecure_channel(
        LOCK_MANAGER
    ) as channel:

        stub = evcharge_pb2_grpc.LockServiceStub(
            channel
        )

        print(
            f"\nNode-{node_id}: Starting "
            f"EVCharge operation"
        )

        print(
            f"Node-{node_id}: Lock order = "
            f"{first} -> {second}"
        )

        # Acquire first resource
        if acquire(
            stub,
            first,
            node_id,
            timestamp
        ):

            held.append(first)

        else:

            print(
                f"Node-{node_id}: Could not acquire "
                f"'{first}'. DONE."
            )

            return

        # Give the second worker time to acquire
        # its first resource.
        time.sleep(5)

        # Try to acquire second resource
        if acquire(
            stub,
            second,
            node_id,
            timestamp
        ):

            held.append(second)

        else:

            print(
                f"Node-{node_id}: ABORTED"
            )

            print(
                f"Node-{node_id}: Releasing held locks "
                f"and retrying..."
            )

            for resource in held:
                release(
                    stub,
                    resource,
                    node_id
                )

            held = []

            time.sleep(1)

            # Retry in the same operation order
            if acquire(
                stub,
                second,
                node_id,
                timestamp
            ):

                held.append(second)

                if acquire(
                    stub,
                    first,
                    node_id,
                    timestamp
                ):

                    held.append(first)

                else:

                    for resource in held:
                        release(
                            stub,
                            resource,
                            node_id
                        )

                    print(
                        f"Node-{node_id}: Retry failed."
                    )

                    return

            else:

                print(
                    f"Node-{node_id}: Retry failed."
                )

                return

        print(
            f"\nNode-{node_id}: ENTERED "
            f"EVCharge critical section"
        )

        print(
            f"Node-{node_id}: Finalizing operation "
            f"using both shared resources..."
        )

        time.sleep(0.5)

        # Release all resources
        for resource in held:

            release(
                stub,
                resource,
                node_id
            )

        print(
            f"Node-{node_id}: DONE"
        )


if __name__ == "__main__":

    if len(sys.argv) != 3:

        print(
            "Usage:"
        )

        print(
            "python backend/deadlock_worker.py "
            "<node_id> <role>"
        )

        print(
            "Roles: reservation / status"
        )

        sys.exit(1)

    node_id = int(sys.argv[1])
    role = sys.argv[2].lower()

    if role == "reservation":

        # Reservation operation:
        # charger -> reservation
        run(
            node_id,
            "charger",
            "reservation"
        )

    elif role == "status":

        # Status operation:
        # reservation -> charger
        run(
            node_id,
            "reservation",
            "charger"
        )

    else:

        print(
            "Role must be 'reservation' "
            "or 'status'"
        )