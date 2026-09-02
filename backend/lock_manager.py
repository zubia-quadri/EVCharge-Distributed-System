import os
import sys
import time
import threading
from concurrent import futures

import grpc

# Add EVCharge project root to Python path
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import evcharge_pb2
import evcharge_pb2_grpc


# Detection mode:
# python backend/lock_manager.py       -> False
# python backend/lock_manager.py detect -> True
DETECT = (
    len(sys.argv) > 1
    and sys.argv[1].lower() == "detect"
)


class LockManager(evcharge_pb2_grpc.LockServiceServicer):

    def __init__(self):

        # Protect shared lock-manager data
        self.mutex = threading.Lock()

        # Shared EVCharge resources
        # resource -> current owner
        self.locks = {
            "charger": None,
            "reservation": None
        }

        # node -> resource it is currently waiting for
        #
        # Example:
        # Node 1 -> reservation
        # Node 2 -> charger
        #
        # This represents the wait-for relationships.
        self.wait_for = {}

        # Conditions used to wake waiting requests
        self.conditions = {
            "charger": threading.Condition(),
            "reservation": threading.Condition()
        }


    # ==========================================================
    # DEADLOCK / CYCLE DETECTION
    # ==========================================================

    def _would_cycle(self, requester, resource):
        """
        Determine whether making 'requester' wait for 'resource'
        would create a circular wait.

        Example:

        Node 1 waits for reservation
        reservation is held by Node 2

        Node 2 waits for charger
        charger is held by Node 1

        Therefore:

        Node 1 -> Node 2 -> Node 1

        This is a cycle and therefore a deadlock.
        """

        # Who currently owns the requested resource?
        owner = self.locks.get(resource)

        # Nobody owns it -> no cycle
        if owner is None:
            return False

        visited = set()

        while owner is not None:

            # We reached the original requester.
            # This means a circular wait exists.
            if owner == requester:
                return True

            # Prevent infinite traversal
            if owner in visited:
                return False

            visited.add(owner)

            # What resource is this owner waiting for?
            waiting_resource = self.wait_for.get(owner)

            if waiting_resource is None:
                return False

            # Who owns that resource?
            owner = self.locks.get(waiting_resource)

        return False


    # ==========================================================
    # ACQUIRE LOCK
    # ==========================================================

    def AcquireLock(self, request, context):

        resource = request.resource_id
        holder = request.holder_id
        timestamp = request.timestamp

        # Validate resource
        if resource not in self.locks:

            return evcharge_pb2.LockReply(
                granted=False,
                message="unknown-resource"
            )

        condition = self.conditions[resource]

        print(
            f"[LockManager] Node-{holder} requests "
            f"'{resource}' at timestamp {timestamp}"
        )

        with condition:

            with self.mutex:

                current_owner = self.locks.get(resource)

                # ------------------------------------------------
                # RESOURCE IS FREE
                # ------------------------------------------------

                if current_owner is None:

                    self.locks[resource] = holder

                    # No longer waiting
                    self.wait_for.pop(holder, None)

                    print(
                        f"[LockManager] GRANTED "
                        f"'{resource}' to Node-{holder}"
                    )

                    return evcharge_pb2.LockReply(
                        granted=True,
                        message="granted"
                    )

                # ------------------------------------------------
                # REQUESTER ALREADY OWNS RESOURCE
                # ------------------------------------------------

                if current_owner == holder:

                    print(
                        f"[LockManager] Node-{holder} already "
                        f"owns '{resource}'"
                    )

                    return evcharge_pb2.LockReply(
                        granted=True,
                        message="already-owner"
                    )

                # ------------------------------------------------
                # CHECK FOR DEADLOCK
                # ------------------------------------------------

                if DETECT:

                    creates_cycle = self._would_cycle(
                        holder,
                        resource
                    )

                    if creates_cycle:

                        print()
                        print("=" * 60)
                        print("[LockManager] DEADLOCK DETECTED")
                        print(
                            f"[LockManager] Node-{holder} "
                            f"cannot acquire '{resource}'"
                        )
                        print(
                            f"[LockManager] Resource is held "
                            f"by Node-{current_owner}"
                        )
                        print(
                            f"[LockManager] ABORTING "
                            f"Node-{holder}"
                        )
                        print("=" * 60)
                        print()

                        # Do not add this request to wait_for.
                        # Returning false causes the worker to abort.
                        return evcharge_pb2.LockReply(
                            granted=False,
                            message="deadlock-abort"
                        )

                # ------------------------------------------------
                # RESOURCE BUSY
                # ------------------------------------------------

                self.wait_for[holder] = resource

                print(
                    f"[LockManager] Node-{holder} WAITING for "
                    f"'{resource}' "
                    f"(held by Node-{current_owner})"
                )

            # ----------------------------------------------------
            # WAIT UNTIL RESOURCE BECOMES AVAILABLE
            # ----------------------------------------------------

            while True:

                with self.mutex:

                    current_owner = self.locks.get(resource)

                    if current_owner is None:

                        self.locks[resource] = holder

                        self.wait_for.pop(holder, None)

                        print(
                            f"[LockManager] GRANTED "
                            f"'{resource}' to Node-{holder} "
                            f"after wait"
                        )

                        return evcharge_pb2.LockReply(
                            granted=True,
                            message="granted-after-wait"
                        )

                # Wake periodically and check again
                condition.wait(timeout=1)


    # ==========================================================
    # RELEASE LOCK
    # ==========================================================

    def ReleaseLock(self, request, context):

        resource = request.resource_id
        holder = request.holder_id

        # Validate resource
        if resource not in self.locks:

            return evcharge_pb2.LockReply(
                granted=False,
                message="unknown-resource"
            )

        condition = self.conditions[resource]

        with condition:

            with self.mutex:

                current_owner = self.locks.get(resource)

                # Only the current owner can release it
                if current_owner == holder:

                    self.locks[resource] = None

                    print(
                        f"[LockManager] Node-{holder} "
                        f"RELEASED '{resource}'"
                    )

                    # Wake waiting workers
                    condition.notify_all()

                    return evcharge_pb2.LockReply(
                        granted=True,
                        message="released"
                    )

                # Someone else attempted to release it
                print(
                    f"[LockManager] Node-{holder} attempted "
                    f"to release '{resource}', but owner is "
                    f"Node-{current_owner}"
                )

                return evcharge_pb2.LockReply(
                    granted=False,
                    message="not-owner"
                )


# ==============================================================
# START LOCK MANAGER SERVER
# ==============================================================

def serve():

    server = grpc.server(
        futures.ThreadPoolExecutor(
            max_workers=10
        )
    )

    evcharge_pb2_grpc.add_LockServiceServicer_to_server(
        LockManager(),
        server
    )

    server.add_insecure_port(
        "localhost:60100"
    )

    server.start()

    print()
    print("=" * 60)
    print("EVCharge Lock Manager")
    print("=" * 60)
    print("Address           : localhost:60100")
    print(f"Deadlock Detection: {DETECT}")
    print("Resources         : charger, reservation")
    print("=" * 60)
    print()

    try:

        while True:
            time.sleep(86400)

    except KeyboardInterrupt:

        print()
        print("[LockManager] Stopping...")
        server.stop(0)


# ==============================================================
# MAIN
# ==============================================================

if __name__ == "__main__":
    serve()