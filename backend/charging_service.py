# -*- coding: utf-8 -*-
"""
charging_service.py - gRPC Server (Charging Service)

This is the gRPC SERVER in the EVCharge distributed architecture.

Responsibilities:
  - Maintain in-memory state for the 3 charging slots
  - Implement the ChargingService RPCs defined in evcharge.proto
    - CheckChargerAvailability
    - ReserveCharger
    - UpdateChargerStatus
  - Maintain its own Lamport Logical Clock
  - Print structured terminal logs showing clock progression

Start with:
    python backend/charging_service.py
"""

import sys
import os
import threading
from concurrent import futures

# -- Path setup so we can import generated pb2 files from project root ---------
_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.join(_BACKEND_DIR, "..")
sys.path.insert(0, _ROOT)
sys.path.insert(0, _BACKEND_DIR)

import grpc
import evcharge_pb2
import evcharge_pb2_grpc

from lamport_clock import LamportClock

# ------------------------------------------------------------------------------
# In-memory charger state
# ------------------------------------------------------------------------------
CHARGERS = {
    "Charger_01": {"status": "AVAILABLE", "ev_id": None},
    "Charger_02": {"status": "AVAILABLE", "ev_id": None},
    "Charger_03": {"status": "AVAILABLE", "ev_id": None},
}
_lock = threading.Lock()   # protect shared CHARGERS dict

# ------------------------------------------------------------------------------
# gRPC Service Implementation
# ------------------------------------------------------------------------------
class ChargingServiceServicer(evcharge_pb2_grpc.ChargingServiceServicer):

    def __init__(self):
        self.clock = LamportClock(owner="Charging Service")

    # -- RPC 1: CheckChargerAvailability ---------------------------------------
    def CheckChargerAvailability(self, request, context):
        # Receive: update clock with sender's timestamp
        self.clock.receive(request.lamport_timestamp)

        charger_id = request.charger_id
        print(f"\n[Charging Service] -- CheckChargerAvailability --")
        print(f"  Charger requested : {charger_id}")

        with _lock:
            charger = CHARGERS.get(charger_id)

        if charger is None:
            print(f"  Charger {charger_id} NOT FOUND")
            ts = self.clock.send()
            return evcharge_pb2.AvailabilityResponse(
                charger_id=charger_id,
                available=False,
                lamport_timestamp=ts,
            )

        available = charger["status"] == "AVAILABLE"
        print(f"  {charger_id}: {'AVAILABLE [OK]' if available else 'OCCUPIED [--]'}")

        ts = self.clock.send()
        return evcharge_pb2.AvailabilityResponse(
            charger_id=charger_id,
            available=available,
            lamport_timestamp=ts,
        )

    # -- RPC 2: ReserveCharger -------------------------------------------------
    def ReserveCharger(self, request, context):
        self.clock.receive(request.lamport_timestamp)

        charger_id = request.charger_id
        ev_id      = request.ev_id
        print(f"\n[Charging Service] -- ReserveCharger --")
        print(f"  EV: {ev_id} -> Charger: {charger_id}")

        with _lock:
            charger = CHARGERS.get(charger_id)
            if charger is None:
                print(f"  ERROR: Charger {charger_id} not found")
                ts = self.clock.send()
                return evcharge_pb2.ReservationResponse(
                    success=False,
                    message=f"Charger {charger_id} does not exist.",
                    lamport_timestamp=ts,
                )

            if charger["status"] != "AVAILABLE":
                print(f"  ERROR: {charger_id} already OCCUPIED by {charger['ev_id']}")
                ts = self.clock.send()
                return evcharge_pb2.ReservationResponse(
                    success=False,
                    message=f"{charger_id} is currently OCCUPIED.",
                    lamport_timestamp=ts,
                )

            # Reserve it
            charger["status"] = "OCCUPIED"
            charger["ev_id"]  = ev_id

        print(f"  {charger_id} reserved for {ev_id} [OK]")

        ts = self.clock.send()
        return evcharge_pb2.ReservationResponse(
            success=True,
            message=f"{charger_id} successfully reserved for {ev_id}.",
            lamport_timestamp=ts,
        )

    # -- RPC 3: UpdateChargerStatus --------------------------------------------
    def UpdateChargerStatus(self, request, context):
        self.clock.receive(request.lamport_timestamp)

        charger_id = request.charger_id
        new_status = request.new_status.upper()
        print(f"\n[Charging Service] -- UpdateChargerStatus --")
        print(f"  {charger_id} -> {new_status}")

        with _lock:
            charger = CHARGERS.get(charger_id)
            if charger is None:
                ts = self.clock.send()
                return evcharge_pb2.StatusUpdateResponse(
                    success=False,
                    message=f"Charger {charger_id} not found.",
                    lamport_timestamp=ts,
                )

            if new_status == "AVAILABLE":
                charger["status"] = "AVAILABLE"
                charger["ev_id"]  = None
            else:
                charger["status"] = "OCCUPIED"

        print(f"  Status updated [OK]")

        ts = self.clock.send()
        return evcharge_pb2.StatusUpdateResponse(
            success=True,
            message=f"{charger_id} status set to {new_status}.",
            lamport_timestamp=ts,
        )

# ------------------------------------------------------------------------------
# Server entry-point
# ------------------------------------------------------------------------------
def serve(port: int = 50051):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    evcharge_pb2_grpc.add_ChargingServiceServicer_to_server(
        ChargingServiceServicer(), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[Charging Service] gRPC server started on port {port}")
    print(f"[Charging Service] Waiting for requests ...\n")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\n[Charging Service] Shutting down.")
        server.stop(grace=1)


if __name__ == "__main__":
    serve()
