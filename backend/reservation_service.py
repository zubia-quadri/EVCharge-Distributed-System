# -*- coding: utf-8 -*-
"""
reservation_service.py - Flask Web Server + gRPC Client (Reservation Service)

This is the gRPC CLIENT and the web-facing layer in the EVCharge architecture.

Responsibilities:
  - Serve the frontend (frontend/index.html + static files)
  - Expose REST endpoints consumed by the UI
    - GET  /slots            -> list all chargers and their status
    - POST /reserve          -> check availability, then reserve via gRPC
    - POST /update_status    -> free a charger via gRPC (admin helper)
  - Communicate with the Charging Service over gRPC
  - Maintain its own Lamport Logical Clock

Start AFTER charging_service.py is running:
    python backend/reservation_service.py
"""

import sys
import os
import json

from flask import Flask, request, jsonify, send_from_directory

# -- Path setup ----------------------------------------------------------------
ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, ROOT)

import grpc
import evcharge_pb2
import evcharge_pb2_grpc

from lamport_clock import LamportClock

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
CHARGING_SERVICE_ADDR = "localhost:50051"
FRONTEND_DIR = os.path.join(ROOT, "frontend")

app   = Flask(__name__, static_folder=FRONTEND_DIR)
clock = LamportClock(owner="Reservation Service")

# ------------------------------------------------------------------------------
# gRPC channel (lazy singleton)
# ------------------------------------------------------------------------------
_channel = None
_stub    = None

def get_stub():
    """Return a cached gRPC stub, creating it on first call."""
    global _channel, _stub
    if _stub is None:
        _channel = grpc.insecure_channel(CHARGING_SERVICE_ADDR)
        _stub    = evcharge_pb2_grpc.ChargingServiceStub(_channel)
    return _stub


# ------------------------------------------------------------------------------
# Frontend route
# ------------------------------------------------------------------------------
@app.route("/")
def index():
    return send_from_directory(FRONTEND_DIR, "index.html")


# ------------------------------------------------------------------------------
# REST: GET /slots
# ------------------------------------------------------------------------------
@app.route("/slots", methods=["GET"])
def get_slots():
    """
    Return the current status of all chargers.
    This calls CheckChargerAvailability for each charger via gRPC.
    """
    stub     = get_stub()
    chargers = ["Charger_01", "Charger_02", "Charger_03"]
    results  = []

    print("\n[Reservation Service] -- GET /slots --")

    for cid in chargers:
        ts = clock.send()
        print(f"[Reservation Service] Checking availability of {cid} ...")

        try:
            response = stub.CheckChargerAvailability(
                evcharge_pb2.ChargerRequest(
                    charger_id=cid,
                    lamport_timestamp=ts,
                )
            )
            clock.receive(response.lamport_timestamp)

            results.append({
                "id":        cid,
                "available": response.available,
                "status":    "AVAILABLE" if response.available else "OCCUPIED",
            })
            print(f"[Reservation Service] {cid}: {'AVAILABLE' if response.available else 'OCCUPIED'}")

        except grpc.RpcError as e:
            print(f"[Reservation Service] gRPC error for {cid}: {e.details()}")
            results.append({"id": cid, "available": False, "status": "ERROR"})

    return jsonify(results)


# ------------------------------------------------------------------------------
# REST: POST /reserve
# ------------------------------------------------------------------------------
@app.route("/reserve", methods=["POST"])
def reserve():
    """
    Reserve a charger for an EV.
    Flow:
      1. CheckChargerAvailability  (gRPC call 1)
      2. If available -> ReserveCharger  (gRPC call 2)
    """
    data       = request.get_json(force=True)
    charger_id = data.get("charger_id", "").strip()
    ev_id      = data.get("ev_id", "").strip()

    print(f"\n[Reservation Service] -- POST /reserve --")
    print(f"[Reservation Service] EV: {ev_id} -> Charger: {charger_id}")

    if not charger_id or not ev_id:
        return jsonify({"success": False, "message": "EV ID and Charger ID are required."}), 400

    stub = get_stub()

    # -- Step 1: Check availability --------------------------------------------
    print(f"\n[Reservation Service] Step 1 - Checking charger availability ...")
    ts1 = clock.send()

    try:
        avail_resp = stub.CheckChargerAvailability(
            evcharge_pb2.ChargerRequest(
                charger_id=charger_id,
                lamport_timestamp=ts1,
            )
        )
        clock.receive(avail_resp.lamport_timestamp)
    except grpc.RpcError as e:
        msg = f"Charging Service unavailable: {e.details()}"
        print(f"[Reservation Service] ERROR: {msg}")
        return jsonify({"success": False, "message": msg}), 503

    if not avail_resp.available:
        print(f"[Reservation Service] {charger_id} is OCCUPIED - reservation aborted.")
        return jsonify({
            "success": False,
            "message": f"{charger_id} is currently OCCUPIED. Please select another charger.",
            "activity": [
                f"{ev_id} requested {charger_id}",
                "Checking charger availability",
                "Charger is OCCUPIED",
                "Reservation failed",
            ],
        })

    print(f"[Reservation Service] Availability confirmed: {charger_id} is AVAILABLE")

    # -- Step 2: Reserve -------------------------------------------------------
    print(f"\n[Reservation Service] Step 2 - Sending reservation request ...")
    ts2 = clock.send()

    try:
        res_resp = stub.ReserveCharger(
            evcharge_pb2.ReservationRequest(
                charger_id=charger_id,
                ev_id=ev_id,
                lamport_timestamp=ts2,
            )
        )
        clock.receive(res_resp.lamport_timestamp)
    except grpc.RpcError as e:
        msg = f"Charging Service error: {e.details()}"
        print(f"[Reservation Service] ERROR: {msg}")
        return jsonify({"success": False, "message": msg}), 503

    if res_resp.success:
        print(f"[Reservation Service] Reservation successful [OK]")
        return jsonify({
            "success": True,
            "message": res_resp.message,
            "activity": [
                f"{ev_id} requested {charger_id}",
                "Checking charger availability",
                "Charger available",
                "Sending reservation request",
                "Charger reserved",
                "Reservation successful [OK]",
            ],
        })
    else:
        print(f"[Reservation Service] Reservation failed: {res_resp.message}")
        return jsonify({
            "success": False,
            "message": res_resp.message,
            "activity": [
                f"{ev_id} requested {charger_id}",
                "Checking charger availability",
                "Reservation failed",
            ],
        })


# ------------------------------------------------------------------------------
# REST: POST /update_status  (helper - frees a charger)
# ------------------------------------------------------------------------------
@app.route("/update_status", methods=["POST"])
def update_status():
    data       = request.get_json(force=True)
    charger_id = data.get("charger_id", "").strip()
    new_status = data.get("status", "AVAILABLE").strip()

    print(f"\n[Reservation Service] -- POST /update_status --")
    print(f"[Reservation Service] {charger_id} -> {new_status}")

    stub = get_stub()
    ts   = clock.send()

    try:
        resp = stub.UpdateChargerStatus(
            evcharge_pb2.StatusUpdateRequest(
                charger_id=charger_id,
                new_status=new_status,
                lamport_timestamp=ts,
            )
        )
        clock.receive(resp.lamport_timestamp)
        return jsonify({"success": resp.success, "message": resp.message})
    except grpc.RpcError as e:
        return jsonify({"success": False, "message": e.details()}), 503


# ------------------------------------------------------------------------------
# Entry-point
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    print("[Reservation Service] Starting Flask server on http://localhost:5000")
    print(f"[Reservation Service] Connecting to Charging Service at {CHARGING_SERVICE_ADDR}\n")
    app.run(host="0.0.0.0", port=5000, debug=False)
