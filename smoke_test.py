# -*- coding: utf-8 -*-
"""
combined_test.py - Runs Charging Service + smoke test in one process
to capture both sides of the Lamport Clock logs for verification.
"""
import sys, os, threading, time
sys.path.insert(0, ".")

# Start the gRPC server in a background thread
from backend.charging_service import serve as start_server
t = threading.Thread(target=start_server, daemon=True)
t.start()
time.sleep(1.5)   # give the server a moment to bind

# Run the gRPC calls
import grpc
import evcharge_pb2, evcharge_pb2_grpc
from backend.lamport_clock import LamportClock

clock = LamportClock("TestClient")
stub  = evcharge_pb2_grpc.ChargingServiceStub(grpc.insecure_channel("localhost:50051"))

print("\n" + "=" * 60)
print("  EVCharge -- Combined gRPC + Lamport Clock Verification")
print("=" * 60)

# Check Charger_01
print("\n[TestClient] Sending CheckChargerAvailability for Charger_01")
ts = clock.send()
r = stub.CheckChargerAvailability(
    evcharge_pb2.ChargerRequest(charger_id="Charger_01", lamport_timestamp=ts))
clock.receive(r.lamport_timestamp)
print(f"[TestClient] Result: {'AVAILABLE' if r.available else 'OCCUPIED'}")

# Reserve Charger_01
print("\n[TestClient] Sending ReserveCharger -- EV102 -> Charger_01")
ts = clock.send()
r2 = stub.ReserveCharger(
    evcharge_pb2.ReservationRequest(charger_id="Charger_01", ev_id="EV102", lamport_timestamp=ts))
clock.receive(r2.lamport_timestamp)
print(f"[TestClient] Result: {'SUCCESS' if r2.success else 'FAILED'} -- {r2.message}")

# Try Charger_02 (pre-set as OCCUPIED)
print("\n[TestClient] Sending CheckChargerAvailability for Charger_02")
ts = clock.send()
r3 = stub.CheckChargerAvailability(
    evcharge_pb2.ChargerRequest(charger_id="Charger_02", lamport_timestamp=ts))
clock.receive(r3.lamport_timestamp)
print(f"[TestClient] Result: {'AVAILABLE' if r3.available else 'OCCUPIED'}")

print("\n" + "=" * 60)
print(f"  DONE  |  Final TestClient Lamport Clock: {clock.value}")
print("=" * 60)
