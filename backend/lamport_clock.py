# -*- coding: utf-8 -*-
"""
lamport_clock.py - Thread-safe Lamport Logical Clock

A Lamport Clock is used to establish a partial ordering of events
in a distributed system without relying on physical clocks.

Rules:
  - Local event  : clock += 1
  - Send event   : clock += 1  (attach this value to the message)
  - Receive event: clock = max(local_clock, received_timestamp) + 1
"""

import threading


class LamportClock:
    """A simple, thread-safe Lamport Logical Clock."""

    def __init__(self, owner: str = "Service"):
        self._clock = 0
        self._lock = threading.Lock()
        self.owner = owner

    # -- Internal --------------------------------------------------------------
    def _increment(self) -> int:
        self._clock += 1
        return self._clock

    # -- Public API ------------------------------------------------------------
    def tick(self) -> int:
        """Local event: increment and return the new timestamp."""
        with self._lock:
            return self._increment()

    def send(self) -> int:
        """
        Send event: increment before sending so the receiver always
        sees a timestamp strictly greater than the sender's previous state.
        Returns the timestamp to attach to the outgoing message.
        """
        with self._lock:
            ts = self._increment()
            print(f"  [{self.owner}] Lamport Clock (send): {ts}")
            return ts

    def receive(self, received_timestamp: int) -> int:
        """
        Receive event: update clock using the standard Lamport rule.
          clock = max(local_clock, received_timestamp) + 1
        Returns the new local timestamp.
        """
        with self._lock:
            self._clock = max(self._clock, received_timestamp) + 1
            print(f"  [{self.owner}] Received Timestamp: {received_timestamp} -> Lamport Clock: {self._clock}")
            return self._clock

    @property
    def value(self) -> int:
        """Read the current clock value without incrementing."""
        with self._lock:
            return self._clock

    def __repr__(self) -> str:
        return f"LamportClock(owner={self.owner!r}, clock={self._clock})"
