# Control the circuit breaker state

import time

from app.circuit_breaker.states import CLOSED, OPEN, HALF_OPEN
from app.circuit_breaker.thresholds import ERROR_RATE_THRESHOLD

# Time to wait before trying recovery
RECOVERY_TIMEOUT_SECONDS = 30

import json
import os
from pathlib import Path

SHARED_STATUS_FILE = Path(__file__).resolve().parent / "shared_circuit_status.json"


def save_shared_status(state, error_rate=0.0, is_alive=True):
    """Persist circuit-breaker state to shared file for backend observability."""
    try:
        data = {
            "state": state,
            "error_rate": float(error_rate),
            "last_updated": time.time(),
            "pid": os.getpid(),
            "is_alive": is_alive,
        }
        SHARED_STATUS_FILE.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        pass


class CircuitBreaker:
    def __init__(self):
        # Circuit starts in closed state
        self.state = CLOSED

        # Store when the circuit was opened
        self.opened_at = None

    def check_circuit(self, error_rate):
        # Open the circuit when error rate is above 2%
        if self.state == CLOSED:
            if error_rate > ERROR_RATE_THRESHOLD:
                self.state = OPEN
                self.opened_at = time.monotonic()

        # Automatically start recovery after the timeout
        elif self.state == OPEN:
            if (
                self.opened_at is not None
                and time.monotonic() - self.opened_at
                >= RECOVERY_TIMEOUT_SECONDS
            ):
                self.state = HALF_OPEN

        save_shared_status(self.state, error_rate, is_alive=True)
        # HALF_OPEN state is handled by recovery_result()
        return self.state

    def start_recovery(self):
        # Allow external/manual recovery testing
        if self.state == OPEN:
            self.state = HALF_OPEN

        save_shared_status(self.state, 0.0, is_alive=True)
        return self.state

    def recovery_result(self, recovery_successful):
        # Close the circuit after successful recovery
        if self.state == HALF_OPEN:
            if recovery_successful:
                self.state = CLOSED
                self.opened_at = None
            else:
                self.state = OPEN
                self.opened_at = time.monotonic()

        save_shared_status(self.state, 0.0, is_alive=True)
        return self.state

# Keep one circuit breaker instance so state is maintained
circuit = CircuitBreaker()

def check_circuit(error_rate):
    return circuit.check_circuit(error_rate)

def start_recovery():
    return circuit.start_recovery()
