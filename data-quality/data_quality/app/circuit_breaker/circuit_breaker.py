# Control the circuit breaker state

import time

from app.circuit_breaker.states import CLOSED, OPEN, HALF_OPEN
from app.circuit_breaker.thresholds import ERROR_RATE_THRESHOLD

# Time to wait before trying recovery
RECOVERY_TIMEOUT_SECONDS = 30

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

        # HALF_OPEN state is handled by recovery_result()
        return self.state

    def start_recovery(self):
        # Allow external/manual recovery testing
        if self.state == OPEN:
            self.state = HALF_OPEN

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

        return self.state

# Keep one circuit breaker instance so state is maintained
circuit = CircuitBreaker()

def check_circuit(error_rate):
    return circuit.check_circuit(error_rate)

def start_recovery():
    return circuit.start_recovery()