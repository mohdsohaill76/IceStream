# Control the circuit breaker state
from app.circuit_breaker.states import CLOSED, OPEN, HALF_OPEN
from app.circuit_breaker.thresholds import ERROR_RATE_THRESHOLD

class CircuitBreaker:
    def __init__(self):
        # Circuit starts in closed state
        self.state = CLOSED

    def check_circuit(self, error_rate):
        # Open the circuit when error rate is above 2%
        if error_rate > ERROR_RATE_THRESHOLD:
            self.state = OPEN
        else:
            self.state = CLOSED

        return self.state

    def start_recovery(self):
        # Move to half-open state for recovery testing
        if self.state == OPEN:
            self.state = HALF_OPEN

        return self.state

    def recovery_result(self, recovery_successful):
        # Close the circuit after successful recovery
        if recovery_successful:
            self.state = CLOSED
        else:
            self.state = OPEN

        return self.state

# Keep these functions for existing code compatibility

def check_circuit(error_rate):
    circuit = CircuitBreaker()
    return circuit.check_circuit(error_rate)

def start_recovery():
    circuit = CircuitBreaker()
    circuit.state = OPEN
    return circuit.start_recovery()