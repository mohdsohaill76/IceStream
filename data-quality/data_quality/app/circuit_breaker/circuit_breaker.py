# Control the circuit breaker based on error rate
from app.circuit_breaker.states import CLOSED, OPEN, HALF_OPEN
from app.circuit_breaker.thresholds import ERROR_RATE_THRESHOLD


def check_circuit(error_rate):
    # Open the circuit when error rate is above the threshold
    if error_rate > ERROR_RATE_THRESHOLD:
        return OPEN

    return CLOSED


def start_recovery():
    # Move the circuit to half-open state for recovery testing
    return HALF_OPEN