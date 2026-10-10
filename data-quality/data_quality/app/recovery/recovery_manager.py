# Manage the recovery state of the circuit breaker
from app.circuit_breaker.states import CLOSED, HALF_OPEN

def recover_circuit(recovery_successful):
    # Move to closed state when recovery is successful
    if recovery_successful:
        return CLOSED

    # Keep testing recovery when it fails
    return HALF_OPEN