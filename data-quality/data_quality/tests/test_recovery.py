# Test circuit breaker recovery
from app.recovery.recovery_manager import recover_circuit
from app.circuit_breaker.states import CLOSED, HALF_OPEN

def test_successful_recovery():
    # Successful recovery should close the circuit
    result = recover_circuit(True)

    assert result == CLOSED

def test_failed_recovery():
    # Failed recovery should keep the circuit half-open
    result = recover_circuit(False)

    assert result == HALF_OPEN