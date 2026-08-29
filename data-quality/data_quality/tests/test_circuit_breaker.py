# Test circuit breaker states
from app.circuit_breaker.states import CLOSED, OPEN, HALF_OPEN

def test_circuit_breaker_states():
    # Check that all circuit breaker states are available
    assert CLOSED == "CLOSED"
    assert OPEN == "OPEN"
    assert HALF_OPEN == "HALF_OPEN"