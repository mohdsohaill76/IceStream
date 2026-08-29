# Test circuit breaker states
from app.circuit_breaker.states import CLOSED, OPEN, HALF_OPEN

# Test circuit breaker error threshold
from app.circuit_breaker.thresholds import ERROR_RATE_THRESHOLD

def test_circuit_breaker_states():
    # Check that all circuit breaker states are available
    assert CLOSED == "CLOSED"
    assert OPEN == "OPEN"
    assert HALF_OPEN == "HALF_OPEN"

def test_error_rate_threshold():
    # Circuit breaker opens when error rate is above 2%
    assert ERROR_RATE_THRESHOLD == 2.0