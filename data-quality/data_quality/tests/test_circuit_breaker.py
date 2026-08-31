# Test circuit breaker states
from app.circuit_breaker.states import CLOSED, OPEN, HALF_OPEN

# Test circuit breaker error threshold
from app.circuit_breaker.thresholds import ERROR_RATE_THRESHOLD

# Test circuit breaker logic
from app.circuit_breaker.circuit_breaker import check_circuit, start_recovery

def test_circuit_breaker_states():
    # Check that all circuit breaker states are available
    assert CLOSED == "CLOSED"
    assert OPEN == "OPEN"
    assert HALF_OPEN == "HALF_OPEN"

def test_error_rate_threshold():
    # Circuit breaker opens when error rate is above 2%
    assert ERROR_RATE_THRESHOLD == 2.0

def test_circuit_opens():
    # Error rate above 2% should open the circuit
    result = check_circuit(5.0)

    assert result == OPEN

def test_circuit_stays_closed():
    # Error rate at or below 2% should keep the circuit closed
    result = check_circuit(2.0)

    assert result == CLOSED

def test_recovery_state():
    # Recovery testing should move the circuit to half-open
    result = start_recovery()

    assert result == HALF_OPEN