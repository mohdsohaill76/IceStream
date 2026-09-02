# Test circuit breaker states
from app.circuit_breaker.states import CLOSED, OPEN, HALF_OPEN
from app.circuit_breaker.thresholds import ERROR_RATE_THRESHOLD
from app.circuit_breaker.circuit_breaker import CircuitBreaker

def test_circuit_breaker_states():
    # Check that all circuit breaker states are available
    assert CLOSED == "CLOSED"
    assert OPEN == "OPEN"
    assert HALF_OPEN == "HALF_OPEN"

def test_error_rate_threshold():
    # Circuit breaker threshold is 2%
    assert ERROR_RATE_THRESHOLD == 2.0

def test_circuit_opens():
    # Error rate above 2% should open the circuit
    circuit = CircuitBreaker()

    result = circuit.check_circuit(5.0)

    assert result == OPEN

def test_circuit_stays_closed():
    # Error rate at or below 2% should keep the circuit closed
    circuit = CircuitBreaker()

    result = circuit.check_circuit(2.0)

    assert result == CLOSED

def test_open_to_half_open():
    # Open circuit should move to half-open during recovery
    circuit = CircuitBreaker()

    circuit.check_circuit(5.0)
    result = circuit.start_recovery()

    assert result == HALF_OPEN

def test_half_open_to_closed():
    # Successful recovery should close the circuit
    circuit = CircuitBreaker()

    circuit.check_circuit(5.0)
    circuit.start_recovery()
    result = circuit.recovery_result(True)

    assert result == CLOSED

def test_half_open_to_open():
    # Failed recovery should open the circuit again
    circuit = CircuitBreaker()

    circuit.check_circuit(5.0)
    circuit.start_recovery()
    result = circuit.recovery_result(False)

    assert result == OPEN