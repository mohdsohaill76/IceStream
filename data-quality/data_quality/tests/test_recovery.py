# Test circuit breaker recovery
from app.recovery.recovery_manager import recover_circuit
from app.circuit_breaker.states import CLOSED, HALF_OPEN
# Test record replay
from app.recovery.replay_handler import replay_record

def test_successful_recovery():
    # Successful recovery should close the circuit
    result = recover_circuit(True)

    assert result == CLOSED

def test_failed_recovery():
    # Failed recovery should keep the circuit half-open
    result = recover_circuit(False)

    assert result == HALF_OPEN

def test_replay_record():
    # Sample record from the DLQ
    record = {
        "transaction_id": "TXN002",
        "customer_id": "CUST001",
        "product_id": "PROD001",
        "quantity": 2,
        "amount": 500,
        "tax": 50
    }

    result = replay_record(record)

    assert result == record