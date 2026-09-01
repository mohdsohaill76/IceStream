# Test monitoring status
from app.monitoring.status_producer import create_status

def test_create_status():
    # Sample circuit breaker status
    result = create_status("CLOSED", 1.5)

    assert result["state"] == "CLOSED"
    assert result["error_rate"] == 1.5