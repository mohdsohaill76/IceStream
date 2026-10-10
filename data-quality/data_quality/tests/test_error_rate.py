# Test error rate calculation
from app.quality.error_rate import calculate_error_rate

def test_error_rate():
    # 2 bad records out of 100 records = 2%
    result = calculate_error_rate(2, 100)

    assert result == 2.0

def test_zero_records():
    # No records should return zero error rate
    result = calculate_error_rate(0, 0)

    assert result == 0.0

def test_high_error_rate():
    # 5 bad records out of 100 records = 5%
    result = calculate_error_rate(5, 100)

    assert result == 5.0