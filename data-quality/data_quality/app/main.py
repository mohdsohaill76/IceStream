# Process transactions through validation, DLQ, circuit breaker, and monitoring
from app.quality.validator import validate_record
from app.quality.error_rate import calculate_error_rate
from app.dlq.dlq_producer import send_to_dlq
from app.circuit_breaker.circuit_breaker import check_circuit
from app.monitoring.status_producer import create_status
from app.config import ERROR_RATE_THRESHOLD

def process_records(records):
    total_records = len(records)
    bad_records = 0
    dlq_records = []

    for record in records:
        is_valid, errors = validate_record(record)

        if not is_valid:
            bad_records += 1

            # Send invalid record to DLQ
            dlq_record = send_to_dlq(record, errors)
            dlq_records.append(dlq_record)

    # Calculate error rate
    error_rate = calculate_error_rate(bad_records, total_records)

    # Check circuit breaker
    circuit_state = check_circuit(error_rate)

    # Create monitoring status
    monitoring_status = create_status(
        circuit_state,
        error_rate
    )

    return {
        "total_records": total_records,
        "bad_records": bad_records,
        "error_rate": error_rate,
        "circuit_breaker_triggered": (
            error_rate > ERROR_RATE_THRESHOLD
        ),
        "dlq_records": dlq_records,
        "monitoring_status": monitoring_status
    }

if __name__ == "__main__":
    sample_records = [
        {
            "transaction_id": "T001",
            "customer_id": "C001",
            "product_id": "P001",
            "quantity": 2,
            "amount": 100,
            "tax": 10
        },
        {
            "transaction_id": "T002",
            "customer_id": "C002",
            "product_id": "P002",
            "quantity": 0,
            "amount": 200,
            "tax": 20
        }
    ]

    result = process_records(sample_records)

    print(result)