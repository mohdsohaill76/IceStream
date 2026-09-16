# Process transactions through validation, DLQ, circuit breaker, and monitoring

import sys
import time

# Use UTF-8 output on Windows/Linux to safely print corrupted payloads
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(
        encoding="utf-8",
        errors="replace"
    )

if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(
        encoding="utf-8",
        errors="replace"
    )

from app.quality.validator import validate_record
from app.quality.error_rate import calculate_error_rate
from app.dlq.dlq_producer import send_to_dlq
from app.dlq.dlq_producer import flush_dlq
from app.circuit_breaker.circuit_breaker import check_circuit
from app.monitoring.status_producer import create_status
from app.circuit_breaker.thresholds import ERROR_RATE_THRESHOLD
from app.circuit_breaker.states import OPEN


def process_records(records):
    # Handle invalid input before processing the batch
    if not isinstance(records, list):
        return {
            "total_records": 0,
            "bad_records": 0,
            "error_rate": 0.0,
            "circuit_breaker_triggered": False,
            "dlq_records": [],
            "valid_records": [],
            "blocked_records": [],
            "monitoring_status": create_status(
                "CLOSED",
                0.0
            )
        }

    total_records = len(records)
    bad_records = 0
    dlq_records = []
    valid_records = []
    blocked_records = []

    # Validate every record in the batch
    for record in records:

        # Handle invalid record input safely
        if not isinstance(record, dict):
            bad_records += 1

            errors = ["record must be a transaction object"]

            dlq_record = send_to_dlq(
                record,
                errors
            )

            dlq_records.append(dlq_record)
            continue

        # Handle Kafka-level payload errors
        if record.get("__consumer_error__") is True:
            bad_records += 1

            # Preserve the original raw payload
            raw_record = {
                "raw_message": record.get("raw_message")
            }

            # Preserve the original error
            errors = [
                record.get(
                    "error",
                    "Invalid Kafka consumer payload"
                )
            ]

            # Avoid sending the same record to DLQ twice
            if record.get("__dlq_sent__") is not True:
                dlq_record = send_to_dlq(
                    raw_record,
                    errors
                )
            else:
                dlq_record = {
                    "record": raw_record,
                    "errors": errors
                }

            dlq_records.append(dlq_record)

            # Do not run normal transaction validation
            continue

        # Normal transaction validation
        is_valid, errors = validate_record(record)

        if not is_valid:
            bad_records += 1

            # Only genuinely invalid records go to DLQ
            dlq_record = send_to_dlq(
                record,
                errors
            )

            dlq_records.append(dlq_record)

        else:
            # Keep valid records temporarily
            valid_records.append(record)

    # Calculate error rate for the complete batch
    error_rate = calculate_error_rate(
        bad_records,
        total_records
    )

    # Update circuit breaker state
    circuit_state = check_circuit(error_rate)

    # Circuit OPEN blocks valid records from downstream processing.
    # Blocked records are not considered invalid and are not sent to DLQ.
    if circuit_state == OPEN:
        blocked_records = valid_records
        valid_records = []

    # Create monitoring status
    monitoring_status = create_status(
        circuit_state,
        error_rate
    )

    # Flush all pending DLQ messages once per batch
    flush_dlq()

    return {
        "total_records": total_records,
        "bad_records": bad_records,
        "error_rate": error_rate,
        "circuit_breaker_triggered": (
            error_rate > ERROR_RATE_THRESHOLD
        ),
        "dlq_records": dlq_records,
        "valid_records": valid_records,
        "blocked_records": blocked_records,
        "monitoring_status": monitoring_status
    }


if __name__ == "__main__":
    from app.kafka_consumer import consume_records

    print("IceStream Data Quality Service Started")
    print("Waiting for Kafka messages...")

    BATCH_SIZE = 100
    MAX_WAIT_SECONDS = 5

    batch = []
    batch_start_time = None

    try:
        for record in consume_records(include_errors=True):

            # Start the batch timer when first record arrives
            if not batch:
                batch_start_time = time.monotonic()

            batch.append(record)

            # Check batch size
            batch_full = len(batch) >= BATCH_SIZE

            # Check maximum wait time
            batch_timed_out = (
                batch_start_time is not None
                and time.monotonic() - batch_start_time >= MAX_WAIT_SECONDS
            )

            # Process when batch is full OR timeout is reached
            if batch_full or batch_timed_out:
                result = process_records(batch)
                print(result)

                batch = []
                batch_start_time = None

    except KeyboardInterrupt:
        # Process remaining records before stopping
        if batch:
            result = process_records(batch)
            print(result)

        flush_dlq()

        print("\nData Quality Service stopped.")