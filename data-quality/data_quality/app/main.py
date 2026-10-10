# Process transactions through validation, DLQ,
# circuit breaker, duplicate detection, and monitoring

import logging
import os
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

logger = logging.getLogger("data_quality.service")

from app.downstream.valid_producer import send_valid_record
from app.downstream.valid_producer import flush_valid_producer
from app.quality.validator import validate_record
from app.quality.error_rate import calculate_error_rate
from app.dlq.dlq_producer import send_to_dlq
from app.dlq.dlq_producer import flush_dlq
from app.circuit_breaker.circuit_breaker import (
    check_circuit,
    circuit,
    save_shared_status
)
from app.circuit_breaker.states import CLOSED, OPEN, HALF_OPEN
from app.monitoring.status_producer import create_status
from app.circuit_breaker.thresholds import ERROR_RATE_THRESHOLD
from app.kafka_consumer import (
    create_consumer,
    poll_records,
    get_offsets_to_commit,
    commit_offsets,
    pause_consumer,
    resume_consumer,
)
from kafka import TopicPartition


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
                CLOSED,
                0.0
            )
        }

    total_records = len(records)
    bad_records = 0
    dlq_records = []
    valid_records = []
    blocked_records = []

    # Track transaction IDs already accepted in this batch.
    # This prevents duplicate transactions from being forwarded.
    seen_transaction_ids = set()

    # Validate every record in the batch
    for record in records:

        # Handle invalid record input safely
        if not isinstance(record, dict):
            bad_records += 1

            errors = [
                "record must be a transaction object"
            ]

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
            continue

        # Duplicate transaction detection
        transaction_id = record.get("transaction_id")

        if transaction_id in seen_transaction_ids:
            bad_records += 1

            duplicate_error = [
                "duplicate transaction_id"
            ]

            dlq_record = send_to_dlq(
                record,
                duplicate_error
            )

            dlq_records.append(dlq_record)
            continue

        # First occurrence of this transaction
        seen_transaction_ids.add(transaction_id)

        # Keep valid records temporarily
        valid_records.append(record)

    # Calculate error rate for the complete batch
    error_rate = calculate_error_rate(
        bad_records,
        total_records
    )

    # Store the circuit state before checking the current batch.
    previous_state = circuit.state

    # Update circuit breaker state
    circuit_state = check_circuit(error_rate)

    # Recovery handling:
    #
    # OPEN -> HALF_OPEN happens automatically after recovery timeout.
    #
    # When HALF_OPEN receives a healthy batch, recovery succeeds
    # and the circuit becomes CLOSED.
    #
    # When HALF_OPEN receives another bad batch, recovery fails
    # and the circuit becomes OPEN again.
    if previous_state == HALF_OPEN or circuit_state == HALF_OPEN:
        recovery_successful = (
            error_rate <= ERROR_RATE_THRESHOLD
        )

        circuit_state = circuit.recovery_result(
            recovery_successful
        )

    # Circuit OPEN blocks valid records from downstream processing.
    # Blocked records are not considered invalid and are not sent to DLQ.
    if circuit_state == OPEN:
        blocked_records = valid_records
        valid_records = []

    # Forward valid records to the downstream Kafka topic (failures propagate)
    for record in valid_records:
        send_valid_record(record)

    # Flush all pending valid records once per batch and verify delivery
    flush_valid_producer()

    # Create monitoring status
    monitoring_status = create_status(
        circuit_state,
        error_rate
    )

    # Flush all pending DLQ messages once per batch and verify delivery
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


BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
MAX_WAIT_SECONDS = float(os.getenv("MAX_WAIT_SECONDS", "5.0"))
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "500"))


def run_service(
    consumer=None,
    batch_size=None,
    max_wait_seconds=None,
    poll_timeout_ms=None,
    clock=time.monotonic,
    stop_event=None,
    max_iterations=None,
    on_batch_processed=None,
    max_retries=3,
    initial_backoff_seconds=0.1,
    backoff_multiplier=2.0,
    max_backoff_seconds=2.0,
    sleep_fn=time.sleep,
):
    """
    Bounded polling loop for Data Quality batch processing.

    Reliability & Delivery Guarantees:
    - At-least-once processing: Kafka consumer offsets are committed strictly AFTER
      valid and DLQ outputs are confirmed delivered by the Kafka broker.
    - Zero data-loss during OPEN circuit breaker: consumer fetching is paused safely
      while polling/heartbeats continue, retaining records in Kafka until recovery.
    - Transient producer and network failures trigger bounded exponential backoff
      retries, preserving batches and uncommitted offsets.
    - Empty polls never create empty batches or busy-spin.
    """
    effective_batch_size = batch_size if batch_size is not None else BATCH_SIZE
    effective_max_wait = max_wait_seconds if max_wait_seconds is not None else MAX_WAIT_SECONDS
    effective_poll_timeout = poll_timeout_ms if poll_timeout_ms is not None else POLL_TIMEOUT_MS

    save_shared_status(CLOSED, 0.0, is_alive=True)

    owns_consumer = False
    if consumer is None:
        consumer = create_consumer()
        owns_consumer = True

    batch = []
    pending_blocked_records = []
    batch_start_time = None
    iterations = 0

    try:
        while True:
            if stop_event is not None and stop_event.is_set():
                break

            if max_iterations is not None and iterations >= max_iterations:
                break

            iterations += 1

            # Check if circuit is OPEN and eligible for recovery check
            if circuit.state == OPEN:
                check_circuit(0.0)
                if circuit.state in (HALF_OPEN, CLOSED):
                    resume_consumer(consumer)

            # If circuit returned to CLOSED, forward any pending blocked records
            if circuit.state == CLOSED and pending_blocked_records:
                try:
                    for r in pending_blocked_records:
                        send_valid_record(r)
                    flush_valid_producer()
                    offsets = get_offsets_to_commit(pending_blocked_records)
                    commit_offsets(consumer, offsets)
                    logger.info(
                        "Successfully delivered %d blocked records after circuit recovery.",
                        len(pending_blocked_records)
                    )
                    pending_blocked_records = []
                except Exception as e:
                    logger.error(
                        "Failed to flush pending blocked records upon recovery: %s",
                        e
                    )

            # 1. Bounded poll (maintains heartbeats even when paused)
            try:
                records = poll_records(
                    consumer,
                    timeout_ms=effective_poll_timeout,
                    include_errors=True
                )
            except Exception as e:
                logger.warning("Kafka poll failed: %s", e)
                records = []

            # 2. Accumulate records if received
            if records:
                if not batch:
                    batch_start_time = clock()
                batch.extend(records)

            now = clock()

            # 3. Determine if batch should flush
            should_flush = False
            if batch:
                batch_full = len(batch) >= effective_batch_size
                batch_timed_out = (
                    batch_start_time is not None
                    and (now - batch_start_time) >= effective_max_wait
                )
                if batch_full or batch_timed_out:
                    should_flush = True

            # 4. Flush batch if condition met
            if should_flush:
                records_to_process = (
                    batch[:effective_batch_size]
                    if len(batch) > effective_batch_size
                    else batch
                )
                remaining = (
                    batch[effective_batch_size:]
                    if len(batch) > effective_batch_size
                    else []
                )

                attempt = 0
                success = False
                result = None
                last_error = None

                while attempt <= max_retries:
                    try:
                        result = process_records(records_to_process)

                        # Check if valid records were held due to OPEN circuit
                        if result.get("blocked_records"):
                            pending_blocked_records.extend(result["blocked_records"])
                            pause_consumer(consumer)

                        # Commit Kafka offsets only after confirmed downstream delivery
                        # If records were blocked, their offsets are committed upon recovery
                        all_blocked = (
                            circuit.state == OPEN
                            and result.get("blocked_records")
                            and not result.get("valid_records")
                            and not result.get("dlq_records")
                        )
                        if not all_blocked:
                            offsets = get_offsets_to_commit(records_to_process)
                            commit_offsets(consumer, offsets)

                        save_shared_status(
                            circuit.state,
                            result.get("error_rate", 0.0),
                            is_alive=True
                        )
                        if on_batch_processed:
                            on_batch_processed(result)

                        success = True
                        break

                    except Exception as e:
                        last_error = e
                        attempt += 1
                        logger.warning(
                            "Batch processing failed on attempt %d/%d: %s",
                            attempt,
                            max_retries,
                            e
                        )
                        if attempt <= max_retries and sleep_fn:
                            backoff = min(
                                initial_backoff_seconds * (backoff_multiplier ** (attempt - 1)),
                                max_backoff_seconds
                            )
                            sleep_fn(backoff)

                if success:
                    batch = remaining
                    batch_start_time = clock() if batch else None
                else:
                    logger.error(
                        "Batch processing failed after %d retries: %s. Preserving batch for replay.",
                        max_retries,
                        last_error
                    )
                    pause_consumer(consumer)
                    save_shared_status(
                        circuit.state,
                        0.0,
                        is_alive=True
                    )
                    batch = records_to_process + remaining
                    batch_start_time = clock()

    finally:
        shutdown_had_errors = False

        # Shutdown cleanup: flush any remaining batch
        if batch:
            try:
                result = process_records(batch)
                if result.get("blocked_records"):
                    pending_blocked_records.extend(result["blocked_records"])

                all_blocked = (
                    circuit.state == OPEN
                    and result.get("blocked_records")
                    and not result.get("valid_records")
                    and not result.get("dlq_records")
                )
                if not all_blocked:
                    offsets = get_offsets_to_commit(batch)
                    commit_offsets(consumer, offsets)

                save_shared_status(
                    circuit.state,
                    result.get("error_rate", 0.0),
                    is_alive=True
                )
                if on_batch_processed:
                    on_batch_processed(result)
                batch = []
            except Exception as e:
                logger.error(
                    "Shutdown batch processing failed: %s. Records remain uncommitted in Kafka.",
                    e
                )
                shutdown_had_errors = True

        try:
            flush_dlq()
        except Exception as e:
            logger.error("Shutdown DLQ flush failed: %s", e)
            shutdown_had_errors = True

        try:
            flush_valid_producer()
        except Exception as e:
            logger.error("Shutdown valid producer flush failed: %s", e)
            shutdown_had_errors = True

        # Rewind consumer if blocked records remain uncommitted
        if pending_blocked_records:
            logger.warning(
                "Service shutdown with %d uncommitted blocked records.",
                len(pending_blocked_records)
            )
            if hasattr(consumer, "seek"):
                for r in pending_blocked_records:
                    t = getattr(r, "_topic", None)
                    p = getattr(r, "_partition", None)
                    o = getattr(r, "_offset", None)
                    if t is not None and p is not None and o is not None:
                        try:
                            consumer.seek(TopicPartition(t, int(p)), int(o))
                        except Exception:
                            pass

        if shutdown_had_errors:
            save_shared_status("STOPPED_WITH_ERRORS", 0.0, is_alive=False)
        else:
            save_shared_status("STOPPED", 0.0, is_alive=False)

        if owns_consumer and hasattr(consumer, "close"):
            try:
                consumer.close()
            except Exception:
                pass


if __name__ == "__main__":
    import os
    target_topic = os.getenv("KAFKA_TOPIC", "processed-transactions")
    print("IceStream Data Quality Service Started")
    print(f"Subscribed to topic: {target_topic}")
    print("Waiting for Kafka messages...")

    try:
        run_service()
    except KeyboardInterrupt:
        print("\nData Quality Service stopped.")
