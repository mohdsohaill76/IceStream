# Send invalid records to the Dead Letter Queue
def send_to_dlq(record, errors):

# Create a DLQ record with the original data and errors
    dlq_record = {
        "record": record,
        "errors": errors
    }

    return dlq_record