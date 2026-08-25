def send_to_dlq(record, errors):
    dlq_record = {
        "record": record,
        "errors": errors
    }

    return dlq_record