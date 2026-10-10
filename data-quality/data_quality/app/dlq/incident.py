# Create an incident for a failed transaction
def create_incident(record, errors):
    # Store the transaction ID and validation errors
    incident = {
        "transaction_id": record.get("transaction_id"),
        "errors": errors
    }

    return incident