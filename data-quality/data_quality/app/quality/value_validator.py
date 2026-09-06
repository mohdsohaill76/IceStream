# Validate transaction values

def validate_values(record):
    errors = []

    # Check amount
    if "amount" in record and record["amount"] is not None:
        if isinstance(record["amount"], bool):
            errors.append("amount must be a number")
        elif not isinstance(record["amount"], (int, float)):
            errors.append("amount must be a number")
        elif record["amount"] <= 0:
            errors.append("amount must be greater than 0")

    return len(errors) == 0, errors