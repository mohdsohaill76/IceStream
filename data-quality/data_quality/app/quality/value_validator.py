# Validate transaction values

def validate_values(record):
    errors = []

    # Check quantity
    if "quantity" in record and record["quantity"] is not None:
        if not isinstance(record["quantity"], bool):
            if not isinstance(record["quantity"], (int, float)):
                errors.append("quantity must be a number")
            elif record["quantity"] <= 0:
                errors.append("quantity must be greater than 0")
        else:
            errors.append("quantity must be a number")

    # Check amount
    if "amount" in record and record["amount"] is not None:
        if isinstance(record["amount"], bool):
            errors.append("amount must be a number")
        elif not isinstance(record["amount"], (int, float)):
            errors.append("amount must be a number")
        elif record["amount"] <= 0:
            errors.append("amount must be greater than 0")

    # Check tax
    if "tax" in record and record["tax"] is not None:
        if isinstance(record["tax"], bool):
            errors.append("tax must be a number")
        elif not isinstance(record["tax"], (int, float)):
            errors.append("tax must be a number")
        elif record["tax"] < 0:
            errors.append("tax cannot be negative")

    return len(errors) == 0, errors