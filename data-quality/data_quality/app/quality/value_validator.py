# Validate transaction values

def validate_values(record):
    errors = []

    # Check quantity
    if "quantity" in record and record["quantity"] is not None:
        if record["quantity"] <= 0:
            errors.append("quantity must be greater than 0")

    # Check amount
    if "amount" in record and record["amount"] is not None:
        if record["amount"] <= 0:
            errors.append("amount must be greater than 0")

    # Check tax
    if "tax" in record and record["tax"] is not None:
        if record["tax"] < 0:
            errors.append("tax cannot be negative")

    return len(errors) == 0, errors