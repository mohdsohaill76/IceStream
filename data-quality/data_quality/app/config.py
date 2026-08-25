# Maximum allowed error rate before the circuit breaker opens
ERROR_RATE_THRESHOLD = 2.0

# Fields required in every transaction
REQUIRED_FIELDS = [
    "transaction_id",
    "customer_id",
    "product_id",
    "quantity",
    "amount",
    "tax"
]