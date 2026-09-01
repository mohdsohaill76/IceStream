# Create the current circuit breaker status

def create_status(state, error_rate):
    # Return monitoring information
    return {
        "state": state,
        "error_rate": error_rate
    }