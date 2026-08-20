from datetime import datetime
from pydantic import BaseModel, Field


class Transaction(BaseModel):
    """Shared transaction data model representing a financial transaction event."""

    transaction_id: str = Field(..., min_length=1, description="Unique transaction identifier")
    customer_id: str = Field(..., min_length=1, description="Customer identifier")
    amount: float = Field(..., gt=0, description="Transaction monetary amount")
    currency: str = Field(..., min_length=1, description="Currency code (e.g. USD, EUR)")
    timestamp: datetime = Field(..., description="Transaction timestamp")
    merchant: str = Field(..., min_length=1, description="Merchant name")
    status: str = Field(..., min_length=1, description="Transaction status (e.g. pending, completed, failed)")
