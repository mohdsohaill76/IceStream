from datetime import datetime
from pydantic import BaseModel, Field


class Incident(BaseModel):
    """Represents an operational incident detected in the IceStream streaming pipeline."""

    incident_id: str = Field(..., min_length=1, description="Unique incident identifier")
    stage: str = Field(
        ...,
        min_length=1,
        description="Pipeline stage where the incident occurred (e.g. kafka, flink, data_quality, iceberg)",
    )
    severity: str = Field(
        ...,
        min_length=1,
        description="Incident severity level (e.g. low, medium, high, critical)",
    )
    message: str = Field(
        ...,
        min_length=1,
        description="Human-readable description of the incident",
    )
    status: str = Field(
        ...,
        min_length=1,
        description="Incident status (e.g. open, acknowledged, resolved)",
    )
    timestamp: datetime = Field(..., description="Timestamp when the incident occurred")
