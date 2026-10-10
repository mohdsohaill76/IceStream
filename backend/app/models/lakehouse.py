from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class LakehouseMetrics(BaseModel):
    """Counts and current snapshot IDs for Iceberg clean and DLQ tables."""

    clean_transactions_count: int = Field(
        ...,
        ge=0,
        description="Total row count committed in lakehouse.clean_transactions",
    )
    clean_snapshot_id: Optional[int] = Field(
        default=None,
        description="Current snapshot ID for lakehouse.clean_transactions (null if empty)",
    )
    dlq_transactions_count: int = Field(
        ...,
        ge=0,
        description="Total row count committed in lakehouse.dlq_transactions",
    )
    dlq_snapshot_id: Optional[int] = Field(
        default=None,
        description="Current snapshot ID for lakehouse.dlq_transactions (null if empty)",
    )


class SnapshotMetadata(BaseModel):
    """Metadata details for a single Apache Iceberg table commit snapshot."""

    snapshot_id: int = Field(
        ...,
        description="Unique 64-bit integer identifier for the Iceberg snapshot",
    )
    table_name: str = Field(
        ...,
        description="Full table name identifier, e.g. 'lakehouse.clean_transactions'",
    )
    committed_at: datetime = Field(
        ...,
        description="UTC timestamp of the snapshot commit in ISO-8601 format",
    )
    timestamp_ms: int = Field(
        ...,
        description="Commit timestamp in milliseconds since Unix epoch",
    )
    operation: Optional[str] = Field(
        default=None,
        description="Operation that produced the snapshot (e.g. 'append', 'replace')",
    )
    summary: Dict[str, Any] = Field(
        default_factory=dict,
        description="Summary metadata key-value pairs recorded by PyIceberg",
    )
    parent_snapshot_id: Optional[int] = Field(
        default=None,
        description="Parent snapshot ID in the history chain, or null if root snapshot",
    )
