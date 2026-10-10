from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, status

from app.models.lakehouse import LakehouseMetrics, SnapshotMetadata
from app.services.lakehouse_service import (
    LakehouseServiceError,
    get_lakehouse_metrics,
    get_lakehouse_snapshots,
)

router = APIRouter()


@router.get("/metrics", response_model=LakehouseMetrics)
def read_lakehouse_metrics() -> LakehouseMetrics:
    """Retrieve row counts and active snapshot IDs for Iceberg clean and DLQ tables."""
    try:
        return get_lakehouse_metrics()
    except LakehouseServiceError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e),
        )


@router.get("/snapshots", response_model=List[SnapshotMetadata])
def read_lakehouse_snapshots(
    table: Optional[str] = Query(
        None,
        description="Filter snapshots by table name: 'clean_transactions' or 'dlq_transactions'",
    ),
) -> List[SnapshotMetadata]:
    """Retrieve historical commit snapshot metadata for Iceberg tables."""
    try:
        return get_lakehouse_snapshots(table=table)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except LakehouseServiceError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e),
        )
