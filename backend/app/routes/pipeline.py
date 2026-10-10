from fastapi import APIRouter

from app.models.pipeline import PipelineStatus
from app.services.pipeline_service import get_pipeline_status

router = APIRouter()


@router.get("/status", response_model=PipelineStatus)
def read_pipeline_status() -> PipelineStatus:
    """Retrieve current operational status of the data pipeline."""
    return get_pipeline_status()
