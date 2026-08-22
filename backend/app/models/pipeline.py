from pydantic import BaseModel, Field


class PipelineStage(BaseModel):
    """Represents an individual stage within a data pipeline."""

    name: str = Field(..., min_length=1, description="Name of the pipeline stage")
    status: str = Field(..., min_length=1, description="Status of the pipeline stage")


class PipelineStatus(BaseModel):
    """Represents the overall health and status of a data pipeline."""

    pipeline: str = Field(..., min_length=1, description="Name or identifier of the pipeline")
    status: str = Field(..., min_length=1, description="Overall status of the pipeline")
    stages: list[PipelineStage] = Field(
        ...,
        min_length=1,
        description="List of constituent stages in the pipeline",
    )
