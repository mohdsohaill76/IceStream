import pytest
from pydantic import ValidationError

from app.models.pipeline import PipelineStage, PipelineStatus


def test_valid_pipeline_stage():
    """Test that a valid PipelineStage is successfully created."""
    stage = PipelineStage(name="kafka", status="healthy")
    assert stage.name == "kafka"
    assert stage.status == "healthy"


def test_pipeline_stage_empty_name():
    """Test that a PipelineStage with an empty name raises ValidationError."""
    with pytest.raises(ValidationError):
        PipelineStage(name="", status="healthy")


def test_pipeline_stage_empty_status():
    """Test that a PipelineStage with an empty status raises ValidationError."""
    with pytest.raises(ValidationError):
        PipelineStage(name="kafka", status="")


def test_valid_pipeline_status_multiple_stages():
    """Test that a valid PipelineStatus with multiple realistic stages is accepted."""
    stages = [
        PipelineStage(name="kafka", status="healthy"),
        PipelineStage(name="flink", status="running"),
        PipelineStage(name="data_quality", status="passing"),
        PipelineStage(name="iceberg", status="committed"),
    ]
    pipeline_status = PipelineStatus(
        pipeline="transaction_stream",
        status="healthy",
        stages=stages,
    )
    assert pipeline_status.pipeline == "transaction_stream"
    assert pipeline_status.status == "healthy"
    assert len(pipeline_status.stages) == 4
    assert pipeline_status.stages[0].name == "kafka"
    assert pipeline_status.stages[1].name == "flink"
    assert pipeline_status.stages[2].name == "data_quality"
    assert pipeline_status.stages[3].name == "iceberg"


def test_pipeline_status_empty_pipeline_name():
    """Test that a PipelineStatus with an empty pipeline name raises ValidationError."""
    stages = [PipelineStage(name="kafka", status="healthy")]
    with pytest.raises(ValidationError):
        PipelineStatus(pipeline="", status="healthy", stages=stages)


def test_pipeline_status_empty_overall_status():
    """Test that a PipelineStatus with an empty overall status raises ValidationError."""
    stages = [PipelineStage(name="kafka", status="healthy")]
    with pytest.raises(ValidationError):
        PipelineStatus(pipeline="transaction_stream", status="", stages=stages)


def test_pipeline_status_empty_stages_list():
    """Test that a PipelineStatus with an empty stages list raises ValidationError."""
    with pytest.raises(ValidationError):
        PipelineStatus(pipeline="transaction_stream", status="healthy", stages=[])
