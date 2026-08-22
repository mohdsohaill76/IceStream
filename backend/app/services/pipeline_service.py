from app.models.pipeline import PipelineStage, PipelineStatus


def get_pipeline_status() -> PipelineStatus:
    """Retrieve the current mock operational status of the transaction stream pipeline."""
    stages = [
        PipelineStage(name="kafka", status="healthy"),
        PipelineStage(name="flink", status="healthy"),
        PipelineStage(name="data_quality", status="healthy"),
        PipelineStage(name="iceberg", status="healthy"),
    ]
    return PipelineStatus(
        pipeline="transaction_stream",
        status="healthy",
        stages=stages,
    )
