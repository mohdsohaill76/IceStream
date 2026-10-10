import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models.pipeline import PipelineStatus

client = TestClient(app)


def test_get_pipeline_status_success():
    """Test GET /api/v1/pipeline/status returns 200, correct pipeline info, and 4 stages."""
    response = client.get("/api/v1/pipeline/status")
    assert response.status_code == 200

    data = response.json()
    assert data["pipeline"] == "transaction_stream"
    assert data["status"] == "healthy"
    assert len(data["stages"]) == 4


def test_pipeline_status_stages_contain_expected_stages():
    """Verify the returned stages contain kafka, flink, data_quality, and iceberg."""
    response = client.get("/api/v1/pipeline/status")
    assert response.status_code == 200

    data = response.json()
    stage_names = [stage["name"] for stage in data["stages"]]
    expected_stages = ["kafka", "flink", "data_quality", "iceberg"]

    assert stage_names == expected_stages
    for expected_stage in expected_stages:
        assert expected_stage in stage_names


def test_pipeline_status_all_stages_healthy():
    """Verify every returned stage currently has status 'healthy'."""
    response = client.get("/api/v1/pipeline/status")
    assert response.status_code == 200

    data = response.json()
    assert len(data["stages"]) > 0
    for stage in data["stages"]:
        assert stage["status"] == "healthy"


def test_pipeline_status_contract_structure():
    """Verify the response structure matches the PipelineStatus contract."""
    response = client.get("/api/v1/pipeline/status")
    assert response.status_code == 200

    data = response.json()
    # Verify required keys in response
    assert "pipeline" in data
    assert "status" in data
    assert "stages" in data
    assert isinstance(data["pipeline"], str)
    assert isinstance(data["status"], str)
    assert isinstance(data["stages"], list)

    for stage in data["stages"]:
        assert "name" in stage
        assert "status" in stage
        assert isinstance(stage["name"], str)
        assert isinstance(stage["status"], str)

    # Validate against PipelineStatus Pydantic model contract
    validated = PipelineStatus(**data)
    assert validated.pipeline == data["pipeline"]
    assert validated.status == data["status"]
    assert len(validated.stages) == len(data["stages"])
