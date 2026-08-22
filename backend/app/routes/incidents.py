from fastapi import APIRouter, HTTPException, status

from app.models.incident import Incident
from app.services.incident_service import get_incident_by_id, get_incidents

router = APIRouter()


@router.get("", response_model=list[Incident])
def read_incidents() -> list[Incident]:
    """Retrieve all operational pipeline incidents."""
    return get_incidents()


@router.get("/{incident_id}", response_model=Incident)
def read_incident(incident_id: str) -> Incident:
    """Retrieve a single operational incident by its identifier."""
    incident = get_incident_by_id(incident_id)
    if incident is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Incident with ID '{incident_id}' not found",
        )
    return incident
