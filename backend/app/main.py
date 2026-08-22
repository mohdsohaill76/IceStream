from fastapi import FastAPI

from app.config import settings
from app.routes.pipeline import router as pipeline_router

app = FastAPI(
    title=settings.app_name,
    description="IceStream Backend & Integration Layer",
    version="0.1.0",
)

app.include_router(pipeline_router, prefix="/api/v1/pipeline", tags=["pipeline"])


@app.get("/health")
def health_check():
    """Health check endpoint to verify backend service status."""
    return {"status": "healthy"}

