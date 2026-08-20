from fastapi import FastAPI

from app.config import settings

app = FastAPI(
    title=settings.app_name,
    description="IceStream Backend & Integration Layer",
    version="0.1.0",
)


@app.get("/health")
def health_check():
    """Health check endpoint to verify backend service status."""
    return {"status": "healthy"}
