from fastapi import FastAPI

from app.config import settings
from app.routes.incidents import router as incidents_router
from app.routes.lakehouse import router as lakehouse_router
from app.routes.pipeline import router as pipeline_router

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title=settings.app_name,
    description="IceStream Backend & Integration Layer",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(pipeline_router, prefix="/api/v1/pipeline", tags=["pipeline"])
app.include_router(incidents_router, prefix="/api/v1/incidents", tags=["incidents"])
app.include_router(lakehouse_router, prefix="/api/v1/lakehouse", tags=["lakehouse"])


@app.get("/health")
def health_check():
    """Health check endpoint to verify backend service status."""
    return {"status": "healthy"}


