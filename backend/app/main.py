from fastapi import FastAPI

from app.config import settings
from app.routes.incidents import router as incidents_router
from app.routes.lakehouse import router as lakehouse_router
from app.routes.pipeline import router as pipeline_router

app = FastAPI(
    title=settings.app_name,
    description="IceStream Backend & Integration Layer",
    version="0.1.0",
)

import os
from fastapi.middleware.cors import CORSMiddleware

# Configure CORS: support explicit production domains with credentials, or wildcard for local dev
_cors_origins_env = os.getenv("CORS_ORIGINS", "*")
if _cors_origins_env == "*":
    _cors_origins = ["*"]
    _cors_credentials = False
else:
    _cors_origins = [origin.strip() for origin in _cors_origins_env.split(",") if origin.strip()]
    _cors_credentials = True

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=_cors_credentials,
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


