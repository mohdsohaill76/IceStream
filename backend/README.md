# Backend Module

## Responsibility
The `backend` module serves as the central API and integration layer for the IceStream observability platform. It aggregates pipeline metrics, manages system state, coordinates incident communication, and exposes REST and WebSocket endpoints for the frontend dashboard.

## Module Owner
**Person 1: Backend + Integration Lead**

## Planned Implementation
- FastAPI application setup (`app/`)
- REST API endpoints for pipeline health, table metrics, and historical incidents
- WebSocket service for real-time state and alert streaming to the frontend
- Integration layer connecting Iceberg metadata, Kafka consumer offsets, and Data Quality metrics
- Comprehensive module tests (`tests/`)
