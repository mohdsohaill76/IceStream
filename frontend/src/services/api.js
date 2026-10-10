/**
 * IceStream API Client Service
 *
 * Connects the React frontend to the FastAPI backend service.
 * Configured via Vite environment variables with local fallbacks.
 */

export const API_BASE_URL =
  import.meta.env.VITE_API_URL || "http://localhost:8000";

export const WS_BASE_URL =
  import.meta.env.VITE_WS_URL || "ws://localhost:8000";

/**
 * Helper to handle fetch responses and extract meaningful error messages.
 */
async function handleResponse(response, context) {
  if (!response.ok) {
    let errorDetail = response.statusText;
    try {
      const errorJson = await response.json();
      if (errorJson && errorJson.detail) {
        errorDetail = errorJson.detail;
      }
    } catch {
      // Body not JSON
    }
    throw new Error(`${context}: [${response.status}] ${errorDetail}`);
  }
  return response.json();
}

/**
 * Fetch current real-time pipeline status from FastAPI backend.
 * Endpoint: GET /api/v1/pipeline/status
 */
export async function fetchPipelineStatus() {
  const response = await fetch(`${API_BASE_URL}/api/v1/pipeline/status`, {
    headers: {
      Accept: "application/json",
    },
  });
  return handleResponse(response, "Failed to fetch pipeline status");
}

/**
 * Fetch real Lakehouse row counts and snapshot IDs from Iceberg.
 * Endpoint: GET /api/v1/lakehouse/metrics
 */
export async function fetchLakehouseMetrics() {
  const response = await fetch(`${API_BASE_URL}/api/v1/lakehouse/metrics`, {
    headers: {
      Accept: "application/json",
    },
  });
  return handleResponse(response, "Failed to fetch Lakehouse metrics");
}

/**
 * Fetch real Iceberg snapshot commit history.
 * Endpoint: GET /api/v1/lakehouse/snapshots?table=...
 *
 * @param {string|null} table - Optional table filter ('clean_transactions' or 'dlq_transactions')
 */
export async function fetchLakehouseSnapshots(table = null) {
  const url = new URL(`${API_BASE_URL}/api/v1/lakehouse/snapshots`);
  if (table) {
    url.searchParams.set("table", table);
  }

  const response = await fetch(url.toString(), {
    headers: {
      Accept: "application/json",
    },
  });
  return handleResponse(response, "Failed to fetch Lakehouse snapshots");
}

/**
 * Fetch operational incidents from FastAPI backend.
 * Endpoint: GET /api/v1/incidents
 */
export async function fetchIncidents() {
  const response = await fetch(`${API_BASE_URL}/api/v1/incidents`, {
    headers: {
      Accept: "application/json",
    },
  });
  return handleResponse(response, "Failed to fetch incidents");
}

/**
 * Fetch a specific operational incident by its identifier.
 * Endpoint: GET /api/v1/incidents/{incidentId}
 *
 * @param {string} incidentId
 */
export async function fetchIncidentById(incidentId) {
  const response = await fetch(`${API_BASE_URL}/api/v1/incidents/${encodeURIComponent(incidentId)}`, {
    headers: {
      Accept: "application/json",
    },
  });
  return handleResponse(response, `Failed to fetch incident ${incidentId}`);
}

/**
 * Check backend health.
 * Endpoint: GET /health
 */
export async function fetchHealth() {
  const response = await fetch(`${API_BASE_URL}/health`, {
    headers: {
      Accept: "application/json",
    },
  });
  return handleResponse(response, "Failed to check backend health");
}
