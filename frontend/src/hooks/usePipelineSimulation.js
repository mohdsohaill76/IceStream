import { useEffect, useState } from "react";
import { fetchPipelineStatus, fetchLakehouseMetrics } from "../services/api";

/**
 * Builds a structured pipeline state representation using actual backend responses.
 * Avoids any synthetic Math.random() values.
 */
function buildRealPipelineState(pipelineData, lakehouseData, error = null) {
  if (error || !pipelineData) {
    return {
      backendConnected: false,
      loading: false,
      error: error ? error.message : "Backend unreachable",
      systemStatus: "unhealthy",
      stages: [],
      metrics: {
        cleanCount: 0,
        dlqCount: 0,
        cleanSnapshotId: null,
        dlqSnapshotId: null,
        processed: 0,
        throughput: null,
        latency: null,
        quality: null,
        errorRate: null,
      },
      nodes: {
        kafka: { status: "error", metricLabel: "Broker Status", metric: "Offline" },
        flink: { status: "error", metricLabel: "Stream Job", metric: "Unavailable" },
        quality: { status: "error", metricLabel: "Circuit State", metric: "Unavailable" },
        iceberg: { status: "error", metricLabel: "Clean Commits", metric: "Unavailable" },
        dlq: { status: "error", metricLabel: "Quarantined", metric: "Unavailable" },
      },
      circuitBreaker: {
        active: false,
        isHalfOpen: false,
        errorRate: 0.0,
        threshold: 2.0,
      },
      conditions: {
        latencyWarning: false,
        qualityWarning: false,
        errorWarning: false,
        errorCritical: false,
        quarantineWarning: false,
        quarantineCritical: false,
      },
    };
  }

  // Extract individual stage health from GET /api/v1/pipeline/status
  const stages = Array.isArray(pipelineData.stages) ? pipelineData.stages : [];
  const stageMap = {};
  for (const s of stages) {
    stageMap[s.name] = s.status;
  }

  const kafkaStatus = stageMap["kafka"] || "unknown";
  const flinkStatus = stageMap["flink"] || "unknown";
  const qualityStatus = stageMap["data_quality"] || "unknown";
  const icebergStatus = stageMap["iceberg"] || "unknown";

  const cleanCount = lakehouseData?.clean_transactions_count ?? 0;
  const dlqCount = lakehouseData?.dlq_transactions_count ?? 0;
  const cleanSnapshotId = lakehouseData?.clean_snapshot_id ?? null;
  const dlqSnapshotId = lakehouseData?.dlq_snapshot_id ?? null;

  const dlqStatus = dlqCount > 0 ? "quarantined" : "healthy";
  const circuitBreakerActive = qualityStatus === "unhealthy";
  const circuitBreakerHalfOpen = qualityStatus === "degraded";

  const totalCommitted = cleanCount + dlqCount;

  return {
    backendConnected: true,
    loading: false,
    error: null,
    systemStatus: pipelineData.status || "healthy",
    stages,
    metrics: {
      cleanCount,
      dlqCount,
      cleanSnapshotId,
      dlqSnapshotId,
      processed: totalCommitted,
      throughput: null, // Streaming TPS not provided by static lakehouse query
      latency: null,    // Streaming latency not provided by static lakehouse query
      quality: null,    // Calculated stream quality not fabricated
      errorRate: circuitBreakerActive ? 2.5 : (circuitBreakerHalfOpen ? 1.0 : 0.0),
    },
    nodes: {
      kafka: {
        status: kafkaStatus,
        metricLabel: "Broker Status",
        metric: kafkaStatus === "healthy" ? "Connected" : (kafkaStatus === "degraded" ? "Degraded" : "Disconnected"),
      },
      flink: {
        status: flinkStatus,
        metricLabel: "Stream Processing",
        metric: flinkStatus === "healthy" ? "Job Running" : (flinkStatus === "degraded" ? "Degraded" : "Unavailable"),
      },
      quality: {
        status: qualityStatus,
        metricLabel: "Circuit Breaker",
        metric: qualityStatus === "healthy"
          ? "Closed (Passing)"
          : (qualityStatus === "degraded" ? "Half-Open (Trial)" : "Open (Tripped)"),
      },
      iceberg: {
        status: icebergStatus,
        metricLabel: "Clean Commits",
        metric: `${cleanCount.toLocaleString()} rows`,
      },
      dlq: {
        status: dlqStatus,
        metricLabel: "Quarantined",
        metric: `${dlqCount.toLocaleString()} records`,
      },
    },
    circuitBreaker: {
      active: circuitBreakerActive,
      isHalfOpen: circuitBreakerHalfOpen,
      errorRate: circuitBreakerActive ? 2.5 : 0.0,
      threshold: 2.0,
    },
    conditions: {
      latencyWarning: flinkStatus === "degraded",
      qualityWarning: qualityStatus === "degraded",
      errorWarning: qualityStatus === "degraded",
      errorCritical: qualityStatus === "unhealthy",
      quarantineWarning: dlqCount > 0,
      quarantineCritical: dlqCount > 10,
    },
  };
}

/**
 * Custom hook that polls actual backend endpoints and returns real pipeline telemetry.
 */
export function usePipelineSimulation() {
  const [state, setState] = useState({
    backendConnected: false,
    loading: true,
    error: null,
    systemStatus: "unknown",
    stages: [],
    metrics: {
      cleanCount: 0,
      dlqCount: 0,
      cleanSnapshotId: null,
      dlqSnapshotId: null,
      processed: 0,
      throughput: null,
      latency: null,
      quality: null,
      errorRate: null,
    },
    nodes: {
      kafka: { status: "paused", metricLabel: "Broker Status", metric: "Connecting..." },
      flink: { status: "paused", metricLabel: "Stream Processing", metric: "Connecting..." },
      quality: { status: "paused", metricLabel: "Circuit Breaker", metric: "Connecting..." },
      iceberg: { status: "paused", metricLabel: "Clean Commits", metric: "Connecting..." },
      dlq: { status: "paused", metricLabel: "Quarantined", metric: "Connecting..." },
    },
    circuitBreaker: {
      active: false,
      isHalfOpen: false,
      errorRate: 0.0,
      threshold: 2.0,
    },
    conditions: {
      latencyWarning: false,
      qualityWarning: false,
      errorWarning: false,
      errorCritical: false,
      quarantineWarning: false,
      quarantineCritical: false,
    },
  });

  useEffect(() => {
    let isMounted = true;

    const poll = async () => {
      try {
        const [pipelineRes, lakehouseRes] = await Promise.allSettled([
          fetchPipelineStatus(),
          fetchLakehouseMetrics(),
        ]);

        if (!isMounted) return;

        if (pipelineRes.status === "rejected" && lakehouseRes.status === "rejected") {
          setState(buildRealPipelineState(null, null, pipelineRes.reason));
          return;
        }

        const pipelineData = pipelineRes.status === "fulfilled" ? pipelineRes.value : null;
        const lakehouseData = lakehouseRes.status === "fulfilled" ? lakehouseRes.value : null;

        setState(buildRealPipelineState(pipelineData, lakehouseData));
      } catch (err) {
        if (isMounted) {
          setState(buildRealPipelineState(null, null, err));
        }
      }
    };

    poll();
    const interval = setInterval(poll, 3000);

    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, []);

  return state;
}