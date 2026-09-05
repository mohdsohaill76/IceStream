import { useEffect, useState } from "react";

function generatePipelineState() {
  const throughput = Math.floor(11500 + Math.random() * 2000);

  const latency = Math.floor(40 + Math.random() * 25);

  const quality = Number(
    (97 + Math.random() * 2.5).toFixed(1)
  );

  const errorRate = Number(
    (0.3 + Math.random() * 2.2).toFixed(1)
  );

  const writes = Math.floor(1800 + Math.random() * 700);

  const quarantined = Math.floor(Math.random() * 6);

  const conditions = {
    latencyWarning: latency > 58,

    qualityWarning: quality < 98,

    errorWarning: errorRate >= 1.5,
    errorCritical: errorRate >= 2.0,

    quarantineWarning: quarantined >= 2,
    quarantineCritical: quarantined >= 4,
  };

  const circuitBreakerActive =
    conditions.errorCritical ||
    conditions.qualityWarning ||
    conditions.quarantineCritical;
const hasWarning =
  conditions.latencyWarning ||
  conditions.qualityWarning ||
  conditions.errorWarning ||
  conditions.quarantineWarning;

const systemStatus = circuitBreakerActive
  ? "critical"
  : hasWarning
    ? "warning"
    : "healthy";

  return {
    metrics: {
      throughput,
      latency,
      quality,
      errorRate,
      quarantined,
      processed: Math.floor(
        1200000 + Math.random() * 50000
      ),
    },

    nodes: {
      kafka: {
        status: "healthy",
        metricLabel: "Throughput",
        metric: `${(throughput / 1000).toFixed(1)}K records/s`,
      },

      flink: {
        status: latency > 58 ? "warning" : "healthy",
        metricLabel: "Latency",
        metric: `${latency} ms`,
      },

      quality: {
        status: quality < 98 ? "warning" : "healthy",
        metricLabel: "Quality Score",
        metric: `${quality}%`,
      },

      iceberg: {
        status: "healthy",
        metricLabel: "Writes",
        metric: `${(writes / 1000).toFixed(1)}K/s`,
      },

      dlq: {
        status:
          quarantined >= 4
            ? "quarantined"
            : "healthy",
        metricLabel: "Quarantined",
        metric:
          quarantined === 0
            ? "0 records"
            : `${quarantined} records`,
      },
    },

    circuitBreaker: {
      active: circuitBreakerActive,
      errorRate,
      threshold: 2.0,
    },

    conditions,

    systemStatus,
  };
}

export function usePipelineSimulation() {
  const [state, setState] = useState(() =>
    generatePipelineState()
  );

  useEffect(() => {
    const interval = setInterval(() => {
      setState(generatePipelineState());
    }, 3000);

    return () => clearInterval(interval);
  }, []);

  return state;
}