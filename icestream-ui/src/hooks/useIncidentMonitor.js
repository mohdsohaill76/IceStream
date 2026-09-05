import { useEffect, useRef, useState } from "react";

function createIncident({
  component,
  severity,
  title,
  description,
  metric,
}) {
  return {
    id: `${component}-${Date.now()}`,
    component,
    severity,
    title,
    description,
    metric,
    status: "open",
    detectedAt: new Date().toISOString(),
    resolvedAt: null,
  };
}

export function useIncidentMonitor(liveState) {
  const [incidents, setIncidents] = useState([]);

  const activeIncidents = useRef(new Map());

  useEffect(() => {
    if (!liveState || !liveState.conditions) {
      return;
    }

    const conditions = liveState.conditions;
    const newConditions = [];

    /* FLINK LATENCY */
    if (conditions.latencyWarning) {
      newConditions.push({
        key: "flink-latency",
        component: "Flink",
        severity: "warning",
        title: "High processing latency",
        description:
          "Flink processing latency has exceeded the configured threshold.",
        metric: `${liveState.metrics.latency} ms`,
      });
    }

    /* DATA QUALITY */
    if (conditions.qualityWarning) {
      newConditions.push({
        key: "data-quality",
        component: "Data Quality",
        severity: "warning",
        title: "Data quality degradation",
        description:
          "Data quality score has fallen below the expected threshold.",
        metric: `${liveState.metrics.quality}%`,
      });
    }

    /* ERROR RATE */
    if (conditions.errorCritical) {
      newConditions.push({
        key: "error-rate",
        component: "Pipeline",
        severity: "critical",
        title: "Critical error rate",
        description:
          "Pipeline error rate has exceeded the circuit breaker threshold.",
        metric: `${liveState.metrics.errorRate}%`,
      });
    } else if (conditions.errorWarning) {
      newConditions.push({
        key: "error-rate",
        component: "Pipeline",
        severity: "warning",
        title: "Elevated error rate",
        description:
          "Pipeline error rate is above the recommended level.",
        metric: `${liveState.metrics.errorRate}%`,
      });
    }

    /* DLQ / QUARANTINE */
    if (conditions.quarantineCritical) {
      newConditions.push({
        key: "dlq-quarantine",
        component: "DLQ",
        severity: "critical",
        title: "DLQ quarantine threshold exceeded",
        description:
          "The number of quarantined records has reached a critical level.",
        metric: `${liveState.metrics.quarantined} records`,
      });
    } else if (conditions.quarantineWarning) {
      newConditions.push({
        key: "dlq-quarantine",
        component: "DLQ",
        severity: "warning",
        title: "Records moved to quarantine",
        description:
          "The number of quarantined records is above the normal level.",
        metric: `${liveState.metrics.quarantined} records`,
      });
    }

    /* DETECT NEW INCIDENTS */
    newConditions.forEach((condition) => {
      if (!activeIncidents.current.has(condition.key)) {
        const incident = createIncident(condition);

        activeIncidents.current.set(
          condition.key,
          incident
        );

        setIncidents((previous) => [
          incident,
          ...previous,
        ]);
      }
    });

    /* RESOLVE RECOVERED INCIDENTS */
    const currentKeys = new Set(
      newConditions.map((condition) => condition.key)
    );

    activeIncidents.current.forEach(
      (incident, key) => {
        if (!currentKeys.has(key)) {
          const resolvedIncident = {
            ...incident,
            status: "resolved",
            resolvedAt: new Date().toISOString(),
          };

          activeIncidents.current.delete(key);

          setIncidents((previous) =>
            previous.map((item) =>
              item.id === incident.id
                ? resolvedIncident
                : item
            )
          );
        }
      }
    );
  }, [liveState]);

  const openIncidents = incidents.filter(
    (incident) => incident.status === "open"
  );

  const criticalIncidents = openIncidents.filter(
    (incident) => incident.severity === "critical"
  );

  return {
    incidents,
    openIncidents,
    criticalIncidents,
    openCount: openIncidents.length,
    criticalCount: criticalIncidents.length,
  };
}