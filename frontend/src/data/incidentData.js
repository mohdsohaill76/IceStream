// src/data/incidentData.js

export const initialIncidents = [
  {
    id: 1,
    severity: "warning",
    title: "Flink processing latency increased",
    component: "Flink",
    message: "Processing latency crossed the warning threshold.",
    value: "61 ms",
    timestamp: new Date().toISOString(),
    status: "open",
  },

  {
    id: 2,
    severity: "info",
    title: "Pipeline configuration updated",
    component: "Pipeline",
    message: "Pipeline configuration was successfully synchronized.",
    value: "Updated",
    timestamp: new Date(Date.now() - 60000).toISOString(),
    status: "resolved",
  },
];