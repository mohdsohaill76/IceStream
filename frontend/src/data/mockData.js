// src/data/mockData.js

export const pipelineData = {
  kafka: {
    type: "kafka",
    status: "healthy",
    metricLabel: "Throughput",
    metric: "12.4K records/s",
  },

  flink: {
    type: "flink",
    status: "healthy",
    metricLabel: "Latency",
    metric: "48 ms",
  },

  quality: {
    type: "quality",
    status: "healthy",
    metricLabel: "Quality Score",
    metric: "98.7%",
  },

  iceberg: {
    type: "iceberg",
    status: "healthy",
    metricLabel: "Writes",
    metric: "2.1K/s",
  },

  dlq: {
    type: "dlq",
    status: "healthy",
    metricLabel: "Quarantined",
    metric: "0 records",
  },
};

export const dashboardMetrics = [
  {
    title: "Records / Second",
    value: "12,450",
    change: "+8.4%",
  },
  {
    title: "Data Quality",
    value: "98.7%",
    change: "+1.2%",
  },
  {
    title: "Error Rate",
    value: "0.8%",
    change: "-0.4%",
  },
  {
    title: "Records Processed",
    value: "1.24M",
    change: "+12.6%",
  },
];