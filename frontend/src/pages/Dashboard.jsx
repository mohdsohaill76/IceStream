import {
  Activity,
  Archive,
  Database,
  Gauge,
  ShieldCheck,
  WifiOff,
} from "lucide-react";

import { usePipelineContext } from "../context/usePipelineContext";
import PipelineFlow from "../components/pipeline/PipelineFlow";
import CircuitBreaker from "../components/dashboard/CircuitBreaker";

function Dashboard() {
  const liveState = usePipelineContext();
  const {
    metrics: liveMetrics,
    nodes,
    circuitBreaker,
    backendConnected,
    error,
    systemStatus,
  } = liveState;

  // Real KPI metrics from Lakehouse and Pipeline status
  const metrics = [
    {
      title: "Clean Lakehouse Commits",
      value: liveMetrics?.cleanCount !== undefined ? liveMetrics.cleanCount.toLocaleString() : "—",
      subtext: liveMetrics?.cleanSnapshotId
        ? `Snapshot: ${String(liveMetrics.cleanSnapshotId).slice(0, 10)}...`
        : "No commits yet",
      badge: liveMetrics?.cleanCount > 0 ? "lakehouse.clean" : "Empty",
      badgeColor: "text-emerald-400 bg-emerald-500/10",
      icon: Database,
    },
    {
      title: "Quarantined in DLQ",
      value: liveMetrics?.dlqCount !== undefined ? liveMetrics.dlqCount.toLocaleString() : "—",
      subtext: liveMetrics?.dlqSnapshotId
        ? `Snapshot: ${String(liveMetrics.dlqSnapshotId).slice(0, 10)}...`
        : "DLQ table empty",
      badge: liveMetrics?.dlqCount > 0 ? "Action Required" : "Clean",
      badgeColor: liveMetrics?.dlqCount > 0 ? "text-amber-400 bg-amber-500/10" : "text-emerald-400 bg-emerald-500/10",
      icon: Archive,
    },
    {
      title: "Circuit Breaker",
      value: circuitBreaker.active
        ? "Tripped (OPEN)"
        : circuitBreaker.isHalfOpen
        ? "Trial (HALF-OPEN)"
        : "Passing (CLOSED)",
      subtext: "2.0% error rate threshold",
      badge: circuitBreaker.active ? "OPEN" : "CLOSED",
      badgeColor: circuitBreaker.active ? "text-red-400 bg-red-500/10" : "text-emerald-400 bg-emerald-500/10",
      icon: ShieldCheck,
    },
    {
      title: "Total Committed Records",
      value: liveMetrics?.processed !== undefined ? liveMetrics.processed.toLocaleString() : "—",
      subtext: "Clean + Quarantined total",
      badge: backendConnected ? "Synced" : "Offline",
      badgeColor: backendConnected ? "text-cyan-400 bg-cyan-500/10" : "text-rose-400 bg-rose-500/10",
      icon: Gauge,
    },
  ];

  return (
    <div className="space-y-6">
      {/* Backend connection alert if disconnected */}
      {!backendConnected && (
        <div className="flex items-center gap-3 rounded-2xl border border-rose-500/40 bg-rose-500/10 p-4 text-rose-300">
          <WifiOff className="h-5 w-5 shrink-0 text-rose-400" />
          <div className="text-sm">
            <span className="font-semibold">Backend Unreachable:</span>{" "}
            {error || "Unable to reach FastAPI backend at localhost:8000. Displaying unavailable state."}
          </div>
        </div>
      )}

      {/* =====================================
          PAGE HEADER
      ====================================== */}
      <div className="flex flex-col justify-between gap-4 sm:flex-row sm:items-center">
        <div>
          <p className="text-sm text-slate-500">
            Real-time pipeline health & Lakehouse telemetry
          </p>
          <h1 className="mt-1 text-2xl font-bold text-white">
            System Overview
          </h1>
        </div>

        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2 rounded-xl border border-slate-800 bg-slate-900/60 px-4 py-2">
            <span
              className={`h-2.5 w-2.5 rounded-full ${
                systemStatus === "healthy"
                  ? "bg-emerald-400 animate-pulse"
                  : systemStatus === "degraded"
                  ? "bg-amber-400 animate-pulse"
                  : "bg-red-400"
              }`}
            />
            <span className="text-xs font-semibold uppercase tracking-wider text-slate-300">
              System: {systemStatus}
            </span>
          </div>
        </div>
      </div>

      {/* =====================================
          METRIC CARDS (REAL LAKEHOUSE VALUES)
      ====================================== */}
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
        {metrics.map((metric) => {
          const Icon = metric.icon;

          return (
            <div
              key={metric.title}
              className="
                rounded-2xl
                border border-slate-800
                bg-slate-900/60
                p-5
                transition-all
                duration-300
                hover:-translate-y-0.5
                hover:border-cyan-500/30
                hover:bg-slate-900
              "
            >
              {/* Icon + Badge */}
              <div className="flex items-center justify-between">
                <div
                  className="
                    flex h-10 w-10
                    items-center justify-center
                    rounded-xl
                    bg-cyan-500/10
                  "
                >
                  <Icon className="h-5 w-5 text-cyan-400" />
                </div>

                <span className={`rounded-full px-2.5 py-1 text-xs font-medium ${metric.badgeColor}`}>
                  {metric.badge}
                </span>
              </div>

              {/* Metric title */}
              <p className="mt-5 text-sm text-slate-500">
                {metric.title}
              </p>

              {/* Metric value */}
              <p className="mt-1 text-2xl font-bold text-white">
                {metric.value}
              </p>

              <p className="mt-1 text-xs text-slate-500">
                {metric.subtext}
              </p>
            </div>
          );
        })}
      </div>

      {/* =====================================
          REAL-TIME PIPELINE (REACT FLOW)
      ====================================== */}
      <div
        className="
          overflow-hidden
          rounded-2xl
          border border-slate-800
          bg-slate-900/60
        "
      >
        {/* Pipeline Header */}
        <div
          className="
            flex
            flex-col
            gap-4
            border-b border-slate-800
            px-6
            py-5
            sm:flex-row
            sm:items-center
            sm:justify-between
          "
        >
          <div>
            <div className="flex items-center gap-3">
              <h2 className="text-lg font-semibold text-white">
                Pipeline Topology
              </h2>

              <span className="rounded-full bg-cyan-500/10 px-2.5 py-0.5 text-xs font-medium text-cyan-400">
                {backendConnected ? "Live Probes" : "Offline"}
              </span>
            </div>

            <p className="mt-1 text-sm text-slate-500">
              Kafka ingestion → Flink processing → Quality validation → Iceberg Lakehouse
            </p>
          </div>

          <div className="flex items-center gap-2">
            <span className="flex h-2 w-2 rounded-full bg-emerald-400" />
            <span className="text-xs text-slate-400">
              Real-time stage connectivity
            </span>
          </div>
        </div>

        {/* React Flow Component */}
        <div className="p-4">
          <PipelineFlow pipeline={nodes} />
        </div>
      </div>

      {/* =====================================
          CIRCUIT BREAKER CARD
      ====================================== */}
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        <div className="lg:col-span-2">
          <CircuitBreaker data={circuitBreaker} />
        </div>

        {/* Telemetry info card */}
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-6 flex flex-col justify-between">
          <div>
            <div className="flex items-center gap-2 text-cyan-400">
              <Activity className="h-5 w-5" />
              <h3 className="font-semibold text-white">Lakehouse Observability</h3>
            </div>
            <p className="mt-3 text-xs leading-relaxed text-slate-400">
              IceStream queries PyIceberg metadata directly from the Lakehouse SQLite catalog.
              Clean transactions and quarantined DLQ records maintain restart-safe idempotency
              and transaction-level auditability.
            </p>
          </div>

          <div className="mt-6 border-t border-slate-800/80 pt-4 text-xs text-slate-500 space-y-1">
            <div className="flex justify-between">
              <span>Clean Table:</span>
              <span className="font-mono text-slate-300">lakehouse.clean_transactions</span>
            </div>
            <div className="flex justify-between">
              <span>DLQ Table:</span>
              <span className="font-mono text-slate-300">lakehouse.dlq_transactions</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Dashboard;