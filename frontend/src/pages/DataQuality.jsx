import {
  ShieldCheck,
  AlertTriangle,
  CheckCircle,
  Database,
  Archive,
} from "lucide-react";

import { usePipelineContext } from "../context/usePipelineContext";

function DataQuality() {
  const { metrics, nodes, circuitBreaker, backendConnected } = usePipelineContext();

  const qualityStatus = nodes?.quality?.status || "unknown";
  const cleanCount = metrics?.cleanCount ?? 0;
  const dlqCount = metrics?.dlqCount ?? 0;

  // Real status derivation
  const isHealthy = qualityStatus === "healthy";
  const isDegraded = qualityStatus === "degraded";
  const isTripped = qualityStatus === "unhealthy" || circuitBreaker?.active;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <p className="text-sm text-slate-500">
          Monitor data integrity and schema validation contracts
        </p>
        <h1 className="mt-1 text-2xl font-bold text-white">
          Data Quality
        </h1>
      </div>

      {/* Quality overview */}
      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        {/* Quality Score (Honest Unavailable State) */}
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
          <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-cyan-500/10">
            <ShieldCheck className="h-5 w-5 text-cyan-400" />
          </div>

          <p className="mt-4 text-sm text-slate-500">
            Quality Score
          </p>

          <p className="mt-1 text-3xl font-bold text-slate-300">
            N/A
          </p>

          <p className="mt-2 text-xs text-slate-500">
            Real-time streaming score not exposed by backend API
          </p>
        </div>

        {/* Error Rate / Circuit Breaker Threshold */}
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
          <div
            className={`flex h-10 w-10 items-center justify-center rounded-xl ${
              isTripped ? "bg-red-500/10" : isDegraded ? "bg-amber-500/10" : "bg-emerald-500/10"
            }`}
          >
            <AlertTriangle
              className={`h-5 w-5 ${
                isTripped ? "text-red-400" : isDegraded ? "text-amber-400" : "text-emerald-400"
              }`}
            />
          </div>

          <p className="mt-4 text-sm text-slate-500">
            Circuit Breaker Threshold
          </p>

          <p
            className={`mt-1 text-3xl font-bold ${
              isTripped ? "text-red-400" : isDegraded ? "text-amber-400" : "text-emerald-400"
            }`}
          >
            {isTripped ? "> 2.0%" : "< 2.0%"}
          </p>

          <p className="mt-2 text-xs text-slate-400">
            {isTripped
              ? "Tripped: Error threshold exceeded"
              : isDegraded
              ? "Half-Open: Recovery evaluation"
              : "Closed: Within healthy bounds (<2%)"}
          </p>
        </div>

        {/* Validation Status */}
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
          <div
            className={`flex h-10 w-10 items-center justify-center rounded-xl ${
              isHealthy ? "bg-emerald-500/10" : isDegraded ? "bg-amber-500/10" : "bg-red-500/10"
            }`}
          >
            <CheckCircle
              className={`h-5 w-5 ${
                isHealthy ? "text-emerald-400" : isDegraded ? "text-amber-400" : "text-red-400"
              }`}
            />
          </div>

          <p className="mt-4 text-sm text-slate-500">
            Validation State
          </p>

          <p
            className={`mt-1 text-2xl font-bold ${
              isHealthy ? "text-emerald-400" : isDegraded ? "text-amber-400" : "text-red-400"
            }`}
          >
            {backendConnected
              ? isHealthy
                ? "Passing"
                : isDegraded
                ? "Recovery"
                : "Tripped"
              : "Unavailable"}
          </p>

          <p className="mt-2 text-xs text-slate-500">
            {backendConnected ? "Live circuit-breaker probe" : "Backend offline"}
          </p>
        </div>
      </div>

      {/* Lakehouse Storage Verification */}
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5 flex items-center justify-between">
          <div>
            <p className="text-sm text-slate-500">Committed Valid Transactions</p>
            <p className="mt-1 text-2xl font-bold text-white">{cleanCount.toLocaleString()} rows</p>
            <p className="mt-1 text-xs text-slate-500">Target: lakehouse.clean_transactions</p>
          </div>
          <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-emerald-500/10 text-emerald-400">
            <Database className="h-6 w-6" />
          </div>
        </div>

        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5 flex items-center justify-between">
          <div>
            <p className="text-sm text-slate-500">Quarantined DLQ Transactions</p>
            <p className="mt-1 text-2xl font-bold text-white">{dlqCount.toLocaleString()} records</p>
            <p className="mt-1 text-xs text-slate-500">Target: lakehouse.dlq_transactions</p>
          </div>
          <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-amber-500/10 text-amber-400">
            <Archive className="h-6 w-6" />
          </div>
        </div>
      </div>

      {/* Validation checks */}
      <div className="rounded-2xl border border-slate-800 bg-slate-900/60">
        <div className="border-b border-slate-800 px-6 py-5">
          <h2 className="text-lg font-semibold text-white">
            Data Quality Rule Contracts
          </h2>
          <p className="mt-1 text-sm text-slate-500">
            Active validation rules enforced by the Data Quality consumer
          </p>
        </div>

        <div className="divide-y divide-slate-800">
          {[
            { name: "Canonical 7-Field Schema Validation", desc: "Enforces required contract fields and types" },
            { name: "Null & Missing Field Detection", desc: "Rejects records with missing mandatory attributes" },
            { name: "Duplicate Transaction Quarantine", desc: "Dedupes on transaction_id with 24h State TTL" },
            { name: "Amount & Positive Value Range", desc: "Ensures amount > 0 and currency code is valid" },
            { name: "Timestamp Format & Drift Guard", desc: "Validates ISO-8601 formatting and clock skew" },
          ].map((check) => (
            <div
              key={check.name}
              className="flex items-center justify-between px-6 py-4"
            >
              <div>
                <span className="text-sm font-medium text-slate-200">
                  {check.name}
                </span>
                <p className="text-xs text-slate-500">{check.desc}</p>
              </div>

              <span className={`flex items-center gap-1.5 text-xs font-semibold uppercase ${
                isHealthy ? "text-emerald-400" : isDegraded ? "text-amber-400" : "text-slate-400"
              }`}>
                <CheckCircle className="h-4 w-4" />
                {isHealthy ? "ACTIVE" : isDegraded ? "EVALUATING" : "ACTIVE"}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export default DataQuality;