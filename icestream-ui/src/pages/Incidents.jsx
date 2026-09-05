import {
  AlertTriangle,
  CheckCircle,
  Activity,
} from "lucide-react";

import { usePipelineContext } from "../context/usePipelineContext";
import { useIncidentMonitor } from "../hooks/useIncidentMonitor";
import IncidentLog from "../components/incidents/IncidentLog";

function Incidents() {
  const liveState = usePipelineContext();

  const {
    incidents,
    openCount,
    criticalCount,
  } = useIncidentMonitor(liveState);

  const systemHealthy =
    liveState.systemStatus === "healthy";

  return (
    <div className="space-y-6">

      {/* Header */}
      <div>
        <p className="text-sm text-slate-500">
          Monitor and investigate pipeline events
        </p>

        <h1 className="mt-1 text-2xl font-bold text-white">
          Incidents
        </h1>
      </div>

      {/* Incident overview */}
      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">

        {/* Open incidents */}
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">

          <div className="flex items-center justify-between">

            <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-amber-500/10">
              <AlertTriangle className="h-5 w-5 text-amber-400" />
            </div>

            <span className="text-xs text-slate-600">
              CURRENT
            </span>

          </div>

          <p className="mt-5 text-sm text-slate-500">
            Open Incidents
          </p>

          <p className="mt-1 text-2xl font-bold text-white">
            {openCount}
          </p>

        </div>

        {/* Critical incidents */}
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">

          <div className="flex items-center justify-between">

            <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-red-500/10">
              <AlertTriangle className="h-5 w-5 text-red-400" />
            </div>

            <span className="text-xs text-slate-600">
              PRIORITY
            </span>

          </div>

          <p className="mt-5 text-sm text-slate-500">
            Critical Incidents
          </p>

          <p className="mt-1 text-2xl font-bold text-white">
            {criticalCount}
          </p>

        </div>

        {/* System health */}
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">

          <div className="flex items-center justify-between">

            <div
              className={`flex h-10 w-10 items-center justify-center rounded-xl ${
                systemHealthy
                  ? "bg-emerald-500/10"
                  : "bg-red-500/10"
              }`}
            >
              <CheckCircle
                className={`h-5 w-5 ${
                  systemHealthy
                    ? "text-emerald-400"
                    : "text-red-400"
                }`}
              />
            </div>

            <Activity className="h-4 w-4 text-slate-600" />

          </div>

          <p className="mt-5 text-sm text-slate-500">
            System Health
          </p>

          <p
            className={`mt-1 text-2xl font-bold ${
              systemHealthy
                ? "text-emerald-400"
                : "text-red-400"
            }`}
          >
            {systemHealthy
              ? "Operational"
              : "Attention Required"}
          </p>

        </div>

      </div>

      {/* Incident log */}
      <IncidentLog incidents={incidents} />

    </div>
  );
}

export default Incidents;