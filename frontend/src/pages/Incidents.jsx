import {
  AlertTriangle,
  CheckCircle,
  Activity,
} from "lucide-react";

import { usePipelineSimulation } from "../hooks/usePipelineSimulation";
import { useIncidentMonitor } from "../hooks/useIncidentMonitor";

import IncidentLog from "../components/incidents/IncidentLog";


function Incidents() {

  /* =========================================
     LIVE PIPELINE STATE
  ========================================== */

  const liveState = usePipelineSimulation();


  /* =========================================
     AUTOMATIC INCIDENT MONITOR
  ========================================== */

  const {
    incidents,
    openCount,
    criticalCount,
  } = useIncidentMonitor(liveState);


  /* =========================================
     SYSTEM STATUS
  ========================================== */

  const systemStatus = liveState.systemStatus;


  return (
    <div className="space-y-6">

      {/* =====================================
          HEADER
      ====================================== */}

      <div>

        <p className="text-sm text-slate-500">
          Monitor and investigate pipeline events
        </p>

        <h1 className="mt-1 text-2xl font-bold text-white">
          Incidents
        </h1>

      </div>


      {/* =====================================
          SUMMARY CARDS
      ====================================== */}

      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">


        {/* ===================================
            OPEN INCIDENTS
        ==================================== */}

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


        {/* ===================================
            CRITICAL INCIDENTS
        ==================================== */}

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


        {/* ===================================
            SYSTEM HEALTH
        ==================================== */}

        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">

          <div className="flex items-center justify-between">

            <div
              className={`
                flex h-10 w-10
                items-center justify-center
                rounded-xl
                ${
                  systemStatus === "critical"
                    ? "bg-red-500/10"
                    : systemStatus === "warning"
                    ? "bg-amber-500/10"
                    : "bg-emerald-500/10"
                }
              `}
            >

              <CheckCircle
                className={`
                  h-5 w-5
                  ${
                    systemStatus === "critical"
                      ? "text-red-400"
                      : systemStatus === "warning"
                      ? "text-amber-400"
                      : "text-emerald-400"
                  }
                `}
              />

            </div>


            <Activity className="h-4 w-4 text-slate-600" />

          </div>


          <p className="mt-5 text-sm text-slate-500">
            System Health
          </p>


          <p
            className={`
              mt-1
              text-2xl
              font-bold
              ${
                systemStatus === "critical"
                  ? "text-red-400"
                  : systemStatus === "warning"
                  ? "text-amber-400"
                  : "text-emerald-400"
              }
            `}
          >

            {systemStatus === "critical"
              ? "Critical"
              : systemStatus === "warning"
              ? "Degraded"
              : "Operational"}

          </p>

        </div>

      </div>


      {/* =====================================
          LIVE PIPELINE STATUS
      ====================================== */}

      <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">

        <div className="flex items-center justify-between">

          <div>

            <p className="text-sm font-medium text-white">
              Live Monitoring
            </p>

            <p className="mt-1 text-xs text-slate-500">
              Incident detection is running automatically
            </p>

          </div>


          <span className="flex items-center gap-2 rounded-full bg-emerald-500/10 px-3 py-1 text-[10px] font-semibold tracking-wider text-emerald-400">

            <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-emerald-400" />

            LIVE

          </span>

        </div>


        {/* Live metrics */}

        <div className="mt-5 grid grid-cols-2 gap-4 md:grid-cols-4">

          <div>

            <p className="text-[10px] uppercase tracking-wider text-slate-600">
              Throughput
            </p>

            <p className="mt-1 text-sm font-semibold text-white">
              {liveState.metrics.throughput.toLocaleString()} records/s
            </p>

          </div>


          <div>

            <p className="text-[10px] uppercase tracking-wider text-slate-600">
              Latency
            </p>

            <p className="mt-1 text-sm font-semibold text-white">
              {liveState.nodes.flink.metric}
            </p>

          </div>


          <div>

            <p className="text-[10px] uppercase tracking-wider text-slate-600">
              Quality
            </p>

            <p className="mt-1 text-sm font-semibold text-white">
              {liveState.metrics.quality}%
            </p>

          </div>


          <div>

            <p className="text-[10px] uppercase tracking-wider text-slate-600">
              Error Rate
            </p>

            <p className="mt-1 text-sm font-semibold text-white">
              {liveState.metrics.errorRate}%
            </p>

          </div>

        </div>

      </div>


      {/* =====================================
          INCIDENT LOG
      ====================================== */}

      <IncidentLog incidents={incidents} />

    </div>
  );
}


export default Incidents;