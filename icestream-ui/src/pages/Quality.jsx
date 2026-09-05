import {
  ShieldCheck,
  AlertTriangle,
  CheckCircle,
  Activity,
} from "lucide-react";

import { usePipelineSimulation } from "../hooks/usePipelineSimulation";


function Quality() {

  // =========================================
  // LIVE PIPELINE DATA
  // =========================================

  const liveState = usePipelineSimulation();

  const quality = liveState.metrics.quality;


  // =========================================
  // AUTOMATIC QUALITY CHECKS
  // =========================================

  const qualityChecks = [
    {
      name: "Schema Validation",
      description: "Incoming records match the expected schema",
      status: quality >= 98 ? "passed" : "warning",
      value: quality >= 98 ? "Passed" : "Warning",
    },

    {
      name: "Null Value Detection",
      description: "Checks for missing values in incoming records",
      status: quality >= 97.5 ? "passed" : "warning",
      value: quality >= 97.5 ? "Passed" : "Warning",
    },

    {
      name: "Data Type Validation",
      description: "Verifies that fields contain valid data types",
      status: quality >= 98.5 ? "passed" : "warning",
      value: quality >= 98.5 ? "Passed" : "Warning",
    },

    {
      name: "Duplicate Detection",
      description: "Identifies duplicate events in the stream",
      status: quality >= 98 ? "passed" : "warning",
      value: quality >= 98 ? "Passed" : "Warning",
    },
  ];


  // =========================================
  // QUALITY STATUS
  // =========================================

  const qualityStatus =
    quality >= 98
      ? "Excellent"
      : quality >= 97
      ? "Acceptable"
      : "Degraded";


  const qualityColor =
    quality >= 98
      ? "text-emerald-400"
      : quality >= 97
      ? "text-amber-400"
      : "text-red-400";


  return (
    <div className="space-y-6">

      {/* =====================================
          HEADER
      ====================================== */}

      <div>

        <p className="text-sm text-slate-500">
          Monitor real-time data validation
        </p>

        <h1 className="mt-1 text-2xl font-bold text-white">
          Data Quality
        </h1>

      </div>


      {/* =====================================
          QUALITY OVERVIEW
      ====================================== */}

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">


        {/* Quality Score */}

        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-6 lg:col-span-2">

          <div className="flex items-center justify-between">

            <div className="flex items-center gap-3">

              <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-cyan-500/10">

                <ShieldCheck className="h-6 w-6 text-cyan-400" />

              </div>

              <div>

                <h2 className="font-semibold text-white">
                  Overall Quality Score
                </h2>

                <p className="text-xs text-slate-500">
                  Live validation performance
                </p>

              </div>

            </div>


            <span
              className={`
                rounded-full
                px-3
                py-1
                text-[10px]
                font-semibold
                tracking-wider
                ${
                  quality >= 98
                    ? "bg-emerald-500/10 text-emerald-400"
                    : quality >= 97
                    ? "bg-amber-500/10 text-amber-400"
                    : "bg-red-500/10 text-red-400"
                }
              `}
            >
              {qualityStatus.toUpperCase()}
            </span>

          </div>


          {/* Score */}

          <div className="mt-8 flex items-end gap-3">

            <p className={`text-5xl font-bold ${qualityColor}`}>
              {quality}%
            </p>

            <p className="pb-2 text-xs text-slate-500">
              current score
            </p>

          </div>


          {/* Progress */}

          <div className="mt-6">

            <div className="h-3 overflow-hidden rounded-full bg-slate-800">

              <div
                className={`
                  h-full
                  rounded-full
                  transition-all
                  duration-700
                  ${
                    quality >= 98
                      ? "bg-emerald-400"
                      : quality >= 97
                      ? "bg-amber-400"
                      : "bg-red-400"
                  }
                `}
                style={{
                  width: `${quality}%`,
                }}
              />

            </div>

          </div>


          <div className="mt-3 flex justify-between text-[10px] text-slate-600">

            <span>
              0%
            </span>

            <span>
              Target: 98%
            </span>

            <span>
              100%
            </span>

          </div>

        </div>


        {/* Live Status */}

        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-6">

          <div className="flex items-center justify-between">

            <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-emerald-500/10">

              <Activity className="h-5 w-5 text-emerald-400" />

            </div>

            <span className="flex items-center gap-2 rounded-full bg-emerald-500/10 px-3 py-1 text-[10px] font-semibold tracking-wider text-emerald-400">

              <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-emerald-400" />

              LIVE

            </span>

          </div>


          <p className="mt-6 text-sm text-slate-500">
            Validation Engine
          </p>

          <p className="mt-1 text-xl font-bold text-white">
            Running
          </p>


          <div className="mt-6 space-y-3">

            <div className="flex justify-between">

              <span className="text-xs text-slate-500">
                Records / Second
              </span>

              <span className="text-xs font-medium text-white">
                {liveState.metrics.throughput.toLocaleString()}
              </span>

            </div>


            <div className="flex justify-between">

              <span className="text-xs text-slate-500">
                Error Rate
              </span>

              <span className="text-xs font-medium text-white">
                {liveState.metrics.errorRate}%
              </span>

            </div>

          </div>

        </div>

      </div>


      {/* =====================================
          QUALITY CHECKS
      ====================================== */}

      <div className="rounded-2xl border border-slate-800 bg-slate-900/60">

        <div className="border-b border-slate-800 px-6 py-5">

          <h2 className="text-lg font-semibold text-white">
            Validation Checks
          </h2>

          <p className="mt-1 text-sm text-slate-500">
            Automated checks running against incoming records
          </p>

        </div>


        <div className="divide-y divide-slate-800">

          {qualityChecks.map((check) => {

            const passed = check.status === "passed";

            return (

              <div
                key={check.name}
                className="flex items-center justify-between px-6 py-5 transition hover:bg-slate-900"
              >

                <div className="flex items-center gap-4">

                  <div
                    className={`
                      flex
                      h-9
                      w-9
                      items-center
                      justify-center
                      rounded-lg
                      ${
                        passed
                          ? "bg-emerald-500/10"
                          : "bg-amber-500/10"
                      }
                    `}
                  >

                    {passed ? (
                      <CheckCircle className="h-5 w-5 text-emerald-400" />
                    ) : (
                      <AlertTriangle className="h-5 w-5 text-amber-400" />
                    )}

                  </div>


                  <div>

                    <p className="text-sm font-medium text-slate-200">
                      {check.name}
                    </p>

                    <p className="mt-1 text-xs text-slate-500">
                      {check.description}
                    </p>

                  </div>

                </div>


                <span
                  className={`
                    rounded-full
                    px-3
                    py-1
                    text-[10px]
                    font-semibold
                    tracking-wider
                    ${
                      passed
                        ? "bg-emerald-500/10 text-emerald-400"
                        : "bg-amber-500/10 text-amber-400"
                    }
                  `}
                >
                  {check.value.toUpperCase()}
                </span>

              </div>

            );

          })}

        </div>

      </div>


      {/* =====================================
          QUALITY EVENTS
      ====================================== */}

      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">

        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">

          <p className="text-xs text-slate-500">
            Quality Score
          </p>

          <p className={`mt-2 text-2xl font-bold ${qualityColor}`}>
            {quality}%
          </p>

        </div>


        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">

          <p className="text-xs text-slate-500">
            Error Rate
          </p>

          <p className="mt-2 text-2xl font-bold text-white">
            {liveState.metrics.errorRate}%
          </p>

        </div>


        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">

          <p className="text-xs text-slate-500">
            Quarantined Records
          </p>

          <p className="mt-2 text-2xl font-bold text-white">
            {liveState.nodes.dlq.metric}
          </p>

        </div>

      </div>

    </div>
  );
}


export default Quality;