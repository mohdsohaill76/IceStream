import {
  ShieldCheck,
  AlertTriangle,
  CheckCircle,
  Database,
} from "lucide-react";

import { usePipelineSimulation } from "../hooks/usePipelineSimulation";

function DataQuality() {

  const { metrics } = usePipelineSimulation();

  const quality = metrics.quality;
  const errorRate = metrics.errorRate;

  return (
    <div className="space-y-6">

      {/* Header */}
      <div>

        <p className="text-sm text-slate-500">
          Monitor data integrity and validation
        </p>

        <h1 className="mt-1 text-2xl font-bold text-white">
          Data Quality
        </h1>

      </div>


      {/* Quality overview */}
      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">

        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">

          <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-emerald-500/10">

            <ShieldCheck className="h-5 w-5 text-emerald-400" />

          </div>

          <p className="mt-4 text-sm text-slate-500">
            Quality Score
          </p>

          <p className="mt-1 text-3xl font-bold text-white">
            {quality}%
          </p>

          <p className="mt-2 text-xs text-emerald-400">
            Excellent data quality
          </p>

        </div>


        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">

          <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-red-500/10">

            <AlertTriangle className="h-5 w-5 text-red-400" />

          </div>

          <p className="mt-4 text-sm text-slate-500">
            Error Rate
          </p>

          <p className="mt-1 text-3xl font-bold text-white">
            {errorRate}%
          </p>

          <p className="mt-2 text-xs text-slate-500">
            Current pipeline errors
          </p>

        </div>


        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">

          <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-cyan-500/10">

            <Database className="h-5 w-5 text-cyan-400" />

          </div>

          <p className="mt-4 text-sm text-slate-500">
            Validation Status
          </p>

          <p className="mt-1 text-2xl font-bold text-emerald-400">
            Passing
          </p>

          <p className="mt-2 text-xs text-slate-500">
            All validation checks active
          </p>

        </div>

      </div>


      {/* Validation checks */}
      <div className="rounded-2xl border border-slate-800 bg-slate-900/60">

        <div className="border-b border-slate-800 px-6 py-5">

          <h2 className="text-lg font-semibold text-white">
            Validation Checks
          </h2>

          <p className="mt-1 text-sm text-slate-500">
            Current data quality validation status
          </p>

        </div>


        <div className="divide-y divide-slate-800">

          {[
            "Schema Validation",
            "Null Value Detection",
            "Duplicate Detection",
            "Range Validation",
            "Data Type Validation",
          ].map((check) => (

            <div
              key={check}
              className="flex items-center justify-between px-6 py-4"
            >

              <span className="text-sm text-slate-300">
                {check}
              </span>

              <span className="flex items-center gap-2 text-xs font-medium text-emerald-400">

                <CheckCircle className="h-4 w-4" />

                PASS

              </span>

            </div>

          ))}

        </div>

      </div>

    </div>
  );
}

export default DataQuality;