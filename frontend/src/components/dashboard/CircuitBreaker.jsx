import {
  Power,
  ShieldAlert,
  ArrowRight,
} from "lucide-react";

function CircuitBreaker({ data }) {

  const isActive = data?.active ?? false;
  const errorRate = data?.errorRate ?? 0;
  const threshold = data?.threshold ?? 2.0;

  const percentage = Math.min(
    (errorRate / threshold) * 100,
    100
  );


  return (
    <div
      className={`
        rounded-2xl
        border
        ${
          isActive
            ? "border-red-500/40 bg-red-500/5"
            : "border-slate-800 bg-slate-900/60"
        }
        p-6
        transition-all
        duration-500
      `}
    >

      {/* =====================================
          HEADER
      ====================================== */}

      <div className="flex items-start justify-between">

        <div className="flex items-center gap-3">

          <div
            className={`
              flex h-10 w-10
              items-center justify-center
              rounded-xl
              ${
                isActive
                  ? "bg-red-500/10"
                  : "bg-cyan-500/10"
              }
            `}
          >

            {isActive ? (
              <ShieldAlert className="h-5 w-5 text-red-400" />
            ) : (
              <Power className="h-5 w-5 text-cyan-400" />
            )}

          </div>


          <div>

            <h2 className="font-semibold text-white">
              Circuit Breaker
            </h2>

            <p className="text-xs text-slate-500">
              Automatic pipeline protection
            </p>

          </div>

        </div>


        {/* Status */}

        <div
          className={`
            rounded-full
            px-3 py-1
            text-[10px]
            font-semibold
            tracking-wider
            ${
              isActive
                ? "bg-red-500/10 text-red-400"
                : "bg-emerald-500/10 text-emerald-400"
            }
          `}
        >
          {isActive ? "TRIPPED" : "NORMAL"}
        </div>

      </div>


      {/* =====================================
          METRICS
      ====================================== */}

      <div className="mt-6 grid grid-cols-2 gap-4">

        {/* Error Rate */}

        <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-4">

          <p className="text-[10px] uppercase tracking-wider text-slate-600">
            Current Error Rate
          </p>

          <p
            className={`
              mt-2
              text-2xl
              font-bold
              ${
                isActive
                  ? "text-red-400"
                  : "text-emerald-400"
              }
            `}
          >
            {errorRate}%
          </p>

        </div>


        {/* Threshold */}

        <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-4">

          <p className="text-[10px] uppercase tracking-wider text-slate-600">
            Threshold
          </p>

          <p className="mt-2 text-2xl font-bold text-white">
            {threshold}%
          </p>

        </div>

      </div>


      {/* =====================================
          ERROR RATE PROGRESS
      ====================================== */}

      <div className="mt-5">

        <div className="mb-2 flex items-center justify-between">

          <span className="text-[10px] uppercase tracking-wider text-slate-600">
            Threshold Utilization
          </span>

          <span className="text-xs text-slate-500">
            {Math.round(percentage)}%
          </span>

        </div>


        <div className="h-2 overflow-hidden rounded-full bg-slate-800">

          <div
            className={`
              h-full
              rounded-full
              transition-all
              duration-700
              ${
                isActive
                  ? "bg-red-500"
                  : percentage >= 75
                  ? "bg-amber-400"
                  : "bg-emerald-400"
              }
            `}
            style={{
              width: `${percentage}%`,
            }}
          />

        </div>

      </div>


      {/* =====================================
          PROTECTION STATUS
      ====================================== */}

      <div
        className={`
          mt-6
          rounded-xl
          border
          p-4
          ${
            isActive
              ? "border-red-500/20 bg-red-500/5"
              : "border-slate-800 bg-slate-950/40"
          }
        `}
      >

        <div className="flex items-center gap-3">

          {isActive ? (
            <ShieldAlert className="h-5 w-5 text-red-400" />
          ) : (
            <Power className="h-5 w-5 text-emerald-400" />
          )}


          <div className="flex-1">

            <p className="text-sm font-medium text-slate-200">

              {isActive
                ? "Pipeline Protection Active"
                : "Pipeline Operating Normally"}

            </p>


            <p className="mt-1 text-xs text-slate-500">

              {isActive
                ? "Error conditions detected. Invalid records are being isolated."
                : "Error rate is below the configured protection threshold."}

            </p>

          </div>


          <ArrowRight className="h-4 w-4 text-slate-600" />

        </div>

      </div>

    </div>
  );
}

export default CircuitBreaker;