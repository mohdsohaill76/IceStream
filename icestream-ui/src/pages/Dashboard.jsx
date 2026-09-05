import {
  Activity,
  Database,
  Gauge,
  ShieldCheck,
} from "lucide-react";

import { usePipelineContext } from "../context/usePipelineContext";
import PipelineFlow from "../components/pipeline/PipelineFlow";
import CircuitBreaker from "../components/dashboard/CircuitBreaker";

function Dashboard() {
  // ---------------------------------------
  // Live pipeline state
  // ---------------------------------------
const liveState = usePipelineContext();
  const {
    metrics: liveMetrics,
    nodes,
    circuitBreaker,
  } = liveState;

  // ---------------------------------------
  // Dashboard metrics
  // ---------------------------------------
  const metrics = [
    {
      title: "Records / Second",
      value: liveMetrics.throughput.toLocaleString(),
      change: "+8.4%",
      icon: Activity,
    },
    {
      title: "Data Quality",
      value: `${liveMetrics.quality}%`,
      change: "+1.2%",
      icon: ShieldCheck,
    },
    {
      title: "Error Rate",
      value: `${liveMetrics.errorRate}%`,
      change: "-0.4%",
      icon: Gauge,
    },
    {
      title: "Records Processed",
      value: `${(liveMetrics.processed / 1000000).toFixed(2)}M`,
      change: "+12.6%",
      icon: Database,
    },
  ];

  // ---------------------------------------
  // System status
  // ---------------------------------------
  const systemActive = circuitBreaker.active;

  return (
    <div className="space-y-6">

      {/* =====================================
          PAGE HEADER
      ====================================== */}
      <div>
        <p className="text-sm text-slate-500">
          Real-time pipeline health
        </p>

        <h1 className="mt-1 text-2xl font-bold text-white">
          System Overview
        </h1>
      </div>


      {/* =====================================
          METRIC CARDS
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

              {/* Icon + Change */}
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

                <span className="text-xs font-medium text-emerald-400">
                  {metric.change}
                </span>

              </div>


              {/* Metric title */}
              <p className="mt-5 text-sm text-slate-500">
                {metric.title}
              </p>


              {/* Live metric value */}
              <p className="mt-1 text-2xl font-bold text-white">
                {metric.value}
              </p>

            </div>
          );
        })}

      </div>


      {/* =====================================
          REAL-TIME PIPELINE
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

          {/* Pipeline title */}
          <div>

            <div className="flex items-center gap-3">

              <h2 className="text-lg font-semibold text-white">
                Real-Time Pipeline
              </h2>

              {/* Live badge */}
              <span
                className={`
                  flex items-center gap-2
                  rounded-full
                  px-3 py-1
                  text-[10px]
                  font-semibold
                  tracking-wider
                  ${
                    systemActive
                      ? "bg-red-500/10 text-red-400"
                      : "bg-emerald-500/10 text-emerald-400"
                  }
                `}
              >

                <span
                  className={`
                    h-1.5 w-1.5
                    rounded-full
                    ${
                      systemActive
                        ? "bg-red-400"
                        : "bg-emerald-400"
                    }
                  `}
                />

                {systemActive ? "ALERT" : "LIVE"}

              </span>

            </div>

            <p className="mt-1 text-sm text-slate-500">
              Live data movement across the IceStream architecture
            </p>

          </div>


          {/* =================================
              SYSTEM STATUS
          ================================== */}
          <div className="flex items-center gap-3">

            <div className="text-right">

              <p className="text-[10px] uppercase tracking-wider text-slate-600">
                System Status
              </p>

              <p
                className={`
                  mt-1
                  text-xs
                  font-medium
                  ${
                    systemActive
                      ? "text-red-400"
                      : "text-emerald-400"
                  }
                `}
              >
                {systemActive
                  ? "Protection active"
                  : "All systems operational"}
              </p>

            </div>


            <div
              className={`
                flex h-9 w-9
                items-center justify-center
                rounded-full
                ${
                  systemActive
                    ? "bg-red-500/10"
                    : "bg-emerald-500/10"
                }
              `}
            >

              <span
                className={`
                  h-2.5 w-2.5
                  animate-pulse
                  rounded-full
                  ${
                    systemActive
                      ? "bg-red-400"
                      : "bg-emerald-400"
                  }
                `}
              />

            </div>

          </div>

        </div>


        {/* =================================
            REACT FLOW PIPELINE
        ================================== */}
        <div className="p-4 sm:p-6">

          <PipelineFlow pipeline={nodes} />

        </div>

      </div>


      {/* =====================================
          CIRCUIT BREAKER
      ====================================== */}
      <CircuitBreaker data={circuitBreaker} />

    </div>
  );
}

export default Dashboard;