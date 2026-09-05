import {
  Activity,
  Radio,
  Cpu,
  ShieldCheck,
  Database,
  Archive,
  ArrowRight,
  Clock3,
  AlertTriangle,
} from "lucide-react";

import { usePipelineContext } from "../context/usePipelineContext";
import PipelineFlow from "../components/pipeline/PipelineFlow";


const pipelineStages = [
  {
    id: "kafka",
    label: "Kafka",
    subtitle: "Event Ingestion",
    icon: Radio,
  },
  {
    id: "flink",
    label: "Flink",
    subtitle: "Stream Processing",
    icon: Cpu,
  },
  {
    id: "quality",
    label: "Data Quality",
    subtitle: "Validation",
    icon: ShieldCheck,
  },
  {
    id: "iceberg",
    label: "Iceberg",
    subtitle: "Data Storage",
    icon: Database,
  },
  {
    id: "dlq",
    label: "DLQ",
    subtitle: "Quarantine",
    icon: Archive,
  },
];


function Pipeline() {

  // =========================================
  // LIVE PIPELINE STATE
  // =========================================

  const liveState = usePipelineContext();

  const {
    metrics,
    nodes,
    circuitBreaker,
  } = liveState;


  // =========================================
  // STATUS HELPERS
  // =========================================

  const getStatusClass = (status) => {

    if (status === "healthy") {
      return "text-emerald-400";
    }

    if (status === "warning") {
      return "text-amber-400";
    }

    if (status === "error") {
      return "text-red-400";
    }

    if (status === "quarantined") {
      return "text-orange-400";
    }

    return "text-slate-400";
  };


  return (
    <div className="space-y-6">


      {/* =====================================
          HEADER
      ====================================== */}

      <div>

        <p className="text-sm text-slate-500">
          Monitor every stage of the streaming pipeline
        </p>

        <div className="mt-1 flex flex-wrap items-center gap-3">

          <h1 className="text-2xl font-bold text-white">
            Pipeline
          </h1>

          <span
            className="
              flex items-center gap-2
              rounded-full
              bg-emerald-500/10
              px-3 py-1
              text-[10px]
              font-semibold
              tracking-wider
              text-emerald-400
            "
          >

            <span
              className="
                h-1.5 w-1.5
                animate-pulse
                rounded-full
                bg-emerald-400
              "
            />

            LIVE

          </span>

        </div>

      </div>


      {/* =====================================
          PIPELINE SUMMARY
      ====================================== */}

      <div
        className="
          grid
          grid-cols-1
          gap-4
          md:grid-cols-3
        "
      >

        {/* Throughput */}

        <div
          className="
            rounded-2xl
            border border-slate-800
            bg-slate-900/60
            p-5
          "
        >

          <div className="flex items-center justify-between">

            <div
              className="
                flex h-10 w-10
                items-center justify-center
                rounded-xl
                bg-cyan-500/10
              "
            >

              <Activity className="h-5 w-5 text-cyan-400" />

            </div>

            <span className="text-[10px] uppercase tracking-wider text-slate-600">
              LIVE
            </span>

          </div>

          <p className="mt-5 text-sm text-slate-500">
            Throughput
          </p>

          <p className="mt-1 text-2xl font-bold text-white">
            {metrics.throughput.toLocaleString()}
            <span className="ml-1 text-sm font-normal text-slate-500">
              records/s
            </span>
          </p>

        </div>


        {/* Quality */}

        <div
          className="
            rounded-2xl
            border border-slate-800
            bg-slate-900/60
            p-5
          "
        >

          <div className="flex items-center justify-between">

            <div
              className="
                flex h-10 w-10
                items-center justify-center
                rounded-xl
                bg-emerald-500/10
              "
            >

              <ShieldCheck className="h-5 w-5 text-emerald-400" />

            </div>

            <span className="text-[10px] uppercase tracking-wider text-slate-600">
              QUALITY
            </span>

          </div>

          <p className="mt-5 text-sm text-slate-500">
            Pipeline Quality
          </p>

          <p className="mt-1 text-2xl font-bold text-emerald-400">
            {metrics.quality}%
          </p>

        </div>


        {/* Error rate */}

        <div
          className="
            rounded-2xl
            border border-slate-800
            bg-slate-900/60
            p-5
          "
        >

          <div className="flex items-center justify-between">

            <div
              className="
                flex h-10 w-10
                items-center justify-center
                rounded-xl
                bg-amber-500/10
              "
            >

              <AlertTriangle className="h-5 w-5 text-amber-400" />

            </div>

            <span className="text-[10px] uppercase tracking-wider text-slate-600">
              MONITORING
            </span>

          </div>

          <p className="mt-5 text-sm text-slate-500">
            Error Rate
          </p>

          <p className="mt-1 text-2xl font-bold text-white">
            {metrics.errorRate}%
          </p>

        </div>

      </div>


      {/* =====================================
          PIPELINE VISUALIZATION
      ====================================== */}

      <div
        className="
          overflow-hidden
          rounded-2xl
          border border-slate-800
          bg-slate-900/60
        "
      >

        <div
          className="
            border-b
            border-slate-800
            px-6 py-5
          "
        >

          <div className="flex items-center justify-between">

            <div>

              <h2 className="text-lg font-semibold text-white">
                Pipeline Architecture
              </h2>

              <p className="mt-1 text-sm text-slate-500">
                Live data flow from ingestion to storage and quarantine
              </p>

            </div>

            <div
              className={`
                rounded-full
                px-3 py-1
                text-[10px]
                font-semibold
                tracking-wider
                ${
                  circuitBreaker.active
                    ? "bg-red-500/10 text-red-400"
                    : "bg-emerald-500/10 text-emerald-400"
                }
              `}
            >

              {circuitBreaker.active
                ? "PROTECTION ACTIVE"
                : "PIPELINE HEALTHY"}

            </div>

          </div>

        </div>


        <div className="p-4 sm:p-6">

          <PipelineFlow pipeline={nodes} />

        </div>

      </div>


      {/* =====================================
          STAGE DETAILS
      ====================================== */}

      <div>

        <div className="mb-4">

          <h2 className="text-lg font-semibold text-white">
            Pipeline Stages
          </h2>

          <p className="mt-1 text-sm text-slate-500">
            Current status of each processing component
          </p>

        </div>


        <div
          className="
            grid
            grid-cols-1
            gap-4
            lg:grid-cols-2
          "
        >

          {pipelineStages.map((stage, index) => {

            const Icon = stage.icon;

            const stageData = nodes[stage.id];

            const status =
              stageData?.status || "healthy";


            return (

              <div
                key={stage.id}
                className="
                  rounded-2xl
                  border border-slate-800
                  bg-slate-900/60
                  p-5
                  transition
                  hover:border-cyan-500/30
                "
              >

                <div className="flex items-center justify-between">

                  <div className="flex items-center gap-3">

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


                    <div>

                      <p className="text-sm font-semibold text-white">
                        {stage.label}
                      </p>

                      <p className="text-xs text-slate-500">
                        {stage.subtitle}
                      </p>

                    </div>

                  </div>


                  <span
                    className={`
                      text-[10px]
                      font-semibold
                      uppercase
                      tracking-wider
                      ${getStatusClass(status)}
                    `}
                  >
                    {status}
                  </span>

                </div>


                <div className="mt-5 flex items-end justify-between">

                  <div>

                    <p className="text-[10px] uppercase tracking-wider text-slate-600">
                      {stageData?.metricLabel || "Status"}
                    </p>

                    <p className="mt-1 text-lg font-semibold text-slate-200">
                      {stageData?.metric || "Operational"}
                    </p>

                  </div>


                  {index < pipelineStages.length - 1 && (
                    <ArrowRight
                      className="
                        h-4 w-4
                        text-slate-700
                      "
                    />
                  )}

                </div>

              </div>

            );

          })}

        </div>

      </div>


      {/* =====================================
          LIVE PROCESSING INFORMATION
      ====================================== */}

      <div
        className="
          rounded-2xl
          border border-slate-800
          bg-slate-900/60
          p-6
        "
      >

        <div className="flex items-center gap-3">

          <Clock3 className="h-5 w-5 text-cyan-400" />

          <div>

            <h2 className="text-sm font-semibold text-white">
              Live Processing Monitor
            </h2>

            <p className="mt-1 text-xs text-slate-500">
              Metrics are simulated and refreshed automatically every 3 seconds.
            </p>

          </div>

        </div>


        <div
          className="
            mt-5
            grid
            grid-cols-2
            gap-4
            md:grid-cols-4
          "
        >

          <div>

            <p className="text-[10px] uppercase tracking-wider text-slate-600">
              Records Processed
            </p>

            <p className="mt-1 text-sm font-semibold text-white">
              {(metrics.processed / 1000000).toFixed(2)}M
            </p>

          </div>


          <div>

            <p className="text-[10px] uppercase tracking-wider text-slate-600">
              Flink Latency
            </p>

            <p className="mt-1 text-sm font-semibold text-white">
              {nodes.flink?.metric || "--"}
            </p>

          </div>


          <div>

            <p className="text-[10px] uppercase tracking-wider text-slate-600">
              Iceberg Writes
            </p>

            <p className="mt-1 text-sm font-semibold text-white">
              {nodes.iceberg?.metric || "--"}
            </p>

          </div>


          <div>

            <p className="text-[10px] uppercase tracking-wider text-slate-600">
              Quarantined
            </p>

            <p className="mt-1 text-sm font-semibold text-white">
              {nodes.dlq?.metric || "--"}
            </p>

          </div>

        </div>

      </div>

    </div>
  );
}


export default Pipeline;