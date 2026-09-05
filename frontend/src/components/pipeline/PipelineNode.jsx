import { Handle, Position } from "@xyflow/react";

import {
  Radio,
  Cpu,
  ShieldCheck,
  Database,
  Archive,
} from "lucide-react";


const nodeConfig = {
  kafka: {
    icon: Radio,
    label: "Kafka",
    subtitle: "Event Ingestion",
  },

  flink: {
    icon: Cpu,
    label: "Flink",
    subtitle: "Stream Processing",
  },

  quality: {
    icon: ShieldCheck,
    label: "Data Quality",
    subtitle: "Validation",
  },

  iceberg: {
    icon: Database,
    label: "Iceberg",
    subtitle: "Data Storage",
  },

  dlq: {
    icon: Archive,
    label: "DLQ",
    subtitle: "Quarantine",
  },
};


const statusConfig = {

  healthy: {
    label: "HEALTHY",
    dot: "bg-emerald-400",
    text: "text-emerald-400",
    border: "border-emerald-500/30",
    iconBg: "bg-emerald-500/10",
    glow: "shadow-emerald-500/10",
  },

  warning: {
    label: "WARNING",
    dot: "bg-amber-400",
    text: "text-amber-400",
    border: "border-amber-500/50",
    iconBg: "bg-amber-500/10",
    glow: "shadow-amber-500/10",
  },

  error: {
    label: "ERROR",
    dot: "bg-red-400",
    text: "text-red-400",
    border: "border-red-500/60",
    iconBg: "bg-red-500/10",
    glow: "shadow-red-500/10",
  },

  paused: {
    label: "PAUSED",
    dot: "bg-slate-400",
    text: "text-slate-400",
    border: "border-slate-500/40",
    iconBg: "bg-slate-500/10",
    glow: "shadow-slate-500/10",
  },

  quarantined: {
    label: "QUARANTINED",
    dot: "bg-orange-400",
    text: "text-orange-400",
    border: "border-orange-500/60",
    iconBg: "bg-orange-500/10",
    glow: "shadow-orange-500/10",
  },

};


function PipelineNode({ data }) {

  const config =
    nodeConfig[data?.type] || nodeConfig.kafka;

  const Icon = config.icon;

  const status =
    data?.status || "healthy";

  const currentStatus =
    statusConfig[status] || statusConfig.healthy;


  return (

    <div
      className={`
        relative
        w-56
        rounded-2xl
        border
        ${currentStatus.border}
        bg-slate-900/95
        px-5
        py-4
        shadow-xl
        ${currentStatus.glow}
        backdrop-blur
        transition-all
        duration-500
        hover:-translate-y-1
      `}
    >

      {/* =====================================
          INPUT HANDLE
      ====================================== */}

      {data?.type !== "kafka" && (

        <Handle
          type="target"
          position={Position.Left}
          className="
            !h-2
            !w-2
            !border-0
            !bg-cyan-400
          "
        />

      )}


      {/* =====================================
          OUTPUT HANDLE
      ====================================== */}

      {data?.type !== "dlq" && (

        <Handle
          type="source"
          position={Position.Right}
          className="
            !h-2
            !w-2
            !border-0
            !bg-cyan-400
          "
        />

      )}


      {/* =====================================
          HEADER
      ====================================== */}

      <div className="flex items-center justify-between">

        <div className="flex items-center gap-3">

          {/* Icon */}

          <div
            className={`
              flex
              h-10
              w-10
              items-center
              justify-center
              rounded-xl
              ${currentStatus.iconBg}
            `}
          >

            <Icon
              className={`
                h-5
                w-5
                ${currentStatus.text}
              `}
            />

          </div>


          {/* Name */}

          <div>

            <h3 className="font-semibold text-white">
              {config.label}
            </h3>

            <p className="text-[10px] text-slate-500">
              {config.subtitle}
            </p>

          </div>

        </div>


        {/* =================================
            STATUS INDICATOR
        ================================== */}

        <span className="relative flex h-2.5 w-2.5">

          <span
            className={`
              absolute
              inline-flex
              h-full
              w-full
              animate-ping
              rounded-full
              ${currentStatus.dot}
              opacity-50
            `}
          />

          <span
            className={`
              relative
              inline-flex
              h-2.5
              w-2.5
              rounded-full
              ${currentStatus.dot}
            `}
          />

        </span>

      </div>


      {/* =====================================
          METRIC
      ====================================== */}

      <div className="mt-4 flex items-end justify-between">

        <div>

          <p className="text-[10px] uppercase tracking-wider text-slate-600">
            {data?.metricLabel || "Status"}
          </p>

          <p className="mt-1 text-sm font-semibold text-slate-200">
            {data?.metric || "Operational"}
          </p>

        </div>


        {/* Status text */}

        <span
          className={`
            text-[9px]
            font-semibold
            tracking-wider
            ${currentStatus.text}
          `}
        >
          {currentStatus.label}
        </span>

      </div>


      {/* =====================================
          STATUS BAR
      ====================================== */}

      <div className="mt-4 h-1 overflow-hidden rounded-full bg-slate-800">

        <div
          className={`
            h-full
            rounded-full
            transition-all
            duration-700
            ${currentStatus.dot}
            ${
              status === "healthy"
                ? "w-full"
                : status === "warning"
                ? "w-2/3"
                : status === "quarantined"
                ? "w-1/3"
                : status === "error"
                ? "w-1/4"
                : "w-1/2"
            }
          `}
        />

      </div>

    </div>

  );
}


export default PipelineNode;