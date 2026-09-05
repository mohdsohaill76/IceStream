import {
  AlertTriangle,
  CheckCircle,
  Info,
  Clock,
} from "lucide-react";

function IncidentLog({ incidents }) {

  const getSeverityIcon = (severity) => {
    if (severity === "critical") {
      return (
        <AlertTriangle className="h-4 w-4 text-red-400" />
      );
    }

    if (severity === "warning") {
      return (
        <AlertTriangle className="h-4 w-4 text-amber-400" />
      );
    }

    return (
      <Info className="h-4 w-4 text-cyan-400" />
    );
  };


  const getSeverityStyle = (severity) => {

    if (severity === "critical") {
      return "bg-red-500/10 text-red-400";
    }

    if (severity === "warning") {
      return "bg-amber-500/10 text-amber-400";
    }

    return "bg-cyan-500/10 text-cyan-400";
  };


  const formatTime = (timestamp) => {
    return new Date(timestamp).toLocaleTimeString(
      [],
      {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      }
    );
  };


  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900/60">

      {/* Header */}
      <div className="flex items-center justify-between border-b border-slate-800 px-6 py-5">

        <div>

          <h2 className="text-lg font-semibold text-white">
            Incident Log
          </h2>

          <p className="mt-1 text-sm text-slate-500">
            Automatically detected pipeline events
          </p>

        </div>


        <div className="rounded-full bg-slate-800 px-3 py-1">

          <span className="text-xs text-slate-400">
            {incidents.length} events
          </span>

        </div>

      </div>


      {/* Incidents */}
      <div className="divide-y divide-slate-800">

        {incidents.length === 0 ? (

          <div className="flex flex-col items-center justify-center py-12">

            <CheckCircle className="h-8 w-8 text-emerald-400" />

            <p className="mt-3 text-sm text-slate-400">
              No incidents detected
            </p>

            <p className="mt-1 text-xs text-slate-600">
              All pipeline components are operating normally.
            </p>

          </div>

        ) : (

          incidents.map((incident) => (

            <div
              key={incident.id}
              className="px-6 py-4 transition hover:bg-slate-800/30"
            >

              <div className="flex items-start gap-4">

                {/* Severity */}
                <div
                  className={`
                    flex h-9 w-9
                    shrink-0
                    items-center justify-center
                    rounded-lg
                    ${getSeverityStyle(incident.severity)}
                  `}
                >
                  {getSeverityIcon(incident.severity)}
                </div>


                {/* Content */}
                <div className="min-w-0 flex-1">

                  <div className="flex flex-wrap items-center gap-2">

                    <h3 className="text-sm font-medium text-slate-200">
                      {incident.title}
                    </h3>

                    <span
                      className={`
                        rounded-full
                        px-2 py-0.5
                        text-[10px]
                        font-medium
                        uppercase
                        ${getSeverityStyle(incident.severity)}
                      `}
                    >
                      {incident.severity}
                    </span>

                  </div>


                  <p className="mt-1 text-xs text-slate-500">
                    {incident.message}
                  </p>


                  <div className="mt-2 flex flex-wrap items-center gap-4">

                    <span className="text-xs text-slate-600">
                      Component:{" "}
                      <span className="text-slate-400">
                        {incident.component}
                      </span>
                    </span>

                    <span className="text-xs text-slate-600">
                      Value:{" "}
                      <span className="text-slate-400">
                        {incident.value}
                      </span>
                    </span>

                    <span className="flex items-center gap-1 text-xs text-slate-600">

                      <Clock className="h-3 w-3" />

                      {formatTime(incident.timestamp)}

                    </span>

                  </div>

                </div>


                {/* Status */}
                <div className="shrink-0">

                  {incident.status === "open" ? (

                    <span className="rounded-full bg-red-500/10 px-2.5 py-1 text-[10px] font-medium text-red-400">
                      OPEN
                    </span>

                  ) : (

                    <span className="rounded-full bg-emerald-500/10 px-2.5 py-1 text-[10px] font-medium text-emerald-400">
                      RESOLVED
                    </span>

                  )}

                </div>

              </div>

            </div>

          ))

        )}

      </div>

    </div>
  );
}

export default IncidentLog;