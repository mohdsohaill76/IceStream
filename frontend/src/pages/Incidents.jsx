import { useEffect, useState } from "react";
import {
  AlertTriangle,
  CheckCircle,
  Activity,
  RefreshCw,
  WifiOff,
} from "lucide-react";

import { usePipelineContext } from "../context/usePipelineContext";
import { fetchIncidents } from "../services/api";
import IncidentLog from "../components/incidents/IncidentLog";

function Incidents() {
  const liveState = usePipelineContext();
  const [incidents, setIncidents] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [refreshing, setRefreshing] = useState(false);

  const loadIncidents = async () => {
    try {
      const data = await fetchIncidents();
      // Normalize backend incident structure for the IncidentLog presentation
      const normalized = (data || []).map((inc) => ({
        id: inc.incident_id,
        incident_id: inc.incident_id,
        component: inc.stage ? inc.stage.toUpperCase() : "PIPELINE",
        severity: inc.severity === "high" ? "critical" : (inc.severity === "low" ? "info" : inc.severity),
        title: inc.message,
        description: `Stage: ${inc.stage} | Status: ${inc.status}`,
        metric: inc.incident_id,
        status: inc.status,
        timestamp: inc.timestamp,
        detectedAt: inc.timestamp,
      }));
      setIncidents(normalized);
      setError(null);
    } catch (err) {
      setError(err.message || "Failed to fetch incidents from backend");
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    let isMounted = true;

    const run = async () => {
      try {
        const data = await fetchIncidents();
        if (!isMounted) return;
        const normalized = (data || []).map((inc) => ({
          id: inc.incident_id,
          incident_id: inc.incident_id,
          component: inc.stage ? inc.stage.toUpperCase() : "PIPELINE",
          severity: inc.severity === "high" ? "critical" : (inc.severity === "low" ? "info" : inc.severity),
          title: inc.message,
          description: `Stage: ${inc.stage} | Status: ${inc.status}`,
          metric: inc.incident_id,
          status: inc.status,
          timestamp: inc.timestamp,
          detectedAt: inc.timestamp,
        }));
        setIncidents(normalized);
        setError(null);
      } catch (err) {
        if (isMounted) {
          setError(err.message || "Failed to fetch incidents");
        }
      } finally {
        if (isMounted) setLoading(false);
      }
    };

    run();
    const interval = setInterval(run, 3500);

    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, []);

  const handleManualRefresh = () => {
    setRefreshing(true);
    loadIncidents();
  };

  const openCount = incidents.filter((i) => i.status === "open").length;
  const criticalCount = incidents.filter((i) => i.severity === "critical").length;
  const systemHealthy = liveState?.systemStatus === "healthy" && openCount === 0;

  return (
    <div className="space-y-6">
      {/* Error alert banner */}
      {error && (
        <div className="flex items-center gap-3 rounded-2xl border border-rose-500/40 bg-rose-500/10 p-4 text-rose-300">
          <WifiOff className="h-5 w-5 shrink-0 text-rose-400" />
          <div className="text-sm">
            <span className="font-semibold">Incident API Error:</span> {error}
          </div>
        </div>
      )}

      {/* Header */}
      <div className="flex flex-col justify-between gap-4 sm:flex-row sm:items-center">
        <div>
          <p className="text-sm text-slate-500">
            Real-time pipeline alerts & Dead Letter Queue quarantines
          </p>
          <h1 className="mt-1 text-2xl font-bold text-white">
            Incidents
          </h1>
        </div>

        <button
          onClick={handleManualRefresh}
          disabled={refreshing}
          className="flex items-center gap-2 rounded-xl border border-slate-800 bg-slate-900/60 px-4 py-2 text-xs font-semibold text-slate-300 hover:bg-slate-800 transition"
        >
          <RefreshCw className={`h-3.5 w-3.5 ${refreshing ? "animate-spin text-cyan-400" : ""}`} />
          Refresh
        </button>
      </div>

      {/* Incident overview */}
      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        {/* Open incidents */}
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
          <div className="flex items-center justify-between">
            <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-amber-500/10">
              <AlertTriangle className="h-5 w-5 text-amber-400" />
            </div>
            <span className="text-xs text-slate-500">LIVE</span>
          </div>

          <p className="mt-5 text-sm text-slate-500">
            Active Incidents
          </p>
          <p className="mt-1 text-2xl font-bold text-white">
            {loading ? "..." : openCount}
          </p>
        </div>

        {/* Critical incidents */}
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
          <div className="flex items-center justify-between">
            <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-red-500/10">
              <AlertTriangle className="h-5 w-5 text-red-400" />
            </div>
            <span className="text-xs text-slate-500">HIGH PRIORITY</span>
          </div>

          <p className="mt-5 text-sm text-slate-500">
            Critical Severity
          </p>
          <p className="mt-1 text-2xl font-bold text-white">
            {loading ? "..." : criticalCount}
          </p>
        </div>

        {/* System health */}
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
          <div className="flex items-center justify-between">
            <div
              className={`flex h-10 w-10 items-center justify-center rounded-xl ${
                systemHealthy ? "bg-emerald-500/10" : "bg-red-500/10"
              }`}
            >
              <CheckCircle
                className={`h-5 w-5 ${
                  systemHealthy ? "text-emerald-400" : "text-red-400"
                }`}
              />
            </div>
            <Activity className="h-4 w-4 text-slate-600" />
          </div>

          <p className="mt-5 text-sm text-slate-500">
            Pipeline Incident Health
          </p>
          <p
            className={`mt-1 text-2xl font-bold ${
              systemHealthy ? "text-emerald-400" : "text-red-400"
            }`}
          >
            {systemHealthy ? "Operational" : "Attention Required"}
          </p>
        </div>
      </div>

      {/* Incident log table */}
      {loading ? (
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-12 text-center text-slate-500 text-sm">
          Loading operational incidents from backend...
        </div>
      ) : (
        <IncidentLog incidents={incidents} />
      )}
    </div>
  );
}

export default Incidents;