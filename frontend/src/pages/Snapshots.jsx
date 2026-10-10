import { useEffect, useState } from "react";
import {
  Camera,
  Clock3,
  Database,
  Archive,
  RefreshCw,
  ChevronRight,
  Layers,
  FileText,
  WifiOff,
} from "lucide-react";

import { fetchLakehouseSnapshots } from "../services/api";

function Snapshots() {
  const [snapshots, setSnapshots] = useState([]);
  const [selectedSnapshot, setSelectedSnapshot] = useState(null);
  const [tableFilter, setTableFilter] = useState(""); // "" | "clean_transactions" | "dlq_transactions"
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [refreshing, setRefreshing] = useState(false);

  const loadSnapshots = async (filter = tableFilter) => {
    try {
      const data = await fetchLakehouseSnapshots(filter || null);
      setSnapshots(data || []);
      setError(null);
      if (data && data.length > 0 && !selectedSnapshot) {
        setSelectedSnapshot(data[0]);
      }
    } catch (err) {
      setError(err.message || "Failed to fetch Lakehouse snapshots");
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    let isMounted = true;

    const run = async () => {
      try {
        const data = await fetchLakehouseSnapshots(tableFilter || null);
        if (!isMounted) return;
        setSnapshots(data || []);
        setError(null);
        if (data && data.length > 0) {
          setSelectedSnapshot((prev) => prev || data[0]);
        }
      } catch (err) {
        if (isMounted) {
          setError(err.message || "Failed to fetch snapshots");
        }
      } finally {
        if (isMounted) setLoading(false);
      }
    };

    run();
    const interval = setInterval(run, 5000);

    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, [tableFilter]);

  const handleFilterChange = (filter) => {
    setTableFilter(filter);
    setLoading(true);
    loadSnapshots(filter);
  };

  const handleManualRefresh = () => {
    setRefreshing(true);
    loadSnapshots(tableFilter);
  };

  const cleanSnapshotsCount = snapshots.filter((s) => s.table_name.includes("clean")).length;
  const dlqSnapshotsCount = snapshots.filter((s) => s.table_name.includes("dlq")).length;

  const formatTimestamp = (isoString) => {
    if (!isoString) return "N/A";
    const date = new Date(isoString);
    if (Number.isNaN(date.getTime())) return isoString;
    return date.toLocaleString();
  };

  return (
    <div className="space-y-6">
      {/* Backend connection error */}
      {error && (
        <div className="flex items-center gap-3 rounded-2xl border border-rose-500/40 bg-rose-500/10 p-4 text-rose-300">
          <WifiOff className="h-5 w-5 shrink-0 text-rose-400" />
          <div className="text-sm">
            <span className="font-semibold">Lakehouse API Error:</span> {error}
          </div>
        </div>
      )}

      {/* Header */}
      <div className="flex flex-col justify-between gap-4 sm:flex-row sm:items-center">
        <div>
          <p className="text-sm text-slate-500">
            Apache Iceberg committed table snapshots & metadata audit
          </p>
          <div className="mt-1 flex items-center gap-3">
            <h1 className="text-2xl font-bold text-white">Lakehouse Snapshots</h1>
            <span className="flex items-center gap-2 rounded-full bg-cyan-500/10 px-3 py-1 text-[10px] font-semibold tracking-wider text-cyan-400">
              <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-cyan-400" />
              LIVE ICEBERG CATALOG
            </span>
          </div>
        </div>

        <div className="flex items-center gap-3">
          {/* Table filter buttons */}
          <div className="flex rounded-xl border border-slate-800 bg-slate-900/60 p-1 text-xs">
            <button
              onClick={() => handleFilterChange("")}
              className={`rounded-lg px-3 py-1.5 font-medium transition ${
                tableFilter === "" ? "bg-cyan-500/20 text-cyan-400" : "text-slate-400 hover:text-white"
              }`}
            >
              All Tables
            </button>
            <button
              onClick={() => handleFilterChange("clean_transactions")}
              className={`rounded-lg px-3 py-1.5 font-medium transition ${
                tableFilter === "clean_transactions" ? "bg-cyan-500/20 text-cyan-400" : "text-slate-400 hover:text-white"
              }`}
            >
              Clean Table
            </button>
            <button
              onClick={() => handleFilterChange("dlq_transactions")}
              className={`rounded-lg px-3 py-1.5 font-medium transition ${
                tableFilter === "dlq_transactions" ? "bg-cyan-500/20 text-cyan-400" : "text-slate-400 hover:text-white"
              }`}
            >
              DLQ Table
            </button>
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
      </div>

      {/* Summary KPI Cards */}
      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        {/* Total Snapshots */}
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
          <div className="flex items-center justify-between">
            <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-cyan-500/10">
              <Camera className="h-5 w-5 text-cyan-400" />
            </div>
            <span className="text-[10px] uppercase tracking-wider text-slate-500">CATALOG TOTAL</span>
          </div>
          <p className="mt-5 text-sm text-slate-500">Committed Snapshots</p>
          <p className="mt-1 text-2xl font-bold text-white">{loading ? "..." : snapshots.length}</p>
        </div>

        {/* Clean Table Snapshots */}
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
          <div className="flex items-center justify-between">
            <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-emerald-500/10">
              <Database className="h-5 w-5 text-emerald-400" />
            </div>
            <span className="text-[10px] uppercase tracking-wider text-slate-500">CLEAN TABLE</span>
          </div>
          <p className="mt-5 text-sm text-slate-500">Clean Transactions Commits</p>
          <p className="mt-1 text-2xl font-bold text-emerald-400">{loading ? "..." : cleanSnapshotsCount}</p>
        </div>

        {/* DLQ Table Snapshots */}
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
          <div className="flex items-center justify-between">
            <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-amber-500/10">
              <Archive className="h-5 w-5 text-amber-400" />
            </div>
            <span className="text-[10px] uppercase tracking-wider text-slate-500">DLQ TABLE</span>
          </div>
          <p className="mt-5 text-sm text-slate-500">Quarantine Commits</p>
          <p className="mt-1 text-2xl font-bold text-amber-400">{loading ? "..." : dlqSnapshotsCount}</p>
        </div>
      </div>

      {/* Main Snapshots Grid: List on Left, Inspector on Right */}
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        {/* Snapshot History List */}
        <div className="lg:col-span-2 rounded-2xl border border-slate-800 bg-slate-900/60 overflow-hidden">
          <div className="border-b border-slate-800 px-6 py-5 flex items-center justify-between">
            <div>
              <h2 className="text-lg font-semibold text-white">Committed Snapshots</h2>
              <p className="mt-1 text-sm text-slate-500">
                Sorted newest commit first from Iceberg catalog metadata
              </p>
            </div>
            <span className="rounded-full bg-slate-800 px-3 py-1 text-xs text-slate-400">
              {snapshots.length} commits
            </span>
          </div>

          <div className="divide-y divide-slate-800 max-h-[580px] overflow-y-auto">
            {loading ? (
              <div className="p-12 text-center text-slate-500 text-sm">
                Loading Iceberg table commit history...
              </div>
            ) : snapshots.length === 0 ? (
              <div className="p-12 text-center">
                <Camera className="mx-auto h-8 w-8 text-slate-700" />
                <p className="mt-3 text-sm text-slate-400">No snapshots committed yet</p>
                <p className="mt-1 text-xs text-slate-600">
                  Snapshots are automatically created when the Iceberg writer commits batches.
                </p>
              </div>
            ) : (
              snapshots.map((snap) => {
                const isSelected = selectedSnapshot?.snapshot_id === snap.snapshot_id;
                const isClean = snap.table_name.includes("clean");
                const addedRecords =
                  snap.summary?.["added-records"] || snap.summary?.["total-records"] || "—";

                return (
                  <div
                    key={snap.snapshot_id}
                    onClick={() => setSelectedSnapshot(snap)}
                    className={`cursor-pointer px-6 py-4 transition flex items-center justify-between ${
                      isSelected ? "bg-cyan-500/10 border-l-4 border-cyan-400" : "hover:bg-slate-800/40"
                    }`}
                  >
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center gap-2">
                        <span
                          className={`rounded px-2 py-0.5 text-[10px] font-semibold tracking-wider uppercase ${
                            isClean ? "bg-emerald-500/10 text-emerald-400" : "bg-amber-500/10 text-amber-400"
                          }`}
                        >
                          {snap.table_name.replace("lakehouse.", "")}
                        </span>

                        <span className="rounded bg-slate-800 px-2 py-0.5 text-[10px] font-mono text-slate-300">
                          ID: {String(snap.snapshot_id).slice(0, 12)}...
                        </span>

                        {snap.operation && (
                          <span className="rounded bg-cyan-500/10 px-2 py-0.5 text-[10px] font-mono text-cyan-300 uppercase">
                            {snap.operation}
                          </span>
                        )}
                      </div>

                      <div className="mt-2 flex items-center gap-4 text-xs text-slate-400">
                        <span className="flex items-center gap-1.5">
                          <Clock3 className="h-3.5 w-3.5 text-slate-500" />
                          {formatTimestamp(snap.committed_at)}
                        </span>
                        <span>•</span>
                        <span>
                          Rows: <strong className="text-white">{addedRecords}</strong>
                        </span>
                      </div>
                    </div>

                    <ChevronRight
                      className={`h-5 w-5 shrink-0 ${isSelected ? "text-cyan-400" : "text-slate-600"}`}
                    />
                  </div>
                );
              })
            )}
          </div>
        </div>

        {/* Snapshot Detail Inspector */}
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-6 flex flex-col justify-between">
          <div>
            <div className="flex items-center gap-2 border-b border-slate-800 pb-4">
              <Layers className="h-5 w-5 text-cyan-400" />
              <h3 className="font-semibold text-white">Snapshot Details</h3>
            </div>

            {selectedSnapshot ? (
              <div className="mt-5 space-y-4 text-xs">
                <div>
                  <span className="text-slate-500 block uppercase tracking-wider text-[10px]">
                    Snapshot ID
                  </span>
                  <span className="mt-1 block font-mono text-sm text-cyan-300 break-all select-all">
                    {selectedSnapshot.snapshot_id}
                  </span>
                </div>

                <div>
                  <span className="text-slate-500 block uppercase tracking-wider text-[10px]">
                    Table Name
                  </span>
                  <span className="mt-1 block font-mono text-slate-200">
                    {selectedSnapshot.table_name}
                  </span>
                </div>

                <div>
                  <span className="text-slate-500 block uppercase tracking-wider text-[10px]">
                    Committed At (UTC)
                  </span>
                  <span className="mt-1 block text-slate-200">
                    {selectedSnapshot.committed_at}
                  </span>
                </div>

                <div>
                  <span className="text-slate-500 block uppercase tracking-wider text-[10px]">
                    Operation
                  </span>
                  <span className="mt-1 block font-mono text-emerald-400 uppercase">
                    {selectedSnapshot.operation || "append"}
                  </span>
                </div>

                <div>
                  <span className="text-slate-500 block uppercase tracking-wider text-[10px]">
                    Parent Snapshot ID
                  </span>
                  <span className="mt-1 block font-mono text-slate-400">
                    {selectedSnapshot.parent_snapshot_id || "Root Snapshot (None)"}
                  </span>
                </div>

                {/* PyIceberg Summary Metadata Object */}
                <div className="pt-2">
                  <span className="text-slate-500 block uppercase tracking-wider text-[10px] mb-2 flex items-center gap-1.5">
                    <FileText className="h-3.5 w-3.5 text-slate-400" />
                    PyIceberg Summary Properties
                  </span>
                  <div className="rounded-xl bg-slate-950 p-3 font-mono text-[11px] text-slate-300 space-y-1 overflow-x-auto max-h-48">
                    {Object.entries(selectedSnapshot.summary || {}).map(([key, val]) => (
                      <div key={key} className="flex justify-between gap-2">
                        <span className="text-slate-500">{key}:</span>
                        <span className="text-cyan-300">{String(val)}</span>
                      </div>
                    ))}
                    {Object.keys(selectedSnapshot.summary || {}).length === 0 && (
                      <span className="text-slate-600">No summary properties recorded.</span>
                    )}
                  </div>
                </div>
              </div>
            ) : (
              <div className="py-12 text-center text-slate-500 text-xs">
                Select a snapshot to inspect its Iceberg commit metadata.
              </div>
            )}
          </div>

          <div className="mt-6 border-t border-slate-800 pt-4 text-[11px] text-slate-500 leading-relaxed">
            Snapshots represent immutable point-in-time state commits in the Apache Iceberg table.
          </div>
        </div>
      </div>
    </div>
  );
}

export default Snapshots;