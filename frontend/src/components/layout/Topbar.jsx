import { Bell, Radio } from "lucide-react";

function Topbar() {
  return (
    <header className="flex h-20 items-center justify-between border-b border-slate-800 bg-slate-950/90 px-8 backdrop-blur">

      <div>
        <p className="text-xs uppercase tracking-widest text-slate-500">
          Real-Time Monitoring
        </p>

        <h2 className="mt-1 text-xl font-semibold text-white">
          Pipeline Overview
        </h2>
      </div>

      <div className="flex items-center gap-6">

        {/* Live indicator */}
        <div className="flex items-center gap-2 rounded-full border border-emerald-500/20 bg-emerald-500/5 px-4 py-2">
          <span className="relative flex h-2.5 w-2.5">
            <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-emerald-400 opacity-60"></span>
            <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-emerald-400"></span>
          </span>

          <Radio className="h-4 w-4 text-emerald-400" />

          <span className="text-xs font-medium text-emerald-400">
            SYSTEM OPERATIONAL
          </span>
        </div>

        {/* Notification */}
        <button className="relative rounded-lg p-2 text-slate-400 transition hover:bg-slate-900 hover:text-white">
          <Bell className="h-5 w-5" />

          <span className="absolute right-1 top-1 h-2 w-2 rounded-full bg-cyan-400"></span>
        </button>
      </div>
    </header>
  );
}

export default Topbar;