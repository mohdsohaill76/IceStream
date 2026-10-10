import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";

import Sidebar from "./components/layout/Sidebar";

import Dashboard from "./pages/Dashboard";
import Pipeline from "./pages/Pipeline";
import DataQuality from "./pages/DataQuality";
import Incidents from "./pages/Incidents";
import Snapshots from "./pages/Snapshots";
import Settings from "./pages/Settings";

function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-slate-950 text-white">

        {/* Sidebar */}
        <Sidebar />

        {/* Main Content */}
        <main className="ml-64 min-h-screen">
          <div className="p-6">

            <Routes>

              {/* Main Dashboard */}
              <Route
                path="/"
                element={<Dashboard />}
              />

              {/* Support direct navigation to /dashboard */}
              <Route
                path="/dashboard"
                element={<Navigate to="/" replace />}
              />

              {/* Pipeline */}
              <Route
                path="/pipeline"
                element={<Pipeline />}
              />

              {/* Data Quality */}
              <Route
                path="/quality"
                element={<DataQuality />}
              />

              {/* Incidents */}
              <Route
                path="/incidents"
                element={<Incidents />}
              />

              {/* Snapshots */}
              <Route
                path="/snapshots"
                element={<Snapshots />}
              />

              {/* Settings */}
              <Route
                path="/settings"
                element={<Settings />}
              />

            </Routes>

          </div>
        </main>

      </div>
    </BrowserRouter>
  );
}

export default App;