import { BrowserRouter, Routes, Route } from "react-router-dom";

import Sidebar from "./components/layout/Sidebar";

import Dashboard from "./pages/Dashboard";
import Incidents from "./pages/Incidents";
import Pipeline from "./pages/Pipeline";
import DataQuality from "./pages/DataQuality";
import Snapshots from "./pages/Snapshots";
import Settings from "./pages/Settings";
import Quality from "./pages/Quality";
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

              <Route
                path="/"
                element={<Dashboard />}
              />

              <Route
                path="/pipeline"
                element={<Pipeline />}
              />

              <Route
                path="/quality"
                element={<DataQuality />}
              />

              <Route path="/quality" element={<Quality />} />

              <Route
                path="/incidents"
                element={<Incidents />}
              />

              <Route
                path="/snapshots"
                element={<Snapshots />}
              />

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