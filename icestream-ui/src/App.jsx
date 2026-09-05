import { BrowserRouter, Routes, Route } from "react-router-dom";

import Sidebar from "./components/layout/Sidebar";

import Dashboard from "./pages/Dashboard";
import Pipeline from "./pages/Pipeline";
import DataQuality from "./pages/DataQuality";
import Incidents from "./pages/Incidents";

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

              <Route
                path="/incidents"
                element={<Incidents />}
              />

            </Routes>

          </div>
        </main>

      </div>
    </BrowserRouter>
  );
}

export default App;