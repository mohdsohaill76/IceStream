import {
  LayoutDashboard,
  Workflow,
  ShieldCheck,
  AlertTriangle,
  History,
  Settings,
  Activity,
} from "lucide-react";

import { NavLink } from "react-router-dom";

const menuItems = [
  {
    name: "Overview",
    icon: LayoutDashboard,
    path: "/",
  },
  {
    name: "Pipeline",
    icon: Workflow,
    path: "/pipeline",
  },
  {
    name: "Data Quality",
    icon: ShieldCheck,
    path: "/quality",
  },
  {
    name: "Incidents",
    icon: AlertTriangle,
    path: "/incidents",
  },
  {
    name: "Snapshots",
    icon: History,
    path: "/snapshots",
  },
];

function Sidebar() {
  return (
    <aside
      className="
        fixed left-0 top-0 z-40
        flex h-screen w-64 flex-col
        border-r border-slate-800
        bg-slate-950
      "
    >

      {/* =====================================
          LOGO
      ====================================== */}

      <div
        className="
          flex h-20 items-center gap-3
          border-b border-slate-800
          px-6
        "
      >

        <div
          className="
            flex h-10 w-10
            items-center justify-center
            rounded-xl
            bg-cyan-500/10
          "
        >
          <Activity className="h-6 w-6 text-cyan-400" />
        </div>

        <div>
          <h1 className="text-lg font-bold tracking-wide text-white">
            IceStream
          </h1>

          <p className="text-[10px] uppercase tracking-widest text-slate-500">
            Data Observability
          </p>
        </div>

      </div>


      {/* =====================================
          NAVIGATION
      ====================================== */}

      <nav className="flex-1 px-4 py-6">

        <p
          className="
            mb-3 px-3
            text-[10px]
            font-semibold
            uppercase
            tracking-widest
            text-slate-500
          "
        >
          Monitoring
        </p>


        <div className="space-y-1">

          {menuItems.map((item) => {

            const Icon = item.icon;

            return (
              <NavLink
                key={item.name}
                to={item.path}
                end={item.path === "/"}
                className={({ isActive }) =>
                  `
                  group
                  relative
                  flex w-full
                  items-center gap-3
                  rounded-lg
                  px-3 py-3
                  text-sm
                  transition-all
                  duration-200

                  ${
                    isActive
                      ? "bg-cyan-500/10 text-cyan-400"
                      : "text-slate-400 hover:bg-slate-900 hover:text-white"
                  }
                  `
                }
              >

                {({ isActive }) => (
                  <>

                    {/* Active left indicator */}

                    {isActive && (
                      <span
                        className="
                          absolute left-0
                          h-6 w-0.5
                          rounded-r-full
                          bg-cyan-400
                        "
                      />
                    )}


                    {/* Icon */}

                    <Icon
                      className={`
                        h-5 w-5
                        transition-all
                        duration-200

                        ${
                          isActive
                            ? "text-cyan-400"
                            : "text-slate-500 group-hover:text-cyan-400"
                        }
                      `}
                    />


                    {/* Name */}

                    <span>
                      {item.name}
                    </span>


                    {/* Active dot */}

                    {isActive && (
                      <span
                        className="
                          ml-auto
                          h-1.5 w-1.5
                          rounded-full
                          bg-cyan-400
                          shadow-[0_0_8px_rgba(34,211,238,0.8)]
                        "
                      />
                    )}

                  </>
                )}

              </NavLink>
            );

          })}

        </div>

      </nav>


      {/* =====================================
          SETTINGS
      ====================================== */}

      <div
        className="
          border-t
          border-slate-800
          p-4
        "
      >

        <NavLink
          to="/settings"
          className={({ isActive }) =>
            `
            group
            relative
            flex w-full
            items-center gap-3
            rounded-lg
            px-3 py-3
            text-sm
            transition-all
            duration-200

            ${
              isActive
                ? "bg-cyan-500/10 text-cyan-400"
                : "text-slate-400 hover:bg-slate-900 hover:text-white"
            }
            `
          }
        >

          {({ isActive }) => (
            <>

              {/* Active left indicator */}

              {isActive && (
                <span
                  className="
                    absolute left-0
                    h-6 w-0.5
                    rounded-r-full
                    bg-cyan-400
                  "
                />
              )}


              {/* Icon */}

              <Settings
                className={`
                  h-5 w-5
                  transition-colors

                  ${
                    isActive
                      ? "text-cyan-400"
                      : "text-slate-500 group-hover:text-cyan-400"
                  }
                `}
              />


              {/* Text */}

              <span>
                Settings
              </span>


              {/* Active dot */}

              {isActive && (
                <span
                  className="
                    ml-auto
                    h-1.5 w-1.5
                    rounded-full
                    bg-cyan-400
                    shadow-[0_0_8px_rgba(34,211,238,0.8)]
                  "
                />
              )}

            </>
          )}

        </NavLink>

      </div>

    </aside>
  );
}

export default Sidebar;