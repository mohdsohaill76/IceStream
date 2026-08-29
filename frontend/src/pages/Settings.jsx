import { useEffect, useState } from "react";
import {
  Settings as SettingsIcon,
  ShieldAlert,
  Database,
  Camera,
  Activity,
  Save,
  RotateCcw,
  CheckCircle,
} from "lucide-react";

function Settings() {
  // =========================================
  // DEFAULT CONFIGURATION
  // =========================================

  const defaultSettings = {
    errorThreshold: 2.0,
    qualityThreshold: 97.8,
    snapshotInterval: 30,
    autoRefresh: true,
  };

  // =========================================
  // SETTINGS STATE
  // =========================================

  const [settings, setSettings] = useState(() => {

    const saved = localStorage.getItem(
      "icestream-settings"
    );

    return saved
      ? JSON.parse(saved)
      : defaultSettings;

  });


  const [saved, setSaved] = useState(false);


  // =========================================
  // HANDLE INPUT
  // =========================================

  const handleChange = (key, value) => {

    setSettings((current) => ({
      ...current,
      [key]: value,
    }));

    setSaved(false);

  };


  // =========================================
  // SAVE SETTINGS
  // =========================================

  const handleSave = () => {

    localStorage.setItem(
      "icestream-settings",
      JSON.stringify(settings)
    );

    setSaved(true);

    setTimeout(() => {
      setSaved(false);
    }, 2500);

  };


  // =========================================
  // RESET SETTINGS
  // =========================================

  const handleReset = () => {

    setSettings(defaultSettings);

    localStorage.setItem(
      "icestream-settings",
      JSON.stringify(defaultSettings)
    );

    setSaved(false);

  };


  // =========================================
  // LOAD SETTINGS EVENT
  // =========================================

  useEffect(() => {

    const savedSettings =
      localStorage.getItem(
        "icestream-settings"
      );

    if (savedSettings) {
      setSettings(
        JSON.parse(savedSettings)
      );
    }

  }, []);


  return (

    <div className="space-y-6">


      {/* =====================================
          HEADER
      ====================================== */}

      <div>

        <p className="text-sm text-slate-500">
          Configure monitoring and pipeline protection
        </p>

        <div className="mt-1 flex items-center gap-3">

          <div
            className="
              flex h-9 w-9
              items-center justify-center
              rounded-xl
              bg-cyan-500/10
            "
          >

            <SettingsIcon
              className="
                h-5 w-5
                text-cyan-400
              "
            />

          </div>


          <h1 className="text-2xl font-bold text-white">
            Settings
          </h1>

        </div>

      </div>


      {/* =====================================
          SUCCESS MESSAGE
      ====================================== */}

      {saved && (

        <div
          className="
            flex items-center gap-3
            rounded-xl
            border border-emerald-500/20
            bg-emerald-500/5
            px-5 py-4
          "
        >

          <CheckCircle
            className="
              h-5 w-5
              text-emerald-400
            "
          />

          <div>

            <p className="text-sm font-medium text-emerald-400">
              Settings saved
            </p>

            <p className="text-xs text-slate-500">
              Your monitoring configuration has been updated.
            </p>

          </div>

        </div>

      )}


      {/* =====================================
          CIRCUIT BREAKER
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

          <div
            className="
              flex h-10 w-10
              items-center justify-center
              rounded-xl
              bg-red-500/10
            "
          >

            <ShieldAlert
              className="
                h-5 w-5
                text-red-400
              "
            />

          </div>


          <div>

            <h2 className="font-semibold text-white">
              Circuit Breaker
            </h2>

            <p className="text-xs text-slate-500">
              Automatic protection against pipeline failures
            </p>

          </div>

        </div>


        <div className="mt-6">

          <label className="text-sm text-slate-300">
            Error Rate Threshold
          </label>

          <p className="mt-1 text-xs text-slate-500">
            Activate protection when the error rate reaches this value.
          </p>


          <div className="mt-4 flex items-center gap-4">

            <input
              type="range"
              min="0.5"
              max="5"
              step="0.1"
              value={settings.errorThreshold}
              onChange={(event) =>
                handleChange(
                  "errorThreshold",
                  Number(event.target.value)
                )
              }
              className="
                h-1.5
                w-full
                cursor-pointer
                accent-cyan-400
              "
            />


            <div
              className="
                w-20
                rounded-lg
                border border-slate-800
                bg-slate-950
                px-3 py-2
                text-center
              "
            >

              <span className="text-sm font-semibold text-white">
                {settings.errorThreshold}%
              </span>

            </div>

          </div>

        </div>

      </div>


      {/* =====================================
          DATA QUALITY
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

          <div
            className="
              flex h-10 w-10
              items-center justify-center
              rounded-xl
              bg-emerald-500/10
            "
          >

            <Activity
              className="
                h-5 w-5
                text-emerald-400
              "
            />

          </div>


          <div>

            <h2 className="font-semibold text-white">
              Data Quality
            </h2>

            <p className="text-xs text-slate-500">
              Configure the minimum acceptable quality score
            </p>

          </div>

        </div>


        <div className="mt-6">

          <label className="text-sm text-slate-300">
            Minimum Quality Score
          </label>

          <p className="mt-1 text-xs text-slate-500">
            Values below this threshold will trigger a warning.
          </p>


          <div className="mt-4 flex items-center gap-4">

            <input
              type="range"
              min="90"
              max="100"
              step="0.1"
              value={settings.qualityThreshold}
              onChange={(event) =>
                handleChange(
                  "qualityThreshold",
                  Number(event.target.value)
                )
              }
              className="
                h-1.5
                w-full
                cursor-pointer
                accent-emerald-400
              "
            />


            <div
              className="
                w-20
                rounded-lg
                border border-slate-800
                bg-slate-950
                px-3 py-2
                text-center
              "
            >

              <span className="text-sm font-semibold text-white">
                {settings.qualityThreshold}%
              </span>

            </div>

          </div>

        </div>

      </div>


      {/* =====================================
          SNAPSHOT CONFIGURATION
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

          <div
            className="
              flex h-10 w-10
              items-center justify-center
              rounded-xl
              bg-cyan-500/10
            "
          >

            <Camera
              className="
                h-5 w-5
                text-cyan-400
              "
            />

          </div>


          <div>

            <h2 className="font-semibold text-white">
              Snapshot Configuration
            </h2>

            <p className="text-xs text-slate-500">
              Control automatic pipeline state capture
            </p>

          </div>

        </div>


        <div className="mt-6">

          <label className="text-sm text-slate-300">
            Snapshot Interval
          </label>

          <p className="mt-1 text-xs text-slate-500">
            Automatically capture the pipeline state at this interval.
          </p>


          <select
            value={settings.snapshotInterval}
            onChange={(event) =>
              handleChange(
                "snapshotInterval",
                Number(event.target.value)
              )
            }
            className="
              mt-4
              w-full
              rounded-xl
              border border-slate-800
              bg-slate-950
              px-4 py-3
              text-sm
              text-slate-200
              outline-none
              transition
              focus:border-cyan-500/40
            "
          >

            <option value={15}>
              Every 15 seconds
            </option>

            <option value={30}>
              Every 30 seconds
            </option>

            <option value={60}>
              Every 1 minute
            </option>

            <option value={120}>
              Every 2 minutes
            </option>

          </select>

        </div>

      </div>


      {/* =====================================
          LIVE REFRESH
      ====================================== */}

      <div
        className="
          rounded-2xl
          border border-slate-800
          bg-slate-900/60
          p-6
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

              <Database
                className="
                  h-5 w-5
                  text-cyan-400
                "
              />

            </div>


            <div>

              <h2 className="font-semibold text-white">
                Live Monitoring
              </h2>

              <p className="text-xs text-slate-500">
                Automatically refresh pipeline metrics
              </p>

            </div>

          </div>


          <button
            onClick={() =>
              handleChange(
                "autoRefresh",
                !settings.autoRefresh
              )
            }
            className={`
              relative
              h-6 w-11
              rounded-full
              transition
              ${
                settings.autoRefresh
                  ? "bg-cyan-500"
                  : "bg-slate-700"
              }
            `}
          >

            <span
              className={`
                absolute
                top-1
                h-4 w-4
                rounded-full
                bg-white
                transition
                ${
                  settings.autoRefresh
                    ? "left-6"
                    : "left-1"
                }
              `}
            />

          </button>

        </div>

      </div>


      {/* =====================================
          ACTIONS
      ====================================== */}

      <div
        className="
          flex
          flex-col-reverse
          gap-3
          sm:flex-row
          sm:justify-end
        "
      >

        <button
          onClick={handleReset}
          className="
            flex
            items-center
            justify-center
            gap-2
            rounded-xl
            border border-slate-800
            bg-slate-900
            px-5 py-3
            text-sm
            font-medium
            text-slate-400
            transition
            hover:bg-slate-800
            hover:text-white
          "
        >

          <RotateCcw className="h-4 w-4" />

          Reset Defaults

        </button>


        <button
          onClick={handleSave}
          className="
            flex
            items-center
            justify-center
            gap-2
            rounded-xl
            bg-cyan-500
            px-5 py-3
            text-sm
            font-semibold
            text-slate-950
            transition
            hover:bg-cyan-400
          "
        >

          <Save className="h-4 w-4" />

          Save Settings

        </button>

      </div>

    </div>

  );
}


export default Settings;