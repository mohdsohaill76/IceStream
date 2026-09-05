import {
  useEffect,
  useRef,
  useState,
} from "react";

import {
  Camera,
  Clock3,
  Activity,
  ShieldCheck,
  AlertTriangle,
  Database,
  ChevronRight,
} from "lucide-react";

import { usePipelineSimulation } from "../hooks/usePipelineSimulation";


function Snapshots() {

  // =========================================
  // LIVE PIPELINE STATE
  // =========================================

  const liveState = usePipelineSimulation();

  // Keep the latest live state available
  // to the 30-second snapshot timer.
  const liveStateRef = useRef(liveState);


  useEffect(() => {
    liveStateRef.current = liveState;
  }, [liveState]);


  // =========================================
  // SNAPSHOT STATE
  // =========================================

  const [snapshots, setSnapshots] = useState([]);

  const [selectedSnapshot, setSelectedSnapshot] =
    useState(null);


  // =========================================
  // AUTOMATIC SNAPSHOT CREATION
  // =========================================

  useEffect(() => {

    const createSnapshot = () => {

      // Always use the latest pipeline state
      const state = liveStateRef.current;


      const snapshot = {

        id: Date.now(),

        timestamp: new Date().toISOString(),


        // -------------------------------
        // Pipeline metrics
        // -------------------------------

        metrics: {

          throughput:
            state.metrics.throughput,

          quality:
            state.metrics.quality,

          errorRate:
            state.metrics.errorRate,

          processed:
            state.metrics.processed,

        },


        // -------------------------------
        // Pipeline nodes
        // -------------------------------

        nodes: {

          kafka: {
            ...state.nodes.kafka,
          },

          flink: {
            ...state.nodes.flink,
          },

          quality: {
            ...state.nodes.quality,
          },

          iceberg: {
            ...state.nodes.iceberg,
          },

          dlq: {
            ...state.nodes.dlq,
          },

        },


        // -------------------------------
        // Circuit breaker
        // -------------------------------

        circuitBreaker: {
          ...state.circuitBreaker,
        },

      };


      // Add newest snapshot at the top
      setSnapshots((current) => {

        const updated = [
          snapshot,
          ...current,
        ];


        // Keep only latest 10 snapshots
        return updated.slice(0, 10);

      });

    };


    // =====================================
    // FIRST SNAPSHOT
    // =====================================

    createSnapshot();


    // =====================================
    // AUTOMATIC SNAPSHOT EVERY 30 SECONDS
    // =====================================

    const interval = setInterval(
      createSnapshot,
      30000
    );


    // Cleanup timer when page is removed
    return () => {
      clearInterval(interval);
    };

  }, []);


  // =========================================
  // FORMAT TIME
  // =========================================

  const formatTime = (timestamp) => {

    return new Date(timestamp).toLocaleTimeString();

  };


  // =========================================
  // QUALITY STATUS
  // =========================================

  const getQualityStatus = (quality) => {

    if (quality >= 98) {

      return {
        label: "Excellent",
        className: "text-emerald-400",
      };

    }


    if (quality >= 97) {

      return {
        label: "Acceptable",
        className: "text-amber-400",
      };

    }


    return {

      label: "Degraded",
      className: "text-red-400",

    };

  };


  // =========================================
  // RENDER
  // =========================================

  return (

    <div className="space-y-6">


      {/* =====================================
          HEADER
      ====================================== */}

      <div>

        <p className="text-sm text-slate-500">
          Historical pipeline states and system checkpoints
        </p>


        <div className="mt-1 flex items-center gap-3">

          <h1 className="text-2xl font-bold text-white">
            Snapshots
          </h1>


          <span
            className="
              flex items-center gap-2
              rounded-full
              bg-cyan-500/10
              px-3 py-1
              text-[10px]
              font-semibold
              tracking-wider
              text-cyan-400
            "
          >

            <span
              className="
                h-1.5 w-1.5
                animate-pulse
                rounded-full
                bg-cyan-400
              "
            />

            AUTO CAPTURE

          </span>

        </div>

      </div>


      {/* =====================================
          SUMMARY CARDS
      ====================================== */}

      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">


        {/* Stored Snapshots */}

        <div
          className="
            rounded-2xl
            border border-slate-800
            bg-slate-900/60
            p-5
          "
        >

          <div className="flex items-center justify-between">

            <div
              className="
                flex h-10 w-10
                items-center justify-center
                rounded-xl
                bg-cyan-500/10
              "
            >

              <Camera className="h-5 w-5 text-cyan-400" />

            </div>


            <span
              className="
                text-[10px]
                uppercase
                tracking-wider
                text-slate-600
              "
            >
              HISTORY
            </span>

          </div>


          <p className="mt-5 text-sm text-slate-500">
            Stored Snapshots
          </p>


          <p className="mt-1 text-2xl font-bold text-white">
            {snapshots.length}
          </p>

        </div>


        {/* Current Quality */}

        <div
          className="
            rounded-2xl
            border border-slate-800
            bg-slate-900/60
            p-5
          "
        >

          <div className="flex items-center justify-between">

            <div
              className="
                flex h-10 w-10
                items-center justify-center
                rounded-xl
                bg-emerald-500/10
              "
            >

              <ShieldCheck
                className="h-5 w-5 text-emerald-400"
              />

            </div>


            <span
              className="
                text-[10px]
                uppercase
                tracking-wider
                text-slate-600
              "
            >
              CURRENT
            </span>

          </div>


          <p className="mt-5 text-sm text-slate-500">
            Current Quality
          </p>


          <p className="mt-1 text-2xl font-bold text-emerald-400">
            {liveState.metrics.quality}%
          </p>

        </div>


        {/* Circuit Breaker */}

        <div
          className="
            rounded-2xl
            border border-slate-800
            bg-slate-900/60
            p-5
          "
        >

          <div className="flex items-center justify-between">

            <div
              className={`
                flex h-10 w-10
                items-center justify-center
                rounded-xl
                ${
                  liveState.circuitBreaker.active
                    ? "bg-red-500/10"
                    : "bg-emerald-500/10"
                }
              `}
            >

              <AlertTriangle
                className={`
                  h-5 w-5
                  ${
                    liveState.circuitBreaker.active
                      ? "text-red-400"
                      : "text-emerald-400"
                  }
                `}
              />

            </div>


            <span
              className="
                text-[10px]
                uppercase
                tracking-wider
                text-slate-600
              "
            >
              PROTECTION
            </span>

          </div>


          <p className="mt-5 text-sm text-slate-500">
            Circuit Breaker
          </p>


          <p
            className={`
              mt-1 text-2xl font-bold
              ${
                liveState.circuitBreaker.active
                  ? "text-red-400"
                  : "text-emerald-400"
              }
            `}
          >

            {liveState.circuitBreaker.active
              ? "ACTIVE"
              : "NORMAL"}

          </p>

        </div>

      </div>


      {/* =====================================
          SNAPSHOT HISTORY
      ====================================== */}

      <div
        className="
          rounded-2xl
          border border-slate-800
          bg-slate-900/60
        "
      >

        {/* History Header */}

        <div
          className="
            border-b border-slate-800
            px-6 py-5
          "
        >

          <h2 className="text-lg font-semibold text-white">
            Snapshot History
          </h2>


          <p className="mt-1 text-sm text-slate-500">
            Automatically captured pipeline states
          </p>

        </div>


        {/* Snapshot List */}

        <div className="divide-y divide-slate-800">

          {snapshots.length === 0 ? (

            <div className="px-6 py-12 text-center">

              <Camera
                className="
                  mx-auto
                  h-8 w-8
                  text-slate-700
                "
              />


              <p className="mt-3 text-sm text-slate-500">
                Waiting for the first snapshot...
              </p>

            </div>

          ) : (

            snapshots.map((snapshot) => {

              const qualityStatus =
                getQualityStatus(
                  snapshot.metrics.quality
                );


              return (

                <button
                  key={snapshot.id}
                  onClick={() =>
                    setSelectedSnapshot(snapshot)
                  }
                  className="
                    flex
                    w-full
                    items-center
                    justify-between
                    px-6 py-5
                    text-left
                    transition
                    hover:bg-slate-900
                  "
                >

                  {/* Snapshot name */}

                  <div className="flex items-center gap-4">

                    <div
                      className="
                        flex h-10 w-10
                        items-center justify-center
                        rounded-xl
                        bg-slate-800
                      "
                    >

                      <Clock3
                        className="
                          h-5 w-5
                          text-slate-400
                        "
                      />

                    </div>


                    <div>

                      <p className="text-sm font-medium text-slate-200">
                        Pipeline Snapshot
                      </p>


                      <p className="mt-1 text-xs text-slate-500">
                        {formatTime(snapshot.timestamp)}
                      </p>

                    </div>

                  </div>


                  {/* Snapshot metrics */}

                  <div
                    className="
                      hidden
                      items-center
                      gap-8
                      md:flex
                    "
                  >

                    {/* Throughput */}

                    <div>

                      <p
                        className="
                          text-[10px]
                          uppercase
                          tracking-wider
                          text-slate-600
                        "
                      >
                        Throughput
                      </p>


                      <p className="mt-1 text-xs font-medium text-white">
                        {snapshot.metrics.throughput.toLocaleString()}/s
                      </p>

                    </div>


                    {/* Quality */}

                    <div>

                      <p
                        className="
                          text-[10px]
                          uppercase
                          tracking-wider
                          text-slate-600
                        "
                      >
                        Quality
                      </p>


                      <p
                        className={`
                          mt-1
                          text-xs
                          font-medium
                          ${qualityStatus.className}
                        `}
                      >
                        {snapshot.metrics.quality}%
                      </p>

                    </div>


                    {/* Error rate */}

                    <div>

                      <p
                        className="
                          text-[10px]
                          uppercase
                          tracking-wider
                          text-slate-600
                        "
                      >
                        Error Rate
                      </p>


                      <p className="mt-1 text-xs font-medium text-white">
                        {snapshot.metrics.errorRate}%
                      </p>

                    </div>


                    <ChevronRight
                      className="
                        h-4 w-4
                        text-slate-600
                      "
                    />

                  </div>

                </button>

              );

            })

          )}

        </div>

      </div>


      {/* =====================================
          SELECTED SNAPSHOT DETAILS
      ====================================== */}

      {selectedSnapshot && (

        <div
          className="
            rounded-2xl
            border border-cyan-500/20
            bg-slate-900/70
            p-6
          "
        >

          {/* Details Header */}

          <div className="flex items-center justify-between">

            <div>

              <h2 className="text-lg font-semibold text-white">
                Snapshot Details
              </h2>


              <p className="mt-1 text-xs text-slate-500">
                Captured at{" "}
                {formatTime(
                  selectedSnapshot.timestamp
                )}
              </p>

            </div>


            <button
              onClick={() =>
                setSelectedSnapshot(null)
              }
              className="
                text-xs
                text-slate-500
                transition
                hover:text-white
              "
            >
              Close
            </button>

          </div>


          {/* Detail Metrics */}

          <div
            className="
              mt-6
              grid
              grid-cols-1
              gap-4
              md:grid-cols-4
            "
          >

            {/* Throughput */}

            <div
              className="
                rounded-xl
                border border-slate-800
                bg-slate-950/50
                p-4
              "
            >

              <Activity className="h-4 w-4 text-cyan-400" />


              <p className="mt-3 text-xs text-slate-500">
                Throughput
              </p>


              <p className="mt-1 text-lg font-bold text-white">
                {selectedSnapshot.metrics.throughput.toLocaleString()}
              </p>

            </div>


            {/* Quality */}

            <div
              className="
                rounded-xl
                border border-slate-800
                bg-slate-950/50
                p-4
              "
            >

              <ShieldCheck className="h-4 w-4 text-emerald-400" />


              <p className="mt-3 text-xs text-slate-500">
                Quality
              </p>


              <p className="mt-1 text-lg font-bold text-emerald-400">
                {selectedSnapshot.metrics.quality}%
              </p>

            </div>


            {/* Error Rate */}

            <div
              className="
                rounded-xl
                border border-slate-800
                bg-slate-950/50
                p-4
              "
            >

              <AlertTriangle className="h-4 w-4 text-amber-400" />


              <p className="mt-3 text-xs text-slate-500">
                Error Rate
              </p>


              <p className="mt-1 text-lg font-bold text-white">
                {selectedSnapshot.metrics.errorRate}%
              </p>

            </div>


            {/* Records Processed */}

            <div
              className="
                rounded-xl
                border border-slate-800
                bg-slate-950/50
                p-4
              "
            >

              <Database className="h-4 w-4 text-cyan-400" />


              <p className="mt-3 text-xs text-slate-500">
                Records Processed
              </p>


              <p className="mt-1 text-lg font-bold text-white">
                {
                  (
                    selectedSnapshot.metrics.processed /
                    1000000
                  ).toFixed(2)
                }M
              </p>

            </div>

          </div>

        </div>

      )}

    </div>

  );
}


export default Snapshots;