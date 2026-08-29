import { useEffect, useState } from "react";


// =========================================
// DEFAULT SETTINGS
// =========================================

const DEFAULT_SETTINGS = {
  errorThreshold: 2.0,
  qualityThreshold: 97.8,
  snapshotInterval: 30,
  autoRefresh: true,
};


// =========================================
// READ SETTINGS FROM LOCAL STORAGE
// =========================================

function getSettings() {
  try {
    const saved = localStorage.getItem(
      "icestream-settings"
    );

    if (!saved) {
      return DEFAULT_SETTINGS;
    }

    return {
      ...DEFAULT_SETTINGS,
      ...JSON.parse(saved),
    };

  } catch (error) {
    console.error(
      "Unable to read IceStream settings:",
      error
    );

    return DEFAULT_SETTINGS;
  }
}


// =========================================
// GENERATE PIPELINE STATE
// =========================================

function generatePipelineState(settings) {

  const throughput = Math.floor(
    11500 + Math.random() * 2000
  );


  const latency = Math.floor(
    40 + Math.random() * 25
  );


  const quality = Number(
    (97 + Math.random() * 2.5).toFixed(1)
  );


  const errorRate = Number(
    (0.3 + Math.random() * 2.2).toFixed(1)
  );


  const writes = Math.floor(
    1800 + Math.random() * 700
  );


  const quarantined = Math.floor(
    Math.random() * 6
  );


  // =========================================
  // USE SAVED SETTINGS
  // =========================================

  const circuitBreakerActive =
    errorRate >= settings.errorThreshold ||
    quality < settings.qualityThreshold ||
    quarantined >= 4;


  // =========================================
  // NODE STATUS
  // =========================================

  const kafkaStatus = "healthy";


  const flinkStatus =
    latency > 58
      ? "warning"
      : "healthy";


  const qualityStatus =
    quality < settings.qualityThreshold
      ? "warning"
      : "healthy";


  const icebergStatus = "healthy";


  const dlqStatus =
    quarantined >= 4
      ? "quarantined"
      : "healthy";


  return {

    // =======================================
    // GLOBAL METRICS
    // =======================================

    metrics: {

      throughput,

      quality,

      errorRate,

      processed:
        Math.floor(
          1200000 +
          Math.random() * 50000
        ),

    },


    // =======================================
    // PIPELINE NODES
    // =======================================

    nodes: {

      kafka: {

        status: kafkaStatus,

        metricLabel: "Throughput",

        metric:
          `${(throughput / 1000).toFixed(1)}K records/s`,

      },


      flink: {

        status: flinkStatus,

        metricLabel: "Latency",

        metric:
          `${latency} ms`,

      },


      quality: {

        status: qualityStatus,

        metricLabel: "Quality Score",

        metric:
          `${quality}%`,

      },


      iceberg: {

        status: icebergStatus,

        metricLabel: "Writes",

        metric:
          `${(writes / 1000).toFixed(1)}K/s`,

      },


      dlq: {

        status: dlqStatus,

        metricLabel: "Quarantined",

        metric:
          quarantined === 0
            ? "0 records"
            : `${quarantined} records`,

      },

    },


    // =======================================
    // CIRCUIT BREAKER
    // =======================================

    circuitBreaker: {

      active: circuitBreakerActive,

      errorRate,

      threshold:
        settings.errorThreshold,

    },


    // =======================================
    // CURRENT SETTINGS
    // =======================================

    settings,

  };

}


// =========================================
// CUSTOM HOOK
// =========================================

export function usePipelineSimulation() {

  const [settings, setSettings] = useState(
    getSettings
  );


  const [state, setState] = useState(() =>
    generatePipelineState(
      getSettings()
    )
  );


  // =========================================
  // SIMULATION REFRESH
  // =========================================

  useEffect(() => {

    if (!settings.autoRefresh) {
      return;
    }


    const interval = setInterval(() => {

      const latestSettings =
        getSettings();


      setSettings(latestSettings);


      setState(
        generatePipelineState(
          latestSettings
        )
      );

    }, 3000);


    return () =>
      clearInterval(interval);

  }, [settings.autoRefresh]);


  // =========================================
  // LISTEN FOR SETTINGS CHANGES
  // =========================================

  useEffect(() => {

    const handleStorageChange = () => {

      const latestSettings =
        getSettings();


      setSettings(
        latestSettings
      );


      setState(
        generatePipelineState(
          latestSettings
        )
      );

    };


    window.addEventListener(
      "storage",
      handleStorageChange
    );


    return () => {

      window.removeEventListener(
        "storage",
        handleStorageChange
      );

    };

  }, []);


  return state;
}