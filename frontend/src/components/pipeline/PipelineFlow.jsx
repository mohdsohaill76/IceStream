import { useCallback, useEffect } from "react";

import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  addEdge,
} from "@xyflow/react";

import "@xyflow/react/dist/style.css";

import PipelineNode from "./PipelineNode";


const nodeTypes = {
  pipeline: PipelineNode,
};


/* =========================================
   PIPELINE CONNECTIONS
========================================= */

const initialEdges = [
  {
    id: "kafka-flink",
    source: "kafka",
    target: "flink",
    animated: true,
  },

  {
    id: "flink-quality",
    source: "flink",
    target: "quality",
    animated: true,
  },

  {
    id: "quality-iceberg",
    source: "quality",
    target: "iceberg",
    animated: true,
  },

  {
    id: "quality-dlq",
    source: "quality",
    target: "dlq",
    animated: true,
  },
];


/* =========================================
   PIPELINE FLOW
========================================= */

function PipelineFlow({ pipeline }) {

  /* -----------------------------------------
     Protect against missing pipeline data
  ------------------------------------------ */

  const safePipeline = pipeline || {};


  /* -----------------------------------------
     Initial Nodes
  ------------------------------------------ */

  const createNodes = () => [

    {
      id: "kafka",

      type: "pipeline",

      position: {
        x: 50,
        y: 180,
      },

      data: {
        type: "kafka",
        ...(safePipeline.kafka || {}),
      },
    },


    {
      id: "flink",

      type: "pipeline",

      position: {
        x: 340,
        y: 180,
      },

      data: {
        type: "flink",
        ...(safePipeline.flink || {}),
      },
    },


    {
      id: "quality",

      type: "pipeline",

      position: {
        x: 630,
        y: 180,
      },

      data: {
        type: "quality",
        ...(safePipeline.quality || {}),
      },
    },


    {
      id: "iceberg",

      type: "pipeline",

      position: {
        x: 930,
        y: 100,
      },

      data: {
        type: "iceberg",
        ...(safePipeline.iceberg || {}),
      },
    },


    {
      id: "dlq",

      type: "pipeline",

      position: {
        x: 930,
        y: 300,
      },

      data: {
        type: "dlq",
        ...(safePipeline.dlq || {}),
      },
    },

  ];


  /* -----------------------------------------
     React Flow State
  ------------------------------------------ */

  const [nodes, setNodes, onNodesChange] =
    useNodesState(createNodes());

  const [edges, setEdges, onEdgesChange] =
    useEdgesState(initialEdges);


  /* =========================================
     LIVE NODE UPDATE
  ========================================== */

  useEffect(() => {

    if (!pipeline) {
      return;
    }


    setNodes((currentNodes) =>

      currentNodes.map((node) => {

        const liveData = pipeline[node.id];

        if (!liveData) {
          return node;
        }


        return {
          ...node,

          data: {
            ...node.data,
            ...liveData,
          },
        };

      })

    );

  }, [pipeline, setNodes]);


  /* =========================================
     CONNECTION HANDLER
  ========================================== */

  const onConnect = useCallback(
    (connection) => {

      setEdges((currentEdges) =>

        addEdge(
          {
            ...connection,
            animated: true,
          },
          currentEdges
        )

      );

    },
    [setEdges]
  );


  /* =========================================
     RENDER
  ========================================== */

  return (

    <div className="h-[520px] w-full overflow-hidden rounded-2xl border border-slate-800 bg-slate-950">

      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}

        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}

        fitView

        fitViewOptions={{
          padding: 0.2,
        }}

        minZoom={0.5}
        maxZoom={1.5}

        proOptions={{
          hideAttribution: true,
        }}
      >

        {/* Background */}

        <Background
          gap={24}
          size={1}
          className="opacity-30"
        />


        {/* Zoom Controls */}

        <Controls />


        {/* Mini Map */}

        <MiniMap
          nodeColor={(node) => {

            const status = node.data?.status;

            if (status === "warning") {
              return "#f59e0b";
            }

            if (
              status === "critical" ||
              status === "quarantined"
            ) {
              return "#ef4444";
            }

            return "#10b981";

          }}

          maskColor="rgba(2, 6, 23, 0.7)"
        />

      </ReactFlow>

    </div>

  );
}


export default PipelineFlow;