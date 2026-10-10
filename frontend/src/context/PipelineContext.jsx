import { PipelineContext } from "./usePipelineContext";
import { usePipelineSimulation } from "../hooks/usePipelineSimulation";

export function PipelineProvider({ children }) {
  const pipelineState = usePipelineSimulation();

  return (
    <PipelineContext.Provider value={pipelineState}>
      {children}
    </PipelineContext.Provider>
  );
}