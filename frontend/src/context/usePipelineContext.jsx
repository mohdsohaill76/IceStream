import { createContext, useContext } from "react";

export const PipelineContext = createContext(null);

export function usePipelineContext() {
  const context = useContext(PipelineContext);

  if (!context) {
    throw new Error(
      "usePipelineContext must be used inside PipelineProvider"
    );
  }

  return context;
}