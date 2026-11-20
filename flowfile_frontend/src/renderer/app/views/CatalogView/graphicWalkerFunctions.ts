// Graphic Walker's conversion functions, loaded without mounting a chart builder.

import type { IChart } from "@kanaries/graphic-walker/interfaces";

import type { GraphicWalkerFunctions } from "./chartRequest";

/** The functions `chartRequest.ts` runs, plus the one that turns a chart into the query it runs. */
export interface GraphicWalkerToolkit extends GraphicWalkerFunctions {
  chartToWorkflow: (chart: IChart) => Record<string, unknown>;
}

type GraphicWalkerModule = typeof import("@kanaries/graphic-walker");

export const graphicWalkerToolkit = (gw: GraphicWalkerModule): GraphicWalkerToolkit => ({
  normalize: gw.normalize,
  expandTerse: gw.expandTerse,
  projectTerse: gw.projectTerse,
  parseShorthand: gw.parseShorthand,
  terseComputedFid: gw.terseComputedFid,
  chartToWorkflow: (chart) => gw.chartToWorkflow(chart) as unknown as Record<string, unknown>,
});

let loading: Promise<GraphicWalkerToolkit> | null = null;

/** The toolkit of the installed package; loaded once, and asked for again after a failed load. */
export const loadGraphicWalkerToolkit = (): Promise<GraphicWalkerToolkit> => {
  loading ??= import("@kanaries/graphic-walker").then(graphicWalkerToolkit).catch((error) => {
    loading = null;
    throw error;
  });
  return loading;
};
