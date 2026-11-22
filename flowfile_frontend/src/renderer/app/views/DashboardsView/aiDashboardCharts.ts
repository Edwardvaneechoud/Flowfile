// Turns a chart request for a catalog table into a Graphic Walker chart and the query it runs.

import type { IChart, IMutField } from "@kanaries/graphic-walker/interfaces";

import { CatalogApi } from "../../api/catalog.api";
import { chartFromRequest, type ChartRequest } from "../CatalogView/chartRequest";
import {
  loadGraphicWalkerToolkit,
  type GraphicWalkerToolkit,
} from "../CatalogView/graphicWalkerFunctions";

export type ConvertedChart =
  | { ok: true; chart: IChart; payload: Record<string, unknown> | null }
  | { ok: false; error: string };

export type ChartConverter = (tableId: number, request: ChartRequest) => Promise<ConvertedChart>;

export interface ChartConverterDeps {
  loadToolkit: () => Promise<GraphicWalkerToolkit>;
  loadFields: (tableId: number) => Promise<IMutField[]>;
}

const tableFields = async (tableId: number): Promise<IMutField[]> => {
  const result = await CatalogApi.getVisualizationFields({
    source_type: "table",
    table_id: tableId,
  });
  if (result.error) throw new Error(result.error);
  return result.fields as IMutField[];
};

export const NOT_PREPARED = "The chart could not be prepared in this browser.";

/** A converter for one build: each table's fields are read once. Never rejects. */
export function createChartConverter(
  deps: ChartConverterDeps = { loadToolkit: loadGraphicWalkerToolkit, loadFields: tableFields },
): ChartConverter {
  const fields = new Map<number, Promise<IMutField[]>>();
  const fieldsOf = (tableId: number): Promise<IMutField[]> => {
    let pending = fields.get(tableId);
    if (!pending) {
      pending = deps.loadFields(tableId).catch((error) => {
        fields.delete(tableId);
        throw error;
      });
      fields.set(tableId, pending);
    }
    return pending;
  };

  return async (tableId, request) => {
    try {
      const [gw, tableFieldList] = await Promise.all([deps.loadToolkit(), fieldsOf(tableId)]);
      const built = chartFromRequest(request, { fields: tableFieldList, current: null, gw });
      if (!built.ok) return { ok: false, error: built.error };
      let payload: Record<string, unknown> | null = null;
      try {
        payload = gw.chartToWorkflow(built.chart);
      } catch {
        payload = null;
      }
      return { ok: true, chart: built.chart, payload };
    } catch {
      return { ok: false, error: NOT_PREPARED };
    }
  };
}
