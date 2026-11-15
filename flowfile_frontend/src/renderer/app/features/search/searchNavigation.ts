import type { RouteLocationRaw } from "vue-router";
import type { SearchHit } from "../../types/search.types";

/** The catalog item a route opens, which the catalog page finds in its cached overview. */
export interface CatalogTarget {
  kind: "namespace" | "table" | "flow" | "schedule" | "artifact";
  id: number;
}

export type SearchHitAction =
  | { type: "route"; to: RouteLocationRaw; catalogTarget?: CatalogTarget }
  | { type: "notebook"; notebookId: number }
  | { type: "designer"; flowPath: string; name: string; catalogRef?: string; catalogId: number };

export interface SecondaryAction {
  label: string;
  action: SearchHitAction;
}

const catalog = (query: Record<string, string>): RouteLocationRaw => ({ name: "catalog", query });

const inCatalog = (
  query: Record<string, string>,
  kind: CatalogTarget["kind"],
  id: number | null,
): SearchHitAction => ({
  type: "route",
  to: catalog(query),
  ...(id !== null ? { catalogTarget: { kind, id } } : {}),
});

const numericId = (value: unknown): number | null => {
  if (value === null || value === undefined || value === "") return null;
  const n = typeof value === "number" ? value : Number(value);
  return Number.isFinite(n) ? n : null;
};

/** Where a hit opens on Enter or click: the page that shows that one item. */
export function primaryAction(hit: SearchHit): SearchHitAction {
  const id = String(hit.id);
  switch (hit.kind) {
    case "namespace": {
      // The catalog page's detail is a catalog's; a schema opens its catalog (the catalog refresh still looks for
      // the schema itself, which may be newer than the page's tree).
      const catalogId = hit.details.level === 0 ? numericId(hit.id) : numericId(hit.namespace_id);
      const shown = catalogId ?? numericId(hit.id);
      return inCatalog(
        { tab: "catalog", namespaceId: String(shown) },
        "namespace",
        numericId(hit.id),
      );
    }
    case "table":
      return inCatalog({ tab: "catalog", tableId: id }, "table", numericId(hit.id));
    case "column": {
      const tableId = numericId(hit.details.table_id) ?? numericId(hit.parent?.id);
      return inCatalog({ tab: "catalog", tableId: String(tableId) }, "table", tableId);
    }
    case "flow":
      return inCatalog({ tab: "catalog", flowId: id }, "flow", numericId(hit.id));
    case "schedule":
      return inCatalog({ tab: "schedules", scheduleId: id }, "schedule", numericId(hit.id));
    case "artifact":
      return inCatalog({ tab: "catalog", artifactId: id }, "artifact", numericId(hit.id));
    case "notebook":
      return { type: "notebook", notebookId: Number(hit.id) };
    case "visualization":
      return { type: "route", to: catalog({ tab: "visuals", vizId: id }) };
    case "dashboard":
      return { type: "route", to: { name: "dashboard-view", params: { id } } };
    case "kernel":
      return { type: "route", to: { name: "compute", query: { tab: "kernels", kernelId: id } } };
    case "custom_node": {
      const fileName = hit.details.file_name;
      // Mounted folders are read-only, so those open in the catalog's list instead of the designer.
      if (!hit.details.mounted && typeof fileName === "string" && fileName) {
        return { type: "route", to: { name: "nodeDesigner", query: { openFile: fileName } } };
      }
      return { type: "route", to: catalog({ tab: "customNodes" }) };
    }
  }
}

/** The catalog reference the designer records for a flow: `catalog.schema.flow`. */
export function flowCatalogRef(hit: SearchHit): string | undefined {
  return hit.path.length ? `${hit.path.join(".")}.${hit.name}` : undefined;
}

/** A second way to open a hit (Mod+Enter), when there is one: a flow opens in the designer. */
export function secondaryAction(hit: SearchHit): SecondaryAction | null {
  if (hit.kind === "flow" && typeof hit.details.flow_path === "string" && hit.details.flow_path) {
    return {
      label: "Open in designer",
      action: {
        type: "designer",
        flowPath: hit.details.flow_path,
        name: hit.name,
        catalogRef: flowCatalogRef(hit),
        catalogId: Number(hit.id),
      },
    };
  }
  if (hit.kind === "schedule" && hit.parent) {
    const flowId = String(hit.parent.id);
    return {
      label: "Open flow",
      action: inCatalog({ tab: "catalog", flowId }, "flow", numericId(hit.parent.id)),
    };
  }
  if ((hit.kind === "column" || hit.kind === "visualization") && hit.parent?.kind === "table") {
    const tableId = String(hit.parent.id);
    return {
      label: "Open table",
      action: inCatalog({ tab: "catalog", tableId }, "table", numericId(hit.parent.id)),
    };
  }
  return null;
}
