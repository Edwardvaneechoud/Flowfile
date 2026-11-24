// What the catalog writer offers when its target schema belongs to a catalog kept elsewhere (an Iceberg REST catalog).
import type { NamespaceTree } from "../../../../../types/catalog.types";
import type { CatalogWriteMode, CatalogWriteSettings } from "../../../../../types/node.types";

/** The write modes such a catalog takes: the table is created when missing, then appended to or replaced. */
export const OUTSIDE_WRITE_MODES: readonly CatalogWriteMode[] = ["overwrite", "append"];

/** The writer's options besides the mode that such a catalog does not take. */
export type OutsideOption = "track_changes" | "partition_by";

type Refusable = CatalogWriteMode | OutsideOption;

/** The drawer's label of each refused choice; core's ``outside_write_refusal`` names them the same. */
const REFUSED_LABELS: Partial<Record<Refusable, string>> = {
  error: "Error if exists",
  upsert: "Upsert",
  update: "Update",
  delete: "Delete",
  scd2: "SCD2",
  virtual: "Virtual Table",
  track_changes: "Track changes",
  partition_by: "Partition by",
};

const takes = (catalog: string): string =>
  `${catalog} keeps this table. Flowfile can Append to it or Overwrite it`;

const MODE_LABELS: Record<CatalogWriteMode, string> = {
  overwrite: "Overwrite",
  error: "Error if exists",
  append: "Append",
  upsert: "Upsert",
  update: "Update",
  delete: "Delete",
  scd2: "SCD2",
  virtual: "Virtual Table",
};

export type WriterTargetSettings = Pick<
  CatalogWriteSettings,
  "source" | "namespace_id" | "namespace_full_name"
>;

/**
 * The catalog kept elsewhere that holds the writer's target schema, found in the catalog tree by the schema's id,
 * else by its ``catalog.schema`` name; null for a hub target, a local schema or one the tree lacks.
 */
export function outsideCatalogNode(
  tree: NamespaceTree[],
  settings: WriterTargetSettings | null | undefined,
): NamespaceTree | null {
  if (!settings || settings.source === "hub") return null;
  const { namespace_id: id, namespace_full_name: full } = settings;
  if (id == null && !full) return null;
  for (const catalog of tree) {
    if (catalog.source === "hub") continue;
    for (const schema of catalog.children ?? []) {
      const hit = id != null ? schema.id === id : `${catalog.name}.${schema.name}` === full;
      if (hit) return catalog.kept_elsewhere || schema.kept_elsewhere ? catalog : null;
    }
  }
  return null;
}

/** The name of the catalog ``outsideCatalogNode`` finds; null when it finds none. */
export function outsideCatalogName(
  tree: NamespaceTree[],
  settings: WriterTargetSettings | null | undefined,
): string | null {
  return outsideCatalogNode(tree, settings)?.name ?? null;
}

/** Whether a catalog kept elsewhere takes this write mode; every mode is taken for an ordinary target. */
export function outsideAllows(catalog: string | null, mode: CatalogWriteMode): boolean {
  return catalog === null || OUTSIDE_WRITE_MODES.includes(mode);
}

/**
 * Why a mode or option cannot be chosen for a target kept elsewhere, in core's words for a refused save or run, shown
 * only as the disabled control's hover title; null when it can.
 */
export function outsideRefusal(catalog: string | null, what: Refusable): string | null {
  if (catalog === null) return null;
  const label = REFUSED_LABELS[what];
  if (!label) return null;
  return `${takes(catalog)}; ${label} is not available there.`;
}

export interface OutsideFallback {
  /** The fields to set so the node takes only what the catalog allows; empty when nothing needs to change. */
  patch: Partial<CatalogWriteSettings>;
  /** What the drawer says about the change, in one sentence; null when nothing changed. */
  note: string | null;
}

/**
 * What a saved writer becomes when its target is kept elsewhere: a refused mode falls back to Overwrite, and
 * change tracking and partitioning are dropped, so the node never keeps a setting that fails at run time.
 */
export function outsideFallback(
  catalog: string | null,
  settings: Pick<
    CatalogWriteSettings,
    "write_mode" | "track_changes" | "partition_by" | "merge_keys"
  >,
): OutsideFallback {
  if (catalog === null) return { patch: {}, note: null };
  const patch: Partial<CatalogWriteSettings> = {};
  const modeRefused = !outsideAllows(catalog, settings.write_mode);
  if (modeRefused) {
    patch.write_mode = "overwrite";
    if (settings.merge_keys?.length) patch.merge_keys = [];
  }
  if (settings.track_changes) patch.track_changes = false;
  if (settings.partition_by?.length) patch.partition_by = [];
  if (modeRefused) {
    const from = MODE_LABELS[settings.write_mode];
    return {
      patch,
      note: `${takes(catalog)}, so this writer was set from ${from} to Overwrite.`,
    };
  }
  if (Object.keys(patch).length > 0) {
    return {
      patch,
      note: `${takes(catalog)}, so this writer no longer asks for Track changes or Partition by.`,
    };
  }
  return { patch, note: null };
}
