import { NodeTemplate } from "./types";
import { flowfileCorebaseURL } from "../../../config/constants";
import { fetchNodeTemplates } from "../../composables/useNodes";

// First-party node glyphs. Built-in nodes reference these .svg names directly
// (configs/node_store/nodes.py image="<name>"); SVG renders crisply in-app.
const SVG_NODE_ICONS = [
  "api_response.svg",
  "apply_model.svg",
  "catalog_reader.svg",
  "catalog_writer.svg",
  "cloud_storage_reader.svg",
  "cloud_storage_writer.svg",
  "cross_join.svg",
  "data_cleansing.svg",
  "database_reader.svg",
  "database_writer.svg",
  "dynamic_rename.svg",
  "evaluate_model.svg",
  "explore_data.svg",
  "external_source.svg",
  "filter.svg",
  "flow_input.svg",
  "flow_output.svg",
  "formula.svg",
  "fuzzy_match.svg",
  "gate.svg",
  "google_analytics.svg",
  "graph_solver.svg",
  "group_by.svg",
  "input_data.svg",
  "join.svg",
  "kafka_source.svg",
  "manual_input.svg",
  "output.svg",
  "pivot.svg",
  "polars_code.svg",
  "python_code.svg",
  "random_split.svg",
  "record_count.svg",
  "record_id.svg",
  "rest_api_reader.svg",
  "run_flow.svg",
  "sample.svg",
  "select.svg",
  "sort.svg",
  "sql_query.svg",
  "text_to_rows.svg",
  "train_model.svg",
  "union.svg",
  "unique.svg",
  "unpivot.svg",
  "wait_for.svg",
  "window_functions.svg",
];

// Brand/connector marks aren't offered as generic operation glyphs.
const NON_STANDARD_SVG = new Set(["google_analytics.svg", "kafka_source.svg"]);

const DEFAULT_ICON = "user-defined-icon.png"; // persisted contract key (state.py / codegen)
const DEFAULT_ICON_ASSET = "user-defined-icon.svg"; // bundled artwork for that key

// Standard glyphs offered to custom-node authors. These render as SVG in-app like
// every other node; the community store requires PNG, but that conversion is
// backend-only — publish.py maps a picked glyph to its bundled PNG twin in
// flowfile_core/.../community_nodes/standard_icons/. No PNG ships in this bundle.
export const STANDARD_NODE_ICONS: string[] = SVG_NODE_ICONS.filter(
  (name) => !NON_STANDARD_SVG.has(name),
).sort();

const BUILTIN_ICONS = new Set<string>([
  "airbyte.png",
  "google_sheet.png",
  "view.png",
  DEFAULT_ICON,
  ...SVG_NODE_ICONS,
]);

/**
 * Check if an icon is a built-in icon
 */
export const isBuiltinIcon = (name: string): boolean => {
  return BUILTIN_ICONS.has(name);
};

/**
 * Get the URL for a node icon
 * Returns a static asset URL for built-in icons, or an API URL for custom icons
 */
export const getImageUrl = (name: string): string => {
  if (!name || name === DEFAULT_ICON) {
    return new URL(`./assets/icons/${DEFAULT_ICON_ASSET}`, import.meta.url).href;
  }

  if (isBuiltinIcon(name)) {
    return new URL(`./assets/icons/${name}`, import.meta.url).href;
  }

  return `${flowfileCorebaseURL}user_defined_components/icon/${name}`;
};

/**
 * Get the URL for the default fallback icon
 */
export const getDefaultIconUrl = (): string => {
  return new URL(`./assets/icons/${DEFAULT_ICON_ASSET}`, import.meta.url).href;
};

/**
 * Get the URL for a custom icon from the backend API
 */
export const getCustomIconUrl = (name: string): string => {
  return `${flowfileCorebaseURL}user_defined_components/icon/${name}`;
};

export const fetchNodes = async (): Promise<NodeTemplate[]> => fetchNodeTemplates();
