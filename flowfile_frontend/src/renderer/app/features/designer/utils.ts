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

// Standard glyphs offered to custom-node authors ship as PNG twins: the community
// store requires PNG icons, so a published node's node_icon must be a .png. Each
// twin sits next to its .svg here (bundled for the renderer) and is also copied to
// flowfile_core/.../community_nodes/standard_icons/ (the bytes publish.py ships).
export const STANDARD_NODE_ICONS: string[] = SVG_NODE_ICONS.filter(
  (name) => !NON_STANDARD_SVG.has(name),
)
  .map((name) => name.replace(/\.svg$/, ".png"))
  .sort();

const BUILTIN_ICONS = new Set<string>([
  "airbyte.png",
  "google_sheet.png",
  "view.png",
  DEFAULT_ICON,
  ...SVG_NODE_ICONS,
  ...STANDARD_NODE_ICONS,
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
