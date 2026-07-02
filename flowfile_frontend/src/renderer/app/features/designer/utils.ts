import { NodeTemplate } from "./types";
import { flowfileCorebaseURL } from "../../../config/constants";
import { fetchNodeTemplates } from "../../composables/useNodes";

const BUILTIN_ICONS = new Set([
  "airbyte.png",
  "catalog_reader.svg",
  "catalog_writer.svg",
  "api_response.svg",
  "kafka_source.svg",
  "google_analytics.svg",
  "rest_api_reader.svg",
  "database_reader.svg",
  "database_writer.svg",
  "dynamic_rename.svg",
  "google_sheet.png",
  "python_code.svg",
  "sql_query.svg",
  "random_split.svg",
  "user-defined-icon.png",
  "view.png",
  "window_functions.svg",
  "train_model.svg",
  "apply_model.svg",
  "evaluate_model.svg",
  "wait_for.svg",
  "cloud_storage_reader.svg",
  "cloud_storage_writer.svg",
  "cross_join.svg",
  "explore_data.svg",
  "external_source.svg",
  "filter.svg",
  "formula.svg",
  "fuzzy_match.svg",
  "graph_solver.svg",
  "group_by.svg",
  "input_data.svg",
  "join.svg",
  "manual_input.svg",
  "output.svg",
  "pivot.svg",
  "polars_code.svg",
  "record_count.svg",
  "record_id.svg",
  "sample.svg",
  "select.svg",
  "sort.svg",
  "text_to_rows.svg",
  "union.svg",
  "unique.svg",
  "unpivot.svg",
  "run_flow.svg",
]);

const DEFAULT_ICON = "user-defined-icon.png";

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
  if (!name) {
    return new URL(`./assets/icons/${DEFAULT_ICON}`, import.meta.url).href;
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
  return new URL(`./assets/icons/${DEFAULT_ICON}`, import.meta.url).href;
};

/**
 * Get the URL for a custom icon from the backend API
 */
export const getCustomIconUrl = (name: string): string => {
  return `${flowfileCorebaseURL}user_defined_components/icon/${name}`;
};

export const fetchNodes = async (): Promise<NodeTemplate[]> => fetchNodeTemplates();
