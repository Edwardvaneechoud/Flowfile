import axios from "axios";
import { NodeTemplate } from "./types";
import { flowfileCorebaseURL } from "../../../config/constants";

// List of built-in icons that are bundled with the app
const BUILTIN_ICONS = new Set([
  'Output2.png',
  'airbyte.png',
  'cloud_storage_reader.png',
  'cloud_storage_writer.png',
  'cross_join.png',
  'database_reader.svg',
  'database_writer.svg',
  'explore_data.png',
  'external_source.png',
  'filter.png',
  'formula.png',
  'fuzzy_match.png',
  'google_sheet.png',
  'graph_solver.png',
  'group_by.png',
  'input_data.png',
  'join.png',
  'manual_input.png',
  'old_join.png',
  'output.png',
  'pivot.png',
  'polars_code.png',
  'record_count.png',
  'record_id.png',
  'sample.png',
  'select.png',
  'sort.png',
  'summarize.png',
  'text_to_rows.png',
  'union.png',
  'unique.png',
  'unpivot.png',
  'user-defined-icon.png',
  'view.png',
]);

// Default fallback icon
const DEFAULT_ICON = 'user-defined-icon.png';

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

  // If it's a built-in icon, use the static asset
  if (isBuiltinIcon(name)) {
    return new URL(`./assets/icons/${name}`, import.meta.url).href;
  }

  // Otherwise, it's a custom icon served from the backend API (use full URL)
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

export const fetchNodes = async (): Promise<NodeTemplate[]> => {
  const response = await axios.get("/node_list");
  const listNodes = response.data as NodeTemplate[];
  return listNodes;
};
