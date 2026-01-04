import axios from "axios";
import { NodeTemplate } from "./types";

// List of built-in icons that are bundled with the app
const BUILTIN_ICONS = new Set([
  'default.png',
  'user-defined-icon.png',
  'add_id_column.png',
  'aggregate.png',
  'cast.png',
  'cross_join.png',
  'custom_formula.png',
  'drop_duplicates.png',
  'external_source.png',
  'filter.png',
  'formula.png',
  'fuzzy_match.png',
  'group_by.png',
  'import_file.png',
  'interval_join.png',
  'join.png',
  'limit.png',
  'macro_input.png',
  'macro_output.png',
  'melt.png',
  'mock.png',
  'multi_input.png',
  'output.png',
  'pivot.png',
  'record_id.png',
  'remove_columns.png',
  'rename_columns.png',
  'sample.png',
  'select.png',
  'sort.png',
  'text_transformation.png',
  'transpose.png',
  'union.png',
  'unique_values.png',
  'write_file.png',
  'write_output.png',
  'write_to_database.png',
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
  if (!name) {
    return new URL(`./assets/icons/default.png`, import.meta.url).href;
  }

  // If it's a built-in icon, use the static asset
  if (isBuiltinIcon(name)) {
    return new URL(`./assets/icons/${name}`, import.meta.url).href;
  }

  // Otherwise, it's a custom icon served from the backend API
  return `/user_defined_components/icon/${name}`;
};

/**
 * Get the URL for a custom icon from the backend API
 */
export const getCustomIconUrl = (name: string): string => {
  return `/user_defined_components/icon/${name}`;
};

export const fetchNodes = async (): Promise<NodeTemplate[]> => {
  const response = await axios.get("/node_list");
  const listNodes = response.data as NodeTemplate[];
  return listNodes;
};
