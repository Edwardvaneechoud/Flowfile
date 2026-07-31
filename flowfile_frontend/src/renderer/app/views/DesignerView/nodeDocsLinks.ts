// Maps a node type to its section in the published node reference, backing the
// "Read more" entry in the palette and canvas right-click menus.
//
// Keyed by `NodeTemplate.item`; the key set's source of truth is
// `flowfile_core/flowfile_core/configs/node_store/nodes.py`. Anchors are the
// heading slugs in `docs/users/visual-editor/nodes/*.md` — renaming a heading
// degrades to "right page, wrong scroll position", it does not 404.
//
// Do NOT derive the page from `node_group`: two nodes are deliberately
// documented on a page that disagrees with their palette group —
// `window_functions` (group `aggregate`) lives on transform.html, and
// `wait_for` (group `combine`) lives on ml.html.
import { docsUrl } from "../../lib/docsLinks";
import type { NodeTemplate } from "../../types";
import type { ContextMenuOption } from "../../components/common/ContextMenu/types";

const NODES_DIR = "users/visual-editor/nodes/";
const NODES_INDEX = `${NODES_DIR}index.html`;
const CUSTOM_NODES_PAGE = "users/visual-editor/creating-custom-nodes.html";

export const READ_MORE_ACTION = "docs";
export const READ_MORE_LABEL = "Read more";

export const NODE_DOC_PAGES: Readonly<Record<string, string>> = {
  // input.html
  read: "input.html#read-data",
  manual_input: "input.html#manual-input",
  database_reader: "input.html#database-reader",
  cloud_storage_reader: "input.html#cloud-storage-reader",
  catalog_reader: "input.html#catalog-reader",
  kafka_source: "input.html#kafka-source",
  google_analytics_reader: "input.html#google-analytics-reader",
  rest_api_reader: "input.html#rest-api-reader",
  flow_input: "input.html#flow-input",

  // transform.html
  formula: "transform.html#formula",
  select: "transform.html#select-data",
  dynamic_rename: "transform.html#rename-columns",
  filter: "transform.html#filter-data",
  sort: "transform.html#sort-data",
  record_id: "transform.html#add-record-id",
  sample: "transform.html#take-sample",
  unique: "transform.html#drop-duplicates",
  text_to_rows: "transform.html#text-to-rows",
  polars_code: "transform.html#polars-code",
  sql_query: "transform.html#sql-query",
  python_script: "transform.html#python-script",
  // Palette group is "aggregate", but the docs section lives on transform.html.
  window_functions: "transform.html#window-functions",

  // combine.html
  join: "combine.html#join",
  fuzzy_match: "combine.html#fuzzy-match",
  union: "combine.html#union-data",
  graph_solver: "combine.html#graph-solver",
  cross_join: "combine.html#cross-join",
  run_flow: "combine.html#run-flow",

  // aggregate.html
  group_by: "aggregate.html#group-by",
  pivot: "aggregate.html#pivot-data",
  unpivot: "aggregate.html#unpivot-data",
  record_count: "aggregate.html#count-records",

  // ml.html
  random_split: "ml.html#random-split",
  train_model: "ml.html#train-model",
  apply_model: "ml.html#apply-model",
  evaluate_model: "ml.html#evaluate-model",
  // Palette group is "combine", but the docs section lives on ml.html.
  wait_for: "ml.html#wait-for",

  // output.html
  output: "output.html#write-data",
  api_response: "output.html#api-response",
  explore_data: "output.html#explore-data",
  database_writer: "output.html#database-writer",
  catalog_writer: "output.html#catalog-writer",
  cloud_storage_writer: "output.html#cloud-storage-writer",
  flow_output: "output.html#flow-output",
};

/**
 * Resolve the docs URL for a node, always returning something reachable.
 *
 * Undocumented or unknown types fall back to the node-reference index rather
 * than hiding the menu entry: the palette menu has a single item, so hiding it
 * would render an empty box, and a node added to the backend without a map
 * entry degrades to a generic landing page instead of silently breaking.
 */
export function nodeDocsUrl(node: NodeTemplate | undefined | null): string {
  if (!node) return docsUrl(NODES_INDEX);
  // Checked before the map so a custom node whose author picked a built-in
  // item name (e.g. "filter") can't be sent to that built-in's docs.
  if (node.custom_node) return docsUrl(CUSTOM_NODES_PAGE);
  const page = NODE_DOC_PAGES[node.item];
  // typeof, not truthiness — neutralises prototype reads like item "constructor".
  return docsUrl(typeof page === "string" ? NODES_DIR + page : NODES_INDEX);
}

export function nodeDocsMenuOptions(): ContextMenuOption[] {
  return [{ label: READ_MORE_LABEL, action: READ_MORE_ACTION }];
}
