// Server-side rationale fallback rendering on the client.
//
// The planner emits ``arg_summary`` on every ``tool_call_*`` payload that
// already covers the same shapes; this module mirrors the backend's logic
// for consumers that want to render a tool call without round-tripping
// through the server (e.g. an optimistic-UI re-render of a queued call,
// or a debug-mode renderer that has the raw args but not the server's
// summary). Keep both implementations in lockstep when extending.
//
// All functions are pure — no Vue / Pinia imports — so the helpers can be
// unit-tested in vitest without a component harness.

const ADD_PREFIX = "flowfile.graph.add_";

export type ToolOpKind = "meta" | "graph" | "schema" | "codegen" | "unknown";

/**
 * Map a fully-qualified tool name to its op_kind. Mirrors
 * ``flowfile_core.ai.agents.planner._classify_op_kind``.
 *
 * Used by the chat trail to decide whether to render a tool_step at all
 * (meta ops are hidden) and to pick a CSS variant (graph vs schema vs
 * codegen).
 */
export const classifyOpKind = (toolName: string): ToolOpKind => {
  if (toolName.startsWith("flowfile.meta.")) return "meta";
  if (toolName.startsWith("flowfile.graph.")) return "graph";
  if (toolName.startsWith("flowfile.schema.")) return "schema";
  if (toolName.startsWith("flowfile.codegen.")) return "codegen";
  return "unknown";
};

const _isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const _formatColumnList = (value: unknown): string | null => {
  if (!Array.isArray(value)) return null;
  const names: string[] = [];
  for (const item of value) {
    if (typeof item === "string" && item) names.push(item);
    else if (_isRecord(item)) {
      const nm = item.name ?? item.column;
      if (typeof nm === "string" && nm) names.push(nm);
    }
  }
  return names.length ? names.join(", ") : null;
};

const _summaryForAddNode = (nodeType: string, args: Record<string, unknown>): string => {
  // The LLM's tool call args follow the per-node Pydantic settings schema
  // directly (e.g. NodeFilter has filter_input at the root); some surfaces
  // wrap them under settings_input. Try the wrapper first, fall back to root.
  const nested = args.settings_input;
  const settings: Record<string, unknown> = _isRecord(nested) ? nested : args;
  const prettyType = nodeType.replace(/_/g, " ");

  if (nodeType === "filter") {
    const predicate = settings.filter_input;
    if (_isRecord(predicate)) {
      const expr = predicate.advanced_filter ?? predicate.basic_filter;
      if (typeof expr === "string" && expr.trim()) {
        return `Filter on \`${expr.trim()}\``;
      }
    }
    return "Adding filter";
  }

  if (nodeType === "sort") {
    const cols = settings.sort_by;
    if (Array.isArray(cols) && cols.length > 0) {
      const names: string[] = [];
      for (const item of cols) {
        if (_isRecord(item)) {
          const nm = item.column;
          const direction = (item.how ?? item.direction ?? "asc") as string;
          if (typeof nm === "string" && nm) names.push(`${nm} ${direction}`);
        }
      }
      if (names.length) return `Sort by ${names.join(", ")}`;
    }
    return "Adding sort";
  }

  if (nodeType === "join") {
    const joinInput = settings.join_input;
    if (_isRecord(joinInput)) {
      const keys = joinInput.join_mapping ?? joinInput.join_keys;
      const how = typeof joinInput.how === "string" ? joinInput.how : "inner";
      if (Array.isArray(keys) && keys.length > 0) {
        const keyStrs: string[] = [];
        for (const k of keys) {
          if (_isRecord(k)) {
            const left = k.left_col ?? k.left;
            const right = k.right_col ?? k.right;
            if (typeof left === "string" && typeof right === "string") {
              keyStrs.push(`${left}=${right}`);
            }
          } else if (typeof k === "string") {
            keyStrs.push(k);
          }
        }
        if (keyStrs.length) {
          return `${how.charAt(0).toUpperCase() + how.slice(1)} join on ${keyStrs.join(", ")}`;
        }
      }
    }
    return "Adding join";
  }

  if (nodeType === "select") {
    const cols = _formatColumnList(settings.select_input);
    if (cols) return `Select columns: ${cols}`;
    return "Adding select";
  }

  if (
    nodeType === "formula" ||
    nodeType === "polars_code" ||
    nodeType === "python_script" ||
    nodeType === "sql_query"
  ) {
    const target = settings.function ?? settings.output_column;
    if (_isRecord(target)) {
      const field = target.field ?? target.column;
      if (typeof field === "string" && field) {
        return `Adding ${prettyType} → \`${field}\``;
      }
    }
    return `Adding ${prettyType}`;
  }

  if (nodeType === "group_by") {
    const cols = _formatColumnList(settings.group_by_input);
    if (cols) return `Group by ${cols}`;
    return "Adding group_by";
  }

  if (nodeType === "unique") {
    const cols = _formatColumnList(settings.unique_input);
    if (cols) return `Unique on ${cols}`;
    return "Adding unique";
  }

  if (nodeType === "union") return "Adding union";

  if (nodeType.startsWith("read_") || nodeType.endsWith("_source") || nodeType === "manual_input") {
    const path = settings.path ?? settings.file_path;
    const table = settings.table_name;
    if (typeof path === "string" && path) return `Reading from \`${path}\``;
    if (typeof table === "string" && table) return `Reading from \`${table}\``;
    return `Adding ${prettyType}`;
  }

  return `Adding ${prettyType}`;
};

/**
 * Best-effort one-line summary of a tool call's arguments.
 *
 * Returns ``null`` for meta ops (the UI hides them anyway) or when the
 * shape is too generic to produce useful text. Mirrors
 * ``flowfile_core.ai.agents.planner._arg_summary``.
 */
export const summarizeToolArgs = (
  toolName: string,
  rawArgs: Record<string, unknown> | null | undefined,
): string | null => {
  const args: Record<string, unknown> = _isRecord(rawArgs) ? rawArgs : {};

  if (toolName.startsWith(ADD_PREFIX)) {
    const nodeType = toolName.slice(ADD_PREFIX.length);
    return _summaryForAddNode(nodeType, args);
  }

  if (toolName === "flowfile.graph.connect") {
    const upstream = args.upstream_node_id ?? args.from_node_id;
    const downstream = args.downstream_node_id ?? args.to_node_id;
    if (typeof upstream === "number" && typeof downstream === "number") {
      return `Connecting node ${upstream} → node ${downstream}`;
    }
    return "Connecting nodes";
  }

  if (toolName === "flowfile.graph.delete_node") {
    const nid = args.node_id;
    if (typeof nid === "number") return `Removing node ${nid}`;
    return "Removing a node";
  }

  if (toolName === "flowfile.graph.delete_connection") {
    const upstream = args.upstream_node_id;
    const downstream = args.downstream_node_id;
    if (typeof upstream === "number" && typeof downstream === "number") {
      return `Disconnecting node ${upstream} ↛ node ${downstream}`;
    }
    return "Removing a connection";
  }

  if (toolName === "flowfile.schema.read_node_schema") {
    const nid = args.node_id;
    if (typeof nid === "number") return `Reading schema for node ${nid}`;
    return "Reading node schema";
  }

  if (toolName === "flowfile.schema.read_node_preview") {
    const nid = args.node_id;
    if (typeof nid === "number") return `Reading preview for node ${nid}`;
    return "Reading node preview";
  }

  if (toolName.startsWith("flowfile.codegen.")) {
    return `Generating code (${toolName.slice("flowfile.codegen.".length)})`;
  }

  if (toolName.startsWith("flowfile.meta.")) return null;

  return null;
};

/**
 * Predicate for the chat-trail filter: true when an event should be
 * suppressed from the user-visible timeline.
 *
 * - ``op_kind === "meta"`` SUCCESSFUL events are LLM-internal routing
 *   (``flowfile.meta.pick_category`` ✓, ``classify_intent`` ✓,
 *   ``pick_node_type`` ✓, ``pick_upstream`` ✓) and never appear in
 *   the user's chat — the host renders the resulting state via
 *   ``stage_advanced`` events or by narrowing the catalog.
 * - ``op_kind === "meta"`` REJECTED events are the user's only
 *   debugging signal when the LLM mis-fills a meta tool — e.g.
 *   llama-70b emitting ``upstream_node_ids`` as a string. We MUST
 *   show them so the refusal_detail surfaces in the chat trail;
 *   otherwise the user sees only "Retrying step (attempt 1 of 3)"
 *   with no clue why.
 * - ``info`` events without a message are server housekeeping
 *   (e.g. resume re-snapshots) — UI doesn't need to surface them
 *   either.
 */
export const isAgentEventHidden = (
  kind: string,
  payload: Record<string, unknown> | null | undefined,
): boolean => {
  const opKind = payload && typeof payload.op_kind === "string" ? payload.op_kind : "";
  if (opKind === "meta" && kind !== "tool_call_rejected") return true;
  if (kind === "info") {
    const message = payload && typeof payload.message === "string" ? payload.message : "";
    if (!message) return true;
  }
  return false;
};
