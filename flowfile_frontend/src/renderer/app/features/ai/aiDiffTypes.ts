// W35 — TS mirrors of the W41 `GraphDiff` Pydantic shapes.
//
// The wire layer (`aiDiffClient.ts`) deliberately keeps payloads as
// `Record<string, unknown>` so the W41 client stays generic. The diff
// renderer needs typed access to `staged_node_payload` fields, so we
// project each W31 staged shape into a TS interface here.
//
// Field names stay snake_case to match the wire contract — there is no
// adapter between the W41 stage response and the renderer, so the
// trade-off is "snake_case in templates" vs "duplicate the same dict
// twice with two name conventions". Snake_case wins; W34/W18 normalise
// at the boundary because they bridge two long-lived surfaces, but the
// diff payloads here are short-lived and read by exactly one component.

import type {
  AcceptDiffResponse,
  RejectDiffResponse,
  StageDiffRequest,
} from "../../services/aiDiffClient";

export interface StagedInsertionContext {
  upstream_node_ids: number[];
  right_input_node_id: number | null;
  pos_x: number;
  pos_y: number;
}

export interface StagedSchemaColumn {
  name: string;
  data_type: string | null;
  nullable: boolean | null;
}

export interface StagedAddition {
  node_type: string;
  settings: Record<string, unknown>;
  insertion_context: StagedInsertionContext;
  predicted_output_schema: StagedSchemaColumn[] | null;
  audit_id: number | null;
}

export interface StagedConnection {
  connection: Record<string, unknown>;
  audit_id: number | null;
}

export interface StagedDeletion {
  delete_node_id: number;
  audit_id: number | null;
}

/**
 * The renderer's view of a staged `GraphDiff`. Matches the four W41
 * buckets in their apply order. Synthesised client-side from the
 * `StageDiffRequest` body the user just posted, since the backend
 * exposes no `GET /ai/diff/{id}` endpoint.
 */
export interface GraphDiffPayload {
  diff_id: string;
  session_id: string;
  flow_id: number;
  rationale: string | null;
  additions: StagedAddition[];
  connections_added: StagedConnection[];
  deletions: StagedDeletion[];
  connections_removed: StagedConnection[];
}

export interface DriftDetail {
  kind: "drift";
  status: 409;
  message: string;
  missingNodeIds: number[];
}

export interface HttpErrorDetail {
  kind: "http";
  status: number;
  message: string;
}

export type DiffStoreError = DriftDetail | HttpErrorDetail;

/**
 * Build a renderer-shaped diff from the inputs that produced a
 * `stageDiff` request. The W41 backend returns only `{diff_id, op_count}`
 * so the client reconstructs the payload from the request body it
 * already has. `_bin_staged_results` mirrors W41's
 * `diff_routes.py::_bin_staged_results` for parity.
 */
export const synthesiseDiffFromStageRequest = (
  request: StageDiffRequest,
  diffId: string,
): GraphDiffPayload => {
  const additions: StagedAddition[] = [];
  const connectionsAdded: StagedConnection[] = [];
  const deletions: StagedDeletion[] = [];
  const connectionsRemoved: StagedConnection[] = [];

  for (const entry of request.staged_results) {
    const tool = entry.tool_name;
    const payload = entry.staged_node_payload ?? {};

    if (tool.startsWith("flowfile.graph.add_")) {
      const nodeType = tool.slice("flowfile.graph.add_".length);
      const insertion = (payload.insertion_context ?? {}) as Partial<StagedInsertionContext>;
      additions.push({
        node_type: (payload.node_type as string | undefined) ?? nodeType,
        settings: (payload.settings as Record<string, unknown> | undefined) ?? {},
        insertion_context: {
          upstream_node_ids: insertion.upstream_node_ids ?? [],
          right_input_node_id: insertion.right_input_node_id ?? null,
          pos_x: insertion.pos_x ?? 0,
          pos_y: insertion.pos_y ?? 0,
        },
        predicted_output_schema:
          (payload.predicted_output_schema as StagedSchemaColumn[] | null | undefined) ?? null,
        audit_id: entry.audit_id ?? null,
      });
    } else if (tool === "flowfile.graph.connect") {
      connectionsAdded.push({
        connection: (payload.connection as Record<string, unknown> | undefined) ?? {},
        audit_id: entry.audit_id ?? null,
      });
    } else if (tool === "flowfile.graph.delete_node") {
      const nodeId = payload.delete_node_id;
      if (typeof nodeId === "number") {
        deletions.push({ delete_node_id: nodeId, audit_id: entry.audit_id ?? null });
      }
    } else if (tool === "flowfile.graph.delete_connection") {
      connectionsRemoved.push({
        connection: (payload.delete_connection as Record<string, unknown> | undefined) ?? {},
        audit_id: entry.audit_id ?? null,
      });
    }
    // Unknown tool names are silently dropped — the backend already
    // 422'd them via `_bin_staged_results`, so they cannot reach here
    // when the wire round-trip succeeded.
  }

  return {
    diff_id: diffId,
    session_id: request.session_id,
    flow_id: request.flow_id,
    rationale: request.rationale ?? null,
    additions,
    connections_added: connectionsAdded,
    deletions,
    connections_removed: connectionsRemoved,
  };
};

export const opCount = (diff: GraphDiffPayload): number =>
  diff.additions.length +
  diff.connections_added.length +
  diff.deletions.length +
  diff.connections_removed.length;

export type { AcceptDiffResponse, RejectDiffResponse, StageDiffRequest };
