import axios from "axios";

/**
 * Returns true for paths that live inside Flowfile's internal storage
 * (``~/.flowfile/`` on local, ``/data/user/...`` that maps to the user data
 * root in Docker, and legacy temp/flows paths).  Such paths should never be
 * used as a starting directory for the file browser, since users don't
 * browse internal storage manually.
 */
export const isInternalFlowfilePath = (path: string | null | undefined): boolean => {
  if (!path) return true;
  return (
    path.includes("/.flowfile/") ||
    path.includes("\\.flowfile\\") ||
    path.includes("/temp/flows/") ||
    path.includes("\\temp\\flows\\")
  );
};

export const saveFlow = async (
  flowId: number,
  flowPath: string,
  namespaceId?: number | null,
): Promise<number> => {
  const params: Record<string, unknown> = {
    flow_id: flowId,
    flow_path: flowPath,
  };
  if (namespaceId !== undefined && namespaceId !== null) {
    params.namespace_id = namespaceId;
  }
  const response = await axios.post("/save_flow", null, {
    params,
    headers: {
      accept: "application/json",
    },
  });
  return response.data as number;
};

export const saveFlowSilent = async (flowId: number): Promise<number> => {
  const response = await axios.post("/save_flow", null, {
    params: { flow_id: flowId },
    headers: { accept: "application/json" },
  });
  return response.data as number;
};

/**
 * Save a flow into the managed catalog flows directory. The backend builds the
 * final filename as ``{flow_id}_{flow_name}.yaml`` so two flows with the same
 * user-chosen name in different namespaces cannot overwrite each other.
 */
export const saveFlowToCatalog = async (
  flowId: number,
  flowName: string,
  namespaceId: number,
): Promise<number> => {
  const response = await axios.post("/save_flow_to_catalog", null, {
    params: {
      flow_id: flowId,
      flow_name: flowName,
      namespace_id: namespaceId,
    },
    headers: { accept: "application/json" },
  });
  return response.data as number;
};
