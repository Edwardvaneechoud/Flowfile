import axios from "axios";

export const saveFlow = async (flowId: number, flowPath: string): Promise<number> => {
  const response = await axios.get("/save_flow", {
    params: {
      flow_id: flowId,
      flow_path: flowPath,
    },
    headers: {
      accept: "application/json",
    },
  });
  return response.data as number;
};
