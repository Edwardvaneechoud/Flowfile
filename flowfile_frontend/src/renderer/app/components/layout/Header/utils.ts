import axios from "axios";

export const saveFlow = async (flowId: number, flowPath: string) => {
  try {
    await axios.get("/save_flow", {
      params: {
        flow_id: flowId,
        flow_path: flowPath,
      },
      headers: {
        accept: "application/json",
      },
    });
  } catch (error) {
    console.error("There was an error saving the flow:", error);
  }
};
