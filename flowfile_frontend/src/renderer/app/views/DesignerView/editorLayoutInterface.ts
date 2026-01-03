import axios from 'axios';

const handleApiError = (error: any): never => {
  throw {
    message: error.response?.data?.detail || 'An unknown error occurred',
    status: error.response?.status || 500,
  };
};

/**
 * Applies the standard layout to a flow.
 * @param flowId The ID of the flow.
 * @param baseURL The base URL of the API.
 * @returns A promise that resolves with the response data on success.
 */
export const applyStandardLayout = async (
  flowId: number,
): Promise<any> => {
  const url = "/flow/apply_standard_layout/";
  try {
    const response = await axios.post<any>(url, null, {
      params: {
        flow_id: flowId,
      },
    });
    return response.data;
  } catch (error) {
    return handleApiError(error);
  }
};
