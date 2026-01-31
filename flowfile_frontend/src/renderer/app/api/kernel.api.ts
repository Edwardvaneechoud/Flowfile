import axios from "../services/axios.config";
import type { KernelConfig, KernelInfo } from "../types";

const API_BASE_URL = "/kernels";

export class KernelApi {
  static async getAll(): Promise<KernelInfo[]> {
    try {
      const response = await axios.get<KernelInfo[]>(`${API_BASE_URL}/`);
      return response.data;
    } catch (error) {
      console.error("API Error: Failed to load kernels:", error);
      const errorMsg = (error as any).response?.data?.detail || "Failed to load kernels";
      throw new Error(errorMsg);
    }
  }

  static async get(kernelId: string): Promise<KernelInfo> {
    try {
      const response = await axios.get<KernelInfo>(
        `${API_BASE_URL}/${encodeURIComponent(kernelId)}`,
      );
      return response.data;
    } catch (error) {
      console.error("API Error: Failed to get kernel:", error);
      throw error;
    }
  }

  static async create(config: KernelConfig): Promise<KernelInfo> {
    try {
      const response = await axios.post<KernelInfo>(`${API_BASE_URL}/`, config);
      return response.data;
    } catch (error) {
      console.error("API Error: Failed to create kernel:", error);
      const errorMsg = (error as any).response?.data?.detail || "Failed to create kernel";
      throw new Error(errorMsg);
    }
  }

  static async delete(kernelId: string): Promise<void> {
    try {
      await axios.delete(`${API_BASE_URL}/${encodeURIComponent(kernelId)}`);
    } catch (error) {
      console.error("API Error: Failed to delete kernel:", error);
      throw error;
    }
  }

  static async start(kernelId: string): Promise<KernelInfo> {
    try {
      const response = await axios.post<KernelInfo>(
        `${API_BASE_URL}/${encodeURIComponent(kernelId)}/start`,
      );
      return response.data;
    } catch (error) {
      console.error("API Error: Failed to start kernel:", error);
      const errorMsg = (error as any).response?.data?.detail || "Failed to start kernel";
      throw new Error(errorMsg);
    }
  }

  static async stop(kernelId: string): Promise<void> {
    try {
      await axios.post(`${API_BASE_URL}/${encodeURIComponent(kernelId)}/stop`);
    } catch (error) {
      console.error("API Error: Failed to stop kernel:", error);
      throw error;
    }
  }
}
