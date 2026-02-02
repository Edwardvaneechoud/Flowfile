import axios from "../services/axios.config";
import type { DockerStatus, KernelConfig, KernelInfo, PersistenceInfo } from "../types";

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

  static async getArtifacts(kernelId: string): Promise<Record<string, any>> {
    try {
      const response = await axios.get<Record<string, any>>(
        `${API_BASE_URL}/${encodeURIComponent(kernelId)}/artifacts`,
      );
      return response.data;
    } catch (error) {
      console.error("API Error: Failed to get artifacts:", error);
      return {};
    }
  }

  static async getPersistenceInfo(kernelId: string): Promise<PersistenceInfo> {
    try {
      const response = await axios.get<PersistenceInfo>(
        `${API_BASE_URL}/${encodeURIComponent(kernelId)}/persistence`,
      );
      return response.data;
    } catch (error) {
      console.error("API Error: Failed to get persistence info:", error);
      return {
        persistence_enabled: false,
        total_artifacts: 0,
        persisted_count: 0,
        memory_only_count: 0,
        disk_usage_bytes: 0,
        artifacts: {},
      };
    }
  }

  static async recoverArtifacts(
    kernelId: string,
  ): Promise<{ status: string; artifacts: Record<string, string> }> {
    const response = await axios.post<{ status: string; artifacts: Record<string, string> }>(
      `${API_BASE_URL}/${encodeURIComponent(kernelId)}/recover`,
    );
    return response.data;
  }

  static async cleanupArtifacts(
    kernelId: string,
    artifactNames: string[],
  ): Promise<{ status: string; deleted: string[] }> {
    const response = await axios.post<{ status: string; deleted: string[] }>(
      `${API_BASE_URL}/${encodeURIComponent(kernelId)}/cleanup`,
      { artifact_names: artifactNames },
    );
    return response.data;
  }

  static async getDockerStatus(): Promise<DockerStatus> {
    try {
      const response = await axios.get<DockerStatus>(`${API_BASE_URL}/docker-status`);
      return response.data;
    } catch (error) {
      console.error("API Error: Failed to check Docker status:", error);
      return { available: false, image_available: false, error: "Failed to reach server" };
    }
  }
}
