import axios from "../services/axios.config";
import type { KeptSecret, Secret, SecretInput, SecretValueResponse } from "../types";

const API_BASE_URL = "/secrets/secrets";

export class SecretsApi {
  /**
   * Fetches the list of secrets from the API
   */
  static async getAll(): Promise<Secret[]> {
    try {
      const response = await axios.get<Secret[]>(API_BASE_URL);
      return response.data;
    } catch (error) {
      console.error("API Error: Failed to load secrets:", error);
      throw error;
    }
  }

  /**
   * Adds a new secret via the API
   */
  static async create(secretData: SecretInput): Promise<void> {
    try {
      await axios.post(API_BASE_URL, secretData);
    } catch (error) {
      console.error("API Error: Failed to add secret:", error);
      const errorMsg = (error as any).response?.data?.detail || "Failed to add secret";
      throw new Error(errorMsg);
    }
  }

  /**
   * Fetches the actual value of a specific secret for copying
   */
  static async getValue(secretName: string): Promise<string> {
    try {
      const response = await axios.get<SecretValueResponse>(
        `${API_BASE_URL}/${encodeURIComponent(secretName)}`,
      );
      return response.data.value;
    } catch (error) {
      console.error("API Error: Failed to get secret value:", error);
      throw error;
    }
  }

  /**
   * Gives an own secret a new value; only a server that keeps its secrets on its hub takes it
   */
  static async change(secretName: string, value: string): Promise<void> {
    try {
      await axios.put(`${API_BASE_URL}/${encodeURIComponent(secretName)}`, { value });
    } catch (error) {
      console.error("API Error: Failed to change secret:", error);
      const errorMsg = (error as any).response?.data?.detail || "Failed to change secret";
      throw new Error(errorMsg);
    }
  }

  /**
   * The secrets that stayed on a server whose other secrets moved to its hub, each with why
   */
  static async getKept(): Promise<KeptSecret[]> {
    const response = await axios.get<KeptSecret[]>("/secrets/kept");
    return response.data;
  }

  /**
   * Deletes a secret via the API
   */
  static async delete(secretName: string): Promise<void> {
    try {
      await axios.delete(`${API_BASE_URL}/${encodeURIComponent(secretName)}`);
    } catch (error) {
      console.error("API Error: Failed to delete secret:", error);
      throw error;
    }
  }
}

// Legacy function exports for backward compatibility

export const fetchSecretsApi = SecretsApi.getAll;
export const addSecretApi = SecretsApi.create;
export const getSecretValueApi = SecretsApi.getValue;
export const deleteSecretApi = SecretsApi.delete;
