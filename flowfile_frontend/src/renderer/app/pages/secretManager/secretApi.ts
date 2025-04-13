// src/components/secrets/secretApi.ts

import axios from 'axios';
import type { Secret, SecretInput, SecretValueResponse } from './secretTypes';

const API_BASE_URL = '/secrets/secrets';

/**
 * Fetches the list of secrets from the API.
 * @returns A promise that resolves to an array of Secret objects.
 */
export const fetchSecretsApi = async (): Promise<Secret[]> => {
  try {
    const response = await axios.get<Secret[]>(API_BASE_URL);
    return response.data;
  } catch (error) {
    console.error('API Error: Failed to load secrets:', error);
    throw error;
  }
};

/**
 * Adds a new secret via the API.
 * @param secretData - The name and value of the secret to add.
 * @returns A promise that resolves when the secret is added.
 */
export const addSecretApi = async (secretData: SecretInput): Promise<void> => {
  try {
    await axios.post(API_BASE_URL, secretData);
  } catch (error) {
    console.error('API Error: Failed to add secret:', error);
     const errorMsg = (error as any).response?.data?.detail || 'Failed to add secret';
     throw new Error(errorMsg);
  }
};

/**
 * Fetches the actual value of a specific secret for copying.
 * @param secretName - The name of the secret to fetch.
 * @returns A promise that resolves to the secret's actual value.
 */
export const getSecretValueApi = async (secretName: string): Promise<string> => {
    try {
      // Assuming the API returns an object like { name: '...', value: '...' }
      const response = await axios.get<SecretValueResponse>(`${API_BASE_URL}/${encodeURIComponent(secretName)}`);
      return response.data.value;
    } catch (error) {
      console.error('API Error: Failed to get secret value:', error);
      throw error;
    }
  };


/**
 * Deletes a secret via the API.
 * @param secretName - The name of the secret to delete.
 * @returns A promise that resolves when the secret is deleted.
 */
export const deleteSecretApi = async (secretName: string): Promise<void> => {
  try {
    await axios.delete(`${API_BASE_URL}/${encodeURIComponent(secretName)}`);
  } catch (error) {
    console.error('API Error: Failed to delete secret:', error);
    throw error;
  }
};