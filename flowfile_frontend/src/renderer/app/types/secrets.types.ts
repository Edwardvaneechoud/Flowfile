// Secrets management related TypeScript interfaces and types
// Consolidated from pages/secretManager/secretTypes.ts

// ============================================================================
// Secret Types
// ============================================================================

/**
 * Interface representing a secret fetched from the backend.
 * Note: The 'value' might be encrypted or masked depending on the API.
 */
export interface Secret {
  name: string
  value: string
  user_id?: string
}

/**
 * Interface representing the input for creating a new secret.
 */
export interface SecretInput {
  name: string
  value: string
}

/**
 * Interface for the actual secret value fetched for copying.
 * Assumes the API returns the decrypted value in a specific structure.
 */
export interface SecretValueResponse {
  name: string
  value: string
}
