// Axios wrappers for the BYOK endpoints (/ai/providers,
// /ai/providers/{name}, /ai/providers/{name}/test). Mirrors the
// DatabaseView/api.ts pattern: TS-side camelCase, snake_case mappers
// at the boundary.

import axios from "../../services/axios.config";
import type {
  AiFeatureFlagState,
  AiProvider,
  AiProviderCredential,
  AiProviderCredentialInput,
  AiProviderTestResult,
} from "./aiProviderTypes";
import { AI_DISABLED_DETAIL } from "./aiProviderTypes";

const API_BASE_URL = "/ai/providers";
// Admin feature-flag endpoint lives outside the gated /ai/* router so
// admins can flip the AI gate from the UI without first satisfying it.
const ADMIN_FEATURE_FLAG_URL = "/system/feature_flags/ai";

interface PyAiProviderCredential {
  provider: string;
  has_key: boolean;
  api_base: string | null;
  default_model: string | null;
  models: string[] | null;
  last_tested_at: string | null;
  last_test_status: "ok" | "error" | null;
  last_test_error: string | null;
  created_at: string | null;
  updated_at: string | null;
}

interface PyAiProvider {
  provider: string;
  supports_tools: boolean;
  supports_streaming: boolean;
  default_model: string;
  surfaces: Record<string, string>;
  status: "configured" | "env_fallback" | "unconfigured";
  credential: PyAiProviderCredential | null;
}

interface PyAiProviderCredentialInput {
  api_key: string | null;
  clear_api_key: boolean;
  api_base: string | null;
  default_model: string | null;
  models: string[] | null;
  clear_models: boolean;
}

interface PyAiProviderTestResult {
  ok: boolean;
  error: string | null;
}

const fromPyCredential = (raw: PyAiProviderCredential): AiProviderCredential => ({
  provider: raw.provider,
  hasKey: raw.has_key,
  apiBase: raw.api_base,
  defaultModel: raw.default_model,
  models: raw.models,
  lastTestedAt: raw.last_tested_at,
  lastTestStatus: raw.last_test_status,
  lastTestError: raw.last_test_error,
  createdAt: raw.created_at,
  updatedAt: raw.updated_at,
});

const fromPyProvider = (raw: PyAiProvider): AiProvider => ({
  provider: raw.provider,
  supportsTools: raw.supports_tools,
  supportsStreaming: raw.supports_streaming,
  defaultModel: raw.default_model,
  surfaces: raw.surfaces,
  status: raw.status,
  credential: raw.credential ? fromPyCredential(raw.credential) : null,
});

const toPyInput = (input: AiProviderCredentialInput): PyAiProviderCredentialInput => ({
  api_key: input.apiKey,
  clear_api_key: input.clearApiKey,
  api_base: input.apiBase,
  default_model: input.defaultModel,
  models: input.models,
  clear_models: input.clearModels,
});

// Thrown by every fetcher when the backend reports the AI subsystem
// is off (503 with the disabled-detail marker). Lets the view render
// a dedicated empty-state without sniffing axios error shapes
// everywhere.
export class AiDisabledError extends Error {
  constructor(message: string = AI_DISABLED_DETAIL) {
    super(message);
    this.name = "AiDisabledError";
  }
}

const isAiDisabledError = (error: unknown): boolean => {
  const detail = (error as { response?: { data?: { detail?: unknown }; status?: number } })
    ?.response?.data?.detail;
  const status = (error as { response?: { status?: number } })?.response?.status;
  return status === 503 && typeof detail === "string" && detail === AI_DISABLED_DETAIL;
};

export const fetchAiProviders = async (): Promise<AiProvider[]> => {
  try {
    const response = await axios.get<PyAiProvider[]>(API_BASE_URL);
    return response.data.map(fromPyProvider);
  } catch (error) {
    if (isAiDisabledError(error)) throw new AiDisabledError();
    console.error("API Error: Failed to load AI providers:", error);
    throw error;
  }
};

export const upsertAiProvider = async (
  provider: string,
  payload: AiProviderCredentialInput,
): Promise<AiProviderCredential> => {
  try {
    const response = await axios.post<PyAiProviderCredential>(
      `${API_BASE_URL}/${encodeURIComponent(provider)}`,
      toPyInput(payload),
    );
    return fromPyCredential(response.data);
  } catch (error) {
    if (isAiDisabledError(error)) throw new AiDisabledError();
    console.error("API Error: Failed to save provider credential:", error);
    const detail = (error as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
    throw new Error(detail || "Failed to save provider credential");
  }
};

export const deleteAiProvider = async (provider: string): Promise<void> => {
  try {
    await axios.delete(`${API_BASE_URL}/${encodeURIComponent(provider)}`);
  } catch (error) {
    if (isAiDisabledError(error)) throw new AiDisabledError();
    console.error("API Error: Failed to delete provider credential:", error);
    throw error;
  }
};

export const testAiProvider = async (provider: string): Promise<AiProviderTestResult> => {
  try {
    const response = await axios.post<PyAiProviderTestResult>(
      `${API_BASE_URL}/${encodeURIComponent(provider)}/test`,
    );
    return { ok: response.data.ok, error: response.data.error };
  } catch (error) {
    if (isAiDisabledError(error)) throw new AiDisabledError();
    console.error("API Error: Failed to test provider:", error);
    throw error;
  }
};

// Admin endpoint for flipping FEATURE_FLAG_AI on the running
// process. Returns the new state on success. Backend reuses
// get_current_admin_user, so non-admin callers see a 403 ("Admin
// privileges required").
export const setAiFeatureFlag = async (enabled: boolean): Promise<AiFeatureFlagState> => {
  try {
    const response = await axios.post<AiFeatureFlagState>(ADMIN_FEATURE_FLAG_URL, { enabled });
    return response.data;
  } catch (error) {
    console.error("API Error: Failed to toggle AI feature flag:", error);
    const detail = (error as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
    throw new Error(detail || "Failed to toggle AI feature flag");
  }
};

export { AI_DISABLED_DETAIL };
