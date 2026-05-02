// Axios wrappers for the W12 BYOK endpoints (/ai/providers, /ai/providers/{name},
// /ai/providers/{name}/test). Mirrors the DatabaseView/api.ts pattern: TS-side
// camelCase, snake_case mappers at the boundary.

import axios from "../../services/axios.config";
import type {
  AiProvider,
  AiProviderCredential,
  AiProviderCredentialInput,
  AiProviderTestResult,
} from "./aiProviderTypes";
import { AI_DISABLED_DETAIL } from "./aiProviderTypes";

const API_BASE_URL = "/ai/providers";

interface PyAiProviderCredential {
  provider: string;
  has_key: boolean;
  api_base: string | null;
  default_model: string | null;
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
});

// Thrown by every fetcher when the backend reports the AI subsystem is off
// (503 with W17's DISABLED_DETAIL). Lets the view render a dedicated empty-state
// without sniffing axios error shapes everywhere.
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

export { AI_DISABLED_DETAIL };
