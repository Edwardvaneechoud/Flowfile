// TypeScript types mirroring W12's Pydantic schemas in
// flowfile_core/flowfile_core/ai/credentials.py.
//
// FE convention: camelCase fields. The api.ts layer maps to/from the
// snake_case shapes the backend exposes (matches DatabaseView/api.ts).

export type AiProviderStatus = "configured" | "env_fallback" | "unconfigured";

export type AiProviderTestStatus = "ok" | "error";

export interface AiProviderCredential {
  provider: string;
  hasKey: boolean;
  apiBase: string | null;
  defaultModel: string | null;
  lastTestedAt: string | null;
  lastTestStatus: AiProviderTestStatus | null;
  lastTestError: string | null;
  createdAt: string | null;
  updatedAt: string | null;
}

export interface AiProvider {
  provider: string;
  supportsTools: boolean;
  supportsStreaming: boolean;
  defaultModel: string;
  surfaces: Record<string, string>;
  status: AiProviderStatus;
  credential: AiProviderCredential | null;
}

// Body for POST /ai/providers/{name}.
//
// Field semantics (must match credentials.ProviderCredentialInput):
// - apiKey=null     → keep existing secret untouched
// - apiKey="sk-..." → store/rotate in place
// - clearApiKey     → drop secret (mutually exclusive with apiKey, 422 if both)
// - apiBase / defaultModel: null leaves the field as-is, otherwise overwrites.
export interface AiProviderCredentialInput {
  apiKey: string | null;
  clearApiKey: boolean;
  apiBase: string | null;
  defaultModel: string | null;
}

export interface AiProviderTestResult {
  ok: boolean;
  error: string | null;
}

// Server-side detail string raised by W17's require_ai_enabled when the flag
// is off. The api.ts layer matches on this to convert the 503 into a typed
// AiDisabledError that the view renders as a dedicated empty-state.
//
// Mirrors flowfile_core.ai.feature_flag.DISABLED_DETAIL byte-for-byte; bumping
// this string is a contract change shared with the backend.
export const AI_DISABLED_DETAIL = "AI features are disabled. Set FEATURE_FLAG_AI=true to enable.";

// W18 — admin AI feature-flag toggle.
//
// Mirrors flowfile_core.ai.admin_routes.FeatureFlagState. `persisted` is
// always false in the W18 contract: the toggle lives in process memory.
// Cross-restart persistence requires the user to set FEATURE_FLAG_AI in
// their .env (which the UI surfaces as a hint).
export interface AiFeatureFlagState {
  enabled: boolean;
  persisted: boolean;
}
