// Client for the notebook code-intelligence (Jedi) surface. capabilities() is a
// cached, kernel-independent probe; the analysis calls go through core's owner-checked
// /kernels/{id}/lsp/* bridge. Every call is failure-safe: errors resolve to empty
// shapes so the editor silently falls back to its client-side completion sources.
import axios from "../services/axios.config";

export interface LspRequestPayload {
  code: string;
  line: number; // 1-based line within the cell
  column: number; // 0-based column
  flow_id: number; // namespace/session key (node flow_id or notebook sessionFlowId)
  node_id?: number | null;
}

export interface LspCompletionItem {
  label: string;
  type: string;
  detail: string;
  documentation: string;
  insert_text: string | null;
}
export interface LspCompleteResponse {
  items: LspCompletionItem[];
}
export interface LspHoverResponse {
  contents: string | null;
}
export interface LspSignatureInfo {
  label: string;
  parameters: string[];
  active_parameter: number;
  documentation: string;
}
export interface LspSignatureResponse {
  signatures: LspSignatureInfo[];
  active_signature: number;
}
export interface LspDiagnostic {
  line: number; // 1-based
  column: number; // 0-based
  end_line: number;
  end_column: number;
  message: string;
  severity: string; // error | warning
  source: string; // jedi | pyflakes
}
export interface LspDiagnosticsResponse {
  diagnostics: LspDiagnostic[];
}
export interface LspCapabilities {
  enabled: boolean;
  version: string;
  features: string[];
}

const EMPTY_COMPLETE: LspCompleteResponse = { items: [] };
const EMPTY_HOVER: LspHoverResponse = { contents: null };
const EMPTY_SIGNATURE: LspSignatureResponse = { signatures: [], active_signature: 0 };
const EMPTY_DIAGNOSTICS: LspDiagnosticsResponse = { diagnostics: [] };
const DISABLED: LspCapabilities = { enabled: false, version: "", features: [] };

let _capabilities: Promise<LspCapabilities> | null = null;

export class LspApi {
  /** Cached for the session — capabilities live on core (always up), so a single probe suffices. */
  static capabilities(): Promise<LspCapabilities> {
    if (!_capabilities) {
      _capabilities = axios
        .get<LspCapabilities>("/lsp/capabilities")
        .then((r) => r.data ?? DISABLED)
        .catch(() => DISABLED);
    }
    return _capabilities;
  }

  /** Test seam: force the next capabilities() to re-probe. */
  static resetCapabilitiesCache(): void {
    _capabilities = null;
  }

  static async complete(
    kernelId: string,
    req: LspRequestPayload,
    signal?: AbortSignal,
  ): Promise<LspCompleteResponse> {
    try {
      const r = await axios.post<LspCompleteResponse>(
        `/kernels/${encodeURIComponent(kernelId)}/lsp/complete`,
        req,
        { signal },
      );
      return r.data ?? EMPTY_COMPLETE;
    } catch {
      return EMPTY_COMPLETE;
    }
  }

  static async hover(
    kernelId: string,
    req: LspRequestPayload,
    signal?: AbortSignal,
  ): Promise<LspHoverResponse> {
    try {
      const r = await axios.post<LspHoverResponse>(
        `/kernels/${encodeURIComponent(kernelId)}/lsp/hover`,
        req,
        { signal },
      );
      return r.data ?? EMPTY_HOVER;
    } catch {
      return EMPTY_HOVER;
    }
  }

  static async signature(
    kernelId: string,
    req: LspRequestPayload,
    signal?: AbortSignal,
  ): Promise<LspSignatureResponse> {
    try {
      const r = await axios.post<LspSignatureResponse>(
        `/kernels/${encodeURIComponent(kernelId)}/lsp/signature`,
        req,
        { signal },
      );
      return r.data ?? EMPTY_SIGNATURE;
    } catch {
      return EMPTY_SIGNATURE;
    }
  }

  static async diagnostics(
    kernelId: string,
    req: LspRequestPayload,
    signal?: AbortSignal,
  ): Promise<LspDiagnosticsResponse> {
    try {
      const r = await axios.post<LspDiagnosticsResponse>(
        `/kernels/${encodeURIComponent(kernelId)}/lsp/diagnostics`,
        req,
        { signal },
      );
      return r.data ?? EMPTY_DIAGNOSTICS;
    } catch {
      return EMPTY_DIAGNOSTICS;
    }
  }
}
