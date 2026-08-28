/**
 * Extract a user-facing message from a failed settings save.
 * Handles the core's string `detail` (e.g. validation errors) and FastAPI's
 * structured `detail` array, falling back to a generic message.
 */
export function extractSaveErrorMessage(error: unknown): string {
  const detail = (error as any)?.response?.data?.detail;
  if (typeof detail === "string" && detail.trim()) {
    return detail;
  }
  if (Array.isArray(detail)) {
    const messages = detail.map((item) => item?.msg).filter(Boolean);
    if (messages.length) {
      return messages.join("; ");
    }
  }
  return "Failed to save settings. Please check the node configuration.";
}

export const CATALOG_PERMISSION_MESSAGE =
  "You have read-only access to this catalog. To save here you need edit (manage) access — " +
  "pick a catalog you manage, or ask its owner to share it with you.";

/**
 * Generic axios-error → user message: the backend's string `detail`, else the
 * FastAPI validation-array messages, else the error's own message, else the
 * fallback. The shared home for the per-panel `detail()` helpers.
 */
export function detailMessage(error: unknown, fallback: string): string {
  const detail = (error as any)?.response?.data?.detail;
  if (typeof detail === "string" && detail.trim()) {
    return detail;
  }
  if (Array.isArray(detail)) {
    const messages = detail.map((item) => item?.msg).filter(Boolean);
    if (messages.length) {
      return messages.join("; ");
    }
  }
  const message = (error as any)?.message;
  return typeof message === "string" && message ? message : fallback;
}

/**
 * Message for a failed catalog save/move: permission refusals (403) get a
 * human explanation instead of the backend's raw detail string; everything
 * else falls back to the response detail or the given fallback.
 */
export function catalogSaveErrorMessage(error: unknown, fallback = "Failed to save"): string {
  if ((error as any)?.response?.status === 403) {
    return CATALOG_PERMISSION_MESSAGE;
  }
  const detail = (error as any)?.response?.data?.detail;
  // Structured details (e.g. the stale_write 409) carry their copy in .message.
  if (
    detail !== null &&
    typeof detail === "object" &&
    !Array.isArray(detail) &&
    typeof detail.message === "string"
  ) {
    return detail.message;
  }
  return detailMessage(error, fallback);
}
