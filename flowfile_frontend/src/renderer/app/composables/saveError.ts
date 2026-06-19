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
