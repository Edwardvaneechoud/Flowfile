/** A failed request's detail: a string as is, FastAPI's validation list as its messages. */
export function detailOf(error: unknown, fallback: string): string {
  const detail = (error as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    const messages = detail
      .map((entry) => (entry as { msg?: unknown })?.msg)
      .filter((msg): msg is string => typeof msg === "string");
    if (messages.length) return messages.join("; ");
  }
  return fallback;
}
