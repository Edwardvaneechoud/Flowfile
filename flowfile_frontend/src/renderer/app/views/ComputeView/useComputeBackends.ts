import { ref } from "vue";
import type { Ref } from "vue";
import {
  addComputeBackend,
  handshakeComputeBackend,
  listComputeBackends,
  removeComputeBackend,
  type ComputeBackend,
  type ComputeBackendInput,
  type HandshakeVerdict,
} from "../../api/computeBackends.api";

const LOOPBACK_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);

/** Whether `url` reaches a worker over plain HTTP on another host; a URL that does not parse yet is not. */
export function plainHttpOffLoopback(url: string): boolean {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return false;
  }
  return parsed.protocol === "http:" && !LOOPBACK_HOSTS.has(parsed.hostname);
}

/** The route's own words: a string detail as is, FastAPI's validation list as its messages. */
function detailOf(error: unknown, fallback: string): string {
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

/** `input` without the optional fields left empty in the form, so the route stores none for them. */
function withoutEmptyOptionals(input: ComputeBackendInput): ComputeBackendInput {
  if (input.kind === "polars-cloud") {
    const { dashboard_url, ...rest } = input;
    return dashboard_url ? { ...rest, dashboard_url } : rest;
  }
  const { ca_cert, ...rest } = input;
  return ca_cert ? { ...rest, ca_cert } : rest;
}

/**
 * The configured backends and the admin's actions on them; the caller loads, nothing polls. Loading seeds each
 * backend's verdict from the listing, so one a run produced shows without pressing Handshake.
 */
export function useComputeBackends() {
  const backends: Ref<ComputeBackend[]> = ref([]);
  const loading = ref(false);
  const loadError = ref("");
  const adding = ref(false);
  const busy: Ref<Record<string, boolean>> = ref({});
  const verdicts: Ref<Record<string, HandshakeVerdict>> = ref({});

  const load = async () => {
    loading.value = true;
    try {
      backends.value = await listComputeBackends();
      for (const backend of backends.value) {
        if (backend.verdict) verdicts.value[backend.id] = backend.verdict;
      }
      loadError.value = "";
    } catch (error) {
      loadError.value = detailOf(error, "Couldn't read the configured backends.");
    } finally {
      loading.value = false;
    }
  };

  const add = async (input: ComputeBackendInput): Promise<ComputeBackend> => {
    adding.value = true;
    try {
      const row = await addComputeBackend(withoutEmptyOptionals(input));
      await load();
      return row;
    } catch (error) {
      throw new Error(detailOf(error, "Couldn't add the backend."));
    } finally {
      adding.value = false;
    }
  };

  const remove = async (id: string) => {
    busy.value[id] = true;
    try {
      await removeComputeBackend(id);
      delete verdicts.value[id];
      await load();
    } catch (error) {
      throw new Error(detailOf(error, `Couldn't remove ${id}.`));
    } finally {
      busy.value[id] = false;
    }
  };

  const handshake = async (id: string): Promise<HandshakeVerdict> => {
    busy.value[id] = true;
    try {
      const verdict = await handshakeComputeBackend(id);
      verdicts.value[id] = verdict;
      return verdict;
    } catch (error) {
      throw new Error(detailOf(error, `Couldn't handshake ${id}.`));
    } finally {
      busy.value[id] = false;
    }
  };

  return { backends, loading, loadError, adding, busy, verdicts, load, add, remove, handshake };
}
