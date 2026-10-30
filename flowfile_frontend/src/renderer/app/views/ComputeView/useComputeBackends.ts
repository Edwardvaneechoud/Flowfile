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
import { detailOf } from "../../utils/apiDetail";
import {
  handshakeStatus,
  type StatusSummary,
  type StatusTone,
} from "../../components/settings/handshakeStatus";

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

export interface BackendForm {
  name: string;
  url: string;
  token: string;
  ca_cert: string;
  shares_core_disk: boolean;
}

/** The add form as it opens and as it resets after an add: the server's files left unticked. */
export function emptyBackendForm(): BackendForm {
  return { name: "", url: "", token: "", ca_cert: "", shares_core_disk: false };
}

/** The add body the form sends, the "Sees this server's files" tick included. */
export function backendInputFrom(form: BackendForm): ComputeBackendInput {
  return {
    name: form.name,
    url: form.url,
    token: form.token,
    ca_cert: form.ca_cert,
    shares_core_disk: form.shares_core_disk,
  };
}

/**
 * A row's "Server's files" cell: marked on the row, else declared when its last verdict offers `file` (or a
 * refused worker reported it), else "No". No verdict reads "Not checked", one without capabilities "Unknown".
 */
export function serverFilesLabel(
  backend: ComputeBackend,
  verdict: HandshakeVerdict | null | undefined,
): string {
  if (backend.shares_core_disk) return "Yes, marked here";
  if (!verdict) return "Not checked";
  const declared = verdict.offered ?? verdict.backend;
  if (!declared) return "Unknown, the handshake failed";
  return declared.read_schemes.includes("file") || declared.write_schemes.includes("file")
    ? "Yes, declared by the worker"
    : "No";
}

const SUMMARY_PARTS: ReadonlyArray<[StatusTone, string]> = [
  ["error", "can't run flows"],
  ["warning", "with limitations"],
  ["neutral", "not checked"],
];

/** The collapsed Other workers card's line: the count per status worst first, its tone the worst; null without rows. */
export function workersSummary(
  backends: ComputeBackend[],
  verdicts: Record<string, HandshakeVerdict | undefined>,
): StatusSummary | null {
  if (!backends.length) return null;
  const tones = backends.map((backend) => handshakeStatus(verdicts[backend.id]).tone);
  const parts = SUMMARY_PARTS.map(([tone, label]) => {
    const count = tones.filter((t) => t === tone).length;
    return count ? { tone, label: `${count} ${label}` } : null;
  }).filter((part): part is StatusSummary => part !== null);
  if (!parts.length) {
    return { tone: "success", label: backends.length === 1 ? "Matches this server" : "All match" };
  }
  return { tone: parts[0].tone, label: parts.map((part) => part.label).join(" · ") };
}

/**
 * The configured workers and the admin's actions on them; the caller loads, nothing polls. A load seeds the
 * verdicts from the listing, so one a run produced shows without pressing Handshake.
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
      loadError.value = detailOf(error, "Couldn't read the configured workers.");
    } finally {
      loading.value = false;
    }
  };

  const add = async (input: ComputeBackendInput): Promise<ComputeBackend> => {
    adding.value = true;
    const { ca_cert, ...rest } = input;
    try {
      const row = await addComputeBackend(ca_cert?.trim() ? { ...rest, ca_cert } : rest);
      await load();
      return row;
    } catch (error) {
      throw new Error(detailOf(error, "Couldn't add the worker."));
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
