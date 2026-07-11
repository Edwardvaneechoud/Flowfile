// Community media (icons + screenshots) is served by a JWT-gated, pin-verified
// core endpoint, so plain <img src> URLs 401 and raw CDN URLs are forbidden by
// the Tauri CSP — fetch every asset as an authed blob and hand out object URLs.
// Clone of useCustomNodeIcon.ts, keyed by node id + file name.
import { ref, watchEffect, type Ref } from "vue";
import axios from "axios";
import { getDefaultIconUrl } from "../features/designer/utils";

const objectUrls = new Map<string, string>();
const inflight = new Map<string, Promise<string | null>>();

function cacheKey(nodeId: string, fileName: string): string {
  return `${nodeId}/${fileName}`;
}

async function fetchMediaObjectUrl(nodeId: string, fileName: string): Promise<string | null> {
  const key = cacheKey(nodeId, fileName);
  const cached = objectUrls.get(key);
  if (cached) return cached;
  const pending = inflight.get(key);
  if (pending) return pending;

  const request = axios
    .get(`/community_nodes/media/${nodeId}/${fileName}`, { responseType: "blob" })
    .then((response) => {
      const url = URL.createObjectURL(response.data as Blob);
      objectUrls.set(key, url);
      return url;
    })
    .catch(() => null)
    .finally(() => {
      inflight.delete(key);
    });
  inflight.set(key, request);
  return request;
}

export function invalidateCommunityMedia(): void {
  objectUrls.forEach((url) => URL.revokeObjectURL(url));
  objectUrls.clear();
}

/** Reactive object-URL for one community media file; falls back to the default
 *  cube icon while loading or when the file is absent / fails to fetch. */
export function useCommunityMediaUrl(
  nodeId: () => string,
  fileName: () => string | undefined | null,
): Ref<string> {
  const url = ref(getDefaultIconUrl());
  watchEffect(async () => {
    const name = fileName();
    const id = nodeId();
    if (!name || !id) {
      url.value = getDefaultIconUrl();
      return;
    }
    url.value = (await fetchMediaObjectUrl(id, name)) ?? getDefaultIconUrl();
  });
  return url;
}

/** Resolve a list of screenshot file names to object URLs (preserves order,
 *  drops any that fail). Used to feed el-image's preview-src-list lightbox. */
export async function loadScreenshotUrls(nodeId: string, fileNames: string[]): Promise<string[]> {
  const resolved = await Promise.all(fileNames.map((name) => fetchMediaObjectUrl(nodeId, name)));
  return resolved.filter((u): u is string => u !== null);
}

export { fetchMediaObjectUrl };
