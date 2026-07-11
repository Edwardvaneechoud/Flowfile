// Custom-node icons are served by a JWT-gated endpoint, so plain <img src>
// URLs 401 — fetch them as authed blobs and hand out object URLs instead.
import { ref, watchEffect, type Ref } from "vue";
import axios from "axios";
import { getDefaultIconUrl, getImageUrl, isBuiltinIcon } from "../features/designer/utils";

const objectUrls = new Map<string, string>();
const inflight = new Map<string, Promise<string | null>>();

async function fetchIconObjectUrl(name: string): Promise<string | null> {
  const cached = objectUrls.get(name);
  if (cached) return cached;
  const pending = inflight.get(name);
  if (pending) return pending;

  const request = axios
    .get(`/user_defined_components/icon/${name}`, { responseType: "blob" })
    .then((response) => {
      const url = URL.createObjectURL(response.data as Blob);
      objectUrls.set(name, url);
      return url;
    })
    .catch(() => null)
    .finally(() => {
      inflight.delete(name);
    });
  inflight.set(name, request);
  return request;
}

export function invalidateIconCache(name?: string) {
  if (name === undefined) {
    objectUrls.forEach((url) => URL.revokeObjectURL(url));
    objectUrls.clear();
    return;
  }
  const url = objectUrls.get(name);
  if (url) {
    URL.revokeObjectURL(url);
    objectUrls.delete(name);
  }
}

/** Reactive icon URL for any node icon name: bundled assets resolve directly,
 *  uploaded icons resolve through the authed blob fetch, failures fall back
 *  to the default icon. */
export function useNodeIconUrl(name: () => string | undefined | null): Ref<string> {
  const url = ref(getDefaultIconUrl());
  watchEffect(async () => {
    const iconName = name();
    if (!iconName || isBuiltinIcon(iconName)) {
      url.value = getImageUrl(iconName ?? "");
      return;
    }
    url.value = (await fetchIconObjectUrl(iconName)) ?? getDefaultIconUrl();
  });
  return url;
}

export { fetchIconObjectUrl };
