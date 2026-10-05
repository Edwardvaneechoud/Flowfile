// Compare a kernel's own image tag against its flavour's current registry tag, so the UI can say whether
// the kernel runs an outdated image. Non-version tags (a :local dev build) and other repos compare to null.

export function parseImageVersion(tag: string): number[] | null {
  const idx = tag.lastIndexOf(":");
  if (idx === -1) return null;
  const nums = tag
    .slice(idx + 1)
    .split(".")
    .map(Number);
  return nums.some((n) => !Number.isInteger(n)) ? null : nums;
}

function isOlder(a: number[], b: number[]): boolean {
  const len = Math.max(a.length, b.length);
  for (let i = 0; i < len; i++) {
    const av = a[i] ?? 0;
    const bv = b[i] ?? 0;
    if (av !== bv) return av < bv;
  }
  return false;
}

export interface ImageUpdateInfo {
  available: boolean;
  latest: string;
}

/** Whether `latest` (the flavour's current tag) is a newer release of `current` (the kernel's image). */
export function imageUpdateAvailable(
  current: string | null | undefined,
  latest: string | null | undefined,
): ImageUpdateInfo | null {
  if (!current || !latest) return null;
  const repo = (t: string) => t.slice(0, t.lastIndexOf(":"));
  if (repo(current) !== repo(latest)) return null;
  const cv = parseImageVersion(current);
  const lv = parseImageVersion(latest);
  if (!cv || !lv) return null;
  return { available: isOlder(cv, lv), latest };
}
