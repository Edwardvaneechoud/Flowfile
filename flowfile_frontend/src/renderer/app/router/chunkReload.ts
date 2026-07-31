/**
 * Recovery decision for a route chunk that can't be fetched.
 *
 * Every route is a lazy `import()`, and one failed request inside that chunk
 * rejects the whole navigation with no retry — the route becomes a dead link
 * that keeps failing identically. Two real causes, both fixed by a reload: in
 * dev, Vite's pre-bundled deps go stale under a running server ("504 Outdated
 * Optimize Dep"); in production, an open tab asks for a chunk hash a redeploy
 * removed. Reload once per target and then stop — a second failure on the same
 * target means reloading cannot help, and looping would only hide the problem.
 */

export const CHUNK_RELOAD_KEY = "flowfile:chunk-reload";

const CHUNK_LOAD_ERROR =
  /Failed to fetch dynamically imported module|error loading dynamically imported module|Importing a module script failed|Unable to preload CSS/i;

export type ChunkRecovery = "not-a-chunk-error" | "reload" | "give-up";

export function isChunkLoadError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return CHUNK_LOAD_ERROR.test(message);
}

export function decideChunkRecovery(
  error: unknown,
  targetPath: string,
  previousAttempt: string | null,
): ChunkRecovery {
  if (!isChunkLoadError(error)) return "not-a-chunk-error";
  return previousAttempt === targetPath ? "give-up" : "reload";
}
