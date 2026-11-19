/**
 * Where a tab goes once its session has ended: the sign-in page on the address it is on now. Only the hash
 * changes, so a page served under a path (`/ui/`) stays under it, and the reload that follows drops whatever the
 * stores still held of the old session, as the router's chunk recovery does.
 */

// Kept in sessionStorage so the sign-in page, after the reload, can say why the tab is there.
const SESSION_ENDED = "auth_session_ended";

/** The sign-in route for a tab at `hash`, bringing the person back to where they were. */
export function signInRoute(hash: string): string {
  const route = hash.replace(/^#/, "");
  const path = route.split("?")[0];
  if (!route.startsWith("/") || route === "/" || path === "/login") return "/login";
  return `/login?redirect=${encodeURIComponent(route)}`;
}

/** Send this tab to the sign-in page and reload it. */
export function leaveForSignIn(): void {
  sessionStorage.setItem(SESSION_ENDED, "1");
  window.location.hash = signInRoute(window.location.hash);
  window.location.reload();
}

/** Whether this tab came to the sign-in page because its session ended; answered once. */
export function takeSessionEnded(): boolean {
  const ended = sessionStorage.getItem(SESSION_ENDED) !== null;
  sessionStorage.removeItem(SESSION_ENDED);
  return ended;
}
