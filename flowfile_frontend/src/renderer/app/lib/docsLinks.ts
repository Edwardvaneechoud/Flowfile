// Single renderer-side source of truth for links into the published docs site.
//
// Mirrored by `src-tauri/src/menu.rs` (`DOCS_URL`, `bug_report_url`), which powers the native
// Help menu — change both together.
//
// In-app docs always open in the system browser (`desktop.openExternal`): the
// Tauri CSP only allows `frame-src 'self' https://accounts.google.com`, so the
// docs site cannot be iframed inside the desktop shell.

export const DOCS_BASE_URL = "https://edwardvaneechoud.github.io/Flowfile/";
export const REPO_URL = "https://github.com/edwardvaneechoud/Flowfile";
export const ISSUES_URL = `${REPO_URL}/issues`;
export const DISCUSSIONS_URL = `${REPO_URL}/discussions`;
export const NEW_ISSUE_URL = `${ISSUES_URL}/new`;
export const NODE_REQUEST_ISSUE_URL = `${NEW_ISSUE_URL}?template=node_request.yml`;

export interface BugReportContext {
  version?: string;
  /** Where Flowfile runs, e.g. "Desktop app (mac)" or "Browser". */
  environment?: string;
  userAgent?: string;
}

/**
 * New-issue URL prefilled with the bug-report sections of
 * `.github/ISSUE_TEMPLATE/bug_report.md` plus the running version and environment,
 * so a report arrives with the details a maintainer would otherwise ask for.
 * Nothing is sent: GitHub only shows the draft for the user to edit and submit.
 */
export function bugReportUrl(context: BugReportContext = {}): string {
  const env = [
    `- Flowfile version: ${context.version || "unknown"}`,
    `- Environment: ${context.environment || "unknown"}`,
  ];
  if (context.userAgent) env.push(`- Browser: ${context.userAgent}`);
  const body = [
    "**Describe the bug**",
    "",
    "",
    "**To reproduce**",
    "1. ",
    "",
    "**Expected behavior**",
    "",
    "",
    "**Screenshots / flow file**",
    "If applicable, attach a screenshot or the flow (.yaml) that shows the problem.",
    "",
    "**Environment**",
    ...env,
    "",
  ].join("\n");
  return `${NEW_ISSUE_URL}?${new URLSearchParams({ body })}`;
}

/**
 * Build an absolute docs URL from a site-relative path.
 *
 * `path` must include the `.html` extension: mkdocs is configured with
 * `use_directory_urls: false`, so `users/visual-editor/nodes/input/` is a 404
 * while `users/visual-editor/nodes/input.html` resolves. A `#fragment` may be
 * appended to the path.
 */
export function docsUrl(path = ""): string {
  return DOCS_BASE_URL + path.replace(/^\/+/, "");
}
