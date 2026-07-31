// Single renderer-side source of truth for links into the published docs site.
//
// Mirrored by `src-tauri/src/menu.rs` (`DOCS_URL`), which powers the native
// Help menu — change both together.
//
// In-app docs always open in the system browser (`desktop.openExternal`): the
// Tauri CSP only allows `frame-src 'self' https://accounts.google.com`, so the
// docs site cannot be iframed inside the desktop shell.

export const DOCS_BASE_URL = "https://edwardvaneechoud.github.io/Flowfile/";

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
