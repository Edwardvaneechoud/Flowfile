// session.ts
import { session } from "electron";

export function modifySessionHeaders() {
  session.defaultSession.webRequest.onHeadersReceived((details, callback) => {
    if (details.responseHeaders) {
      const isGoogleOAuth = details.url.includes("accounts.google.com");

      if (isGoogleOAuth) {
        callback({
          responseHeaders: {
            ...details.responseHeaders,
            "Cross-Origin-Opener-Policy": ["unsafe-none"],
            "Cross-Origin-Embedder-Policy": ["unsafe-none"],
          },
        });
      } else {
        const csp =
          details.responseHeaders["Content-Security-Policy"] ||
          details.responseHeaders["content-security-policy"];

        if (csp) {
          const newCsp = csp.map((policy) =>
            policy
              .replace(
                "script-src",
                "script-src 'self' 'unsafe-inline' 'unsafe-eval' blob: https://apis.google.com",
              )
              .replace("worker-src", "worker-src 'self' blob:"),
          );

          callback({
            responseHeaders: {
              ...details.responseHeaders,
              "Content-Security-Policy": newCsp,
            },
          });
        } else {
          callback({
            responseHeaders: {
              ...details.responseHeaders,
              "Content-Security-Policy": [
                "script-src 'self' 'unsafe-inline' 'unsafe-eval' blob: https://apis.google.com; worker-src 'self' blob:",
              ],
            },
          });
        }
      }
    } else {
      callback({ responseHeaders: details.responseHeaders });
    }
  });
}
