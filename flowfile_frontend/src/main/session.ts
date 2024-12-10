import { session } from "electron";
import { platform } from 'os';

export function modifySessionHeaders() {
  // Determine platform
  const isWindows = platform() === 'win32';
  const isMac = platform() === 'darwin';

  session.defaultSession.webRequest.onHeadersReceived((details, callback) => {
    if (!details.responseHeaders) {
      callback({ responseHeaders: details.responseHeaders });
      return;
    }

    const isGoogleOAuth = details.url.includes("accounts.google.com");
    
    if (isGoogleOAuth) {
      callback({
        responseHeaders: {
          ...details.responseHeaders,
          "Cross-Origin-Opener-Policy": ["unsafe-none"],
          "Cross-Origin-Embedder-Policy": ["unsafe-none"],
        },
      });
      return;
    }

    // Define platform-specific connect sources
    const platformSpecificConnectSources = [
      "'self'",
      "https://apis.google.com",
      "http://localhost:*",
      "ws://localhost:*",
      "http://127.0.0.1:*",
      "ws://127.0.0.1:*"
    ];

    // Add platform-specific sources
    if (isWindows) {
      // Windows-specific sources if needed
      platformSpecificConnectSources.push(
        // Add Windows-specific URLs if needed
      );
    }

    if (isMac) {
      // macOS-specific sources if needed
      platformSpecificConnectSources.push(
        // Add macOS-specific URLs if needed
      );
    }

    // Define strict CSP defaults with additional style sources
    const defaultDirectives = {
      'default-src': ["'self'"],
      'script-src': ["'self'", "https://apis.google.com"],
      'style-src': [
        "'self'",
        "'unsafe-inline'",
        "https://fonts.googleapis.com",
        "https://cdn.jsdelivr.net",
      ],
      'style-src-elem': [
        "'self'",
        "'unsafe-inline'",
        "https://fonts.googleapis.com",
        "https://cdn.jsdelivr.net",
      ],
      'font-src': [
        "'self'",
        "https://fonts.gstatic.com",
        "data:", // Added to allow data URI fonts
      ],
      'worker-src': ["'self'", "blob:"],
      'connect-src': platformSpecificConnectSources,
      'img-src': ["'self'", "data:", "blob:"],
      'object-src': ["'none'"],
      'media-src': ["'self'"],
      'frame-src': ["'self'", "https://accounts.google.com"],
      'child-src': ["'self'", "blob:"],
      'base-uri': ["'self'"],
      'form-action': ["'self'"],
      'frame-ancestors': ["'none'"],
      'upgrade-insecure-requests': [],
    };

    // Add development-specific rules
    if (process.env.NODE_ENV === 'development') {
      defaultDirectives['script-src'].push("'unsafe-eval'", "'unsafe-inline'");
      
      // Add development-specific connect sources
      defaultDirectives['connect-src'].push(
        // Add any development-specific URLs here
      );
    }

    // Construct CSP string
    const cspString = Object.entries(defaultDirectives)
      .map(([directive, sources]) => {
        return sources.length > 0
          ? `${directive} ${sources.join(' ')}`
          : directive;
      })
      .join('; ');

    callback({
      responseHeaders: {
        ...details.responseHeaders,
        'Content-Security-Policy': [cspString],
      },
    });
  });
}