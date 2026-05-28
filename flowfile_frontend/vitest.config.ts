// Vitest config — narrow unit-test runner for pure store helpers.
//
// Frontend integration coverage lives in Playwright (`tests/`). This config
// only picks up `src/**/*.test.ts` so adding a unit test alongside a module
// is the easy path; full DOM-bound testing (jsdom / happy-dom) is out of
// scope until something needs it.

import Path from "path";
import { fileURLToPath } from "url";
import { defineConfig } from "vitest/config";

const __dirname = Path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    environment: "node",
    globals: false,
  },
  resolve: {
    alias: {
      "@": Path.resolve(__dirname, "./src/renderer/app"),
    },
  },
});
