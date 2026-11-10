// Vitest config — narrow unit-test runner for pure store helpers.
//
// Frontend integration coverage lives in Playwright (`tests/`). This config
// only picks up `src/**/*.test.ts` so adding a unit test alongside a module
// is the easy path. The Vue plugin lets a test import a `.vue` file and mount
// it under happy-dom (`__fixtures__/mount.ts`); a test opts into the DOM with
// `// @vitest-environment happy-dom`.

import Path from "path";
import { fileURLToPath } from "url";
import vue from "@vitejs/plugin-vue";
import { defineConfig } from "vitest/config";

const __dirname = Path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  plugins: [vue()],
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
