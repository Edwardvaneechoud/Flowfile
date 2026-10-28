// Vitest config — narrow unit-test runner for pure store helpers.
//
// Frontend integration coverage lives in Playwright (`tests/`). This config
// picks up `src/**/*.test.ts`; component tests opt into happy-dom per file.

import Path from "path";
import { fileURLToPath } from "url";
import { defineConfig } from "vitest/config";
import vue from "@vitejs/plugin-vue";

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
