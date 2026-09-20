//playwright.config.ts
import { PlaywrightTestConfig } from '@playwright/test';

const BASE_URL = process.env.TEST_URL || 'http://localhost:8080';

const config: PlaywrightTestConfig = {
  testDir: './tests',
  // Increase timeout to 120 seconds to allow for slower Windows CI startup
  // (SERVICES_STARTUP_TIMEOUT is 90 seconds, so we need headroom)
  timeout: 120000,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  // Use 1 worker for Electron tests to prevent port conflicts
  workers: 1,
  reporter: 'html',
  use: {
    actionTimeout: 15000,
    trace: 'on-first-retry',
    video: 'on-first-retry',
    screenshot: 'only-on-failure',
    // Every context starts with the one-time telemetry consent already answered:
    // against a real core it is undecided, so its modal would race any designer
    // test and swallow the clicks underneath it.
    storageState: {
      cookies: [],
      origins: [
        {
          origin: new URL(BASE_URL).origin,
          localStorage: [{ name: 'flowfile-telemetry-consent-answered', value: '1' }],
        },
      ],
    },
  },
};

export default config;
