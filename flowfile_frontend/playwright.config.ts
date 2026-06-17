//playwright.config.ts
import { PlaywrightTestConfig } from '@playwright/test';

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
  },
};

export default config;
