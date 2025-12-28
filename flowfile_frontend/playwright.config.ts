//playwright.config.ts
import { PlaywrightTestConfig } from '@playwright/test';

const config: PlaywrightTestConfig = {
  testDir: './tests',
  timeout: 60000,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  reporter: 'html',
  // Use 1 worker for Electron tests to prevent port conflicts
  // Web tests can override this via CLI: --workers=3
  workers: 1,
  use: {
    actionTimeout: 15000,
    trace: 'on-first-retry',
    video: 'on-first-retry',
    screenshot: 'only-on-failure',
  },
  projects: [
    {
      // Electron tests - run with the built Electron app
      // These launch their own Electron app with bundled backend services
      // MUST run serially since multiple Electron apps can't bind to same ports
      name: 'electron',
      testMatch: ['app.spec.ts', 'complex-flow.spec.ts'],
      fullyParallel: false,
    },
    {
      // Web tests - require external backend and frontend servers
      // Run these with: make test_e2e or make test_e2e_dev
      name: 'web',
      testMatch: ['web-flow.spec.ts'],
      fullyParallel: true,
    },
  ],
};

export default config;
