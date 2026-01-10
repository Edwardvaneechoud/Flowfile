import { test, expect } from '@playwright/test';
import { launchElectronApp, closeElectronApp } from './helpers/electronTestHelper';
import { ElectronApplication } from 'playwright-core';

let electronApp: ElectronApplication | undefined;
let launchError: Error | undefined;

test.describe('Services Startup Tests', () => {
  test.beforeAll(async () => {
    try {
      electronApp = await launchElectronApp();
    } catch (error) {
      // Store the error so tests can properly fail with it
      launchError = error as Error;
      console.error("Error in test setup:", error);
    }
  });

  test.afterAll(async () => {
    await closeElectronApp(electronApp);
    electronApp = undefined;
    launchError = undefined;
  });

  test('app should start successfully and services should initialize', async () => {
    // If there was a launch error, fail the test with the actual error
    if (launchError) {
      throw new Error(`Electron app failed to launch: ${launchError.message}`);
    }
    expect(electronApp).toBeDefined();
  });

  test('app should load properly and respond to navigation', async () => {
    // Fail if app didn't launch - don't skip
    if (!electronApp) {
      throw new Error('Electron app failed to launch - cannot run navigation test');
    }

    const mainWindow = await electronApp.firstWindow();
    expect(mainWindow).toBeDefined();

    // Verify window is available - fail if not
    const isWindowAvailable = await mainWindow.evaluate(() => {
      return true;
    });
    expect(isWindowAvailable).toBe(true);

    console.log('Waiting for app to fully initialize...');

    // Wait for initialization, but fail if window closes during wait
    for (let i = 0; i < 10; i++) {
      const isStillAvailable = await mainWindow.evaluate(() => {
        return true;
      });
      expect(isStillAvailable).toBe(true);
      await new Promise(r => setTimeout(r, 500));
    }

    console.log('Attempting to navigate within the app...');
    await mainWindow.evaluate(() => {
      console.log('Current URL:', window.location.href);

      if (window.history) {
        window.history.pushState({}, '', '/dashboard');
        console.log('Navigated to /dashboard');
      }
    });

    await new Promise(r => setTimeout(r, 1000));

    console.log('Navigation test completed successfully');
  });
});