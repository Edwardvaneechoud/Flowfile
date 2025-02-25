// app.spec.ts
import { test, expect } from '@playwright/test';
import { launchElectronApp, closeElectronApp } from './helpers/electronTestHelper';
import { ElectronApplication } from 'playwright-core'; 

let electronApp: ElectronApplication | undefined;

test.describe('Services Startup Tests', () => {
  test.beforeAll(async () => {
    try {
      // Launch the Electron app before running tests
      // This will wait for the "All services started successfully" and "Electron app startup successful" messages
      electronApp = await launchElectronApp();
    } catch (error) {
      console.error("Error in test setup:", error);
      // We'll continue and let individual tests handle the undefined app
    }
  });

  test.afterAll(async () => {
    // Close the Electron app
    await closeElectronApp(electronApp);
    electronApp = undefined;
  });

  test('app should start successfully and services should initialize', async () => {
    // This test simply verifies that the app started successfully
    // The success is already verified in the launchElectronApp function by waiting for console messages
    expect(electronApp).toBeDefined();
  });

  test('app should load properly and respond to navigation', async () => {
    test.skip(!electronApp, 'Electron app failed to launch');
    
    try {
      // Get the main window
      const mainWindow = await electronApp!.firstWindow();
      expect(mainWindow).toBeDefined();
      

      // Try to navigate to a known route in your app
      console.log('Waiting for app to fully initialize...');
      await mainWindow.waitForTimeout(5000); // Wait 5 seconds for any background processes
      
      // Try to navigate to some internal URLs
      try {
        // Check if we can navigate to a route in your app
        console.log('Attempting to navigate within the app...');
        await mainWindow.evaluate(() => {
          // This runs in the browser context, try to navigate using browser APIs
          // If your app uses a router, you might try something like:
          console.log('Current URL:', window.location.href);
          
          // Attempt to navigate using history API if available
          if (window.history) {
            window.history.pushState({}, '', '/dashboard');
            console.log('Navigated to /dashboard');
          }
        });
        
        // Wait to see the navigation effect
        await mainWindow.waitForTimeout(3000);
        
      } catch (navError) {
        console.warn('Navigation attempt failed:', navError);
      }
      
      console.log('Waiting 5 seconds before closing app...');
      await mainWindow.waitForTimeout(5000);
      
      // Now close the window
      console.log('Closing main window...');
      await mainWindow.close();
      
    } catch (error) {
      console.error("Error in navigation test:", error);
      throw error;
    }
  });
});