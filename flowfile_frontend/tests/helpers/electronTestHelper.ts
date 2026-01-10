import { _electron as electron } from 'playwright';
import { ElectronApplication, Page } from 'playwright-core';
import * as path from 'path';

const SERVICES_STARTUP_TIMEOUT = 90000;

export async function launchElectronApp(): Promise<ElectronApplication> {
  const electronPath = process.platform === 'win32'
    ? './dist/win-unpacked/Flowfile.exe'
    : process.platform === 'darwin'
      ? './dist/mac-arm64/Flowfile.app/Contents/MacOS/Flowfile'
      : './dist/linux-unpacked/flowfile';

  const appPath = path.join(__dirname, '../../');
  const fullPath = path.resolve(appPath, electronPath);

  console.log(`Launching Electron app from: ${fullPath}`);

  let electronApp: ElectronApplication | null = null;

  const startupSuccessPromise = new Promise<ElectronApplication>((resolve, reject) => {
    let servicesStarted = false;
    let startupReceived = false;
    let windowReady = false;

    const timeout = setTimeout(() => {
      reject(new Error('Timeout waiting for startup messages in console'));
    }, SERVICES_STARTUP_TIMEOUT);

    const checkComplete = () => {
      // All platforms should wait for the same signals: services started, startup received, and window ready
      const isComplete = servicesStarted && startupReceived && windowReady && electronApp;

      if (isComplete) {
        clearTimeout(timeout);
        resolve(electronApp!);
      }
    };

    electron.launch({
      executablePath: fullPath,
      args: ['--no-sandbox'],
      timeout: 60000,
    })
    .then(app => {
      electronApp = app;

      app.on('console', (msg) => {
        const text = msg.text();
        console.log(`Electron app console: ${text}`);

        if (text.includes('All services started successfully')) {
          console.log('Detected services started successfully');
          servicesStarted = true;
          checkComplete();
        }

        if (text.includes('Electron app startup successful')) {
          console.log('Detected startup success message');
          startupReceived = true;
          checkComplete();
        }
        
        if (text.includes('Window ready to show')) {
          console.log('Detected window ready');
          windowReady = true;
          checkComplete();
        }
      });

      // Safety timeout for Windows: console message detection can be unreliable,
      // but we should still require all startup signals (including windowReady)
      // before considering the app ready.
      if (process.platform === 'win32') {
        setTimeout(() => {
          if (electronApp && servicesStarted && startupReceived && windowReady) {
            console.log('Safety timeout reached, but Electron app appears to be running with all required signals');
            clearTimeout(timeout);
            resolve(electronApp);
          } else {
            console.log(`Safety timeout: servicesStarted=${servicesStarted}, startupReceived=${startupReceived}, windowReady=${windowReady}`);
          }
        }, 30000);
      }

      return app;
    })
    .catch(error => {
      clearTimeout(timeout);
      reject(error);
    });
  });

  try {
    const app = await startupSuccessPromise;
    console.log('Application started successfully according to console logs');
    return app;
  } catch (error) {
    console.error('Error launching Electron app:', error);
    if (electronApp) {
      await closeElectronApp(electronApp);
    }
    throw error;
  }
}

export async function closeElectronApp(app: ElectronApplication | undefined): Promise<void> {
  if (!app) {
    console.log('No Electron app to close');
    return;
  }

  try {
    let mainWindow;
    try {
      // Use getMainWindow to get the actual main window, not the closed loading window
      mainWindow = await getMainWindow(app).catch(() => null);
      if (mainWindow) {
        console.log('Giving app chance to clean up resources...');
        
        try {
          await mainWindow.evaluate(() => {
            const api = (window as any).electronAPI;
            if (api && api.quitApp) {
              console.log('Received quit-app command, quitting...');
              api.quitApp();
              return true;
            }
            return false;
          }).catch(e => {
            console.log('Evaluation error (expected if window already closing):', e.message);
            return false;
          });
        } catch (e) {
          console.log('Command evaluation failed:', e.message);
        }

        const waitTime = process.platform === 'win32' ? 5000 : 3000;
        await new Promise(resolve => setTimeout(resolve, waitTime));
      }
    } catch (gracefulError) {
      console.log('Graceful shutdown attempt failed (expected):', gracefulError.message);
    }

    console.log('Closing Electron app...');
    await app.close().catch(e => {
      console.log('Error during app.close():', e.message);
    });
    console.log('Electron app closed');
  } catch (error) {
    console.error('Error in closeElectronApp:', error);
  }
}

/**
 * Gets the main application window (not the loading window).
 *
 * The app opens a loading window first, then the main window.
 * When the main window is ready, the loading window is closed.
 * Using firstWindow() would return the loading window which may be closed.
 * This function finds the currently open window that's not the loading screen.
 */
export async function getMainWindow(app: ElectronApplication): Promise<Page> {
  // Get all currently open windows
  const windows = app.windows();

  // If there's only one window, return it (should be the main window after loading closes)
  if (windows.length === 1) {
    return windows[0];
  }

  // If multiple windows, find the main one (not loading.html)
  for (const win of windows) {
    try {
      const url = win.url();
      // The loading window loads loading.html, the main window loads localhost or index.html
      if (!url.includes('loading.html')) {
        return win;
      }
    } catch {
      // Window might be closed, skip it
      continue;
    }
  }

  // Fallback: wait a bit for loading window to close and try again
  await new Promise(resolve => setTimeout(resolve, 1000));
  const retryWindows = app.windows();
  if (retryWindows.length > 0) {
    return retryWindows[0];
  }

  throw new Error('No main window found');
}