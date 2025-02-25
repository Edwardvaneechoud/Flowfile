// flowfile_frontend/tests/helpers/electronTestHelper.ts
import { _electron as electron } from 'playwright';
import { ElectronApplication } from 'playwright-core';
import * as path from 'path';

const SERVICES_STARTUP_TIMEOUT = 60000;

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

    const timeout = setTimeout(() => {
      reject(new Error('Timeout waiting for startup messages in console'));
    }, SERVICES_STARTUP_TIMEOUT);

    const checkComplete = () => {
      if (servicesStarted && startupReceived && electronApp) {
        clearTimeout(timeout);
        resolve(electronApp);
      }
    };

    electron.launch({
      executablePath: fullPath,
      args: ['--no-sandbox'],
      timeout: 30000,
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
      });

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
    try {
      const mainWindow = await app.firstWindow();
      if (mainWindow) {
        console.log('Giving app chance to clean up resources...');
        await mainWindow.evaluate(() => {
          console.log('Starting application cleanup...');
          // Type assertion to inform TypeScript that electronAPI might exist
          const api = (window as any).electronAPI;
          if (api && api.quitApp) {
            api.quitApp();
            return true;
          }
          return false;
        }).catch(e => {
          console.log('Evaluation error (expected if electronAPI is not available):', e.message);
          return false;
        });

        await new Promise(resolve => setTimeout(resolve, 3000));
      }
    } catch (gracefulError) {
      console.log('Graceful shutdown attempt failed (expected):', gracefulError.message);
    }

    console.log('Closing Electron app...');
    await app.close();
    console.log('Electron app closed');
  } catch (error) {
    console.error('Error in closeElectronApp:', error);
  }
}
