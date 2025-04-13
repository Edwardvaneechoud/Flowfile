import { _electron as electron } from 'playwright';
import { ElectronApplication } from 'playwright-core';
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
      const isComplete = process.platform === 'win32' 
        ? (servicesStarted && startupReceived && electronApp)
        : (servicesStarted && startupReceived && windowReady && electronApp);
        
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

      if (process.platform === 'win32') {
        setTimeout(() => {
          if (electronApp && (servicesStarted || startupReceived)) {
            console.log('Safety timeout reached, but Electron app appears to be running');
            clearTimeout(timeout);
            resolve(electronApp);
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
      mainWindow = await app.firstWindow().catch(() => null);
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