process.env.NODE_ENV = 'development';

const Vite = require('vite');
const ChildProcess = require('child_process');
const Path = require('path');
const Chalk = require('chalk');
const Chokidar = require('chokidar');
const Electron = require('electron');
const compileTs = require('./private/tsc');
const FileSystem = require('fs');
const { EOL } = require('os');

let viteServer = null;
let electronProcess = null;
let electronProcessLocker = false;
let rendererPort = 0;
let mkdocsProcess = null; // Global variable for MkDocs process

async function startRenderer() {
  viteServer = await Vite.createServer({
    configFile: Path.join(__dirname, '..', 'vite.config.mjs'),
    mode: 'development',
  });

  return viteServer.listen();
}

async function startElectron() {
  if (electronProcess) {
    // Prevent starting multiple Electron instances.
    return;
  }

  try {
    await compileTs(Path.join(__dirname, '..', 'src', 'main'));
  } catch {
    console.log(Chalk.redBright('Could not start Electron because of the above TypeScript error(s).'));
    electronProcessLocker = false;
    return;
  }

  const args = [
    Path.join(__dirname, '..', 'build', 'main', 'main.js'),
    rendererPort,
  ];
  electronProcess = ChildProcess.spawn(Electron, args);
  electronProcessLocker = false;

  electronProcess.stdout.on('data', data => {
    if (data == EOL) {
      return;
    }
    process.stdout.write(Chalk.blueBright(`[electron] `) + Chalk.white(data.toString()));
  });

  electronProcess.stderr.on('data', data =>
    process.stderr.write(Chalk.blueBright(`[electron] `) + Chalk.white(data.toString()))
  );

  // When Electron exits, ensure the whole process stops.
  electronProcess.on('exit', () => stop());
}

function restartElectron() {
  if (electronProcess) {
    electronProcess.removeAllListeners('exit');
    electronProcess.kill();
    electronProcess = null;
  }

  if (!electronProcessLocker) {
    electronProcessLocker = true;
    startElectron();
  }
}

function copyLoadingHtml() {
  console.log(Chalk.blueBright(`[electron] `) + 'Copying loading.html...');

  const sourcePath = Path.join(__dirname, '..', 'src', 'main', 'loading.html');
  const destPath = Path.join(__dirname, '..', 'build', 'main', 'loading.html');

  try {
    FileSystem.mkdirSync(Path.dirname(destPath), { recursive: true });
    FileSystem.copyFileSync(sourcePath, destPath);
    console.log(Chalk.greenBright(`[electron] `) + 'Successfully copied loading.html');
  } catch (error) {
    console.error(Chalk.redBright(`[electron] `) + 'Error handling loading.html:', error);
    console.error(Chalk.gray(`[electron] Error details: ${JSON.stringify(error, null, 2)}`));
  }
}

function copyStaticFiles() {
  copy('static');
}

/*
The working directory of Electron is build/main instead of src/main because of TS.
tsc does not copy static files, so we copy them over manually for the dev server.
*/
function copy(relativePath) {
  FileSystem.cpSync(
    Path.join(__dirname, '..', 'src', 'main', relativePath),
    Path.join(__dirname, '..', 'build', 'main', relativePath),
    { recursive: true }
  );
}

function stop() {
  stopMkDocs();
  if (viteServer) {
    viteServer.close();
  }
  process.exit();
}

async function start() {
  console.log(`${Chalk.greenBright('=======================================')}`);
  console.log(`${Chalk.greenBright('Starting Electron + Vite Dev Server...')}`);
  console.log(`${Chalk.greenBright('=======================================')}`);

  if (process.env.NODE_ENV === 'development') {
    console.log('Starting local MkDocs server...');
    startMkDocs();
  }

  const devServer = await startRenderer();
  rendererPort = devServer.config.server.port;
  copyLoadingHtml();
  copyStaticFiles();
  startElectron();

  // Watch the main directory for changes.
  const watchPath = Path.join(__dirname, '..', 'src', 'main');
  Chokidar.watch(watchPath, {
    cwd: watchPath,
  }).on('change', (changedPath) => {
    console.log(Chalk.blueBright(`[electron] `) + `Change in ${changedPath}. Reloading... 🚀`);

    if (changedPath.startsWith(Path.join('static', '/'))) {
      copy(changedPath);
    }

    restartElectron();
  });
}

function startMkDocs() {
  // Set the working directory to where mkdocs.yml is located (two levels up)
  const mkdocsWorkingDirectory = Path.join(__dirname, '..', '..');

  // Spawn the MkDocs server on port 8000 with the correct cwd.
  mkdocsProcess = ChildProcess.spawn('mkdocs', ['serve', '--dev-addr=127.0.0.1:8000'], {
    cwd: mkdocsWorkingDirectory,
    shell: true,
    stdio: 'inherit'
  });

  mkdocsProcess.on('close', (code) => {
    console.log(`MkDocs process exited with code ${code}`);
  });
}

// Ensure the MkDocs process is killed when your dev server stops.
function stopMkDocs() {
  if (mkdocsProcess) {
    console.log('Stopping MkDocs process...');
    mkdocsProcess.kill();
  }
}

// Register signal handlers to ensure cleanup happens even if the process is terminated.
process.on('SIGINT', () => {
  console.log('Received SIGINT, shutting down...');
  stopMkDocs();
  process.exit();
});

process.on('SIGTERM', () => {
  console.log('Received SIGTERM, shutting down...');
  stopMkDocs();
  process.exit();
});

// This handler is called when the process is about to exit.
process.on('exit', () => {
  stopMkDocs();
});

start();
