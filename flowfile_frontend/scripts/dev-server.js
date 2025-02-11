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

async function startRenderer() {
    viteServer = await Vite.createServer({
        configFile: Path.join(__dirname, '..', 'vite.config.js'),
        mode: 'development',
    });

    return viteServer.listen();
}

async function buildAndCopyDocs() {
    console.log(Chalk.blueBright('[docs] ') + 'Building documentation...');
    
    try {
        const rootDir = Path.join(__dirname, '..', '..');
        const docsDestDir = Path.join(__dirname, '..', 'src', 'renderer', 'public', 'docs');
        const mkdocsBuildDir = Path.join(rootDir, 'site');

        // Build MkDocs
        ChildProcess.execSync('mkdocs build', {
            cwd: rootDir,
            stdio: 'inherit'
        });

        // Clear existing docs
        if (FileSystem.existsSync(docsDestDir)) {
            FileSystem.rmSync(docsDestDir, { recursive: true, force: true });
        }

        // Copy new docs
        copyDirForDocs(mkdocsBuildDir, docsDestDir);
        
        console.log(Chalk.greenBright('[docs] ') + 'Documentation built and copied successfully!');
    } catch (error) {
        console.error(Chalk.redBright('[docs] ') + 'Failed to build documentation:', error);
    }
}

function copyDirForDocs(src, dest) {
    if (!FileSystem.existsSync(dest)) {
        FileSystem.mkdirSync(dest, { recursive: true });
    }

    const entries = FileSystem.readdirSync(src, { withFileTypes: true });

    for (const entry of entries) {
        const srcPath = Path.join(src, entry.name);
        const destPath = Path.join(dest, entry.name);

        if (entry.isDirectory()) {
            copyDirForDocs(srcPath, destPath);
        } else {
            FileSystem.copyFileSync(srcPath, destPath);
        }
    }
}


async function startElectron() {
    if (electronProcess) { // single instance lock
        return;
    }

    try {
        await compileTs(Path.join(__dirname, '..', 'src', 'main'));
    } catch {
        console.log(Chalk.redBright('Could not start Electron because of the above typescript error(s).'));
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

        process.stdout.write(Chalk.blueBright(`[electron] `) + Chalk.white(data.toString()))
    });

    electronProcess.stderr.on('data', data =>
        process.stderr.write(Chalk.blueBright(`[electron] `) + Chalk.white(data.toString()))
    );

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
The working dir of Electron is build/main instead of src/main because of TS.
tsc does not copy static files, so copy them over manually for dev server.
*/
function copy(path) {
    FileSystem.cpSync(
        Path.join(__dirname, '..', 'src', 'main', path),
        Path.join(__dirname, '..', 'build', 'main', path),
        { recursive: true }
    );
}

function stop() {
    viteServer.close();
    process.exit();
}


async function start() {
    console.log(`${Chalk.greenBright('=======================================')}`);
    console.log(`${Chalk.greenBright('Starting Electron + Vite Dev Server...')}`);
    console.log(`${Chalk.greenBright('=======================================')}`);

    // Build docs first
    await buildAndCopyDocs();

    const devServer = await startRenderer();
    rendererPort = devServer.config.server.port;
    copyLoadingHtml();
    copyStaticFiles();
    startElectron();

    // Watch main directory
    const path = Path.join(__dirname, '..', 'src', 'main');
    Chokidar.watch(path, {
        cwd: path,
    }).on('change', (path) => {
        console.log(Chalk.blueBright(`[electron] `) + `Change in ${path}. reloading... 🚀`);

        if (path.startsWith(Path.join('static', '/'))) {
            copy(path);
        }

        restartElectron();
    });

    // Watch docs directory
    const docsPath = Path.join(__dirname, '..', '..', 'docs');
    const mkdocsFile = Path.join(__dirname, '..', '..', 'mkdocs.yml');
    
    Chokidar.watch([docsPath, mkdocsFile], {
        ignoreInitial: true
    }).on('all', (event, path) => {
        console.log(Chalk.blueBright('[docs] ') + `Change detected in ${path}. Rebuilding... 🚀`);
        buildAndCopyDocs();
    });
}

start();
