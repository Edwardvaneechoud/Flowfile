const Path = require('path');
const Chalk = require('chalk');
const FileSystem = require('fs');
const Vite = require('vite');
const compileTs = require('./private/tsc');


function copyLoadingHtml() {
    console.log(Chalk.blueBright('Copying loading.html...'));
    const sourcePath = Path.join(__dirname, '..', 'src', 'main', 'loading.html');
    const destPath = Path.join(__dirname, '..', 'build', 'main', 'loading.html');

    try {
        FileSystem.mkdirSync(Path.dirname(destPath), { recursive: true });

        FileSystem.copyFileSync(sourcePath, destPath);
        console.log(Chalk.greenBright('Successfully copied loading.html'));
        console.log(Chalk.gray(`From: ${sourcePath}`));
        console.log(Chalk.gray(`To: ${destPath}`));
    } catch (error) {
        console.error(Chalk.redBright('Failed to copy loading.html:'), error);
        throw error;
    }
}

function buildRenderer() {
    return Vite.build({
        configFile: Path.join(__dirname, '..', 'vite.config.js'),
        base: './',
        mode: 'production'
    });
}

function buildMain() {
    const mainPath = Path.join(__dirname, '..', 'src', 'main');
    return compileTs(mainPath);
}

FileSystem.rmSync(Path.join(__dirname, '..', 'build'), {
    recursive: true,
    force: true,
})

console.log(Chalk.blueBright('Transpiling renderer & main...'));


Promise.allSettled([
    buildRenderer(),
    buildMain(),
]).then(async () => {
    try {
        copyLoadingHtml();
        console.log(Chalk.greenBright('Build process completed successfully!'));
    } catch (error) {
        console.error(Chalk.redBright('Build process failed:'), error);
        process.exit(1);
    }
});
