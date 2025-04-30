// vite.config.js
const Path = require('path');
const vuePlugin = require('@vitejs/plugin-vue');
const reactPlugin = require('@vitejs/plugin-react');

const { defineConfig } = require('vite');

const config = defineConfig({
    root: Path.join(__dirname, 'src', 'renderer'),
    publicDir: 'public',
    server: {
        host: '0.0.0.0',
        port: 8080,
    },
    open: false,
    build: {
        outDir: Path.join(__dirname, 'build', 'renderer'),
        emptyOutDir: true,
        minify: false,
    },
    plugins: [
        vuePlugin(),
        reactPlugin()
    ],
});

module.exports = config;