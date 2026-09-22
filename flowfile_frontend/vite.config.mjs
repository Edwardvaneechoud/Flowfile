import Path from 'path';
import { createHash } from 'crypto';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import vuePlugin from '@vitejs/plugin-vue';
import { defineConfig } from 'vite';
// Note: no @vitejs/plugin-react. The project has no JSX/TSX files;
// React is only used as a dynamic `import("react")` inside two Vue wrappers
// (VueGraphicWalker, VueGraphicRenderer) for the @kanaries/graphic-walker
// integration, which works natively via Vite's ESM handling.

const __dirname = Path.dirname(fileURLToPath(import.meta.url));

// flowfile_core dev port. The renderer talks to it via the /api proxy below so
// requests stay same-origin (mirrors the nginx setup in Docker / web mode).
// FLOWFILE_CORE_PORT lets a second dev instance target a core on another port.
const CORE_PORT = Number(process.env.FLOWFILE_CORE_PORT ?? 63578);

// Web-mode fallback for the app version shown on the welcome screen; the
// desktop shell reports its real version via the get_app_version command.
const pkg = JSON.parse(readFileSync(Path.join(__dirname, 'package.json'), 'utf-8'));

// Tauri only enforces its CSP on pages it serves itself (the packaged asset protocol), never
// on a devUrl, so the dev/preview servers send the same policy to keep dev honest. The only
// addition is Vite's HMR websocket, which the packaged app doesn't have.
const tauriConf = JSON.parse(readFileSync(Path.join(__dirname, 'src-tauri', 'tauri.conf.json'), 'utf-8'));
const DESKTOP_CSP = tauriConf.app.security.csp;
const DEV_CSP = DESKTOP_CSP.replace('connect-src ', 'connect-src ws://localhost:* ws://127.0.0.1:* ');

// graphic-walker injects a CDN <link> for leaflet.css into its shadow root; the Tauri CSP
// (`style-src 'self'`) blocks it in packaged builds only, so serve a same-origin copy instead.
const LEAFLET_CDN_CSS = 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css';
const LEAFLET_LOCAL_CSS = '/leaflet.css';
const LEAFLET_CSS_FILE = Path.join(__dirname, 'node_modules', 'leaflet', 'dist', 'leaflet.css');
const GRAPHIC_WALKER_BUNDLE = /@kanaries[\/\\]graphic-walker[\/\\]dist[\/\\]graphic-walker\.es\.js$/;

function leafletCssLocal() {
    let rewrites = 0;
    const rewrite = (code) => {
        const out = code.split(LEAFLET_CDN_CSS).join(LEAFLET_LOCAL_CSS);
        if (out !== code) rewrites += 1;
        return out;
    };
    // The rewritten <link> keeps its SRI attribute, so the local file must match unpkg's bytes.
    const assertIntegrity = (code) => {
        const url = LEAFLET_CDN_CSS.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const match = code.match(new RegExp(`${url}", integrity: "(sha256-[^"]+)"`));
        if (!match) return;
        const local = 'sha256-' + createHash('sha256').update(readFileSync(LEAFLET_CSS_FILE)).digest('base64');
        if (local !== match[1]) {
            throw new Error(
                `leaflet.css SRI mismatch: graphic-walker pins ${match[1]} but node_modules/leaflet serves ${local}; ` +
                'align the leaflet version with the one graphic-walker embeds.',
            );
        }
    };
    return {
        name: 'flowfile:leaflet-css-local',
        // Dev serves graphic-walker from the esbuild pre-bundle, which Vite's transform hook never sees.
        config: () => ({
            optimizeDeps: {
                esbuildOptions: {
                    plugins: [{
                        name: 'flowfile:leaflet-css-local',
                        setup(build) {
                            build.onLoad({ filter: GRAPHIC_WALKER_BUNDLE }, (args) => {
                                const code = readFileSync(args.path, 'utf-8');
                                assertIntegrity(code);
                                return { contents: rewrite(code), loader: 'js' };
                            });
                        },
                    }],
                },
            },
        }),
        configureServer(server) {
            server.middlewares.use(LEAFLET_LOCAL_CSS, (_req, res) => {
                res.setHeader('Content-Type', 'text/css');
                res.end(readFileSync(LEAFLET_CSS_FILE));
            });
        },
        buildStart() {
            rewrites = 0;
        },
        transform(code, id) {
            if (!GRAPHIC_WALKER_BUNDLE.test(id)) return null;
            assertIntegrity(code);
            return { code: rewrite(code), map: null };
        },
        generateBundle() {
            if (rewrites === 0) {
                this.error(`${LEAFLET_CDN_CSS} not found in graphic-walker; update leafletCssLocal() in vite.config.mjs`);
            }
            this.emitFile({ type: 'asset', fileName: LEAFLET_LOCAL_CSS.slice(1), source: readFileSync(LEAFLET_CSS_FILE) });
        },
    };
}

export default defineConfig({
    root: Path.join(__dirname, 'src', 'renderer'),
    publicDir: 'public',
    define: {
        __APP_VERSION__: JSON.stringify(pkg.version),
    },
    // Pin the dep-optimization cache to a stable path under the repo so it
    // doesn't get confused by parallel Vite instances (`npm run dev:web` vs
    // `tauri dev` invoking `npm run dev:web` as `beforeDevCommand`).
    cacheDir: Path.join(__dirname, 'node_modules', '.vite'),
    server: {
        // Listen dual-stack (::), not 0.0.0.0: browsers try ::1 first for `localhost`, and
        // Windows silently drops SYNs to unbound loopback ports, costing ~300ms per new socket.
        host: true,
        port: 8080,
        // Don't silently jump to 8082 when 8080/8081 are busy — fail fast so
        // Tauri's hard-coded devUrl doesn't end up pointing at the wrong port.
        strictPort: true,
        headers: { 'Content-Security-Policy': DEV_CSP },
        proxy: {
            '/api': {
                target: `http://localhost:${CORE_PORT}`,
                changeOrigin: true,
                rewrite: (path) => path.replace(/^\/api/, ''),
                // Replicate nginx's built-in `proxy_redirect default` (see the
                // comment in flowfile_frontend/nginx.conf). FastAPI normalizes
                // trailing slashes with an ABSOLUTE 307 redirect
                configure: (proxy) => {
                    const backendOrigin = new RegExp(`^https?://(localhost|127\\.0\\.0\\.1):${CORE_PORT}`);
                    proxy.on('proxyRes', (proxyRes) => {
                        const location = proxyRes.headers['location'];
                        if (!location) return;
                        let path;
                        if (backendOrigin.test(location)) {
                            path = location.replace(backendOrigin, ''); // absolute backend redirect
                        } else if (location.startsWith('/')) {
                            path = location;                            // already-relative redirect
                        } else {
                            return;                                     // external host — leave untouched
                        }
                        if (!path.startsWith('/api/')) {
                            proxyRes.headers['location'] = '/api' + path;
                        }
                    });
                },
            },
        },
    },
    open: false,
    build: {
        outDir: Path.join(__dirname, 'build', 'renderer'),
        emptyOutDir: true,
        minify: false,
    },
    // Dep pre-bundling is left on Vite's own hash-based invalidation (it keys the
    // cache on lockfile + config, so a branch switch rebuilds it). `force: true`
    // used to live here, but it deletes the shared deps cache on every `vite`
    // start — including a start that then dies on strictPort — which leaves a
    // still-running server serving "504 (Outdated Optimize Dep)" forever. The
    // router's onError guard now recovers from a stale cache; to rebuild it by
    // hand use `npm run dev:web:force`.
    plugins: [
        vuePlugin(),
        leafletCssLocal(),
    ],
    resolve: {
        alias: {
            '@': Path.resolve(__dirname, './src/renderer/app'),
            '@/api': Path.resolve(__dirname, './src/renderer/app/api'),
            '@/types': Path.resolve(__dirname, './src/renderer/app/types'),
            '@/stores': Path.resolve(__dirname, './src/renderer/app/stores'),
            '@/composables': Path.resolve(__dirname, './src/renderer/app/composables'),
        },
    },
});
