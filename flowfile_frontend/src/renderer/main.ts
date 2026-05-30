// src/main.ts
import { createApp } from "vue";
import stores from "./app/stores";
import router from "./app/router";
import App from "./app/App.vue";
import ClickOutsideDirective from "./app/directives/ClickOutsideDirective";
import ElementPlus from "element-plus";
import "element-plus/dist/index.css";
import i18n from "./app/i18n";
import "@fortawesome/fontawesome-free/css/all.css";
// Bundle Material Icons locally instead of pulling them from fonts.googleapis.com.
// The CDN fetch was being blocked by Tauri's CSP and would never have worked
// offline either. The npm package ships the same .woff2 + CSS, Vite hashes
// them into the bundle next to the rest of the assets.
import "material-icons/iconfont/material-icons.css";
import "./styles/main.css";

// Import auth service and configured axios
import authService from "./app/services/auth.service";
import setupService from "./app/services/setup.service";
import "./app/services/axios.config";
import { useThemeStore } from "./app/stores/theme-store";
import { checkForUpdatesOnStartup } from "./app/composables/useDesktopUpdater";

const app = createApp(App);

app.directive("click-outside", ClickOutsideDirective);
app.use(stores);
app.use(router);
app.use(i18n);
app.use(ElementPlus, {
  size: "large",
  zIndex: 2000,
});

// Initialize theme before mounting app
const themeStore = useThemeStore();
themeStore.initialize();

// Initialize auth before mounting app
setupService
  .getSetupStatus()
  .then((status) => {
    authService.setModeFromBackend(status.mode);
    return authService.initialize();
  })
  .then((authenticated) => {
    console.log("Auth initialized:", authenticated ? "Authenticated" : "Not authenticated");
    console.log("Desktop mode:", authService.isInDesktopMode());

    // Mount the app first
    app.mount("#app");

    // If not authenticated and not in desktop mode, redirect to login
    if (!authenticated && !authService.isInDesktopMode()) {
      console.log("Redirecting to login page");
      router.push({ name: "login" });
    }

    // Desktop-only: check for app updates on startup (no-op in web/dev).
    checkForUpdatesOnStartup();
  })
  .catch((error) => {
    console.error("Auth initialization failed:", error);
    app.mount("#app");

    // On error, if not in desktop mode, redirect to login
    if (!authService.isInDesktopMode()) {
      router.push({ name: "login" });
    }
  });
