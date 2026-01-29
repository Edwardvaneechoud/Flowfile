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
import "./styles/main.css";

// Import auth service and configured axios
import authService from "./app/services/auth.service";
import "./app/services/axios.config";
import setupService from "./app/services/setup.service";
import { useThemeStore } from "./app/stores/theme-store";

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

// Fetch backend mode first, then initialize auth.
// When running "flowfile run ui" in a browser, electronAPI doesn't exist
// but the backend reports mode="electron". We need to know this before
// auth initialization so auto-authentication works correctly.
setupService
  .getSetupStatus()
  .then((status) => {
    authService.setModeFromBackend(status.mode);
    return authService.initialize();
  })
  .then((authenticated) => {
    console.log("Auth initialized:", authenticated ? "Authenticated" : "Not authenticated");
    console.log("Electron mode:", authService.isInElectronMode());

    // Mount the app first
    app.mount("#app");

    // If not authenticated and not in Electron mode, redirect to login
    if (!authenticated && !authService.isInElectronMode()) {
      console.log("Redirecting to login page");
      router.push({ name: "login" });
    }
  })
  .catch((error) => {
    console.error("Auth initialization failed:", error);
    app.mount("#app");

    // On error, if not in Electron mode, redirect to login
    if (!authService.isInElectronMode()) {
      router.push({ name: "login" });
    }
  });
