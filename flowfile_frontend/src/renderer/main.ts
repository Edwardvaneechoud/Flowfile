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
import "./app/services/axios-setup";

const app = createApp(App);

app.directive("click-outside", ClickOutsideDirective);
app.use(stores);
app.use(router);
app.use(i18n);
app.use(ElementPlus, {
  size: "large",
  zIndex: 2000,
});

// Initialize auth before mounting app
authService
  .initialize()
  .then((authenticated) => {
    console.log("Auth initialized:", authenticated ? "Authenticated" : "Not authenticated");
    app.mount("#app");
  })
  .catch((error) => {
    console.error("Auth initialization failed:", error);
    app.mount("#app");
  });
