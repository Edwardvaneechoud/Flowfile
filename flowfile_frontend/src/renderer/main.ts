import { createApp } from "vue";
import { flowfileCorebaseURL } from "./config/constants";
import stores from "./app/stores";
import router from "./app/router";
import App from "./app/App.vue";
import ClickOutsideDirective from "./app/directives/ClickOutsideDirective";
import ElementPlus from "element-plus";
import "element-plus/dist/index.css";
import i18n from "./app/i18n";
import "@fortawesome/fontawesome-free/css/all.css";
import "./styles/main.css";

import axios from "axios";
axios.defaults.withCredentials = true;
axios.defaults.baseURL = flowfileCorebaseURL; // the FastAPI backend
const app = createApp(App);
app.directive("click-outside", ClickOutsideDirective);
app.use(stores);
app.use(router);
app.use(i18n);
app.use(ElementPlus, {
  size: "large", // or 'large', 'small', etc., based on your preference
  zIndex: 2000, // a default zIndex value; adjust based on your needs
});
app.mount("#app");
