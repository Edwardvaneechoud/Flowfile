<template>
  <router-view />
</template>

<style lang="scss">
#app {
  font-family: "Roboto", "Source Sans Pro", Avenir, Helvetica, Arial, sans-serif;
  color: #2c3e50;
}

body {
  margin: 0;
}

html,
body,
#app {
  height: 100%;
  margin: 0;
  overflow: hidden; /* Prevent root overflow */
}

/* Add the custom styles for el-notification__title */
.el-notification {
  color: #333 !important; /* Ensure this style takes precedence */
  border-radius: 5px !important; /* Ensure this style takes precedence */
  text-align: left !important; /* Ensure this style takes precedence */
  font-family:
    Roboto,
    Source Sans Pro,
    Avenir,
    Helvetica,
    Arial,
    sans-serif !important; /* Ensure this style takes precedence */
}

.el-notification__title {
  font-size: 14px !important; /* Ensure this style takes precedence */
  color: #333 !important; /* Ensure this style takes precedence */
  font-weight: 500 !important; /* Ensure this style takes precedence */
}
</style>
