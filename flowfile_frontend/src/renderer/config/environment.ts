interface Environment {
  isDevelopment: boolean;
  isProduction: boolean;
  enableDevTools: boolean;
  enableLogging: boolean;
  enableGlobalShortcuts: boolean;
  enableProcessMonitoring: boolean;
  cacheClearingEnabled: boolean;
}

export const ENV: Environment = {
  isDevelopment: process.env.NODE_ENV === "development",
  isProduction: process.env.NODE_ENV === "production",
  enableDevTools: process.env.NODE_ENV === "development",
  enableLogging: true, // You might want this in both environments
  enableGlobalShortcuts: process.env.NODE_ENV === "development",
  enableProcessMonitoring: true, // You might want this in both environments
  cacheClearingEnabled: process.env.NODE_ENV === "development",
};
