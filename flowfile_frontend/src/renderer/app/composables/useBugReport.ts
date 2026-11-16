import { desktop, desktopPlatform, isDesktop } from "../../lib/desktop";
import { bugReportUrl } from "../lib/docsLinks";

/** Open a prefilled GitHub bug report in the system browser. */
export function openBugReport(version: string): void {
  const url = bugReportUrl({
    version,
    environment: isDesktop ? `Desktop app (${desktopPlatform ?? "unknown OS"})` : "Browser",
    userAgent: isDesktop || typeof navigator === "undefined" ? undefined : navigator.userAgent,
  });
  void desktop.openExternal(url);
}
