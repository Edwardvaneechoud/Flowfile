export interface ServicesStatus {
  status: "not_started" | "starting" | "ready" | "error";
  error: string | null;
}
