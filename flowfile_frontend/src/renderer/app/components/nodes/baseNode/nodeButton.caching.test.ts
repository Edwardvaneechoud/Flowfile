// @vitest-environment happy-dom
import { beforeEach, expect, it, vi } from "vitest";
import { createApp, nextTick, reactive } from "vue";
import NodeButton from "./nodeButton.vue";
import type { PlacementPreview } from "../../../types";

const state = reactive({
  cached: false,
  status: "success",
  placements: {} as Record<number, PlacementPreview>,
});
vi.mock("../../../stores/flow-store", () => ({
  useFlowStore: () => ({ flowId: 83, isNodeCached: () => state.cached }),
}));
vi.mock("../../../stores/results-store", () => ({
  useResultsStore: () => ({ placements: state.placements }),
}));
vi.mock("../../../stores/column-store", () => ({
  useNodeStore: () => ({
    node_id: -1,
    getNodeResult: () => ({
      node_id: 1,
      is_running: state.status === "running",
      success: state.status !== "failure",
      skipped: state.status === "skipped",
      error: state.status === "failure" ? "failed" : "",
      start_timestamp: 1,
    }),
    getNodeValidation: () =>
      state.status === "warning"
        ? { isValid: false, validationTime: 2, error: "Missing column" }
        : undefined,
  }),
}));
vi.mock("../../../composables/useCustomNodeIcon", () => ({ useNodeIconUrl: () => "icon.svg" }));

function mountNode() {
  const host = document.createElement("div");
  const app = createApp(NodeButton, {
    nodeId: 1,
    imageSrc: "icon.svg",
    title: "Input",
    nodeTitleInfo: { nodeId: 1, title: "Input" } as any,
  });
  app.mount(host);
  return { host, app };
}

beforeEach(() => {
  state.cached = false;
  state.status = "success";
  state.placements = {};
});
it.each(["success", "running", "failure", "warning", "skipped"])(
  "preserves %s status and shows the cache marker only beside success",
  (status) => {
    state.cached = true;
    state.status = status;
    const { host, app } = mountNode();
    try {
      expect(host.querySelector('[aria-label="Cached result"]') !== null).toBe(
        status === "success",
      );
      expect(host.querySelector(`.status-indicator.${status}`)).not.toBeNull();
    } finally {
      app.unmount();
    }
  },
);
it("uses the bundled cache icon and a frozen tool treatment with an explanation", () => {
  state.cached = true;
  state.placements[83] = {
    available: true,
    assignment: { 1: "worker" },
    boundaries: [],
    refusals: [],
    fallbacks: [
      { node_id: 1, backend_id: "custom", fallback_id: "worker", reason: "not supported" },
    ],
  };
  const { host, app } = mountNode();
  try {
    const button = host.querySelector("button.cached-node");
    expect(button).not.toBeNull();
    expect(host.querySelector(".cache-indicator.material-icons")?.textContent?.trim()).toBe(
      "cached",
    );
    expect(button?.getAttribute("title")).toContain("settings and upstream configuration");
    expect(
      host.querySelector(`#${button?.getAttribute("aria-describedby")}`)?.textContent,
    ).toContain("Cache validity");
    expect(host.querySelector('[data-testid="placement-badge"]')?.textContent).toContain(
      "default worker",
    );
    expect(host.querySelector('[data-testid="placement-badge-tooltip"]')?.textContent).toContain(
      "doesn't support",
    );
  } finally {
    app.unmount();
  }
});
it("does not mark an uncached result", () => {
  const { host, app } = mountNode();
  try {
    expect(host.querySelector('[aria-label="Cached result"]')).toBeNull();
  } finally {
    app.unmount();
  }
});
it("reactively clears the badge when cache knowledge is invalidated", async () => {
  state.cached = true;
  const { host, app } = mountNode();
  try {
    expect(host.querySelector('[aria-label="Cached result"]')).not.toBeNull();
    state.cached = false;
    await nextTick();
    expect(host.querySelector('[aria-label="Cached result"]')).toBeNull();
    expect(host.querySelector(".status-indicator.success")).not.toBeNull();
  } finally {
    app.unmount();
  }
});
