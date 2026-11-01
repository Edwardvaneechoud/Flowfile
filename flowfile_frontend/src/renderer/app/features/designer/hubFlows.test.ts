import { describe, expect, it } from "vitest";

import type { HubFlowVersion } from "../../api/hubFlows.api";
import {
  conflictLabel,
  latestVersionLabel,
  openInsteadLabel,
  publishOverLabel,
  publishedLabel,
  refusalMessage,
  versionFacts,
  warningLines,
} from "./hubFlows";

const base: HubFlowVersion = {
  flowUuid: "u",
  version: 1,
  authorId: 1,
  publishedAt: "2026-11-01T12:00:00Z",
  message: "mine",
};
const current: HubFlowVersion = { ...base, version: 2, authorId: 7, message: "theirs" };

describe("publish dialog words", () => {
  it("names the version a publish became", () => {
    expect(publishedLabel(3)).toBe("Published as version 3");
  });

  it("warns once per node with a local path, naming the node and the path", () => {
    expect(
      warningLines([
        { nodeId: 2, nodeType: "read", label: "sales.csv (csv)", path: "/data/sales.csv" },
        { nodeId: 5, nodeType: "output", label: "out (csv)", path: "/out/out.csv" },
      ]),
    ).toEqual([
      "Uses a local file path: sales.csv (csv) (/data/sales.csv)",
      "Uses a local file path: out (csv) (/out/out.csv)",
    ]);
    expect(warningLines([])).toEqual([]);
  });
});

describe("conflict panel", () => {
  it("says which version went stale and who published the current one", () => {
    expect(conflictLabel(base, current)).toBe(
      "Version 1 is no longer the latest; user #7 published v2",
    );
  });

  it("copes with a first publish that found the flow already on the hub, and with no current version", () => {
    expect(conflictLabel(null, current)).toBe(
      "This flow already has v2 on the hub; user #7 published it",
    );
    expect(conflictLabel(base, null)).toMatch(/no version/);
  });

  it("offers the two ways out with the current version's number", () => {
    expect(publishOverLabel(current)).toBe("Publish over v2");
    expect(openInsteadLabel(current)).toBe("Open v2 instead");
  });

  it("lists version, author, published_at and message side by side, with a placeholder for no base", () => {
    const facts = versionFacts(current);
    expect(facts.map((f) => f.label)).toEqual(["Version", "Author", "Published", "Message"]);
    expect(facts[0].value).toBe("v2");
    expect(facts[1].value).toBe("user #7");
    expect(facts[3].value).toBe("theirs");
    expect(versionFacts({ ...current, message: null })[3].value).toBe("—");
    expect(versionFacts(null)).toEqual([{ label: "Version", value: "none" }]);
  });
});

describe("refusalMessage", () => {
  it("has a line for every kind", () => {
    expect(refusalMessage({ kind: "stale_base", flowUuid: "u", base, current })).toMatch(
      /no longer the latest/,
    );
    expect(refusalMessage({ kind: "refused", status: 403, detail: "no write access" })).toBe(
      "The hub refused the request (403): no write access",
    );
    expect(refusalMessage({ kind: "other_hub", hubUrl: "https://other" })).toMatch(
      /another hub \(https:\/\/other\)/,
    );
    expect(refusalMessage({ kind: "local_copy_exists", flowPath: "/f.yaml" })).toMatch(/\/f\.yaml/);
  });
});

describe("latestVersionLabel", () => {
  it("shows the newest hub version, or that there is none", () => {
    expect(latestVersionLabel(4)).toBe("v4");
    expect(latestVersionLabel(null)).toBe("not published yet");
  });
});
