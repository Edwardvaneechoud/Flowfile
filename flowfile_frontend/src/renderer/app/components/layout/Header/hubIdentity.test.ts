import { describe, expect, it } from "vitest";

import type { HubConnection } from "../../../api/hubConnection.api";
import { hubAddress, hubIdentity, hubIdentityTitle } from "./hubIdentity";

const none: HubConnection = {
  connected: false,
  hubUrl: null,
  deviceId: null,
  deviceName: "ana-laptop",
  expiresAt: null,
  clientVersion: "0.24.0",
  contractVersion: "0.1.0",
  orgName: null,
  hubVersion: null,
  coreVersion: null,
  vendMode: null,
  reachable: false,
  error: null,
  nodesSynced: null,
  syncError: null,
  nodeName: null,
  account: null,
};

const joined: HubConnection = {
  ...none,
  connected: true,
  hubUrl: "https://hub.acme.test:8443",
  orgName: "Acme Data",
  reachable: true,
  account: "Ana Lima",
};

describe("the header's who and where", () => {
  it("says nothing on a desktop with no hub", () => {
    expect(hubIdentity(none)).toBeNull();
    expect(hubIdentity(null)).toBeNull();
  });

  it("names who is signed in and the hub's organisation, linking back to the hub", () => {
    expect(hubIdentity(joined)).toEqual({
      who: "Ana Lima",
      where: "Acme Data",
      url: "https://hub.acme.test:8443",
    });
  });

  it("names the hub by its address when it does not answer its name", () => {
    const unreachable = { ...joined, orgName: null, reachable: false };
    expect(hubIdentity(unreachable)?.where).toBe("hub.acme.test:8443");
  });

  it("names only the hub when the connection does not say who", () => {
    const identity = hubIdentity({ ...joined, account: null })!;
    expect(identity.who).toBeNull();
    expect(hubIdentityTitle(identity)).toBe("Connected to Acme Data. Open the hub.");
  });

  it("says who, through which hub, and what a click does", () => {
    expect(hubIdentityTitle(hubIdentity(joined)!)).toBe(
      "Signed in as Ana Lima through Acme Data. Open the hub.",
    );
  });

  it("keeps an address it cannot parse as given", () => {
    expect(hubAddress("not a url")).toBe("not a url");
    expect(hubAddress("http://127.0.0.1:63580")).toBe("127.0.0.1:63580");
  });
});
