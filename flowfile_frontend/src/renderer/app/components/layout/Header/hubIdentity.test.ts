import { describe, expect, it } from "vitest";

import { desktop, none } from "../../../__fixtures__/hubConnection";
import { hubAddress, hubIdentity, hubIdentityTitle } from "./hubIdentity";

describe("the header's who and where", () => {
  it("says nothing on a desktop with no hub", () => {
    expect(hubIdentity(none)).toBeNull();
    expect(hubIdentity(null)).toBeNull();
  });

  it("names who is signed in and the hub's organisation, linking back to the hub", () => {
    expect(hubIdentity(desktop)).toEqual({
      who: "Ana Lima",
      where: "Acme Data",
      url: "https://hub.acme.test:8443",
    });
  });

  it("names the hub by its address when it does not answer its name", () => {
    const unreachable = { ...desktop, org_name: null, reachable: false };
    expect(hubIdentity(unreachable)?.where).toBe("hub.acme.test:8443");
  });

  it("names only the hub when the connection does not say who", () => {
    const identity = hubIdentity({ ...desktop, account: null })!;
    expect(identity.who).toBeNull();
    expect(hubIdentityTitle(identity)).toBe("Connected to Acme Data. Open the hub.");
  });

  it("says who, through which hub, and what a click does", () => {
    expect(hubIdentityTitle(hubIdentity(desktop)!)).toBe(
      "Signed in as Ana Lima through Acme Data. Open the hub.",
    );
  });

  it("keeps an address it cannot parse as given", () => {
    expect(hubAddress("not a url")).toBe("not a url");
    expect(hubAddress("http://127.0.0.1:63580")).toBe("127.0.0.1:63580");
  });
});
