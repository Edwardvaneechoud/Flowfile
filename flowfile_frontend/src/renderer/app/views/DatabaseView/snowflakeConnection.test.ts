import { describe, expect, it } from "vitest";

import type { FullDatabaseConnection } from "./databaseConnectionTypes";
import {
  SIGN_IN_POLL_INTERVAL_MS,
  SIGN_IN_TIMEOUT_MS,
  accountLabel,
  buildPayloadFields,
  canDiscover,
  identityText,
  isSnowflakeFormValid,
  methodDescription,
  methodLabel,
  nextPollDelayMs,
  previewAccount,
  selectOptions,
  signInDeadline,
  signInStatusText,
  snowflakeResetFields,
  storedSignInStillValid,
  testSuccessText,
} from "./snowflakeConnection";

const HOST = "myorg-myaccount.snowflakecomputing.com";

const connection = (overrides: Partial<FullDatabaseConnection> = {}): FullDatabaseConnection => ({
  connectionName: "sf",
  databaseType: "snowflake",
  username: "",
  password: "",
  host: "myorg-myaccount",
  database: "",
  sslEnabled: false,
  authMethod: "access_token",
  ...overrides,
});

const NEW = { isEditing: false };

describe("previewAccount", () => {
  it.each([
    ["myorg-myaccount", HOST],
    ["  MyOrg-MyAccount  ", HOST],
    ["myorg_myaccount", HOST],
    ["xy12345.eu-west-1", "xy12345.eu-west-1.snowflakecomputing.com"],
    [HOST, HOST],
    ["myorg-myaccount.snowflakecomputing.com.", HOST],
    ["acct.snowflakecomputing.cn", "acct.snowflakecomputing.cn"],
    [`https://${HOST}/console`, HOST],
    [`https://${HOST.toUpperCase()}:443/`, HOST],
    [`${HOST}/path`, HOST],
    ["https://app.snowflake.com/myorg/myaccount/worksheets", HOST],
    [
      "https://app.snowflake.com/eu-west-1/xy12345/#/home",
      "xy12345.eu-west-1.snowflakecomputing.com",
    ],
    ["app.snowflake.com/myorg/myaccount", HOST],
    ["https://evil.com/x", "evil.com.snowflakecomputing.com"],
  ])("resolves %s", (text, host) => {
    expect(previewAccount(text)).toEqual({ host });
  });

  it.each(["", "   ", "my org", "-myorg", "myorg!", "https://", "https://app.snowflake.com/myorg"])(
    "refuses %j",
    (text) => {
      expect(previewAccount(text)).toHaveProperty("error");
    },
  );

  it("explains a Snowsight link without an account", () => {
    expect(previewAccount("https://app.snowflake.com/myorg")).toEqual({
      error: expect.stringContaining("does not name an account"),
    });
  });
});

describe("labels", () => {
  it("strips the Snowflake domain from a host", () => {
    expect(accountLabel(HOST)).toBe("myorg-myaccount");
    expect(accountLabel("acct.SNOWFLAKECOMPUTING.CN")).toBe("acct");
    expect(accountLabel(undefined)).toBe("");
  });

  it("names and describes each method", () => {
    expect(methodLabel("oauth")).toBe("Sign in with Snowflake");
    expect(methodLabel("access_token")).toBe("Access token");
    expect(methodLabel("key_pair")).toBe("Key-pair");
    for (const method of ["oauth", "access_token", "key_pair"]) {
      expect(methodDescription(method)).not.toBe("");
    }
    expect(methodLabel("other")).toBe("other");
  });

  it("formats an identity", () => {
    expect(identityText("Connected as", "ANA", "ANALYST")).toBe("Connected as ANA (role ANALYST)");
    expect(identityText("Connected as", "ANA", null)).toBe("Connected as ANA");
  });
});

describe("isSnowflakeFormValid", () => {
  it("needs a name and a valid account", () => {
    const filled = { username: "ana", password: "tok" };
    expect(isSnowflakeFormValid(connection(filled), NEW)).toBe(true);
    expect(isSnowflakeFormValid(connection({ ...filled, connectionName: " " }), NEW)).toBe(false);
    expect(isSnowflakeFormValid(connection({ ...filled, host: "" }), NEW)).toBe(false);
    expect(isSnowflakeFormValid(connection({ ...filled, host: "bad host" }), NEW)).toBe(false);
  });

  it.each(["access_token", "key_pair"])("%s needs a username and a secret", (authMethod) => {
    expect(isSnowflakeFormValid(connection({ authMethod, password: "s" }), NEW)).toBe(false);
    expect(isSnowflakeFormValid(connection({ authMethod, username: "ana" }), NEW)).toBe(false);
    expect(
      isSnowflakeFormValid(connection({ authMethod, username: "ana", password: "s" }), NEW),
    ).toBe(true);
  });

  it("keeps a stored secret only while the method is unchanged", () => {
    const edited = connection({ authMethod: "key_pair", username: "ana" });
    expect(isSnowflakeFormValid(edited, { isEditing: true, storedAuthMethod: "key_pair" })).toBe(
      true,
    );
    expect(
      isSnowflakeFormValid(edited, { isEditing: true, storedAuthMethod: "access_token" }),
    ).toBe(false);
  });

  it("needs a completed sign-in for oauth", () => {
    expect(isSnowflakeFormValid(connection({ authMethod: "oauth" }), NEW)).toBe(false);
    expect(
      isSnowflakeFormValid(connection({ authMethod: "oauth", oauthFlowId: "flow-1" }), NEW),
    ).toBe(true);
  });

  it("keeps a stored sign-in unless the account or role changed", () => {
    const ctx = { isEditing: true, storedAuthMethod: "oauth", storedHost: HOST, storedRole: "R" };
    const edited = connection({ authMethod: "oauth", host: HOST, role: "R" });
    expect(isSnowflakeFormValid(edited, ctx)).toBe(true);
    expect(storedSignInStillValid(edited, ctx)).toBe(true);
    expect(isSnowflakeFormValid({ ...edited, role: "OTHER" }, ctx)).toBe(false);
    expect(isSnowflakeFormValid({ ...edited, host: "other-acct" }, ctx)).toBe(false);
    expect(isSnowflakeFormValid({ ...edited, host: "myorg-myaccount" }, ctx)).toBe(true);
    expect(isSnowflakeFormValid(edited, { ...ctx, storedAuthMethod: "key_pair" })).toBe(false);
  });

  it("refuses an unknown method", () => {
    expect(isSnowflakeFormValid(connection({ authMethod: null }), NEW)).toBe(false);
  });
});

describe("canDiscover", () => {
  it("needs an account, a username and a secret or a stored one", () => {
    expect(canDiscover(connection({ username: "ana", password: "t" }), NEW)).toBe(true);
    expect(canDiscover(connection({ username: "ana" }), NEW)).toBe(false);
    expect(
      canDiscover(connection({ username: "ana" }), {
        isEditing: true,
        storedAuthMethod: "access_token",
      }),
    ).toBe(true);
    expect(
      canDiscover(connection({ authMethod: "oauth", username: "a", password: "t" }), NEW),
    ).toBe(false);
  });
});

describe("sign-in poll", () => {
  it("polls every 1.5 s and stops after 5 minutes", () => {
    const deadline = signInDeadline(1000);
    expect(deadline).toBe(1000 + SIGN_IN_TIMEOUT_MS);
    expect(nextPollDelayMs(1000, deadline)).toBe(SIGN_IN_POLL_INTERVAL_MS);
    expect(nextPollDelayMs(deadline - 200, deadline)).toBe(200);
    expect(nextPollDelayMs(deadline, deadline)).toBeNull();
  });

  it("words each phase", () => {
    expect(signInStatusText("waiting")).toBe("Waiting for you to sign in in your browser…");
    expect(signInStatusText("ok", { username: "ANA", role: "R" })).toBe(
      "Signed in as ANA (role R)",
    );
    expect(signInStatusText("error", { message: "No warehouse" })).toBe("No warehouse");
    expect(signInStatusText("error")).not.toBe("");
    expect(signInStatusText("timeout")).toContain("5 minutes");
    expect(signInStatusText("idle")).toBe("");
  });
});

describe("buildPayloadFields", () => {
  it("sends an access token trimmed, with no port or SSL", () => {
    expect(
      buildPayloadFields(
        connection({
          host: " myorg-myaccount ",
          username: " ana ",
          password: " tok ",
          port: 443,
          sslEnabled: true,
          role: " ",
          warehouse: " WH ",
          database: " DB ",
          oauthFlowId: "stale",
          privateKeyPassphrase: "stale",
        }),
      ),
    ).toEqual({
      host: "myorg-myaccount",
      port: undefined,
      sslEnabled: false,
      url: "",
      username: "ana",
      database: "DB",
      authMethod: "access_token",
      role: null,
      warehouse: "WH",
      password: "tok",
      privateKeyPassphrase: null,
      oauthFlowId: null,
    });
  });

  it("sends the key text and passphrase for key-pair", () => {
    const key = "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n";
    const fields = buildPayloadFields(
      connection({ authMethod: "key_pair", password: key, privateKeyPassphrase: "pw" }),
    );
    expect(fields.password).toBe(key);
    expect(fields.privateKeyPassphrase).toBe("pw");
    expect(
      buildPayloadFields(connection({ authMethod: "key_pair", privateKeyPassphrase: "" }))
        .privateKeyPassphrase,
    ).toBeNull();
  });

  it("sends no secret and the flow id for oauth", () => {
    const fields = buildPayloadFields(
      connection({ authMethod: "oauth", password: "x", oauthFlowId: "flow-1", role: "R" }),
    );
    expect(fields.password).toBe("");
    expect(fields.oauthFlowId).toBe("flow-1");
    expect(fields.role).toBe("R");
  });
});

describe("helpers", () => {
  it("resets the Snowflake-only fields", () => {
    expect(snowflakeResetFields()).toEqual({
      password: "",
      authMethod: null,
      role: null,
      warehouse: null,
      privateKeyPassphrase: null,
      oauthFlowId: null,
    });
  });

  it("keeps a typed value among the select options", () => {
    expect(selectOptions(["A", "B"], "B")).toEqual(["A", "B"]);
    expect(selectOptions(["A"], "MINE")).toEqual(["MINE", "A"]);
    expect(selectOptions([], null)).toEqual([]);
  });

  it("words a successful test", () => {
    expect(
      testSuccessText({
        success: true,
        message: "ok",
        details: { user: "ANA", role: "R", warehouse: "WH" },
      }),
    ).toBe("Connected as ANA, role R, warehouse WH");
    expect(testSuccessText({ success: true, message: "Connection works", details: null })).toBe(
      "Connection works",
    );
    expect(testSuccessText({ success: true, message: "", details: {} })).toBe(
      "Connection succeeded",
    );
  });
});
