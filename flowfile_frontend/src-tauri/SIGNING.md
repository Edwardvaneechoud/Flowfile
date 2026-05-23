# Code Signing & Auto-Update — Operator Notes

This file documents the secrets and configuration needed to produce signed,
auto-updatable desktop builds. Nothing here is read by `cargo` or `tauri` at
build time; it's a runbook for whoever sets up the GitHub Actions release
workflow.

## Tauri updater signing keypair

Tauri's updater verifies downloaded bundles against a public key embedded in
the binary at build time. The matching private key signs the bundles at
release time.

### Generate (one-time, do this on a trusted machine)

```bash
cargo install tauri-cli --version "^2"
cargo tauri signer generate -w ~/.tauri/flowfile.key
```

This produces two files. Save the public key string and copy it into
`tauri.conf.json` under `plugins.updater.pubkey` (replacing
`REPLACE_WITH_TAURI_SIGNER_PUBKEY`).

### Wire the private key into CI

Add these as GitHub Actions secrets on the repository:

- `TAURI_SIGNING_PRIVATE_KEY` — contents of `~/.tauri/flowfile.key`
- `TAURI_SIGNING_PRIVATE_KEY_PASSWORD` — the password you set during generation

The Tauri CLI reads both env vars automatically during `tauri build` and
produces a `*.sig` file next to each bundle. Upload both to the GitHub Release.

### Update manifest (`latest.json`)

The endpoint `https://github.com/Edwardvaneechoud/Flowfile/releases/latest/download/latest.json`
must serve a file with this schema (Tauri 2 format):

```json
{
  "version": "0.11.0",
  "notes": "Release notes",
  "pub_date": "2026-01-15T10:00:00Z",
  "platforms": {
    "darwin-aarch64": {
      "signature": "<contents of Flowfile_0.11.0_aarch64.app.tar.gz.sig>",
      "url": "https://github.com/.../Flowfile_0.11.0_aarch64.app.tar.gz"
    },
    "darwin-x86_64":  { "signature": "...", "url": "..." },
    "linux-x86_64":   { "signature": "...", "url": "..." },
    "windows-x86_64": { "signature": "...", "url": "..." }
  }
}
```

The release workflow assembles this file from `.sig` artifacts.

## macOS code signing + notarization

Required GitHub Actions secrets:

| Secret | Value |
|---|---|
| `APPLE_CERTIFICATE` | base64 of your `Developer ID Application` .p12 |
| `APPLE_CERTIFICATE_PASSWORD` | .p12 password |
| `APPLE_SIGNING_IDENTITY` | `Developer ID Application: Your Name (TEAMID)` |
| `APPLE_ID` | Apple ID email for notarization |
| `APPLE_PASSWORD` | App-specific password (not your iCloud password) |
| `APPLE_TEAM_ID` | 10-character team ID |

In the workflow, install the cert with `apple-actions/import-codesign-certs@v3`
and then run `tauri build`. The Tauri CLI picks up the env vars and:

1. Signs the `.app` bundle with the Developer ID identity.
2. Submits to Apple Notary via `xcrun notarytool`.
3. Staples the notarization ticket to the `.dmg`.

Entitlements are in `entitlements.mac.plist`. They mirror what the Electron
app needed (Docker socket, JIT, network) — adjust only if you actually need
new capabilities.

## Windows Authenticode signing

**For releases (production EV cert):**

| Secret | Value |
|---|---|
| `WINDOWS_CERTIFICATE` | base64 of the .pfx |
| `WINDOWS_CERTIFICATE_PASSWORD` | .pfx password |

Add to `tauri.conf.json` under `bundle.windows`:

```json
"certificateThumbprint": "<sha1 thumbprint>",
"digestAlgorithm": "sha256",
"timestampUrl": "http://timestamp.digicert.com"
```

The Tauri CLI calls `signtool.exe` for you.

**For dev / unsigned-but-functional installers:**

Skip the certificate fields entirely. The resulting `.msi` / `.nsis` installer
runs but triggers a SmartScreen warning the first time. Contributors testing
locally are fine with this.

## What goes in the repository

| File | Status |
|---|---|
| `tauri.conf.json` (`plugins.updater.pubkey` field) | Committed (public key) |
| `entitlements.mac.plist` | Committed |
| `Info.plist` | Committed |
| `.p12` / `.pfx` / `.key` / `.pem` files | NEVER committed (in `.gitignore`) |
