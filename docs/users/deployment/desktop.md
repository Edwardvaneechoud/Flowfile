# Desktop App

Download and run Flowfile as a native desktop app on macOS, Windows, or Linux.

## Download

Download the latest release for your platform:

[**Download the latest installer →**](https://github.com/edwardvaneechoud/Flowfile/releases/latest)

Installers are built for macOS (Apple Silicon and Intel), Windows, and Linux.

## Installation

### macOS

1. Download the `.dmg` file
2. Open it and drag Flowfile to **Applications**
3. Launch from Applications — the app is signed and notarized, so it opens without security prompts

### Windows

1. Download the `.exe` installer
2. Run the installer and follow the wizard
3. Launch from the Start menu

### Linux

1. Download the `.deb` package
2. Install it with your package manager (`sudo dpkg -i Flowfile_*.deb`)
3. Launch Flowfile from your application menu

## Updating

The app checks for a newer release each time it starts. When one exists, a dialog names the new version and the version you are running, links to its release notes, and offers three choices:

- **Install now** — downloads and installs the update, then restarts Flowfile.
- **Remind me later** — closing the dialog with Esc, the X, or a click outside does the same. You are asked again next launch.
- **Skip this version** — that version is not offered again at launch (a manual check from About still shows it), and a later release still prompts.

An install runs inside the dialog: the update downloads, Flowfile snapshots its [catalog database](backups.md), the background services stop, and the installer runs. If the snapshot fails you can choose to continue without one. If the install itself fails, the dialog offers **Restart Flowfile** — the background services are already stopped by then, so restart before working on anything else.

The last step differs per platform:

- **macOS** — Flowfile replaces itself and restarts.
- **Windows** — Windows asks for permission to run the installer, and SmartScreen may ask you to confirm an installer it has not seen before. The installer restarts Flowfile when it finishes.
- **Linux** — your system may ask for your password to install the `.deb` package.

To check for an update without restarting, open **About** from the home screen and choose **Check for updates**.

## What you get

The desktop app runs offline with no Docker or terminal commands; data stays on your machine, and the setup screen generates and stores the encryption master key on first launch. It is single-user, with local flow storage (manual backup needed); your secrets are encrypted on-device for you alone, with no shared, team-wide secret store (teammates can't be granted use of your saved secrets and connections).

For multiple users, centralized flow storage, team collaboration, or server/production deployment, use [Docker deployment](docker.md) instead.
