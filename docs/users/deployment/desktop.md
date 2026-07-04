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

1. Download the `.AppImage` (or `.deb`) build
2. For the AppImage, mark it executable (`chmod +x Flowfile*.AppImage`) and run it
3. For the `.deb`, install with your package manager (`sudo dpkg -i flowfile_*.deb`)

## What you get

The desktop app runs offline with no Docker or terminal commands; data stays on your machine, and the setup screen generates and stores the encryption master key on first launch. It is single-user, with local flow storage (manual backup needed) and no centralized secrets management.

For multiple users, centralized flow storage, team collaboration, or server/production deployment, use [Docker deployment](docker.md) instead.
