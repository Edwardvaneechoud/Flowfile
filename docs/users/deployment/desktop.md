# Desktop App

The easiest way to get started with Flowfile.

## Download

Download the latest release for your platform:

[**Download for macOS / Windows →**](https://github.com/edwardvaneechoud/Flowfile/releases)

## Installation

### macOS

1. Download the `.dmg` file
2. Open it and drag Flowfile to **Applications**
3. Launch from Applications

!!! note "First launch"
    macOS may ask you to confirm opening an app from an unidentified developer. Go to **System Preferences → Security & Privacy** and click "Open Anyway".

### Windows

1. Download the `.exe` installer
2. Run the installer and follow the wizard
3. Launch from the Start menu

## Why Desktop?

**Pros**

- Zero configuration - just download and run
- No Docker, no terminal commands
- Works offline
- Data stays on your machine
- Master key managed automatically

**Cons**

- Single user only (no team sharing)
- No centralized secrets management
- Flows stored locally (manual backup needed)

## When to Use Docker Instead

Consider [Docker deployment](docker.md) if you need:

- Multiple users with separate accounts
- Centralized flow storage
- Team collaboration
- Server/production deployment
