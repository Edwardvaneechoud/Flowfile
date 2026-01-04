# Dark Mode

Flowfile supports light, dark, and system theme modes to customize your visual experience.

## Overview

Choose the theme that works best for you:

- **Light Mode** - Bright interface ideal for well-lit environments
- **Dark Mode** - Reduced eye strain in low-light conditions
- **System Mode** - Automatically matches your operating system's theme preference

## Switching Themes

### Finding the Theme Toggle

The theme toggle is located in the application header/toolbar area. Look for the theme icon (sun/moon) to access theme options.

### Available Options

| Mode | Description |
|------|-------------|
| **Light** | Always displays the light theme, regardless of system settings |
| **Dark** | Always displays the dark theme, regardless of system settings |
| **System** | Automatically follows your operating system's theme preference |

### Changing Your Theme

1. Click the theme toggle icon in the header
2. Select your preferred mode from the options:
   - Light
   - Dark
   - System
3. The interface updates immediately to reflect your choice

!!! tip "Try System Mode"
    System mode is convenient if you already have your OS set to change themes based on time of day. Flowfile will automatically switch between light and dark modes as your system preference changes.

## Theme Persistence

Your theme preference is automatically saved and persists across browser sessions.

### How It Works

- Your preference is stored in your browser's localStorage
- When you return to Flowfile, your chosen theme is automatically applied
- Clearing browser data will reset to the default theme

### Cross-Device

!!! note "Browser-Specific"
    Theme preferences are stored per browser. If you use Flowfile on multiple devices or browsers, you'll need to set your preference on each one.

## System Theme Detection

When using **System** mode, Flowfile detects your operating system's theme preference:

- **Windows** - Follows the Windows color mode setting (Settings > Personalization > Colors)
- **macOS** - Follows the macOS appearance setting (System Preferences > General > Appearance)
- **Linux** - Follows the desktop environment's dark mode setting

Flowfile responds to real-time changes. If you switch your OS theme while Flowfile is open, the interface updates automatically.

## Visual Comparison

<!-- TODO: Add screenshots showing light vs dark mode comparison -->

The dark theme applies consistently across all areas of the application:

- Main canvas area
- Node panels and sidebar
- Settings drawers
- Data preview tables
- Dialogs and modals

## Accessibility Considerations

Both themes are designed with accessibility in mind:

- Sufficient color contrast for readability
- Consistent visual hierarchy
- Clear focus indicators for keyboard navigation

!!! info "Feedback Welcome"
    If you encounter any accessibility issues with either theme, please report them on our [GitHub issues page](https://github.com/edwardvaneechoud/Flowfile/issues).
