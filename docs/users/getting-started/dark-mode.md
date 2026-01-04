# Dark Mode

Flowfile supports light, dark, and system theme modes to match your preferred visual style and reduce eye strain.

## Overview

Choose from three theme options:

| Mode | Description |
|------|-------------|
| **Light** | Classic light background with dark text |
| **Dark** | Dark background with light text, easier on the eyes in low-light environments |
| **System** | Automatically follows your operating system's theme preference |

## Switching Themes

The theme toggle is located in the **sidebar footer** (bottom of the left sidebar).

### To Change Your Theme

1. Look for the sun/moon icon at the bottom of the left sidebar
2. Click the icon to toggle between light and dark mode
3. The theme changes immediately

**Icon Indicators:**

- **Sun icon** - Currently in dark mode (click to switch to light)
- **Moon icon** - Currently in light mode (click to switch to dark)

!!! tip "System Theme"
    To use system theme mode, the application follows your OS preference automatically. On Windows, this matches your Windows theme setting. On macOS, it follows your Appearance setting in System Preferences.

## Theme Persistence

Your theme preference is saved automatically and persists across browser sessions.

**How it works:**

- Your selection is stored in browser localStorage
- When you return to Flowfile, your preferred theme loads automatically
- The setting is per-browser (different browsers may have different preferences)

**Storage key:** `flowfile-theme-preference`

## System Theme Behavior

When using system theme mode:

- Flowfile monitors your OS theme setting
- Switching your OS to dark mode automatically updates Flowfile
- Switching your OS to light mode automatically updates Flowfile
- No manual intervention needed after initial setup

### Platform-Specific Settings

**Windows 10/11:**
Settings > Personalization > Colors > Choose your mode

**macOS:**
System Preferences > General > Appearance

**Linux (GNOME):**
Settings > Appearance > Style

## Screenshots

!!! note "Coming Soon"
    Screenshots comparing light and dark mode will be added in a future documentation update.

<!--
TODO: Add screenshots showing:
- The theme toggle location in the sidebar
- Light mode interface
- Dark mode interface
-->

## Troubleshooting

### Theme Not Persisting

If your theme preference resets:

1. **Check browser privacy settings** - Ensure localStorage is not blocked
2. **Clear localStorage** - Try clearing and re-setting your preference
3. **Check incognito mode** - Private browsing may not persist settings

### System Theme Not Detected

If system mode isn't following your OS theme:

1. **Check browser support** - Ensure your browser supports `prefers-color-scheme`
2. **Refresh the page** - The listener may need to reinitialize
3. **Restart the application** - Full restart may be needed after OS theme change

### Colors Look Wrong

If the theme appears incorrect:

1. **Hard refresh** - Press Ctrl+Shift+R (Cmd+Shift+R on Mac)
2. **Clear browser cache** - Outdated CSS may be cached
3. **Check for extensions** - Browser extensions may interfere with themes

---

[← Getting Started](index.md) | [Visual Editor Guide →](../visual-editor/index.md)
