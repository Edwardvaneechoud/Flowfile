//! Reads the filesystem paths of an in-flight drag off the macOS drag pasteboard.
//!
//! WKWebView strips file:// URLs out of the DOM's DataTransfer (it exposes only
//! http/https/data/blob), so the renderer can never learn where a dropped file
//! lives. AppKit does populate NSPasteboardNameDrag with the real file paths,
//! which leaves it as the only path source we have. The renderer prefetches this
//! at drag-enter, while the session is provably live, and pairs the result with
//! the dropped files by basename — any mismatch falls back to a copy import.

/// Absolute paths on the drag pasteboard, or an empty vec when there are none.
#[cfg(target_os = "macos")]
// NSFilenamesPboardType is soft-deprecated but AppKit still fills it on every drag; it is the
// same type wry reads (from the live drag delegate), and it hands back plain paths, not URLs.
#[allow(deprecated)]
pub fn read() -> Vec<String> {
    use objc2_app_kit::{NSFilenamesPboardType, NSPasteboard, NSPasteboardNameDrag};
    use objc2_foundation::{NSArray, NSString};

    // SAFETY: extern statics plus AppKit reads, which the sync command runs on the main thread.
    unsafe {
        let pasteboard = NSPasteboard::pasteboardWithName(NSPasteboardNameDrag);
        let Some(plist) = pasteboard.propertyListForType(NSFilenamesPboardType) else {
            return Vec::new();
        };
        let Ok(paths) = plist.downcast::<NSArray>() else {
            return Vec::new();
        };
        paths
            .into_iter()
            .filter_map(|path| path.downcast::<NSString>().ok())
            .map(|path| path.to_string())
            .collect()
    }
}

/// No native drag-path source outside macOS; callers fall back to uploading.
#[cfg(not(target_os = "macos"))]
pub fn read() -> Vec<String> {
    Vec::new()
}
