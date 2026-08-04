//! Recovers the filesystem paths of files dropped into the WebView2 webview.
//!
//! Webviews sanitize dropped-file paths out of the DOM, so a `drop` event alone can
//! never tell the renderer where a file lives. WebView2's documented recovery route
//! is `postMessageWithAdditionalObjects`: the page hands the dropped `File` objects
//! back to the host, which reads `ICoreWebView2File::Path` off each one. The renderer
//! correlates the reply by token, pairs paths to files by basename, and falls back to
//! a copy import whenever any step here comes up short.

#[cfg(windows)]
use tauri::Emitter;
#[cfg(windows)]
use webview2_com::{
    take_pwstr,
    Microsoft::Web::WebView2::Win32::{
        ICoreWebView2File, ICoreWebView2WebMessageReceivedEventArgs,
        ICoreWebView2WebMessageReceivedEventArgs2,
    },
    WebMessageReceivedEventHandler,
};
#[cfg(windows)]
use windows_core::{Interface, PWSTR};

/// Tauri event carrying the resolved paths; the payload echoes the request token.
#[cfg(windows)]
const DROP_PATHS_EVENT: &str = "file-drop-paths";

/// Only messages with this prefix are ours — every other one belongs to wry's IPC.
#[cfg(windows)]
const DROP_MESSAGE_PREFIX: &str = "ff-drop-";

/// Registers a WebMessageReceived handler answering the renderer's `ff-drop-*` messages.
#[cfg(windows)]
pub fn attach(window: &tauri::WebviewWindow) {
    let handle = window.clone();
    let registered = window.with_webview(move |platform| {
        // SAFETY: WebView2 COM calls; with_webview runs this on the webview's UI thread.
        unsafe {
            let core = match platform.controller().CoreWebView2() {
                Ok(core) => core,
                Err(err) => {
                    log::warn!("webview2 drop paths: no CoreWebView2 ({err}); drops stay copies");
                    return;
                }
            };
            let handler = WebMessageReceivedEventHandler::create(Box::new(move |_, args| {
                if let Some(args) = args {
                    on_web_message(&handle, &args);
                }
                Ok(())
            }));
            // Our own registration token: wry keeps its IPC handler on a separate one.
            let mut token = 0i64;
            if let Err(err) = core.add_WebMessageReceived(&handler, &mut token) {
                log::warn!("webview2 drop paths: add_WebMessageReceived failed: {err}");
            }
        }
    });
    if let Err(err) = registered {
        log::warn!("webview2 drop paths: with_webview failed: {err}");
    }
}

#[cfg(windows)]
fn on_web_message(window: &tauri::WebviewWindow, args: &ICoreWebView2WebMessageReceivedEventArgs) {
    let Some(token) = read_message(args) else {
        return;
    };
    if !token.starts_with(DROP_MESSAGE_PREFIX) {
        return;
    }
    let paths = collect_paths(args);
    let payload = serde_json::json!({ "token": token, "paths": paths });
    if let Err(err) = window.emit(DROP_PATHS_EVENT, payload) {
        log::warn!("webview2 drop paths: emit failed: {err}");
    }
}

/// None whenever the message is absent or not a string — i.e. never one of ours.
#[cfg(windows)]
fn read_message(args: &ICoreWebView2WebMessageReceivedEventArgs) -> Option<String> {
    let mut raw = PWSTR::null();
    // SAFETY: `raw` is a valid out-param; take_pwstr frees the buffer WebView2 allocated.
    unsafe {
        args.TryGetWebMessageAsString(&mut raw).ok()?;
        Some(take_pwstr(raw))
    }
}

/// Paths of the `File` objects the page passed as additional objects, best-effort.
#[cfg(windows)]
fn collect_paths(args: &ICoreWebView2WebMessageReceivedEventArgs) -> Vec<String> {
    let Ok(args2) = args.cast::<ICoreWebView2WebMessageReceivedEventArgs2>() else {
        return Vec::new();
    };
    // SAFETY: COM reads on the args this handler was just handed. Every step is checked,
    // and a short result only costs the renderer its link — it copies the files instead.
    unsafe {
        let Ok(objects) = args2.AdditionalObjects() else {
            return Vec::new();
        };
        let mut count = 0u32;
        if objects.Count(&mut count).is_err() {
            return Vec::new();
        }
        let mut paths = Vec::with_capacity(count as usize);
        for index in 0..count {
            // Entries WebView2 could not represent come back null, which fails here.
            let Ok(object) = objects.GetValueAtIndex(index) else {
                continue;
            };
            let Ok(file) = object.cast::<ICoreWebView2File>() else {
                continue;
            };
            let mut raw = PWSTR::null();
            if file.Path(&mut raw).is_err() {
                continue;
            }
            let path = take_pwstr(raw);
            if !path.is_empty() {
                paths.push(path);
            }
        }
        paths
    }
}

/// No WebView2 outside Windows; those platforms import a copy of the dropped file.
#[cfg(not(windows))]
pub fn attach(_window: &tauri::WebviewWindow) {}
