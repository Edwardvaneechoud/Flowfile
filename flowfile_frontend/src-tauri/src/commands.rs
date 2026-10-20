use crate::oauth;
use crate::popout::PopoutRef;
use crate::state::{AppState, ServicePorts, ServicesStatus};
use crate::window;
use std::sync::Arc;
use tauri::{AppHandle, State, WebviewWindow, Window};

/// Commands are not gated by the capability files, so the ones that end or reshape the app check
/// the calling window themselves: a pop-out window must never quit or update the app.
fn require_main(window: &Window) -> Result<(), String> {
    if window.label() == "main" {
        Ok(())
    } else {
        Err(format!("window '{}' may not do this", window.label()))
    }
}

#[tauri::command]
pub fn get_services_status(state: State<'_, Arc<AppState>>) -> ServicesStatus {
    state.services_status.lock().clone()
}

#[tauri::command]
pub fn get_service_ports(state: State<'_, Arc<AppState>>) -> ServicePorts {
    *state.ports.lock()
}

#[tauri::command]
pub fn get_app_version(app: AppHandle) -> String {
    app.package_info().version.to_string()
}

#[tauri::command]
pub async fn quit_app(app: AppHandle, window: Window) -> Result<(), String> {
    require_main(&window)?;
    crate::sidecar::shutdown::shutdown_all(&app).await;
    app.exit(0);
    Ok(())
}

/// Stop the sidecars before an updater install: on macOS the install replaces the
/// running .app bundle the sidecars execute from, on Windows it hands off to NSIS
/// and exits. `shutdown_all` latches `is_shutting_down` and takes the PIDs, so the
/// supervisor won't respawn and the relaunch that follows is a no-op here.
#[tauri::command]
pub async fn prepare_for_update(app: AppHandle, window: Window) -> Result<(), String> {
    require_main(&window)?;
    crate::sidecar::shutdown::shutdown_all(&app).await;
    Ok(())
}

/// Reload the window that asked, whichever it is.
#[tauri::command]
pub fn app_refresh(webview_window: WebviewWindow) -> Result<(), String> {
    webview_window
        .eval("window.location.reload()")
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn open_oauth(app: AppHandle, url: String) -> Result<Option<String>, String> {
    oauth::open_oauth_window(app, url).await
}

// Sync on purpose: sync commands run on the main thread, which the AppKit read requires.
#[tauri::command]
pub fn read_drag_paths() -> Vec<String> {
    crate::drag_paths::read()
}

// Async on purpose: a sync command runs inside the WebView2 IPC callback on Windows, where
// building a webview deadlocks (wry#583): the new window stays blank forever. From the async
// runtime the build is dispatched to the event loop instead. The same holds for closing one:
// tauri's own window `close` command is async for it, so the commands that close a window are
// async too, while the ones that only post messages stay sync.
#[tauri::command]
pub async fn open_popout_window(
    app: AppHandle,
    window: Window,
    kind: String,
    flow_id: i64,
    hash: String,
    title: String,
) -> Result<(), String> {
    require_main(&window)?;
    window::open_popout_window(&app, &kind, flow_id, &hash, &title)
}

#[tauri::command]
pub fn focus_popout_window(
    app: AppHandle,
    window: Window,
    kind: String,
    flow_id: i64,
) -> Result<(), String> {
    require_main(&window)?;
    window::focus_popout_window(&app, &kind, flow_id);
    Ok(())
}

#[tauri::command]
pub async fn close_popout_window(
    app: AppHandle,
    window: Window,
    kind: String,
    flow_id: i64,
) -> Result<(), String> {
    require_main(&window)?;
    window::close_popout_window(&app, &kind, flow_id);
    Ok(())
}

#[tauri::command]
pub fn list_popout_windows(app: AppHandle, window: Window) -> Result<Vec<PopoutRef>, String> {
    require_main(&window)?;
    Ok(window::popout_windows(&app))
}

// A pop-out acts on itself: the registry entry of the calling window's label says what it hosts,
// so the caller names no kind or flow, and a window without an entry (`main`) is refused there.
// Async: it closes the calling window (see `open_popout_window`).
#[tauri::command]
pub async fn return_popout_window(app: AppHandle, window: Window) -> Result<(), String> {
    window::return_popout_window(&app, window.label())
}

/// The calling pop-out followed its flow's Save As to `to`; the registry follows and `main` is told.
#[tauri::command]
pub fn rekey_popout_window(app: AppHandle, window: Window, to: i64) -> Result<(), String> {
    window::rekey_popout_window(&app, window.label(), to)
}

/// The calling pop-out listens for the designer's messages now; `main` answers with the current state.
#[tauri::command]
pub fn popout_window_ready(app: AppHandle, window: Window) -> Result<(), String> {
    window::popout_window_ready(&app, window.label())
}

/// The designer's message to a flow's pop-out window of one kind (a selection to follow).
#[tauri::command]
pub fn post_to_popout_window(
    app: AppHandle,
    window: Window,
    kind: String,
    flow_id: i64,
    message: serde_json::Value,
) -> Result<(), String> {
    require_main(&window)?;
    window::post_to_popout_window(&app, &kind, flow_id, message)
}
