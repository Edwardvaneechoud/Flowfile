use crate::oauth;
use crate::state::{AppState, ServicePorts, ServicesStatus};
use crate::window;
use std::sync::Arc;
use tauri::{AppHandle, State, WebviewWindow, Window};

/// Commands are not gated by the capability files, so the ones that end or reshape the app check
/// the calling window themselves: a pop-out notebook window must never quit or update the app.
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

// The notebook window commands are sync on purpose: a window built inside an async command
// deadlocks on Windows, and the main thread is where window creation belongs anyway.
#[tauri::command]
pub fn open_notebook_window(app: AppHandle, window: Window, flow_id: i64) -> Result<(), String> {
    require_main(&window)?;
    window::open_notebook_window(&app, flow_id)
}

#[tauri::command]
pub fn focus_notebook_window(app: AppHandle, window: Window, flow_id: i64) -> Result<(), String> {
    require_main(&window)?;
    window::focus_notebook_window(&app, flow_id);
    Ok(())
}

#[tauri::command]
pub fn close_notebook_window(app: AppHandle, window: Window, flow_id: i64) -> Result<(), String> {
    require_main(&window)?;
    window::close_notebook_window(&app, flow_id);
    Ok(())
}

#[tauri::command]
pub fn list_notebook_windows(app: AppHandle, window: Window) -> Result<Vec<i64>, String> {
    require_main(&window)?;
    Ok(window::notebook_windows(&app)
        .into_iter()
        .map(|(id, _)| id)
        .collect())
}
