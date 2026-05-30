use crate::oauth;
use crate::state::{AppState, ServicePorts, ServicesStatus};
use std::sync::Arc;
use tauri::{AppHandle, Manager, State};

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
pub async fn quit_app(app: AppHandle) {
    crate::sidecar::shutdown::shutdown_all(&app).await;
    app.exit(0);
}

#[tauri::command]
pub async fn app_refresh(app: AppHandle) -> Result<(), String> {
    let Some(main) = app.get_webview_window("main") else {
        return Err("main window not found".into());
    };
    main.eval("window.location.reload()")
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn open_oauth(app: AppHandle, url: String) -> Result<Option<String>, String> {
    oauth::open_oauth_window(app, url).await
}
