use tauri::{AppHandle, Manager};

/// Hide the loading window and reveal the main window. Idempotent.
pub fn show_main(app: &AppHandle) {
    if let Some(loading) = app.get_webview_window("loading") {
        let _ = loading.close();
    }
    if let Some(main) = app.get_webview_window("main") {
        let _ = main.show();
        let _ = main.set_focus();
    }
}

/// Surface an error in the loading window — the renderer-side script listens
/// for `services-status` events and renders the message.
pub fn show_error(app: &AppHandle, message: impl Into<String>) {
    let payload = serde_json::json!({
        "status": "error",
        "error": message.into(),
    });
    use tauri::Emitter;
    let _ = app.emit("services-status", payload);
}
