use std::sync::Arc;

use tauri::{AppHandle, Emitter, EventTarget, Manager, WebviewWindow, WindowEvent};

use crate::state::AppState;

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
    let _ = app.emit("services-status", payload);
}

/// A flow's pop-out notebook window is labelled `notebook-<flow id>`; the glob in
/// `capabilities/notebook.json` grants it a reduced permission set.
pub const NOTEBOOK_LABEL_PREFIX: &str = "notebook-";

pub fn notebook_label(flow_id: i64) -> String {
    format!("{NOTEBOOK_LABEL_PREFIX}{flow_id}")
}

pub fn notebook_flow_id(label: &str) -> Option<i64> {
    label.strip_prefix(NOTEBOOK_LABEL_PREFIX)?.parse().ok()
}

/// Open the notebook window of a flow, or focus it when it is already open. It loads the same
/// renderer as the main window, with the same ports injected, on the pop-out route.
pub fn open_notebook_window(app: &AppHandle, flow_id: i64) -> Result<(), String> {
    let label = notebook_label(flow_id);
    if let Some(existing) = app.get_webview_window(&label) {
        let _ = existing.show();
        let _ = existing.set_focus();
        return Ok(());
    }
    let ports = *app.state::<Arc<AppState>>().ports.lock();
    let window = crate::build_app_window(
        app,
        &label,
        &format!("index.html#/notebook?flow={flow_id}"),
        ports,
    )
    .title("Flowfile – Notebook")
    .inner_size(1100.0, 800.0)
    .min_inner_size(720.0, 500.0)
    .visible(true)
    .build()
    .map_err(|e| e.to_string())?;

    // The designer's dock shows a stub while the window is open; it needs to know when it went.
    let handle = app.clone();
    window.on_window_event(move |event| {
        if let WindowEvent::Destroyed = event {
            let _ = handle.emit_to(
                EventTarget::webview_window("main"),
                "notebook-window-closed",
                flow_id,
            );
        }
    });
    Ok(())
}

pub fn focus_notebook_window(app: &AppHandle, flow_id: i64) {
    if let Some(window) = app.get_webview_window(&notebook_label(flow_id)) {
        let _ = window.show();
        let _ = window.set_focus();
    }
}

pub fn close_notebook_window(app: &AppHandle, flow_id: i64) {
    if let Some(window) = app.get_webview_window(&notebook_label(flow_id)) {
        let _ = window.close();
    }
}

pub fn notebook_windows(app: &AppHandle) -> Vec<(i64, WebviewWindow)> {
    let mut windows: Vec<(i64, WebviewWindow)> = app
        .webview_windows()
        .into_iter()
        .filter_map(|(label, window)| notebook_flow_id(&label).map(|id| (id, window)))
        .collect();
    windows.sort_by_key(|(id, _)| *id);
    windows
}

/// Close every notebook window: the main window is closing and the sidecars go with it, so a
/// notebook window left open would sit on a dead backend and keep the app alive.
pub fn close_notebook_windows(app: &AppHandle) {
    for (_, window) in notebook_windows(app) {
        let _ = window.close();
    }
}
