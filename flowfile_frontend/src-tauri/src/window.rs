use std::sync::Arc;

use tauri::{AppHandle, Emitter, EventTarget, Manager, WebviewWindow, WindowEvent};

use crate::popout::{self, PopoutRef, Registration};
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

/// Open a flow's pop-out window of one kind, or focus it when it is already open. It loads the same
/// renderer as the main window, with the same ports injected, on the route the renderer passes as
/// `hash` (`#/notebook?flow=4`): the shell knows neither kinds nor routes. The window gets the next
/// `popout-<kind>-<seq>` label and a registry entry; `capabilities/popout.json` grants every
/// `popout-*` window a reduced permission set.
pub fn open_popout_window(
    app: &AppHandle,
    kind: &str,
    flow_id: i64,
    hash: &str,
    title: &str,
) -> Result<(), String> {
    popout::validate_kind(kind)?;
    if !hash.starts_with("#/") {
        return Err(format!("'{hash}' is not a renderer route"));
    }
    let state = app.state::<Arc<AppState>>();
    let registration = state.popouts.lock().find_or_register(kind, flow_id);
    let label = match registration {
        Registration::Existing(label) => {
            // Open, or still being built by a concurrent open: focus what is there, never a second one.
            if let Some(existing) = app.get_webview_window(&label) {
                let _ = existing.show();
                let _ = existing.set_focus();
            }
            return Ok(());
        }
        Registration::New(label) => label,
    };
    let ports = *state.ports.lock();
    let built = crate::build_app_window(app, &label, &format!("index.html{hash}"), ports)
        .title(title)
        .inner_size(1100.0, 800.0)
        .min_inner_size(720.0, 500.0)
        .visible(true)
        .build();
    let window = match built {
        Ok(window) => window,
        Err(err) => {
            state.popouts.lock().remove(&label);
            return Err(err.to_string());
        }
    };

    // The designer's stub needs the close, reported under the flow id the window carries by then.
    let handle = app.clone();
    window.on_window_event(move |event| {
        if let WindowEvent::Destroyed = event {
            let removed = handle
                .state::<Arc<AppState>>()
                .popouts
                .lock()
                .remove(&label);
            if let Some(closed) = removed {
                let _ = handle.emit_to(
                    EventTarget::webview_window("main"),
                    "popout-window-closed",
                    closed,
                );
            }
        }
    });
    Ok(())
}

fn popout_window(app: &AppHandle, kind: &str, flow_id: i64) -> Option<WebviewWindow> {
    let label = app
        .state::<Arc<AppState>>()
        .popouts
        .lock()
        .find(kind, flow_id)?;
    app.get_webview_window(&label)
}

pub fn focus_popout_window(app: &AppHandle, kind: &str, flow_id: i64) {
    if let Some(window) = popout_window(app, kind, flow_id) {
        let _ = window.show();
        let _ = window.set_focus();
    }
}

pub fn close_popout_window(app: &AppHandle, kind: &str, flow_id: i64) {
    if let Some(window) = popout_window(app, kind, flow_id) {
        let _ = window.close();
    }
}

/// The calling pop-out's "Return to designer", by its label: `main` reopens its panel on the flow
/// the registry says that window hosts (`popout-window-returned`) and comes to the front, then the
/// window closes (its `Destroyed` still reports the close). An unregistered window is refused.
pub fn return_popout_window(app: &AppHandle, label: &str) -> Result<(), String> {
    let returned = app
        .state::<Arc<AppState>>()
        .popouts
        .lock()
        .get(label)
        .cloned()
        .ok_or_else(|| format!("window '{label}' is not a pop-out window"))?;
    let _ = app.emit_to(
        EventTarget::webview_window("main"),
        "popout-window-returned",
        returned,
    );
    if let Some(main) = app.get_webview_window("main") {
        let _ = main.show();
        let _ = main.set_focus();
    }
    if let Some(window) = app.get_webview_window(label) {
        let _ = window.close();
    }
    Ok(())
}

/// The calling pop-out's flow moved to another id (a Save As): the label stays, the registry
/// follows (refusing a flow another window of that kind hosts) and `main` moves its mark
/// (`popout-window-rekeyed`).
pub fn rekey_popout_window(app: &AppHandle, label: &str, to: i64) -> Result<(), String> {
    let before = app
        .state::<Arc<AppState>>()
        .popouts
        .lock()
        .rekey(label, to)?;
    let _ = app.emit_to(
        EventTarget::webview_window("main"),
        "popout-window-rekeyed",
        serde_json::json!({ "kind": before.kind, "from": before.flow_id, "to": to }),
    );
    Ok(())
}

/// The designer's message to the window hosting `(kind, flow_id)` (a selection to follow), emitted to
/// that window alone. No window for the pair is an error, so the caller knows nothing heard it.
pub fn post_to_popout_window(
    app: &AppHandle,
    kind: &str,
    flow_id: i64,
    message: serde_json::Value,
) -> Result<(), String> {
    popout::validate_kind(kind)?;
    let label = app
        .state::<Arc<AppState>>()
        .popouts
        .lock()
        .find(kind, flow_id)
        .ok_or_else(|| format!("no {kind} window hosts flow {flow_id}"))?;
    app.emit_to(EventTarget::webview_window(label), "popout-message", message)
        .map_err(|e| e.to_string())
}

/// The calling pop-out listens now, so `main` answers with what the window should show (a message
/// emitted before a window listens is lost). An unregistered window is refused.
pub fn popout_window_ready(app: &AppHandle, label: &str) -> Result<(), String> {
    let ready = app
        .state::<Arc<AppState>>()
        .popouts
        .lock()
        .get(label)
        .cloned()
        .ok_or_else(|| format!("window '{label}' is not a pop-out window"))?;
    app.emit_to(
        EventTarget::webview_window("main"),
        "popout-window-ready",
        ready,
    )
    .map_err(|e| e.to_string())
}

/// The pop-out windows open right now, by kind then flow id.
pub fn popout_windows(app: &AppHandle) -> Vec<PopoutRef> {
    let listed = app.state::<Arc<AppState>>().popouts.lock().list();
    listed
        .into_iter()
        .filter(|(label, _)| app.get_webview_window(label).is_some())
        .map(|(_, popout)| popout)
        .collect()
}

/// Close every pop-out window, registered or not: the main window is closing and the sidecars go
/// with it, so a window left open would sit on a dead backend and keep the app alive.
pub fn close_popout_windows(app: &AppHandle) {
    for (label, window) in app.webview_windows() {
        if popout::is_popout_label(&label) {
            let _ = window.close();
        }
    }
}
