use std::sync::Arc;

use tauri::{AppHandle, Emitter, EventTarget, Manager, WebviewWindow, WindowEvent};

use crate::popout::{self, PopoutRef};
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
    let registered = state.popouts.lock().find(kind, flow_id);
    if let Some(label) = registered {
        if let Some(existing) = app.get_webview_window(&label) {
            let _ = existing.show();
            let _ = existing.set_focus();
            return Ok(());
        }
        // Registered but gone (its Destroyed never ran): forget it and open afresh.
        state.popouts.lock().remove(&label);
    }
    let ports = *state.ports.lock();
    let label = state.popouts.lock().register(kind, flow_id);
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
            let removed = handle.state::<Arc<AppState>>().popouts.lock().remove(&label);
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

/// The pop-out's "Return to designer": `main` reopens its panel on that flow
/// (`popout-window-returned`) and comes to the front, then the window closes (its `Destroyed`
/// still reports the close).
pub fn return_popout_window(app: &AppHandle, kind: &str, flow_id: i64) {
    let returned = PopoutRef {
        kind: kind.to_string(),
        flow_id,
    };
    let _ = app.emit_to(
        EventTarget::webview_window("main"),
        "popout-window-returned",
        returned,
    );
    if let Some(main) = app.get_webview_window("main") {
        let _ = main.show();
        let _ = main.set_focus();
    }
    close_popout_window(app, kind, flow_id);
}

/// The window's flow moved to another id (a Save As): the label stays, the registry follows, and
/// `main` moves its mark (`popout-window-rekeyed`).
pub fn rekey_popout_window(app: &AppHandle, label: &str, to: i64) -> Result<(), String> {
    let state = app.state::<Arc<AppState>>();
    let before = {
        let mut popouts = state.popouts.lock();
        let before = popouts
            .get(label)
            .cloned()
            .ok_or_else(|| format!("window '{label}' is not a pop-out window"))?;
        popouts.rekey(label, to);
        before
    };
    let _ = app.emit_to(
        EventTarget::webview_window("main"),
        "popout-window-rekeyed",
        serde_json::json!({ "kind": before.kind, "from": before.flow_id, "to": to }),
    );
    Ok(())
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
