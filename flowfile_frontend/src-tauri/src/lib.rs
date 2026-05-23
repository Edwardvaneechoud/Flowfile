mod commands;
mod env;
mod menu;
mod oauth;
mod sidecar;
mod state;
mod window;

use std::sync::Arc;
use tauri::{Emitter, Manager, RunEvent, WebviewUrl, WebviewWindowBuilder, WindowEvent};
use tauri_plugin_log::{Target, TargetKind};

use crate::state::{AppState, ServicePorts};

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let shared_state: Arc<AppState> = Arc::new(AppState::default());

    let mut builder = tauri::Builder::default()
        .manage(shared_state.clone())
        .plugin(
            tauri_plugin_log::Builder::new()
                .targets([
                    Target::new(TargetKind::Stdout),
                    Target::new(TargetKind::LogDir { file_name: Some("flowfile".into()) }),
                ])
                .level(log::LevelFilter::Info)
                .build(),
        )
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_process::init())
        .plugin(tauri_plugin_os::init())
        .plugin(tauri_plugin_window_state::Builder::default().build());

    #[cfg(desktop)]
    {
        builder = builder.plugin(tauri_plugin_updater::Builder::new().build());
    }

    builder
        .invoke_handler(tauri::generate_handler![
            commands::get_services_status,
            commands::get_service_ports,
            commands::get_app_version,
            commands::quit_app,
            commands::app_refresh,
            commands::open_oauth,
        ])
        .menu(|app_handle| menu::build(app_handle))
        .on_menu_event(|app, event| menu::on_menu_event(app, event.id().as_ref()))
        .setup(|app| {
            let handle = app.handle().clone();

            tauri::async_runtime::spawn(async move {
                {
                    let state: Arc<AppState> = handle.state::<Arc<AppState>>().inner().clone();
                    let mut s = state.services_status.lock();
                    s.status = "starting".into();
                    s.error = None;
                }
                let _ = handle.emit(
                    "services-status",
                    serde_json::json!({ "status": "starting", "error": null }),
                );

                match sidecar::start_services(handle.clone()).await {
                    Ok(ports) => {
                        if let Err(err) = create_main_window(&handle, ports) {
                            log::error!("failed to create main window: {}", err);
                            window::show_error(&handle, format!("Failed to create main window: {err}"));
                            return;
                        }
                        let _ = handle.emit("startup-success", ());
                        window::show_main(&handle);
                    }
                    Err(err) => {
                        log::error!("services failed to start: {}", err);
                        {
                            let state: Arc<AppState> =
                                handle.state::<Arc<AppState>>().inner().clone();
                            let mut s = state.services_status.lock();
                            s.status = "error".into();
                            s.error = Some(err.to_string());
                        }
                        window::show_error(&handle, err.to_string());
                    }
                }
            });

            Ok(())
        })
        .on_window_event(|window, event| {
            if window.label() == "main" {
                if let WindowEvent::CloseRequested { .. } = event {
                    let app = window.app_handle().clone();
                    tauri::async_runtime::block_on(async move {
                        sidecar::shutdown::shutdown_all(&app).await;
                    });
                }
            }
        })
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app_handle, event| {
            if let RunEvent::ExitRequested { .. } = event {
                let app = app_handle.clone();
                tauri::async_runtime::block_on(async move {
                    sidecar::shutdown::shutdown_all(&app).await;
                });
            }
        });
}

/// Create the main window programmatically so we can inject the discovered
/// service ports into the page **before** any renderer script runs. This is
/// what lets the renderer build its axios baseURL against the right ports
/// when multiple Flowfile instances coexist.
fn create_main_window(
    app: &tauri::AppHandle,
    ports: ServicePorts,
) -> tauri::Result<tauri::WebviewWindow> {
    // If a previous setup attempt already created it, just hand it back.
    if let Some(existing) = app.get_webview_window("main") {
        return Ok(existing);
    }

    let init_script = format!(
        "window.__FLOWFILE_PORTS__ = Object.freeze({{ core: {}, worker: {} }});",
        ports.core, ports.worker
    );

    WebviewWindowBuilder::new(app, "main", WebviewUrl::App("index.html".into()))
        .title("Flowfile")
        .inner_size(1600.0, 1000.0)
        .min_inner_size(1024.0, 700.0)
        .resizable(true)
        .center()
        .visible(false)
        // Tauri intercepts native drag-drop by default (to fire tauri://drag-drop
        // for files dragged onto the window), which also swallows HTML5 drag
        // events the renderer needs — VueFlow's node-palette drag, AG Grid
        // column reorder, etc. We're not handling file drops in this app, so
        // disable the native capture and let the webview see the events.
        .disable_drag_drop_handler()
        .initialization_script(&init_script)
        .build()
}
