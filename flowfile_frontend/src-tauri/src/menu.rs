use tauri::menu::{AboutMetadata, Menu, MenuBuilder, MenuItem, PredefinedMenuItem, SubmenuBuilder};
use tauri::{AppHandle, Manager, Runtime};
use tauri_plugin_opener::OpenerExt;

const DOCS_URL: &str = "https://github.com/Edwardvaneechoud/Flowfile#readme";
const ISSUES_URL: &str = "https://github.com/Edwardvaneechoud/Flowfile/issues";
const REPO_URL: &str = "https://github.com/Edwardvaneechoud/Flowfile";

pub fn build<R: Runtime>(app: &AppHandle<R>) -> tauri::Result<Menu<R>> {
    let pkg_info = app.package_info();
    let about_metadata = AboutMetadata {
        name: Some(pkg_info.name.clone()),
        version: Some(pkg_info.version.to_string()),
        copyright: Some(format!("© {} Edwardvaneechoud", chrono_year())),
        ..Default::default()
    };

    let mut builder = MenuBuilder::new(app);

    #[cfg(target_os = "macos")]
    {
        let app_menu = SubmenuBuilder::new(app, &pkg_info.name)
            .item(&PredefinedMenuItem::about(app, None, Some(about_metadata.clone()))?)
            .separator()
            .item(&PredefinedMenuItem::services(app, None)?)
            .separator()
            .item(&PredefinedMenuItem::hide(app, None)?)
            .item(&PredefinedMenuItem::hide_others(app, None)?)
            .item(&PredefinedMenuItem::show_all(app, None)?)
            .separator()
            .item(&PredefinedMenuItem::quit(app, None)?)
            .build()?;
        builder = builder.item(&app_menu);
    }

    let file_menu = SubmenuBuilder::new(app, "File")
        .item(&PredefinedMenuItem::close_window(app, None)?)
        .build()?;
    builder = builder.item(&file_menu);

    let edit_menu = SubmenuBuilder::new(app, "Edit")
        .item(&PredefinedMenuItem::undo(app, None)?)
        .item(&PredefinedMenuItem::redo(app, None)?)
        .separator()
        .item(&PredefinedMenuItem::cut(app, None)?)
        .item(&PredefinedMenuItem::copy(app, None)?)
        .item(&PredefinedMenuItem::paste(app, None)?)
        .item(&PredefinedMenuItem::select_all(app, None)?)
        .build()?;
    builder = builder.item(&edit_menu);

    let refresh = MenuItem::with_id(app, "view-refresh", "Refresh", true, Some("CmdOrCtrl+R"))?;
    let toggle_fullscreen = MenuItem::with_id(
        app,
        "view-fullscreen",
        "Toggle Fullscreen",
        true,
        Some("F11"),
    )?;
    let zoom_in = MenuItem::with_id(app, "view-zoom-in", "Zoom In", true, Some("CmdOrCtrl++"))?;
    let zoom_out = MenuItem::with_id(app, "view-zoom-out", "Zoom Out", true, Some("CmdOrCtrl+-"))?;
    let zoom_reset = MenuItem::with_id(
        app,
        "view-zoom-reset",
        "Reset Zoom",
        true,
        Some("CmdOrCtrl+0"),
    )?;

    let view_menu = SubmenuBuilder::new(app, "View")
        .item(&refresh)
        .separator()
        .item(&zoom_in)
        .item(&zoom_out)
        .item(&zoom_reset)
        .separator()
        .item(&toggle_fullscreen)
        .build()?;
    builder = builder.item(&view_menu);

    let window_menu = SubmenuBuilder::new(app, "Window")
        .item(&PredefinedMenuItem::minimize(app, None)?)
        .item(&PredefinedMenuItem::maximize(app, None)?)
        .build()?;
    builder = builder.item(&window_menu);

    let docs = MenuItem::with_id(app, "help-docs", "Documentation", true, None::<&str>)?;
    let issues = MenuItem::with_id(app, "help-issues", "Report an Issue", true, None::<&str>)?;
    let repo = MenuItem::with_id(app, "help-repo", "View on GitHub", true, None::<&str>)?;
    let help_menu = SubmenuBuilder::new(app, "Help")
        .item(&docs)
        .item(&issues)
        .separator()
        .item(&repo)
        .build()?;
    builder = builder.item(&help_menu);

    builder.build()
}

pub fn on_menu_event<R: Runtime>(app: &AppHandle<R>, event_id: &str) {
    match event_id {
        "view-refresh" => {
            if let Some(main) = app.get_webview_window("main") {
                let _ = main.eval("window.location.reload()");
            }
        }
        "view-zoom-in" => emit_zoom(app, "in"),
        "view-zoom-out" => emit_zoom(app, "out"),
        "view-zoom-reset" => emit_zoom(app, "reset"),
        "view-fullscreen" => {
            if let Some(main) = app.get_webview_window("main") {
                if let Ok(current) = main.is_fullscreen() {
                    let _ = main.set_fullscreen(!current);
                }
            }
        }
        "help-docs" => open_external(app, DOCS_URL),
        "help-issues" => open_external(app, ISSUES_URL),
        "help-repo" => open_external(app, REPO_URL),
        _ => {}
    }
}

fn emit_zoom<R: Runtime>(app: &AppHandle<R>, direction: &str) {
    use tauri::Emitter;
    let _ = app.emit("view:zoom", direction);
}

fn open_external<R: Runtime>(app: &AppHandle<R>, url: &str) {
    if let Err(err) = app.opener().open_url(url, None::<&str>) {
        log::warn!("failed to open external url {}: {}", url, err);
    }
}

/// We avoid pulling in `chrono` just for the copyright string — fall back to a
/// fixed year. If the year matters, the user can update this string.
fn chrono_year() -> i32 {
    2025
}
