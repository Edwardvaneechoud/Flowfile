use tauri::{AppHandle, Manager, WebviewUrl, WebviewWindowBuilder};
use tokio::sync::oneshot;
use url::Url;

const OAUTH_WINDOW_LABEL: &str = "oauth";

/// Open a modal OAuth window pointing at `auth_url`, intercept the first
/// redirect carrying a `code=` query parameter, return the captured code.
/// Resolves to `None` if the user closes the window without completing auth.
pub async fn open_oauth_window(app: AppHandle, auth_url: String) -> Result<Option<String>, String> {
    if let Some(existing) = app.get_webview_window(OAUTH_WINDOW_LABEL) {
        let _ = existing.close();
    }

    let url = Url::parse(&auth_url).map_err(|e| format!("invalid url: {e}"))?;

    let (tx, rx) = oneshot::channel::<Option<String>>();
    let tx = std::sync::Arc::new(std::sync::Mutex::new(Some(tx)));

    let builder = WebviewWindowBuilder::new(&app, OAUTH_WINDOW_LABEL, WebviewUrl::External(url))
        .title("Sign in")
        .inner_size(600.0, 700.0)
        .resizable(false)
        .center();

    // `WebviewWindowBuilder::parent` consumes self and returns Result<Self>,
    // so attach it via a chained let-binding rather than a conditional block.
    let builder = match app.get_webview_window("main") {
        Some(parent) => builder.parent(&parent).map_err(|e| e.to_string())?,
        None => builder,
    };

    let tx_for_nav = tx.clone();
    let builder = builder.on_navigation(move |navigated_url| {
        if let Some(code) = extract_code(navigated_url.as_str()) {
            if let Ok(mut guard) = tx_for_nav.lock() {
                if let Some(sender) = guard.take() {
                    let _ = sender.send(Some(code));
                }
            }
            return false; // cancel the navigation; we have what we needed
        }
        true
    });

    let window = builder.build().map_err(|e| e.to_string())?;

    let tx_for_close = tx.clone();
    window.on_window_event(move |event| {
        if let tauri::WindowEvent::Destroyed = event {
            if let Ok(mut guard) = tx_for_close.lock() {
                if let Some(sender) = guard.take() {
                    let _ = sender.send(None);
                }
            }
        }
    });

    let result = rx.await.map_err(|e| e.to_string())?;

    if let Some(window) = app.get_webview_window(OAUTH_WINDOW_LABEL) {
        let _ = window.close();
    }

    Ok(result)
}

fn extract_code(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    parsed
        .query_pairs()
        .find(|(k, _)| k == "code")
        .map(|(_, v)| v.into_owned())
}
