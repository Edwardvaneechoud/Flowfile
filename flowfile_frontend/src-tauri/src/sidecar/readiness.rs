use crate::sidecar::{ServiceStatusEvent, SidecarError};
use crate::state::{HEALTH_CHECK_TIMEOUT_MS, SERVICE_START_TIMEOUT_MS};
use std::time::Duration;
use tauri::{AppHandle, Emitter};
use tokio::time::{sleep, Instant};

/// Single GET against /docs — `true` iff the service is responsive.
pub async fn probe_once(port: u16) -> bool {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_millis(HEALTH_CHECK_TIMEOUT_MS))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    let url = format!("http://127.0.0.1:{port}/docs");
    matches!(client.get(url).send().await, Ok(resp) if resp.status().is_success())
}

/// Polls /docs every HEALTH_CHECK_TIMEOUT_MS until the service responds or SERVICE_START_TIMEOUT_MS elapses.
/// Emits per-attempt status updates to the loading window.
pub async fn wait_until_ready(
    app: &AppHandle,
    name: &'static str,
    port: u16,
) -> Result<(), SidecarError> {
    let started = Instant::now();
    let deadline = started + Duration::from_millis(SERVICE_START_TIMEOUT_MS);

    let _ = app.emit(
        "services-status",
        ServiceStatusEvent {
            name,
            state: "starting",
            message: None,
        },
    );

    loop {
        if probe_once(port).await {
            log::info!("{} is responsive on {} after {:?}", name, port, started.elapsed());
            let _ = app.emit(
                "services-status",
                ServiceStatusEvent {
                    name,
                    state: "ready",
                    message: None,
                },
            );
            return Ok(());
        }

        if Instant::now() >= deadline {
            let _ = app.emit(
                "services-status",
                ServiceStatusEvent {
                    name,
                    state: "error",
                    message: Some(format!(
                        "Did not respond within {} ms",
                        SERVICE_START_TIMEOUT_MS
                    )),
                },
            );
            return Err(SidecarError::ReadinessTimeout {
                name,
                port,
                timeout_ms: SERVICE_START_TIMEOUT_MS,
            });
        }

        sleep(Duration::from_millis(HEALTH_CHECK_TIMEOUT_MS)).await;
    }
}
