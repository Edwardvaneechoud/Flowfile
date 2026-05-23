use crate::state::{AppState, FORCE_KILL_TIMEOUT_MS, SHUTDOWN_TIMEOUT_MS};
use std::sync::Arc;
use std::time::Duration;
use tauri::{AppHandle, Manager};
use tokio::time::sleep;

/// Best-effort graceful shutdown:
/// 1. POST /shutdown to each port (3s timeout).
/// 2. Wait 2s for natural exit.
/// 3. SIGTERM (Unix) / taskkill (Windows) via PID.
/// 4. Still alive → SIGKILL.
pub async fn shutdown_all(app: &AppHandle) {
    let state: Arc<AppState> = app.state::<Arc<AppState>>().inner().clone();

    {
        let mut guard = state.is_shutting_down.lock();
        if *guard {
            log::info!("shutdown already in progress");
            return;
        }
        *guard = true;
    }

    let ports = *state.ports.lock();
    log::info!(
        "shutting down services (core={}, worker={})",
        ports.core,
        ports.worker
    );

    // Step 1: HTTP /shutdown to both services in parallel.
    let core_http = http_shutdown(ports.core);
    let worker_http = http_shutdown(ports.worker);
    let _ = tokio::join!(core_http, worker_http);

    // Step 2: wait for natural exit.
    sleep(Duration::from_millis(FORCE_KILL_TIMEOUT_MS)).await;

    // Step 3 & 4: SIGTERM then SIGKILL on any survivors.
    let core_pid = state.core_pid.lock().take();
    let worker_pid = state.worker_pid.lock().take();

    kill_pid(core_pid, "flowfile_core");
    kill_pid(worker_pid, "flowfile_worker");
}

async fn http_shutdown(port: u16) {
    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_millis(SHUTDOWN_TIMEOUT_MS))
        .build()
    else {
        return;
    };

    let url = format!("http://127.0.0.1:{port}/shutdown");
    match client.post(&url).send().await {
        Ok(_) => log::info!("sent shutdown to {}", port),
        Err(err) => log::debug!(
            "shutdown POST to {} failed (likely already exited): {}",
            port,
            err
        ),
    }
}

fn kill_pid(pid: Option<u32>, name: &str) {
    let Some(pid) = pid else {
        log::debug!("{}: no pid to kill", name);
        return;
    };

    if !send_sigterm(pid) {
        log::debug!("{} (pid {}) already gone or SIGTERM failed", name, pid);
        return;
    }
    log::info!("sent SIGTERM to {} (pid {})", name, pid);

    // Give the process a moment to exit cleanly, then SIGKILL if still alive.
    std::thread::sleep(Duration::from_millis(500));
    if process_alive(pid) {
        if send_sigkill(pid) {
            log::warn!("force-killed {} (pid {})", name, pid);
        }
    }
}

#[cfg(unix)]
fn send_sigterm(pid: u32) -> bool {
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid as i32), Signal::SIGTERM).is_ok()
}

#[cfg(unix)]
fn send_sigkill(pid: u32) -> bool {
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid as i32), Signal::SIGKILL).is_ok()
}

#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    // `kill(pid, 0)` doesn't send a signal but returns Ok iff the process
    // exists and the caller has permission to signal it.
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid as i32), None).is_ok()
}

#[cfg(windows)]
fn send_sigterm(pid: u32) -> bool {
    // Windows has no SIGTERM; `taskkill` without `/F` sends WM_CLOSE first
    // which gives the process a chance to drain its event loop.
    std::process::Command::new("taskkill")
        .args(["/T", "/PID", &pid.to_string()])
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}

#[cfg(windows)]
fn send_sigkill(pid: u32) -> bool {
    std::process::Command::new("taskkill")
        .args(["/F", "/T", "/PID", &pid.to_string()])
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}

#[cfg(windows)]
fn process_alive(pid: u32) -> bool {
    std::process::Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/NH"])
        .output()
        .map(|out| {
            let txt = String::from_utf8_lossy(&out.stdout);
            txt.contains(&pid.to_string())
        })
        .unwrap_or(false)
}
