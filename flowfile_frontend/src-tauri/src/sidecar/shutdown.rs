use crate::state::{
    AppState, FORCE_KILL_TIMEOUT_MS, KILL_POLL_INTERVAL_MS, SHUTDOWN_TIMEOUT_MS, SIGTERM_GRACE_MS,
};
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

/// Reap any sidecars we already spawned, **without** any network I/O. Used on
/// the startup-failure path (`lib.rs`), where calling the full `shutdown_all`
/// would be wrong: the ports may be unresponsive (often *why* startup failed),
/// and in the `NoFreePortPair` case `state.ports` still holds the defaults —
/// which may belong to another Flowfile instance, so POSTing `/shutdown` there
/// could kill an unrelated app.
///
/// Sets `is_shutting_down` first so the supervisor's restart loop
/// (`handle_termination` / its backoff sleep) won't respawn what we kill.
pub fn kill_spawned(app: &AppHandle) {
    let state: Arc<AppState> = app.state::<Arc<AppState>>().inner().clone();

    {
        let mut guard = state.is_shutting_down.lock();
        if *guard {
            return;
        }
        *guard = true;
    }

    let core_pid = state.core_pid.lock().take();
    let worker_pid = state.worker_pid.lock().take();

    if core_pid.is_some() || worker_pid.is_some() {
        log::info!("startup failed — reaping spawned sidecars");
    }
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

    // Poll for a graceful exit rather than sleeping a fixed interval. The worker
    // stays alive until its lifespan `finally` has joined every viz-session
    // child (flowfile_worker/viz_sessions.py), which can take several seconds, so
    // `process_alive` going false is our signal the whole group drained cleanly.
    // We give it up to SIGTERM_GRACE_MS, checking every KILL_POLL_INTERVAL_MS, and
    // return the instant it exits (the common, session-free case is sub-second).
    // If it never exits, the process-group SIGKILL below is the backstop — it
    // reaps the children too, so nothing leaks; it's just not graceful.
    let mut waited = Duration::ZERO;
    let grace = Duration::from_millis(SIGTERM_GRACE_MS);
    let interval = Duration::from_millis(KILL_POLL_INTERVAL_MS);
    while waited < grace {
        if !process_alive(pid) {
            log::info!("{} (pid {}) exited gracefully", name, pid);
            return;
        }
        std::thread::sleep(interval);
        waited += interval;
    }

    if process_alive(pid) && send_sigkill(pid) {
        log::warn!(
            "force-killed {} (pid {}) after {} ms grace",
            name,
            pid,
            SIGTERM_GRACE_MS
        );
    }
}

#[cfg(unix)]
fn send_sigterm(pid: u32) -> bool {
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;
    // Signal the whole process group, not just the leader. Each sidecar is
    // spawned with `process_group(0)` (see sidecar/mod.rs), so its pgid equals
    // its own pid — `killpg(pid, …)` therefore reaps the worker AND the
    // multiprocessing viz-session children it forked into the same group. A
    // bare `kill(pid, …)` would leave those grandchildren orphaned on Unix.
    // (Core's scheduled-flow runs use start_new_session=True and live in their
    // own session, so they are intentionally outside this group and survive.)
    killpg(Pid::from_raw(pid as i32), Signal::SIGTERM).is_ok()
}

#[cfg(unix)]
fn send_sigkill(pid: u32) -> bool {
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;
    killpg(Pid::from_raw(pid as i32), Signal::SIGKILL).is_ok()
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
