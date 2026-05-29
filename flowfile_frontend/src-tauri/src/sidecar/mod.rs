pub mod readiness;
pub mod shutdown;

use crate::env::build_child_env;
use crate::state::{
    AppState, ServicePorts, ServicesStatus, DEFAULT_CORE_PORT, PORT_SCAN_PAIRS,
};
use serde::Serialize;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use tauri::{AppHandle, Emitter, Manager};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::Command as TokioCommand;

#[derive(Debug, thiserror::Error)]
pub enum SidecarError {
    #[error("failed to start sidecar '{name}': {cause}")]
    Spawn {
        name: &'static str,
        cause: String,
    },
    #[error("sidecar '{name}' did not become responsive on port {port} within {timeout_ms} ms")]
    ReadinessTimeout {
        name: &'static str,
        port: u16,
        timeout_ms: u64,
    },
    #[error("could not find a free (core, worker) port pair after scanning {0} pairs from {1}")]
    NoFreePortPair(u16, u16),
    #[error("could not resolve binaries directory: {0}")]
    #[allow(dead_code)] // only constructed in release builds where resource_dir() is used
    PathResolution(String),
    #[error("sidecar binary not found at {0}")]
    BinaryNotFound(PathBuf),
}

#[derive(Debug, Clone, Serialize)]
pub struct ServiceStatusEvent {
    pub name: &'static str,
    pub state: &'static str,
    pub message: Option<String>,
}

/// Find a contiguous free (core, worker) port pair starting from DEFAULT_CORE_PORT.
fn find_free_port_pair() -> Option<(u16, u16)> {
    for k in 0..PORT_SCAN_PAIRS {
        let core = DEFAULT_CORE_PORT.saturating_add(k * 2);
        let worker = core.saturating_add(1);
        if port_available(core) && port_available(worker) {
            return Some((core, worker));
        }
    }
    None
}

fn port_available(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_ok()
}

/// Start both Python services as sidecars on a discovered free port pair.
pub async fn start_services(app: AppHandle) -> Result<ServicePorts, SidecarError> {
    let state: Arc<AppState> = app.state::<Arc<AppState>>().inner().clone();

    let (core_port, worker_port) = find_free_port_pair()
        .ok_or(SidecarError::NoFreePortPair(PORT_SCAN_PAIRS, DEFAULT_CORE_PORT))?;

    *state.ports.lock() = ServicePorts {
        core: core_port,
        worker: worker_port,
    };

    log::info!("allocated ports core={} worker={}", core_port, worker_port);

    spawn_service(&app, &state, SidecarKind::Core, core_port, worker_port)?;
    spawn_service(&app, &state, SidecarKind::Worker, core_port, worker_port)?;

    let core_ready = readiness::wait_until_ready(&app, "flowfile_core", core_port);
    let worker_ready = readiness::wait_until_ready(&app, "flowfile_worker", worker_port);
    let (core, worker) = tokio::join!(core_ready, worker_ready);
    core?;
    worker?;

    {
        let mut s = state.services_status.lock();
        s.status = "ready".into();
        s.error = None;
    }
    let _ = app.emit(
        "services-status",
        ServicesStatus {
            status: "ready".into(),
            error: None,
        },
    );

    Ok(ServicePorts {
        core: core_port,
        worker: worker_port,
    })
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum SidecarKind {
    Core,
    Worker,
}

impl SidecarKind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            SidecarKind::Core => "flowfile_core",
            SidecarKind::Worker => "flowfile_worker",
        }
    }

    fn args(self, core_port: u16, worker_port: u16) -> Vec<String> {
        match self {
            SidecarKind::Core => vec![
                "--host".into(),
                "127.0.0.1".into(),
                "--port".into(),
                core_port.to_string(),
                "--worker-port".into(),
                worker_port.to_string(),
            ],
            SidecarKind::Worker => vec![
                "--host".into(),
                "127.0.0.1".into(),
                "--port".into(),
                worker_port.to_string(),
                "--core-host".into(),
                "127.0.0.1".into(),
                "--core-port".into(),
                core_port.to_string(),
            ],
        }
    }
}

/// Resolve the `binaries/` directory at runtime.
///   - Dev (`debug_assertions`): the source tree's `src-tauri/binaries/`.
///   - Release: `<resource_dir>/binaries/` where Tauri bundled the resource.
fn binaries_dir(app: &AppHandle) -> Result<PathBuf, SidecarError> {
    #[cfg(debug_assertions)]
    {
        let _ = app;
        Ok(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("binaries"))
    }
    #[cfg(not(debug_assertions))]
    {
        app.path()
            .resource_dir()
            .map(|p| p.join("binaries"))
            .map_err(|e| SidecarError::PathResolution(e.to_string()))
    }
}

fn binary_path(app: &AppHandle, kind: SidecarKind) -> Result<PathBuf, SidecarError> {
    let dir = binaries_dir(app)?;
    let triple = env!("FLOWFILE_TARGET_TRIPLE");
    let ext = if cfg!(target_os = "windows") { ".exe" } else { "" };
    let path = dir.join(format!("{}-{}{}", kind.name(), triple, ext));
    if !path.exists() {
        return Err(SidecarError::BinaryNotFound(path));
    }
    Ok(path)
}

/// Ensure the executable bit is set on Unix. Tauri's `bundle.resources` copy
/// may strip it; without this the .app bundle would refuse to spawn the
/// sidecar with EACCES.
#[cfg(unix)]
fn ensure_executable(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(meta) = std::fs::metadata(path) {
        let mode = meta.permissions().mode();
        if mode & 0o111 == 0 {
            let mut perms = meta.permissions();
            perms.set_mode(mode | 0o755);
            if let Err(err) = std::fs::set_permissions(path, perms) {
                log::warn!("could not chmod +x {}: {}", path.display(), err);
            } else {
                log::info!("set executable bit on {}", path.display());
            }
        }
    }
}

#[cfg(not(unix))]
fn ensure_executable(_path: &std::path::Path) {}

pub(crate) fn spawn_service(
    app: &AppHandle,
    state: &Arc<AppState>,
    kind: SidecarKind,
    core_port: u16,
    worker_port: u16,
) -> Result<(), SidecarError> {
    let path = binary_path(app, kind)?;
    ensure_executable(&path);

    let env = build_child_env(core_port, worker_port);
    let name = kind.name();
    log::info!("spawning sidecar {} from {}", name, path.display());

    let mut cmd = TokioCommand::new(&path);
    cmd.args(kind.args(core_port, worker_port))
        .envs(env)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(false);

    // Put each sidecar in its own process group (pgid == this child's pid) so
    // shutdown can signal the whole subtree in one call. The worker forks
    // multiprocessing viz-session children that inherit this group, so the
    // group SIGTERM/SIGKILL in shutdown.rs reaps them too — a plain kill(pid)
    // on Unix would orphan those grandchildren. (Core's scheduled-flow runs use
    // start_new_session=True to escape into their own session, so they are
    // deliberately NOT in this group and survive app exit.)
    #[cfg(unix)]
    cmd.process_group(0);

    // The PyInstaller sidecars are console-subsystem binaries. Without this flag
    // Windows allocates a visible console window for each one when spawned from
    // the GUI shell. CREATE_NO_WINDOW suppresses those terminals; stdout/stderr
    // are still piped to pump_stream for logging. (creation_flags is an inherent
    // method on tokio's Command on Windows.)
    #[cfg(windows)]
    {
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }

    let mut child = cmd.spawn().map_err(|e| SidecarError::Spawn {
        name,
        cause: format!("{}: {}", path.display(), e),
    })?;

    let pid = child.id();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    match kind {
        SidecarKind::Core => {
            *state.core_pid.lock() = pid;
            state.core_restarts.lock().mark_started();
        }
        SidecarKind::Worker => {
            *state.worker_pid.lock() = pid;
            state.worker_restarts.lock().mark_started();
        }
    }

    if let Some(stdout) = stdout {
        tauri::async_runtime::spawn(pump_stream(stdout, name, /*is_stderr=*/ false));
    }
    if let Some(stderr) = stderr {
        tauri::async_runtime::spawn(pump_stream(stderr, name, /*is_stderr=*/ true));
    }

    // Wait task — when the process exits, fan out to the supervisor.
    let app_clone = app.clone();
    tauri::async_runtime::spawn(async move {
        match child.wait().await {
            Ok(status) => log::warn!(
                target: "sidecar",
                "[{}] terminated, status={:?}",
                name,
                status
            ),
            Err(err) => log::error!(target: "sidecar", "[{}] wait failed: {}", name, err),
        }
        handle_termination(&app_clone, kind, core_port, worker_port).await;
    });

    Ok(())
}

async fn pump_stream<R>(stream: R, name: &'static str, is_stderr: bool)
where
    R: AsyncRead + Unpin,
{
    let mut lines = BufReader::new(stream).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if is_stderr {
                    log::warn!(target: "sidecar", "[{} stderr] {}", name, line);
                } else {
                    log::info!(target: "sidecar", "[{} stdout] {}", name, line);
                }
            }
            Ok(None) => return,
            Err(err) => {
                log::error!(
                    target: "sidecar",
                    "[{} {}] read error: {}",
                    name,
                    if is_stderr { "stderr" } else { "stdout" },
                    err
                );
                return;
            }
        }
    }
}

/// Called when a sidecar exits. Decides whether to restart based on:
///   - is_shutting_down (no)
///   - restart budget (capped via RestartCounter)
async fn handle_termination(
    app: &AppHandle,
    kind: SidecarKind,
    core_port: u16,
    worker_port: u16,
) {
    let state: Arc<AppState> = app.state::<Arc<AppState>>().inner().clone();

    if *state.is_shutting_down.lock() {
        log::debug!("{} terminated during shutdown, not restarting", kind.name());
        // TODO(C): this early return leaves the stale pid in state, so
        // shutdown.rs's `.take()` + killpg may target a process/group whose PID
        // has been recycled. Clear the pid here too (as the restart path does
        // just below) so shutdown skips the kill entirely once the process is
        // known dead.
        return;
    }

    // Clear the pid so a stale SIGTERM during shutdown can't target a recycled PID.
    match kind {
        SidecarKind::Core => *state.core_pid.lock() = None,
        SidecarKind::Worker => *state.worker_pid.lock() = None,
    }

    let backoff = match kind {
        SidecarKind::Core => state.core_restarts.lock().next_backoff(),
        SidecarKind::Worker => state.worker_restarts.lock().next_backoff(),
    };

    let Some(backoff) = backoff else {
        let msg = format!(
            "{} crashed {} times within {}s — giving up. Restart Flowfile to recover.",
            kind.name(),
            crate::state::MAX_RESTARTS,
            crate::state::RESTART_WINDOW_SECS
        );
        log::error!("{}", msg);
        {
            let mut s = state.services_status.lock();
            s.status = "error".into();
            s.error = Some(msg.clone());
        }
        let _ = app.emit(
            "services-status",
            ServicesStatus {
                status: "error".into(),
                error: Some(msg),
            },
        );
        return;
    };

    log::warn!(
        "{} died — restarting in {} ms",
        kind.name(),
        backoff.as_millis(),
    );
    let _ = app.emit(
        "services-status",
        ServiceStatusEvent {
            name: kind.name(),
            state: "restarting",
            message: Some(format!("Restart scheduled in {} ms", backoff.as_millis())),
        },
    );

    tokio::time::sleep(backoff).await;

    let port = match kind {
        SidecarKind::Core => core_port,
        SidecarKind::Worker => worker_port,
    };

    // Re-check is_shutting_down and respawn while *holding* the lock, closing the
    // restart↔shutdown race. shutdown_all/kill_spawned flip the flag to true under
    // this same lock and only `.take()` the pid afterwards, so holding the lock
    // across the spawn serializes the two: either we see the flag already set and
    // bail, or we win the lock and store the new pid (inside spawn_service) before
    // releasing — so shutdown, which can't set the flag until we release, then
    // observes that pid when it takes it, and kills the respawn. No orphan in the
    // gap. spawn_service is synchronous, so holding the parking_lot guard across it
    // is safe; we must NOT hold it across the async readiness wait, hence the guard
    // is scoped to this block.
    let respawn = {
        let shutting_down = state.is_shutting_down.lock();
        if *shutting_down {
            return;
        }
        spawn_service(app, &state, kind, core_port, worker_port)
    };

    if let Err(err) = respawn {
        log::error!("failed to respawn {}: {}", kind.name(), err);
        let _ = app.emit(
            "services-status",
            ServiceStatusEvent {
                name: kind.name(),
                state: "error",
                message: Some(err.to_string()),
            },
        );
        return;
    }

    if let Err(err) = readiness::wait_until_ready(app, kind.name(), port).await {
        log::error!("respawned {} did not become ready: {}", kind.name(), err);
    } else {
        log::info!("{} restart successful", kind.name());
    }
}
