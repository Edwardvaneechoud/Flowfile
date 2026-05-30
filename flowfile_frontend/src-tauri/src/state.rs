use parking_lot::Mutex;
use serde::Serialize;
use std::sync::Arc;

/// Starting port for the core/worker pair. The shell scans upward from here
/// in pairs (core, worker) until a free pair is found.
pub const DEFAULT_CORE_PORT: u16 = 63578;
pub const DEFAULT_WORKER_PORT: u16 = 63579;

/// How many pairs we try before giving up.
pub const PORT_SCAN_PAIRS: u16 = 100;

pub const HEALTH_CHECK_TIMEOUT_MS: u64 = 1000;
// Onedir bundles boot in ~2s on warm caches, but the first launch after a
// reboot (or on a slower machine, or with antivirus scanning the binary) can
// easily take 30–60s for the Python interpreter + every router import. We
// give it 2 minutes — the loading window shows live progress so the user
// can see we're still alive even when it's slow.
pub const SERVICE_START_TIMEOUT_MS: u64 = 120_000;
pub const SHUTDOWN_TIMEOUT_MS: u64 = 3_000;
pub const FORCE_KILL_TIMEOUT_MS: u64 = 2_000;

/// After SIGTERM, how long to wait for a graceful exit before escalating to
/// SIGKILL, polled every KILL_POLL_INTERVAL_MS. Sized to cover a typical worker
/// viz-session drain (flowfile_worker/viz_sessions.py uses SHUTDOWN_GRACE_SECONDS
/// = 10 internally) without a long UI hang on quit. The process-group SIGKILL in
/// shutdown.rs is the backstop, so escalating early still leaks nothing — the
/// grace only governs *graceful* vs forced teardown.
pub const SIGTERM_GRACE_MS: u64 = 5_000;
pub const KILL_POLL_INTERVAL_MS: u64 = 100;

/// Supervisor: how many restarts before we give up and mark the service dead.
/// The window resets after RESTART_WINDOW_SECS of continuous uptime, so a
/// service that crashes once per hour will keep being restarted forever.
pub const MAX_RESTARTS: u8 = 5;
pub const RESTART_WINDOW_SECS: u64 = 60;
pub const RESTART_BACKOFF_MS: &[u64] = &[500, 1_000, 2_000, 4_000, 8_000];

#[derive(Debug, Clone, Serialize)]
pub struct ServicesStatus {
    pub status: String,
    pub error: Option<String>,
}

impl Default for ServicesStatus {
    fn default() -> Self {
        Self {
            status: "not_started".into(),
            error: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ServicePorts {
    pub core: u16,
    pub worker: u16,
}

impl Default for ServicePorts {
    fn default() -> Self {
        Self {
            core: DEFAULT_CORE_PORT,
            worker: DEFAULT_WORKER_PORT,
        }
    }
}

#[derive(Debug, Default)]
pub struct RestartCounter {
    pub attempts: u8,
    pub last_started: Option<std::time::Instant>,
}

impl RestartCounter {
    /// Returns the backoff to wait before the next attempt, or `None` if we've
    /// exhausted retries. Records the attempt (and resets the counter if the
    /// previous run survived RESTART_WINDOW_SECS).
    pub fn next_backoff(&mut self) -> Option<std::time::Duration> {
        if let Some(started) = self.last_started {
            if started.elapsed().as_secs() >= RESTART_WINDOW_SECS {
                self.attempts = 0;
            }
        }
        if self.attempts >= MAX_RESTARTS {
            return None;
        }
        let idx = self.attempts.min((RESTART_BACKOFF_MS.len() - 1) as u8) as usize;
        let backoff = RESTART_BACKOFF_MS[idx];
        self.attempts += 1;
        Some(std::time::Duration::from_millis(backoff))
    }

    /// Call when a fresh process has been spawned (resets the clock used to
    /// detect "service ran fine for a while" recovery).
    pub fn mark_started(&mut self) {
        self.last_started = Some(std::time::Instant::now());
    }
}

#[derive(Default)]
pub struct AppState {
    /// PID of the running flowfile_core sidecar (`None` while not running).
    /// We track PIDs instead of tokio::process::Child because the Child is
    /// moved into the wait task that drives the supervisor — we still need a
    /// handle for sending SIGTERM during shutdown, and PIDs serve fine for
    /// that on both Unix (via `nix::sys::signal::kill`) and Windows
    /// (via `taskkill /T /PID`).
    pub core_pid: Mutex<Option<u32>>,
    pub worker_pid: Mutex<Option<u32>>,
    pub services_status: Mutex<ServicesStatus>,
    pub is_shutting_down: Mutex<bool>,
    /// The ports actually bound by the running sidecars (may differ from the
    /// defaults if another instance owned them at startup).
    pub ports: Mutex<ServicePorts>,
    pub core_restarts: Mutex<RestartCounter>,
    pub worker_restarts: Mutex<RestartCounter>,
}

// Re-export commonly used handle alias. Suppress the unused warning — we use
// `Arc<AppState>` directly via `app.state::<Arc<AppState>>()` from invoke
// handlers, but the alias is useful documentation.
#[allow(dead_code)]
pub type SharedState = Arc<AppState>;
