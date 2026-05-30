use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;

/// Build the environment block injected into the Python sidecars.
/// Mirrors `flowfile_frontend/src/main/services.ts::getProcessEnv()`,
/// plus exports the resolved ports so the Python configs pick them up via
/// `FLOWFILE_WORKER_PORT` / `CORE_HOST` / `CORE_PORT`.
pub fn build_child_env(core_port: u16, worker_port: u16) -> HashMap<String, String> {
    let mut env_vars: HashMap<String, String> = env::vars().collect();

    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("/"));
    let tmp = env::temp_dir();
    let flowfile_storage_dir = home.join(".flowfile");

    let required_dirs = [
        flowfile_storage_dir.clone(),
        flowfile_storage_dir.join("cache"),
        flowfile_storage_dir.join("temp"),
        flowfile_storage_dir.join("logs"),
        flowfile_storage_dir.join("system_logs"),
        flowfile_storage_dir.join("flows"),
        flowfile_storage_dir.join("database"),
    ];

    for dir in &required_dirs {
        if let Err(err) = fs::create_dir_all(dir) {
            log::warn!(
                "failed to create flowfile storage dir {}: {}",
                dir.display(),
                err
            );
        }
    }

    env_vars.insert("HOME".into(), home.to_string_lossy().into_owned());
    env_vars.insert("TMPDIR".into(), tmp.to_string_lossy().into_owned());
    env_vars.insert(
        "DOCKER_CONFIG".into(),
        home.join(".docker").to_string_lossy().into_owned(),
    );
    env_vars.insert(
        "FLOWFILE_STORAGE_DIR".into(),
        flowfile_storage_dir.to_string_lossy().into_owned(),
    );
    // The backend has hard-coded checks for `FLOWFILE_MODE == "electron"`
    // across auth/jwt/secrets to enable single-user desktop behavior
    // (auto-issue tokens, skip password validation, load master key from disk).
    // Keep the canonical value "electron" here — the renderer already treats
    // "electron" | "tauri" | "desktop" as synonyms for "desktop mode".
    env_vars.insert("FLOWFILE_MODE".into(), "electron".into());

    // Tell the sidecars who their supervisor is so their parent-death watcher
    // (shared/parent_watcher.py) can detect a crashed/SIGKILLed shell and exit
    // on its own. Presence of this var is also what enables the watcher, so it
    // never fires for standalone/CLI/Docker runs that don't get it.
    env_vars.insert(
        "FLOWFILE_SUPERVISOR_PID".into(),
        std::process::id().to_string(),
    );

    // Mirror the discovered ports into env vars so the Python configs that
    // read them at import time also see the right values (CLI args also pass
    // them, but `flowfile_worker.configs` consults env vars in spawned-child
    // multiprocessing workers where argparse won't run).
    env_vars.insert("FLOWFILE_WORKER_PORT".into(), worker_port.to_string());
    env_vars.insert("CORE_PORT".into(), core_port.to_string());
    env_vars.insert("CORE_HOST".into(), "127.0.0.1".into());
    // Without this, core's settings.py builds the worker client URL with
    // host="0.0.0.0" on Unix, which is fragile as a connect target.
    env_vars.insert("WORKER_HOST".into(), "127.0.0.1".into());

    #[cfg(target_os = "windows")]
    {
        env_vars.insert(
            "DOCKER_HOST".into(),
            r"npipe:////./pipe/docker_engine".into(),
        );
    }

    #[cfg(not(target_os = "windows"))]
    {
        env_vars.insert("DOCKER_HOST".into(), "unix:///var/run/docker.sock".into());
        let existing_path = env_vars.get("PATH").cloned().unwrap_or_default();
        env_vars.insert(
            "PATH".into(),
            format!(
                "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:{}",
                existing_path
            ),
        );
    }

    env_vars
}
