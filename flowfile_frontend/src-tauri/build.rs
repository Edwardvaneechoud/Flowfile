fn main() {
    tauri_build::build();

    // Make the host target triple available at compile time so the sidecar
    // path resolver can build `binaries/<name>-<triple>` without runtime
    // detection. Cargo sets `TARGET` for build scripts (e.g. `aarch64-apple-darwin`).
    let target = std::env::var("TARGET").unwrap_or_default();
    println!("cargo:rustc-env=FLOWFILE_TARGET_TRIPLE={target}");
}
