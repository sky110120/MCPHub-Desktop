/// Bundled runtime environment management.
///
/// Resolves commands like `node`, `npx`, `uv`, `uvx`, `python` to the
/// bundled binaries shipped with the app, so users don't need Node.js or
/// Python installed on their system.
use std::path::{Path, PathBuf};
use std::sync::{OnceLock, RwLock};

static RUNTIMES_DIR: OnceLock<PathBuf> = OnceLock::new();

/// Active Node.js version: "system" or a version string like "22.14.0"
static ACTIVE_NODE: OnceLock<RwLock<String>> = OnceLock::new();
/// Active Python version: "system" or a version string like "3.12"
static ACTIVE_PYTHON: OnceLock<RwLock<String>> = OnceLock::new();

/// Cached enhanced PATH from user's login shell (lazily initialized).
/// GUI apps on macOS/Linux don't inherit the shell PATH, so we need to
/// source the user's shell profile to get tools like nvm, asdf, pyenv, etc.
static ENHANCED_PATH: OnceLock<String> = OnceLock::new();

#[cfg(not(target_os = "windows"))]
const MAX_CAPTURED_OUTPUT_BYTES: usize = 8 * 1024 * 1024;

/// Initialize with the resolved runtimes directory.
/// Call once on app startup before spawning any MCP servers.
pub fn init(runtimes: PathBuf) {
    if !runtimes.exists() {
        log::warn!("[runtime_env] Runtimes directory not found: {:?}", runtimes);
        return;
    }

    // Ensure node and uv binaries are executable (permissions may be lost on some systems)
    #[cfg(unix)]
    ensure_executable(&runtimes);

    let _ = RUNTIMES_DIR.set(runtimes.clone());
    log::info!("[runtime_env] Initialized: {:?}", runtimes);

    // Pre-cache the enhanced PATH from user's shell
    let enhanced = get_enhanced_path();
    let _ = ENHANCED_PATH.set(enhanced);
}

#[cfg(unix)]
fn ensure_executable(runtimes: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let candidates = [
        node_bin_path(runtimes),
        uv_bin_path(runtimes),
        runtimes.join("uv").join("uvx"),
    ];
    for path in &candidates {
        if path.exists() {
            if let Ok(meta) = std::fs::metadata(path) {
                let mut perms = meta.permissions();
                perms.set_mode(0o755);
                let _ = std::fs::set_permissions(path, perms);
            }
        }
    }
}

fn runtimes_dir() -> Option<&'static PathBuf> {
    RUNTIMES_DIR.get()
}

/// Get the enhanced PATH from user's login shell.
/// GUI apps on macOS/Linux don't inherit the shell PATH, so we need to
/// execute the user's shell to get the full PATH with tools like nvm, asdf, etc.
/// Cached login-shell PATH (populated by `init`). Consumers (runtime
/// settings page) must reuse this instead of re-running the 5s shell probe
/// on every call — a hung rc file would stall an async worker each time.
pub fn cached_enhanced_path() -> Option<String> {
    ENHANCED_PATH.get().cloned()
}

/// Cache-first PATH for synchronous subprocess helpers: falls back to the raw
/// process PATH when the login-shell probe hasn't populated the cache yet, so
/// sync callers never need to block_on the shell probe (which would panic on
/// a tokio worker) — review round 8, 2026-10-04.
pub fn cached_enhanced_path_or_env() -> String {
    ENHANCED_PATH.get().cloned().unwrap_or_else(|| std::env::var("PATH").unwrap_or_default())
}

pub(crate) fn get_enhanced_path() -> String {
    log::info!("[runtime_env] Getting enhanced PATH from user's login shell");

    let path = {
        #[cfg(target_os = "windows")]
        { get_windows_path() }

        #[cfg(not(target_os = "windows"))]
        { get_unix_path() }
    };

    let display_path = if path.len() > 300 {
        // Byte-safe truncation: slicing at a fixed byte offset panics when it
        // lands mid-UTF-8 (paths with non-ASCII user directories are common).
        let cut = path.char_indices().map(|(i, _)| i).take_while(|&i| i <= 300).last().unwrap_or(0);
        format!("{}...", &path[..cut])
    } else {
        path.clone()
    };
    log::info!("[runtime_env] Enhanced PATH: {}", display_path);

    path
}

/// Run a command with a wall-clock timeout, returning `None` on timeout or
/// spawn failure. Synchronous `Command::output()` has no timeout — an
/// interactive login shell (`-l -i`) can hang forever on user config that
/// prompts (keychain agents, password prompts), which would freeze app
/// startup (`runtime_env::init`) and the runtime settings page alike.
#[cfg(not(target_os = "windows"))]
pub(crate) fn run_with_timeout(
    mut cmd: std::process::Command,
    timeout: std::time::Duration,
) -> Option<std::process::Output> {
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;

    let deadline = std::time::Instant::now() + timeout;
    // Isolate this command so timeout cleanup never signals the app's group.
    cmd.stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .process_group(0);
    let mut child = cmd.spawn().ok()?;
    let process_group = child.id() as libc::pid_t;
    let (Some(mut stdout), Some(mut stderr)) = (child.stdout.take(), child.stderr.take()) else {
        kill_process_group(process_group);
        let _ = child.kill();
        let _ = child.wait();
        return None;
    };

    // A bounded poll loop drains both streams without reader threads that can
    // remain blocked when a shell descendant inherits a pipe.
    for fd in [stdout.as_raw_fd(), stderr.as_raw_fd()] {
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags == -1 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1
        {
            kill_process_group(process_group);
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
    }

    let mut status = None;
    let mut stdout_open = true;
    let mut stderr_open = true;
    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();

    loop {
        // Decide timeout before observing child status. Otherwise an exit
        // first noticed after the deadline can be accepted by the completed
        // child + closed pipes fast path below.
        if !observed_before_deadline(deadline, std::time::Instant::now()) {
            kill_process_group(process_group);
            if let Some(exit_status) = status {
                return Some(std::process::Output {
                    status: exit_status,
                    stdout: stdout_bytes,
                    stderr: stderr_bytes,
                });
            }
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }

        if status.is_none() {
            match child.try_wait() {
                Ok(Some(exit_status)) => {
                    if !observed_before_deadline(deadline, std::time::Instant::now()) {
                        kill_process_group(process_group);
                        let _ = child.wait();
                        return None;
                    }
                    status = Some(exit_status);
                    // The direct child has finished. Reap any descendants
                    // even when they redirected both captured streams.
                    kill_process_group(process_group);
                }
                Ok(None) => {}
                Err(_) => {
                    kill_process_group(process_group);
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
            }
        }

        if status.is_some() && !stdout_open && !stderr_open {
            break;
        }

        let now = std::time::Instant::now();
        if now >= deadline {
            kill_process_group(process_group);
            if let Some(exit_status) = status {
                return Some(std::process::Output {
                    status: exit_status,
                    stdout: stdout_bytes,
                    stderr: stderr_bytes,
                });
            }
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }

        let mut poll_fds = [
            libc::pollfd {
                fd: if stdout_open { stdout.as_raw_fd() } else { -1 },
                events: libc::POLLIN | libc::POLLHUP | libc::POLLERR,
                revents: 0,
            },
            libc::pollfd {
                fd: if stderr_open { stderr.as_raw_fd() } else { -1 },
                events: libc::POLLIN | libc::POLLHUP | libc::POLLERR,
                revents: 0,
            },
        ];
        let remaining = deadline.saturating_duration_since(now);
        let poll_timeout = remaining.as_millis().clamp(1, 20) as libc::c_int;
        let poll_result =
            unsafe { libc::poll(poll_fds.as_mut_ptr(), poll_fds.len() as _, poll_timeout) };
        if poll_result < 0 {
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            kill_process_group(process_group);
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }

        if stdout_open && (poll_fds[0].revents != 0 || poll_result == 0) {
            match drain_pipe(&mut stdout, &mut stdout_bytes, 64 * 1024) {
                Ok(open) => stdout_open = open,
                Err(()) => {
                    kill_process_group(process_group);
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
            }
        }
        if stderr_open && (poll_fds[1].revents != 0 || poll_result == 0) {
            match drain_pipe(&mut stderr, &mut stderr_bytes, 64 * 1024) {
                Ok(open) => stderr_open = open,
                Err(()) => {
                    kill_process_group(process_group);
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
            }
        }
    }

    Some(std::process::Output {
        status: status?,
        stdout: stdout_bytes,
        stderr: stderr_bytes,
    })
}

#[cfg(not(target_os = "windows"))]
fn kill_process_group(process_group: libc::pid_t) {
    if process_group > 0 {
        unsafe {
            libc::kill(-process_group, libc::SIGKILL);
        }
    }
}

#[cfg(not(target_os = "windows"))]
fn observed_before_deadline(deadline: std::time::Instant, observed_at: std::time::Instant) -> bool {
    observed_at < deadline
}

#[cfg(not(target_os = "windows"))]
fn drain_pipe(
    pipe: &mut impl std::io::Read,
    output: &mut Vec<u8>,
    byte_budget: usize,
) -> Result<bool, ()> {
    let mut buffer = [0; 8192];
    let mut drained = 0;
    loop {
        let remaining_output = MAX_CAPTURED_OUTPUT_BYTES.saturating_sub(output.len());
        let limit = buffer
            .len()
            .min(byte_budget.saturating_sub(drained))
            .min(remaining_output.saturating_add(1));
        if limit == 0 {
            return Ok(true);
        }
        match pipe.read(&mut buffer[..limit]) {
            Ok(0) => return Ok(false),
            Ok(read) => {
                if read > remaining_output {
                    return Err(());
                }
                output.extend_from_slice(&buffer[..read]);
                drained += read;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(true),
            Err(_) => return Ok(false),
        }
    }
}

/// Get PATH from user's login shell on Unix (macOS/Linux)
#[cfg(not(target_os = "windows"))]
fn get_unix_path() -> String {
    if let Some(shell_path) = get_user_shell() {
        log::info!("[runtime_env] Detected user shell: {:?}", shell_path);

        // Execute shell with -l (login) and -i (interactive) flags
        // to load .bash_profile/.zprofile and .bashrc/.zshrc
        // Bounded by a timeout: interactive shells can hang on user config
        // (agents prompting for input), which would block startup forever.
        if let Some(output) = run_with_timeout(
            {
                let mut c = std::process::Command::new(&shell_path);
                c.arg("-l").arg("-i").arg("-c").arg("echo $PATH");
                c
            },
            std::time::Duration::from_secs(5),
        ) {
            if output.status.success() {
                // User rc files often echo extra lines (nvm hints, banners).
                // A PATH value must contain ':' separators — take the last
                // line that looks like a PATH instead of trusting all stdout.
                // Guards: >= 2 separators (a minimal legitimate PATH like
                // "/usr/local/bin:/usr/bin:/bin" has 2) and every segment
                // starts with '/'. NOTE: whitespace is NOT a rejection
                // signal — macOS PATH entries legitimately contain spaces
                // ("/Applications/VMware Fusion.app/Contents/Public");
                // rejecting any line with a space silently discarded such
                // PATHs and broke managed spawns for users with those apps.
                let path = String::from_utf8_lossy(&output.stdout);
                let looks_like_path = |l: &str| {
                    l.matches(':').count() >= 2
                        && !l.contains('=')
                        && l.split(':').filter(|s| !s.is_empty()).all(|s| s.starts_with('/'))
                };
                let path = path
                    .lines()
                    .filter(|l| looks_like_path(l))
                    .last()
                    .map(|l| l.trim().to_string())
                    .unwrap_or_default();
                if !path.is_empty() {
                    log::info!("[runtime_env] Successfully got PATH from shell (length: {})", path.len());
                    return path;
                }
            }
        } else {
            log::warn!("[runtime_env] Shell PATH probe timed out after 5s, using fallback");
        }
    }

    // Fallback: return current PATH
    let fallback_path = std::env::var("PATH").unwrap_or_default();
    log::info!("[runtime_env] Using fallback PATH (length: {})", fallback_path.len());
    fallback_path
}

/// Get the user's default shell from SHELL env var or common locations
#[cfg(not(target_os = "windows"))]
fn get_user_shell() -> Option<PathBuf> {
    // Try SHELL environment variable first
    if let Ok(shell) = std::env::var("SHELL") {
        let path = PathBuf::from(&shell);
        if path.exists() {
            return Some(path);
        }
    }

    // Fallback: common shell locations
    #[cfg(target_os = "macos")]
    let shells = ["/bin/zsh", "/bin/bash", "/usr/local/bin/fish"];

    #[cfg(target_os = "linux")]
    let shells = ["/bin/bash", "/bin/sh", "/usr/bin/zsh", "/usr/bin/fish"];

    for shell in &shells {
        let path = PathBuf::from(shell);
        if path.exists() {
            return Some(path);
        }
    }

    None
}

/// Get PATH on Windows by reading the registry directly (fast, no PowerShell).
#[cfg(target_os = "windows")]
fn get_windows_path() -> String {
    use winreg::enums::*;
    use winreg::RegKey;

    // Read User PATH from registry — instant, no subprocess needed.
    let user_path = RegKey::predef(HKEY_CURRENT_USER)
        .open_subkey("Environment")
        .and_then(|k| k.get_value::<String, _>("Path"))
        .unwrap_or_default();

    let machine_path = RegKey::predef(HKEY_LOCAL_MACHINE)
        .open_subkey(r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment")
        .and_then(|k| k.get_value::<String, _>("Path"))
        .unwrap_or_default();

    let current_path = std::env::var("PATH").unwrap_or_default();

    // Merge: user PATH + machine PATH + process PATH
    let mut parts: Vec<&str> = Vec::new();
    if !user_path.is_empty() {
        parts.push(&user_path);
    }
    if !machine_path.is_empty() {
        parts.push(&machine_path);
    }
    if !current_path.is_empty() {
        parts.push(&current_path);
    }
    let combined = parts.join(";");

    // Expand %VAR% references using process environment.
    // Registry stores paths like %USERPROFILE%\AppData\Local\... which need expansion.
    expand_env_vars(&combined)
}

/// Expand %VAR% references in a string using the current process environment.
/// On Windows, environment variables are case-insensitive, so we match case-insensitively.
#[cfg(target_os = "windows")]
fn expand_env_vars(input: &str) -> String {
    let mut result = input.to_string();
    // Collect all env vars into a vec for case-insensitive matching
    let env_vars: Vec<(String, String)> = std::env::vars().collect();

    // Find all %VAR% patterns in the input and replace them
    let mut start = 0;
    while let Some(open_pos) = result[start..].find('%') {
        let abs_open = start + open_pos;
        if let Some(close_pos) = result[abs_open + 1..].find('%') {
            let abs_close = abs_open + 1 + close_pos;
            let var_name = &result[abs_open + 1..abs_close];
            // Case-insensitive lookup
            let replacement = env_vars.iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(var_name))
                .map(|(_, v)| v.clone());
            if let Some(value) = replacement {
                result = format!("{}{}{}", &result[..abs_open], value, &result[abs_close + 1..]);
                start = abs_open + value.len();
            } else {
                start = abs_close + 1;
            }
        } else {
            break;
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Active version management
// ---------------------------------------------------------------------------

/// Set the active Node.js version ("system" or a version string like "22.14.0").
pub fn set_active_node(version: String) {
    let rw = ACTIVE_NODE.get_or_init(|| RwLock::new("system".to_string()));
    if let Ok(mut v) = rw.write() {
        *v = version;
    }
}

/// Get the currently active Node.js version ("system" by default).
pub fn get_active_node() -> String {
    ACTIVE_NODE
        .get()
        .and_then(|rw| rw.read().ok().map(|v| v.clone()))
        .unwrap_or_else(|| "system".to_string())
}

/// Set the active Python version ("system" or a version string like "3.12").
pub fn set_active_python(version: String) {
    let rw = ACTIVE_PYTHON.get_or_init(|| RwLock::new("system".to_string()));
    if let Ok(mut v) = rw.write() {
        *v = version;
    }
}

/// Get the currently active Python version ("system" by default).
pub fn get_active_python() -> String {
    ACTIVE_PYTHON
        .get()
        .and_then(|rw| rw.read().ok().map(|v| v.clone()))
        .unwrap_or_else(|| "system".to_string())
}

// ---------------------------------------------------------------------------
// Public path helpers used by runtime commands
// ---------------------------------------------------------------------------

/// Returns the base directory where user-managed Node.js versions are installed.
/// e.g. ~/Library/Application Support/mcphub-desktop/node-versions/ (macOS)
pub fn node_versions_base() -> Option<PathBuf> {
    app_data_dir("node-versions")
}

/// Returns the path to the bundled `uv` binary, if available.
pub fn get_uv_path() -> Option<PathBuf> {
    let rt = runtimes_dir()?;
    let p = uv_bin_path(rt);
    if p.exists() { Some(p) } else { None }
}

/// Returns the path to the bundled Node.js binary in the resource directory.
/// This is the read-only copy shipped with the app.
pub fn get_bundled_node_path() -> Option<PathBuf> {
    let rt = runtimes_dir()?;
    let p = node_bin_path(rt);
    if p.exists() { Some(p) } else { None }
}

/// Returns the path to the bundled Python directory in the resource directory.
/// e.g. <resources>/runtimes/uv/python/
/// This is the read-only copy shipped with the app.
pub fn get_bundled_python_dir() -> Option<PathBuf> {
    let rt = runtimes_dir()?;
    let dir = uv_dir(rt).join("python");
    if dir.exists() { Some(dir) } else { None }
}

/// Returns the writable directory where the app installs Python versions at runtime.
/// e.g. ~/Library/Application Support/mcphub-desktop/python/ (macOS)
/// Distinct from the bundled resource directory which may be read-only.
pub fn uv_python_install_dir() -> Option<PathBuf> {
    app_data_dir("python")
}

// ---------------------------------------------------------------------------
// Command resolution
// ---------------------------------------------------------------------------

/// Strip the `\\?\` extended-length path prefix on Windows.
/// `PathBuf::to_string_lossy()` on Windows may produce `\\?\C:\...` for paths
/// under the app's resource directory, but child processes (node, npx, etc.)
/// don't understand this prefix when used as arguments.
#[cfg(target_os = "windows")]
fn normalize_path(p: &Path) -> String {
    let s = p.to_string_lossy();
    s.strip_prefix(r"\\?\").unwrap_or(&s).to_string()
}

#[cfg(not(target_os = "windows"))]
fn normalize_path(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

/// Resolve a command + args, remapping known runtimes to bundled binaries.
///
/// Returns `(resolved_command, resolved_args)`.
pub fn resolve_command(command: &str, args: &[String]) -> (String, Vec<String>) {
    match command {
        "node" | "npx" | "npm" => {
            let active = get_active_node();
            if active != "system" {
                // Specific version → look in user-managed node-versions directory
                if let Some(base) = node_versions_base() {
                    let ver_dir = base.join(&active);
                    if ver_dir.exists() {
                        let resolved = resolve_node_command_in_dir(command, args, &ver_dir);
                        if resolved.is_some() {
                            return resolved.unwrap();
                        }
                    }
                }
            }
            // "system" or version dir not found → fall through to bundled runtime
        }
        "python" | "python3" => {
            let active = get_active_python();
            if active != "system" {
                // For a specific python version, fall through to bundled resolution below
            }
            // "system" → fall through to bundled runtime (don't return bare name yet)
        }
        _ => {}
    }

    let Some(rt) = runtimes_dir() else {
        return (command.to_string(), args.to_vec());
    };

    match command {
        "node" => {
            let bin = node_bin_path(rt);
            if bin.exists() {
                return (normalize_path(&bin), args.to_vec());
            }
        }
        "npx" => {
            // Call `node <npx-cli.js> [args]` directly — avoids shell script issues
            // when binaries are extracted from a bundle.
            let node = node_bin_path(rt);
            let cli = npx_cli_path(rt);
            if node.exists() && cli.exists() {
                let mut new_args = vec![normalize_path(&cli)];
                new_args.extend_from_slice(args);
                return (normalize_path(&node), new_args);
            }
            // Fallback: use npx.cmd directly (Windows batch wrapper)
            #[cfg(target_os = "windows")]
            {
                let npx_cmd = node_bin_dir(rt).join("npx.cmd");
                if npx_cmd.exists() {
                    return (normalize_path(&npx_cmd), args.to_vec());
                }
            }
        }
        "npm" => {
            let node = node_bin_path(rt);
            let cli = npm_cli_path(rt);
            if node.exists() && cli.exists() {
                let mut new_args = vec![normalize_path(&cli)];
                new_args.extend_from_slice(args);
                return (normalize_path(&node), new_args);
            }
            // Fallback: use npm.cmd directly (Windows batch wrapper)
            #[cfg(target_os = "windows")]
            {
                let npm_cmd = node_bin_dir(rt).join("npm.cmd");
                if npm_cmd.exists() {
                    return (normalize_path(&npm_cmd), args.to_vec());
                }
            }
        }
        "uvx" => {
            // Try dedicated uvx binary first; fall back to `uv tool run`
            let uvx = uv_dir(rt).join(uvx_exe());
            if uvx.exists() {
                return (normalize_path(&uvx), args.to_vec());
            }
            let uv = uv_bin_path(rt);
            if uv.exists() {
                let mut new_args = vec!["tool".to_string(), "run".to_string()];
                new_args.extend_from_slice(args);
                return (normalize_path(&uv), new_args);
            }
        }
        "uv" => {
            let bin = uv_bin_path(rt);
            if bin.exists() {
                return (normalize_path(&bin), args.to_vec());
            }
        }
        "python" | "python3" => {
            // Prefer a managed Python inside the bundled uv python dir
            if let Some(py) = find_bundled_python(rt) {
                return (normalize_path(&py), args.to_vec());
            }
            // Fallback: use `uv run python`
            let uv = uv_bin_path(rt);
            if uv.exists() {
                let mut new_args = vec!["run".to_string(), "python".to_string()];
                new_args.extend_from_slice(args);
                return (normalize_path(&uv), new_args);
            }
        }
        _ => {}
    }

    (command.to_string(), args.to_vec())
}

/// Returns extra environment variables to inject when spawning `command`.
///
/// Prepends bundled binary directories to `PATH` and sets runtime-specific
/// cache/install dirs so everything stays self-contained within the app's
/// data directory.
pub fn env_overrides(original_command: &str, server_name: &str) -> Vec<(String, String)> {
    let sep = if cfg!(target_os = "windows") { ";" } else { ":" };
    // Use cached enhanced PATH from user's shell instead of process PATH
    // GUI apps on macOS/Linux don't inherit the shell PATH
    let existing_path = ENHANCED_PATH
        .get()
        .cloned()
        .unwrap_or_else(|| std::env::var("PATH").unwrap_or_default());

    // If runtimes directory is not available, still return the enhanced PATH
    // so that commands installed via homebrew, nvm, etc. can be found
    let Some(rt) = runtimes_dir() else {
        return vec![("PATH".to_string(), existing_path)];
    };

    let mut prepend_dirs: Vec<String> = vec![];

    match original_command {
        "node" | "npx" | "npm" => {
            let active = get_active_node();
            // Always prepend bundled node to PATH so child processes spawned by
            // a GUI app (which may lack the user's shell PATH) can find npx/npm.
            prepend_dirs.push(normalize_path(&node_bin_dir(rt)));
            if active != "system" {
                // User-managed version takes priority over bundled
                if let Some(base) = node_versions_base() {
                    let ver_dir = base.join(&active);
                    if ver_dir.exists() {
                        let bin_dir = node_bin_dir_in(&ver_dir);
                        prepend_dirs.insert(0, normalize_path(&bin_dir));
                    }
                }
            }
        }
        "uv" | "uvx" | "python" | "python3" => {
            prepend_dirs.push(normalize_path(&uv_dir(rt)));
        }
        _ => {
            // For unknown commands, merge enhanced PATH with current process PATH
            // so that commands installed after app startup (e.g. codegraph) can be found
            let current_path = std::env::var("PATH").unwrap_or_default();
            let merged = if current_path.is_empty() {
                existing_path
            } else if existing_path.is_empty() {
                current_path
            } else {
                format!("{}{}{}", current_path, sep, existing_path)
            };
            return vec![("PATH".to_string(), merged)];
        },
    }

    let new_path = format!("{}{}{}", prepend_dirs.join(sep), sep, existing_path);
    let mut env: Vec<(String, String)> = vec![("PATH".to_string(), new_path)];

    // Point npm cache to server-specific directory to avoid file lock conflicts
    // when multiple npx servers start simultaneously.
    if matches!(original_command, "node" | "npx" | "npm") {
        let cache_name = format!("npm-cache-{}", server_name);
        if let Some(cache) = app_local_dir(&cache_name) {
            log::info!("[runtime_env] npm_config_cache (server '{}'): {}", server_name, cache);
            env.push(("npm_config_cache".to_string(), cache));
        }
    }

    // Point uv to server-specific cache/tool dirs to avoid file lock conflicts
    if matches!(original_command, "uv" | "uvx" | "python" | "python3") {
        // Strategy: try version check first with short timeout, fall back to cache on failure.
        // uv behavior: check version (network) → success: use latest → timeout/fail: use cached
        env.push(("UV_HTTP_TIMEOUT".to_string(), "60".to_string()));
        env.push(("UV_CONNECT_TIMEOUT".to_string(), "10".to_string()));
        log::info!("[runtime_env] uv network timeouts: connect=10s, http=60s (version check with cache fallback)");
        if let Some(python_dir) = uv_python_install_dir() {
            log::info!("[runtime_env] UV_PYTHON_INSTALL_DIR: {:?}", python_dir);
            env.push((
                "UV_PYTHON_INSTALL_DIR".to_string(),
                normalize_path(&python_dir),
            ));
        }
        let cache_name = format!("uv-cache-{}", server_name);
        if let Some(cache) = app_local_dir(&cache_name) {
            log::info!("[runtime_env] UV_CACHE_DIR (server '{}'): {}", server_name, cache);
            env.push(("UV_CACHE_DIR".to_string(), cache));
        }
        let tools_name = format!("uv-tools-{}", server_name);
        if let Some(tools) = app_local_dir(&tools_name) {
            log::info!("[runtime_env] UV_TOOL_DIR (server '{}'): {}", server_name, tools);
            env.push(("UV_TOOL_DIR".to_string(), tools));
        }
        // Pin to a specific Python version if user has selected one
        let active_py = get_active_python();
        if active_py != "system" && !active_py.is_empty() {
            env.push(("UV_PYTHON".to_string(), format!("cpython-{active_py}")));
        }
    }

    env
}

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

/// Resolve a node/npx/npm command within a specific Node.js installation directory.
fn resolve_node_command_in_dir(command: &str, args: &[String], dir: &Path) -> Option<(String, Vec<String>)> {
    match command {
        "node" => {
            let bin = node_bin_in(dir);
            if bin.exists() {
                return Some((normalize_path(&bin), args.to_vec()));
            }
        }
        "npx" => {
            let node = node_bin_in(dir);
            let cli = npx_cli_in(dir);
            if node.exists() && cli.exists() {
                let mut new_args = vec![normalize_path(&cli)];
                new_args.extend_from_slice(args);
                return Some((normalize_path(&node), new_args));
            }
        }
        "npm" => {
            let node = node_bin_in(dir);
            let cli = npm_cli_in(dir);
            if node.exists() && cli.exists() {
                let mut new_args = vec![normalize_path(&cli)];
                new_args.extend_from_slice(args);
                return Some((normalize_path(&node), new_args));
            }
        }
        _ => {}
    }
    None
}

fn node_bin_in(dir: &Path) -> PathBuf {
    #[cfg(target_os = "windows")]
    return dir.join("node.exe");
    #[cfg(not(target_os = "windows"))]
    dir.join("bin").join("node")
}

fn npx_cli_in(dir: &Path) -> PathBuf {
    #[cfg(target_os = "windows")]
    return dir.join("node_modules").join("npm").join("bin").join("npx-cli.js");
    #[cfg(not(target_os = "windows"))]
    dir.join("lib").join("node_modules").join("npm").join("bin").join("npx-cli.js")
}

fn npm_cli_in(dir: &Path) -> PathBuf {
    #[cfg(target_os = "windows")]
    return dir.join("node_modules").join("npm").join("bin").join("npm-cli.js");
    #[cfg(not(target_os = "windows"))]
    dir.join("lib").join("node_modules").join("npm").join("bin").join("npm-cli.js")
}

fn node_bin_dir_in(dir: &Path) -> PathBuf {
    #[cfg(target_os = "windows")]
    return dir.to_path_buf();
    #[cfg(not(target_os = "windows"))]
    dir.join("bin")
}

fn node_bin_path(rt: &Path) -> PathBuf {
    #[cfg(target_os = "windows")]
    return rt.join("node").join("node.exe");
    #[cfg(not(target_os = "windows"))]
    rt.join("node").join("bin").join("node")
}

fn node_bin_dir(rt: &Path) -> PathBuf {
    #[cfg(target_os = "windows")]
    return rt.join("node");
    #[cfg(not(target_os = "windows"))]
    rt.join("node").join("bin")
}

fn npx_cli_path(rt: &Path) -> PathBuf {
    // Windows: node_modules/npm/bin/npx-cli.js (no lib/ prefix)
    // macOS/Linux: lib/node_modules/npm/bin/npx-cli.js
    #[cfg(target_os = "windows")]
    return rt
        .join("node")
        .join("node_modules")
        .join("npm")
        .join("bin")
        .join("npx-cli.js");
    #[cfg(not(target_os = "windows"))]
    rt.join("node")
        .join("lib")
        .join("node_modules")
        .join("npm")
        .join("bin")
        .join("npx-cli.js")
}

fn npm_cli_path(rt: &Path) -> PathBuf {
    #[cfg(target_os = "windows")]
    return rt
        .join("node")
        .join("node_modules")
        .join("npm")
        .join("bin")
        .join("npm-cli.js");
    #[cfg(not(target_os = "windows"))]
    rt.join("node")
        .join("lib")
        .join("node_modules")
        .join("npm")
        .join("bin")
        .join("npm-cli.js")
}

fn uv_dir(rt: &Path) -> PathBuf {
    rt.join("uv")
}

fn uv_bin_path(rt: &Path) -> PathBuf {
    #[cfg(target_os = "windows")]
    return uv_dir(rt).join("uv.exe");
    #[cfg(not(target_os = "windows"))]
    uv_dir(rt).join("uv")
}

fn uvx_exe() -> &'static str {
    #[cfg(target_os = "windows")]
    return "uvx.exe";
    #[cfg(not(target_os = "windows"))]
    "uvx"
}

/// Find a Python binary inside the bundled uv Python install dir.
fn find_bundled_python(rt: &Path) -> Option<PathBuf> {
    let python_dir = uv_dir(rt).join("python");
    if !python_dir.exists() {
        return None;
    }
    let bin_name = if cfg!(target_os = "windows") {
        "python.exe"
    } else {
        "python3"
    };
    // Case 1: uv install → python/cpython-3.12.x-{platform}/bin/python3
    let entries = std::fs::read_dir(&python_dir).ok()?;
    for entry in entries.flatten() {
        let candidate = if cfg!(target_os = "windows") {
            entry.path().join(bin_name)
        } else {
            entry.path().join("bin").join(bin_name)
        };
        if candidate.exists() {
            return Some(candidate);
        }
    }
    // Case 2: cross-compilation → python/python.exe (files directly in python/)
    let direct = python_dir.join(bin_name);
    if direct.exists() {
        return Some(direct);
    }
    None
}

/// Returns an app-local cache subdirectory path (creates it if needed).
fn app_local_dir(name: &str) -> Option<String> {
    // Use XDG_CACHE_HOME on Linux, ~/Library/Caches on macOS
    #[cfg(target_os = "macos")]
    let base = std::env::var("HOME")
        .ok()
        .map(|h| PathBuf::from(h).join("Library").join("Caches"));
    #[cfg(target_os = "linux")]
    let base = std::env::var("XDG_CACHE_HOME")
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var("HOME")
                .ok()
                .map(|h| PathBuf::from(h).join(".cache"))
        });
    #[cfg(target_os = "windows")]
    let base = std::env::var("LOCALAPPDATA").ok().map(PathBuf::from);

    let dir = base?.join("mcphub-desktop").join(name);
    std::fs::create_dir_all(&dir).ok()?;
    Some(normalize_path(&dir))
}

/// Returns an app data subdirectory (persistent, not cache) for storing managed runtimes.
/// macOS: ~/Library/Application Support/mcphub-desktop/{name}
/// Linux: ~/.local/share/mcphub-desktop/{name}
/// Windows: %APPDATA%/mcphub-desktop/{name}
pub fn app_data_dir(name: &str) -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    let base = std::env::var("HOME")
        .ok()
        .map(|h| PathBuf::from(h).join("Library").join("Application Support"));
    #[cfg(target_os = "linux")]
    let base = std::env::var("XDG_DATA_HOME")
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var("HOME")
                .ok()
                .map(|h| PathBuf::from(h).join(".local").join("share"))
        });
    #[cfg(target_os = "windows")]
    let base = std::env::var("APPDATA").ok().map(PathBuf::from);

    let dir = base?.join("mcphub-desktop").join(name);
    std::fs::create_dir_all(&dir).ok()?;
    Some(dir)
}

/// Returns the per-server npm cache directory path WITHOUT creating it.
/// Matches the `npm_config_cache` env override (`npm-cache-{server}`) that
/// `env_overrides` sets for node/npx/npm spawns — reinstall/clear_cache must
/// target THIS directory (its `_npx` holds the actual package installs), not
/// the legacy shared `runtimes/_npx` which managed spawns never write.
pub fn npm_server_cache_dir(server_name: &str) -> Option<PathBuf> {
    let base = app_cache_base()?;
    Some(base.join("mcphub-desktop").join(format!("npm-cache-{}", server_name)))
}

/// Returns the npm/npx cache directory used by the managed Node.js runtime.
/// Legacy shared location (kept for reference; managed npx spawns point at
/// `npm_server_cache_dir` via `npm_config_cache` instead).
pub fn npm_cache_dir() -> Option<PathBuf> {
    let rt = runtimes_dir()?;
    Some(rt.join("_npx"))
}

/// Returns the uv/uvx cache directory used by the managed uv runtime.
/// This is the `uv-cache` directory inside the runtimes dir.
pub fn uvx_cache_dir() -> Option<PathBuf> {
    let rt = runtimes_dir()?;
    Some(rt.join("uv-cache"))
}

/// Returns the per-server uvx cache directory path WITHOUT creating it.
/// Matches the `UV_CACHE_DIR` env override (`uv-cache-{server}`) that
/// `env_overrides` sets for uvx servers — reinstalls must clear this
/// directory to force a package re-download.
pub fn uvx_server_cache_dir(server_name: &str) -> Option<PathBuf> {
    let base = app_cache_base()?;
    Some(base.join("mcphub-desktop").join(format!("uv-cache-{}", server_name)))
}

fn app_cache_base() -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    let base = std::env::var("HOME")
        .ok()
        .map(|h| PathBuf::from(h).join("Library").join("Caches"));
    #[cfg(target_os = "linux")]
    let base = std::env::var("XDG_CACHE_HOME")
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var("HOME")
                .ok()
                .map(|h| PathBuf::from(h).join(".cache"))
        });
    #[cfg(target_os = "windows")]
    let base = std::env::var("LOCALAPPDATA").ok().map(PathBuf::from);
    base
}

#[cfg(all(test, not(target_os = "windows")))]
mod timeout_tests {
    use super::run_with_timeout;
    use std::process::Command;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

    fn shell(script: &str) -> Command {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg(script);
        command
    }

    struct TempFixture(std::path::PathBuf);

    impl TempFixture {
        fn new() -> Self {
            let id = FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "mcphub-runtime-timeout-{}-{id}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("create temporary fixture");
            Self(path)
        }
    }

    impl Drop for TempFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn returns_normal_stdout_and_stderr() {
        let output = run_with_timeout(shell("printf out; printf err >&2"), Duration::from_secs(2))
            .expect("shell should complete");

        assert!(output.status.success());
        assert_eq!(output.stdout, b"out");
        assert_eq!(output.stderr, b"err");
    }

    #[test]
    fn drains_output_larger_than_pipe_capacity() {
        let output = run_with_timeout(
            shell("head -c 131072 /dev/zero; head -c 131072 /dev/zero >&2"),
            Duration::from_secs(3),
        )
        .expect("shell should complete");

        assert!(output.status.success());
        assert_eq!(output.stdout.len(), 131072);
        assert_eq!(output.stderr.len(), 131072);
    }

    #[test]
    fn returns_when_descendant_keeps_stdout_open() {
        let started = Instant::now();
        let output = run_with_timeout(
            shell("sleep 6 2>/dev/null & printf done"),
            Duration::from_millis(200),
        )
        .expect("direct shell should exit before the deadline");
        let elapsed = started.elapsed();

        assert!(output.status.success());
        assert_eq!(output.stdout, b"done");
        assert!(elapsed < Duration::from_secs(1), "helper took {elapsed:?}");
    }

    #[test]
    fn returns_when_descendant_keeps_stderr_open() {
        let started = Instant::now();
        let output = run_with_timeout(
            shell("sleep 6 >/dev/null & printf done"),
            Duration::from_millis(200),
        )
        .expect("direct shell should exit before the deadline");
        let elapsed = started.elapsed();

        assert!(output.status.success());
        assert_eq!(output.stdout, b"done");
        assert!(elapsed < Duration::from_secs(1), "helper took {elapsed:?}");
    }

    #[test]
    fn kills_redirected_descendant_after_successful_shell_exit() {
        let fixture = TempFixture::new();
        let started = fixture.0.join("started");
        let survived = fixture.0.join("survived");
        let mut command = shell(
            r#"(printf started > "$1"; sleep 1; printf survived > "$2") >/dev/null 2>&1 & while [ ! -e "$1" ]; do sleep 0.01; done; printf done"#,
        );
        command.arg("sh").arg(&started).arg(&survived);

        let output = run_with_timeout(command, Duration::from_secs(2))
            .expect("shell should complete successfully");
        assert!(output.status.success());
        assert_eq!(output.stdout, b"done");
        assert!(started.exists(), "descendant should have started");

        std::thread::sleep(Duration::from_millis(1200));
        assert!(
            !survived.exists(),
            "redirected descendant survived the shell's successful exit"
        );
    }

    #[test]
    fn deadline_edge_rejects_completion_at_or_after_deadline() {
        let deadline = Instant::now() + Duration::from_secs(1);

        assert!(super::observed_before_deadline(
            deadline,
            deadline - Duration::from_nanos(1)
        ));
        assert!(!super::observed_before_deadline(deadline, deadline));
        assert!(!super::observed_before_deadline(
            deadline,
            deadline + Duration::from_nanos(1)
        ));
    }

    #[test]
    fn times_out_if_child_closes_pipes_but_keeps_running() {
        let started = Instant::now();
        let output = run_with_timeout(
            shell("exec >/dev/null 2>&1; sleep 6"),
            Duration::from_millis(200),
        );

        assert!(output.is_none());
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn times_out_during_continuous_stdout() {
        let started = Instant::now();
        let output = run_with_timeout(shell("yes"), Duration::from_millis(200));

        assert!(output.is_none());
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn stops_continuous_stdout_when_capture_limit_is_reached() {
        let started = Instant::now();
        let output = run_with_timeout(shell("yes"), Duration::from_secs(5));

        assert!(output.is_none());
        assert!(started.elapsed() < Duration::from_secs(3));
    }
}
