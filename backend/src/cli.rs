//! Locating and driving the `dbx` CLI.
//!
//! Every database read in this plugin goes through a fresh `dbx query` process.
//! Nothing is cached between calls: there is no session to hold open, which is
//! also why the plugin can never hold a cross-table consistent read (see
//! PLAN.md §0.1 -- the self-healing diff is what covers that).

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

/// One page is one CLI process. Chosen to match the driver's own default row
/// cap so `--limit` and the SQL `LIMIT` agree exactly.
pub const PAGE_SIZE: usize = 10_000;

const POLL_INTERVAL: Duration = Duration::from_millis(50);
/// Grace on top of the query timeout before the process is killed outright. The
/// CLI's own `--timeout` bounds the server-side query; this bounds the process.
const KILL_GRACE: Duration = Duration::from_secs(30);
/// Used by `paginate`, which runs one page per call.
const PAGE_TIMEOUT: Duration = Duration::from_secs(180);

/// Connection-level failures are worth retrying. Each `dbx query` opens a fresh
/// connection, and one snapshot opens more than a dozen; a single stalled
/// handshake on a remote server would otherwise abort a run that had already
/// read most of its tables. Deliberately narrow: a SQL error is deterministic
/// and retrying it would only waste time and hide the real message.
const CONNECTION_ERROR_MARKERS: &[&str] = &[
    "connection timed out",
    // The server's own pool giving up before the handshake even starts, and the
    // PostgreSQL driver's own wording for the same thing. Different messages,
    // same class of failure, and both observed on this project's servers -- one
    // diff opens more than a dozen connections, so it only takes a busy moment.
    // Several retries, because on a server that flaky two is not enough.
    "connection pool checkout timed out",
    "timeout occurred while creating a new object",
    "connection failed: timeout",
    "can't connect to mysql server",
    "can't connect to server",
    "communications link failure",
    "lost connection to mysql server",
    "connection reset",
    "connection refused",
];
const RETRIES: usize = 3;
const RETRY_BACKOFF: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct CliError {
    pub code: String,
    pub message: String,
}

impl CliError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self { code: code.into(), message: message.into() }
    }
}

struct CliOutput {
    stdout: String,
    stderr: String,
    code: Option<i32>,
    timed_out: bool,
}

/// Run one read-only query. The result is the CLI's own JSON object
/// (`{"connection","columns","rows","row_count"}`).
pub fn query(connection: &str, sql: &str, timeout: Duration) -> Result<Value, CliError> {
    query_with_cancel(connection, sql, timeout, &|| false)
}

/// `is_cancelled` is polled while the child runs, so a long page gets killed
/// instead of waiting out its timeout. That is the whole difference between
/// "cancel" feeling immediate and it hanging until the current query returns.
pub fn query_with_cancel(
    connection: &str,
    sql: &str,
    timeout: Duration,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<Value, CliError> {
    let mut attempt = 0;
    loop {
        match query_once(connection, sql, timeout, is_cancelled) {
            // A cancelled run must not be retried -- the flag is still set.
            Err(error) if error.code != CANCELLED && attempt < RETRIES && is_connection_error(&error) => {
                attempt += 1;
                thread::sleep(RETRY_BACKOFF * attempt as u32);
            }
            other => return other,
        }
    }
}

/// Returned when the child was killed because the job was cancelled. Callers map
/// this to their own "cancelled" outcome rather than reporting it as a failure.
pub const CANCELLED: &str = "CANCELLED";

fn is_connection_error(error: &CliError) -> bool {
    let message = error.message.to_ascii_lowercase();
    CONNECTION_ERROR_MARKERS.iter().any(|marker| message.contains(marker))
}

fn query_once(
    connection: &str,
    sql: &str,
    timeout: Duration,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<Value, CliError> {
    let mut args: Vec<String> = vec![
        "query".into(),
        connection.into(),
        "--json".into(),
        "--limit".into(),
        PAGE_SIZE.to_string(),
        "--timeout".into(),
        format!("{}s", timeout.as_secs().max(1)),
    ];
    // A leading '-' would be read as a flag, so introduce the SQL with '--'.
    if sql.starts_with('-') {
        args.push("--".into());
    }
    args.push(sql.to_string());

    let output = run(&args, timeout + KILL_GRACE, is_cancelled)?;
    let parsed = parse_json(&output).ok_or_else(|| {
        let detail = if output.stderr.trim().is_empty() {
            format!("dbx CLI 退出码 {:?}，且没有可解析的输出", output.code)
        } else {
            output.stderr.trim().to_string()
        };
        CliError::new(if output.timed_out { "TIMEOUT" } else { "ERROR" }, detail)
    })?;

    match parsed.get("error") {
        Some(error) => Err(CliError::new(
            error.get("code").and_then(Value::as_str).unwrap_or("ERROR"),
            error.get("message").and_then(Value::as_str).unwrap_or("dbx CLI reported an error"),
        )),
        None => Ok(parsed),
    }
}

/// The saved connections, straight from the CLI's own JSON.
pub fn connections() -> Result<Value, CliError> {
    let args: Vec<String> = vec!["connections".into(), "list".into(), "--json".into()];
    let output = run(&args, Duration::from_secs(60), &|| false)?;
    let parsed = parse_json(&output).ok_or_else(|| {
        CliError::new(
            "ERROR",
            format!("dbx connections list 输出无法解析: {}", output.stderr.trim()),
        )
    })?;
    match parsed.get("error") {
        Some(error) => Err(CliError::new(
            error.get("code").and_then(Value::as_str).unwrap_or("ERROR"),
            error.get("message").and_then(Value::as_str).unwrap_or("dbx CLI reported an error"),
        )),
        None => Ok(parsed),
    }
}

/// The `type` field of a saved connection, cached for the life of the process.
///
/// Resolved from the CLI rather than sent by the panel, so that every caller --
/// including a test driving the sidecar directly -- gets the right dialect
/// without having to know it exists. One `dbx connections list` per process.
pub fn connection_kind(name: &str) -> Result<String, CliError> {
    static CACHE: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(kind) = cache.lock().ok().and_then(|map| map.get(name).cloned()) {
        return Ok(kind);
    }

    let listed = connections()?;
    let mut guard = cache.lock().map_err(|_| CliError::new("ERROR", "连接类型缓存不可用"))?;
    for connection in listed.get("connections").and_then(Value::as_array).into_iter().flatten() {
        let (Some(name), Some(kind)) = (
            connection.get("name").and_then(Value::as_str),
            connection.get("type").and_then(Value::as_str),
        ) else {
            continue;
        };
        guard.insert(name.to_string(), kind.to_string());
    }
    guard
        .get(name)
        .cloned()
        .ok_or_else(|| CliError::new("ERROR", format!("连接 '{name}' 不在 dbx connections list 里")))
}

/// Column order as the server returned it. This is authoritative -- the row
/// objects are keyed by name, and a name can repeat if the caller selected the
/// same column twice, so callers must dedupe before trusting them.
pub fn result_columns(result: &Value) -> Vec<String> {
    result
        .get("columns")
        .and_then(Value::as_array)
        .map(|columns| columns.iter().filter_map(|c| c.as_str().map(str::to_string)).collect())
        .unwrap_or_default()
}

pub fn result_rows(result: &Value) -> Vec<Value> {
    match result.get("rows").and_then(Value::as_array) {
        Some(rows) => rows.clone(),
        None => Vec::new(),
    }
}

/// Walk a result set in `PAGE_SIZE` chunks using LIMIT/OFFSET.
///
/// `select_sql` must already be stable-ordered and unique -- without an
/// `ORDER BY`, page boundaries are undefined and rows can be both duplicated and
/// dropped. The `LIMIT` appended here must match `--limit` in `query`, which it
/// does because both are `PAGE_SIZE`.
pub fn paginate(
    connection: &str,
    select_sql: &str,
    handle_page: &mut dyn FnMut(&[Value]) -> Result<(), String>,
) -> Result<u64, CliError> {
    let mut total: u64 = 0;
    let mut page: u64 = 0;
    loop {
        let sql = format!("{select_sql} LIMIT {} OFFSET {}", PAGE_SIZE, page * PAGE_SIZE as u64);
        let result = query_with_cancel(connection, &sql, PAGE_TIMEOUT, &|| false)
            .map_err(|error| CliError::new(error.code, format!("{}\n{sql}", error.message)))?;
        let rows = result_rows(&result);
        let fetched = rows.len();
        handle_page(&rows).map_err(|message| CliError::new("ERROR", message))?;
        total += fetched as u64;
        if fetched < PAGE_SIZE {
            return Ok(total);
        }
        page += 1;
    }
}

fn run(args: &[String], kill_after: Duration, is_cancelled: &dyn Fn() -> bool) -> Result<CliOutput, CliError> {
    let cli = resolve().map_err(|message| CliError::new("CLI_NOT_FOUND", message))?;
    let mut command = Command::new(&cli);
    command.args(args).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // Without this the child pops a console window on every query.
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.creation_flags(CREATE_NO_WINDOW);
    }

    let mut child = command
        .spawn()
        .map_err(|error| CliError::new("CLI_NOT_FOUND", format!("无法启动 dbx CLI ({}): {error}", cli.display())))?;

    // Drain both pipes on their own threads; a full pipe buffer would otherwise
    // deadlock the poll loop below.
    let mut child_stdout = child.stdout.take().ok_or_else(|| CliError::new("ERROR", "dbx CLI stdout unavailable"))?;
    let mut child_stderr = child.stderr.take().ok_or_else(|| CliError::new("ERROR", "dbx CLI stderr unavailable"))?;
    let stdout_reader = thread::spawn(move || read_to_string(&mut child_stdout));
    let stderr_reader = thread::spawn(move || read_to_string(&mut child_stderr));

    let deadline = Instant::now() + kill_after;
    let mut timed_out = false;
    let code = loop {
        // Checked before try_wait so a cancel during a long page kills the child
        // straight away instead of waiting for the query to come back.
        if is_cancelled() {
            let _ = child.kill();
            let _ = child.wait();
            return Err(CliError::new(CANCELLED, "已取消"));
        }
        match child.try_wait() {
            Ok(Some(status)) => break status.code(),
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    timed_out = true;
                    break None;
                }
                thread::sleep(POLL_INTERVAL);
            }
            Err(error) => return Err(CliError::new("ERROR", format!("等待 dbx CLI 失败: {error}"))),
        }
    };

    let stdout = stdout_reader.join().unwrap_or_default();
    let stderr = stderr_reader.join().unwrap_or_default();
    Ok(CliOutput { stdout, stderr, code, timed_out })
}

fn read_to_string(reader: &mut impl Read) -> String {
    let mut buffer = String::new();
    let _ = reader.read_to_string(&mut buffer);
    buffer
}

/// The CLI writes its JSON to stdout on success and to stderr on failure, but it
/// can also emit progress noise on either stream, so the whole capture is tried
/// first and the lines are then scanned from the end.
fn parse_json(output: &CliOutput) -> Option<Value> {
    for stream in [&output.stdout, &output.stderr] {
        let trimmed = stream.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(value) = serde_json::from_str(trimmed) {
            return Some(value);
        }
        for line in trimmed.lines().rev() {
            let line = line.trim();
            if line.starts_with('{') {
                if let Ok(value) = serde_json::from_str(line) {
                    return Some(value);
                }
            }
        }
    }
    None
}

/// Where the CLI comes from, in order:
///
///   1. `DBX_CLI_BIN` -- explicit, trusted as-is.
///   2. The npm global layout `npm install -g @dbx-app/cli` produces.
///   3. A `dbx.exe` / `dbx` on PATH.
///
/// Steps 2 and 3 are in this order on purpose: the desktop app's own binary is
/// *also* called `dbx.exe`, so a PATH hit can be the wrong program entirely --
/// launching it boots a second DBX instead of answering a query. The npm layout
/// is unambiguous, so it is tried first. For the same reason the PATH scan skips
/// cargo output trees (`target/debug`, `target/release`), which is exactly where
/// a locally built desktop binary lives.
///
/// The npm shims (`dbx.cmd`, `dbx.ps1`, `dbx`) are scripts and are skipped --
/// spawning one means going through cmd.exe and re-quoting the SQL for a second
/// parser. Only real executables are used.
pub fn resolve() -> Result<PathBuf, String> {
    if let Some(explicit) = std::env::var_os("DBX_CLI_BIN") {
        let path = PathBuf::from(&explicit);
        if path.is_file() {
            return Ok(path);
        }
        return Err(format!("DBX_CLI_BIN 指向的文件不存在: {}", path.display()));
    }

    for candidate in npm_cli_candidates() {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    let executable = if cfg!(windows) { "dbx.exe" } else { "dbx" };
    if let Some(paths) = std::env::var_os("PATH") {
        for directory in std::env::split_paths(&paths) {
            if is_cargo_output_dir(&directory) {
                continue;
            }
            let candidate = directory.join(executable);
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }

    Err(format!(
        "找不到 dbx CLI。请先 `npm install -g @dbx-app/cli`，或设置环境变量 DBX_CLI_BIN 指向 {} 的完整路径。",
        executable
    ))
}

/// `<prefix>/node_modules/@dbx-app/cli/node_modules/@dbx-app/cli-<platform>/bin/dbx`
fn npm_cli_candidates() -> Vec<PathBuf> {
    let platform = platform_package_names();
    let mut roots: Vec<PathBuf> = Vec::new();

    #[cfg(windows)]
    if let Some(appdata) = std::env::var_os("APPDATA") {
        roots.push(Path::new(&appdata).join("npm").join("node_modules"));
    }

    #[cfg(not(windows))]
    {
        if let Some(home) = std::env::var_os("HOME") {
            roots.push(Path::new(&home).join(".npm-global").join("lib").join("node_modules"));
            roots.push(Path::new(&home).join(".local").join("lib").join("node_modules"));
        }
        // `/usr/local` is Homebrew's prefix on Intel Macs; Apple Silicon uses
        // `/opt/homebrew`, and that is the common way Node gets installed there.
        // Missing it means a Mac user has to set DBX_CLI_BIN by hand to use the
        // plugin at all.
        for prefix in ["/opt/homebrew/lib", "/usr/local/lib", "/usr/lib"] {
            roots.push(Path::new(prefix).join("node_modules"));
        }
    }

    let mut candidates = Vec::new();
    for root in roots {
        for package in &platform {
            candidates.push(
                root.join("@dbx-app")
                    .join("cli")
                    .join("node_modules")
                    .join("@dbx-app")
                    .join(package)
                    .join("bin")
                    .join(if cfg!(windows) { "dbx.exe" } else { "dbx" }),
            );
        }
    }
    candidates
}

/// The platform package's `<os>-<arch>` suffix, in the CLI's spelling:
/// `win32-x64`, `darwin-arm64`, `linux-x64-gnu`, ...
fn platform_package_names() -> Vec<String> {
    let os = match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "win32",
        other => other,
    };
    let arch = match std::env::consts::ARCH {
        "x86_64" => "x64",
        "aarch64" => "arm64",
        other => other,
    };
    let mut names = vec![format!("cli-{os}-{arch}")];
    if cfg!(target_os = "linux") {
        names.push(format!("cli-{os}-{arch}-gnu"));
        names.push(format!("cli-{os}-{arch}-musl"));
    }
    names
}

/// `.../target/debug` or `.../target/release` -- a cargo build tree.
fn is_cargo_output_dir(directory: &Path) -> bool {
    match directory.file_name().and_then(|name| name.to_str()) {
        Some("debug") | Some("release") => {}
        _ => return false,
    }
    directory.parent().and_then(|parent| parent.file_name()).and_then(|name| name.to_str()) == Some("target")
}

pub fn describe_cli() -> String {
    resolve().map(|path| path.display().to_string()).unwrap_or_else(|message| message)
}
