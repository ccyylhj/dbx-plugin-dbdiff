//! Locating and driving the `dbx` CLI.
//!
//! Every database read in this plugin goes through a fresh `dbx query` process.
//! Nothing is cached between calls: there is no session to hold open, which is
//! also why the plugin can never hold a cross-table consistent read (see
//! PLAN.md §0.1 -- the self-healing diff is what covers that).

use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::config;

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
///   2. The path saved in the plugin's own config. The escape hatch for a layout
///      none of the searches below understand.
///   3. The npm global layout `npm install -g @dbx-app/cli` produces, under every
///      prefix in `npm_roots`.
///   4. A `dbx` on PATH that is a real program.
///   5. A `dbx` on PATH that is npm's JS shim -- not runnable on its own, but it
///      names the package directory the native binary sits in.
///   6. On macOS and Linux, the same question put to the user's login shell.
///
/// The npm layout comes before PATH, and a real program before a shim, on
/// purpose: the desktop app's own binary is *also* called `dbx`, so a PATH hit
/// can be the wrong program entirely -- launching it boots a second DBX instead
/// of answering a query. The npm layout is unambiguous. For the same reason the
/// PATH scan skips cargo output trees (`target/debug`, `target/release`), which
/// is exactly where a locally built desktop binary lives.
pub fn resolve() -> Result<PathBuf, String> {
    // Only successes are cached. A failure has to stay retryable, because the
    // config override is one of the inputs and the user can fix it in the panel
    // without restarting the plugin.
    static RESOLVED: OnceLock<PathBuf> = OnceLock::new();
    if let Some(path) = RESOLVED.get() {
        return Ok(path.clone());
    }
    let path = locate()?;
    let _ = RESOLVED.set(path.clone());
    Ok(path)
}

fn locate() -> Result<PathBuf, String> {
    if let Some(explicit) = std::env::var_os("DBX_CLI_BIN") {
        let path = PathBuf::from(&explicit);
        if path.is_file() {
            return Ok(path);
        }
        return Err(format!("DBX_CLI_BIN 指向的文件不存在: {}", path.display()));
    }

    let configured = config::load().cli_path;
    if !configured.trim().is_empty() {
        let path = PathBuf::from(configured.trim());
        if path.is_file() {
            return Ok(path);
        }
        return Err(format!("配置里的 dbx CLI 路径不存在: {}", path.display()));
    }

    let packages = platform_package_names();
    let executable = native_name();
    let roots = npm_roots();
    if let Some(found) = roots.iter().find_map(|root| native_under(root, &packages, executable)) {
        return Ok(found);
    }

    // A shim is kept rather than followed straight away: a real program further
    // along PATH is a better answer than a shim earlier on it.
    let mut pointers: Vec<PathBuf> = Vec::new();
    if let Some(paths) = std::env::var_os("PATH") {
        for directory in std::env::split_paths(&paths) {
            if is_cargo_output_dir(&directory) {
                continue;
            }
            let candidate = directory.join(executable);
            if !candidate.is_file() {
                continue;
            }
            if is_script(&candidate) {
                pointers.push(candidate);
            } else {
                return Ok(candidate);
            }
        }
    }

    #[cfg(not(windows))]
    {
        if let Some(found) = shell_lookup(executable) {
            pointers.push(found);
        }
    }

    for pointer in &pointers {
        if !is_script(pointer) {
            return Ok(pointer.clone());
        }
        if let Some(found) = shim_node_modules(pointer)
            .iter()
            .find_map(|node_modules| native_under(node_modules, &packages, executable))
        {
            return Ok(found);
        }
    }

    Err(not_found_message(&roots, &pointers))
}

/// Everything that was looked at, so a machine where this fails can be diagnosed
/// from the message alone -- the alternative is telling a Mac user to set an
/// environment variable that a GUI app gives them no way to set.
fn not_found_message(roots: &[PathBuf], pointers: &[PathBuf]) -> String {
    let mut tried: Vec<String> = roots.iter().map(|root| root.display().to_string()).collect();
    for pointer in pointers {
        tried.push(format!("{}（npm 脚本，推不出原生二进制）", pointer.display()));
    }
    if tried.is_empty() {
        tried.push("（没有可查找的位置）".to_string());
    }
    let mut message =
        String::from("找不到 dbx CLI。请确认 `npm install -g @dbx-app/cli` 装好了，或在插件面板填写 dbx CLI 的完整路径。");
    #[cfg(not(windows))]
    message.push_str("\n也问过登录 shell 的 `command -v dbx`。");
    message.push_str("\n已查找：\n");
    message.push_str(&tried.join("\n"));
    message
}

/// The platform package's `<os>-<arch>` suffix, in the CLI's spelling:
/// `win32-x64`, `darwin-arm64`, `linux-x64-gnu`, ...
fn platform_package_names_for(os: &str, arch: &str) -> Vec<String> {
    let os = match os {
        "macos" => "darwin",
        "windows" => "win32",
        other => other,
    };
    let arch = match arch {
        "x86_64" => "x64",
        "aarch64" => "arm64",
        other => other,
    };
    let mut names = vec![format!("cli-{os}-{arch}")];
    if os == "linux" {
        // The Linux builds are published with an explicit libc suffix.
        names.push(format!("cli-{os}-{arch}-gnu"));
        names.push(format!("cli-{os}-{arch}-musl"));
    }
    if os == "darwin" {
        // The two macOS packages are built per architecture, and which one a user
        // installed is not necessarily the one this sidecar was built for --
        // picking the Intel package for an Apple Silicon Mac is an easy mistake,
        // and Node there is arm64. Either binary runs on either Mac (one of them
        // under Rosetta), so the host's own stays first and the other follows.
        names.push(format!("cli-{os}-{}", if arch == "arm64" { "x64" } else { "arm64" }));
    }
    names
}

fn platform_package_names() -> Vec<String> {
    platform_package_names_for(std::env::consts::OS, std::env::consts::ARCH)
}

fn native_name_for(os: &str) -> &'static str {
    if os == "windows" {
        "dbx.exe"
    } else {
        "dbx"
    }
}

fn native_name() -> &'static str {
    native_name_for(std::env::consts::OS)
}

/// The native binary inside one `node_modules` directory, in either shape npm can
/// produce: nested under the CLI package (what npm itself does), or hoisted
/// beside it (which is what a pnpm store looks like -- `@dbx-app/cli` there is a
/// symlink into `.pnpm/...`, and its dependencies are its siblings).
fn native_under(node_modules: &Path, packages: &[String], executable: &str) -> Option<PathBuf> {
    for package in packages {
        for base in [
            node_modules.join("@dbx-app").join("cli").join("node_modules"),
            node_modules.to_path_buf(),
        ] {
            let candidate = base.join("@dbx-app").join(package).join("bin").join(executable);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

/// The npm global prefixes worth looking under, as `node_modules` directories.
///
/// Node gets installed a dozen ways and each puts its global packages somewhere
/// else, so this guesses widely: a miss costs one `stat`, and the alternative is
/// a user setting a path by hand. It does not have to be complete -- step 6 of
/// `resolve` covers whatever it misses.
fn npm_roots() -> Vec<PathBuf> {
    let mut roots: Vec<PathBuf> = Vec::new();

    #[cfg(windows)]
    if let Some(appdata) = std::env::var_os("APPDATA") {
        // Where `npm install -g` puts things on Windows, and the one prefix that
        // is an absolute path rather than a guess.
        roots.push(Path::new(&appdata).join("npm").join("node_modules"));
    }

    #[cfg(not(windows))]
    {
        if let Some(home) = std::env::var_os("HOME") {
            let home = PathBuf::from(home);
            // Plain prefixes, including the ones the alternative package managers
            // default to.
            for relative in [
                ".npm-global/lib",
                ".local/lib",
                ".npm/lib",
                ".config/yarn/global",
                ".bun/install/global",
            ] {
                roots.push(home.join(relative).join("node_modules"));
            }
            // Version managers: a known base, an unknown version directory.
            for (base, middle) in [
                (".nvm/versions/node", ""),
                (".asdf/installs/nodejs", ""),
                (".volta/tools/image/node", ""),
                ("Library/Application Support/fnm/node-versions", "installation"),
                (".local/share/fnm/node-versions", "installation"),
                // pnpm numbers its global layout the same way: global/5, ...
                ("Library/pnpm/global", ""),
                (".local/share/pnpm/global", ""),
            ] {
                roots.extend(versioned_node_modules(&home.join(base), middle));
            }
        }
        // Both of these move a directory the list above has already guessed at.
        if let Some(directory) = std::env::var_os("NVM_DIR") {
            roots.extend(versioned_node_modules(&Path::new(&directory).join("versions").join("node"), ""));
        }
        if let Some(directory) = std::env::var_os("PNPM_HOME") {
            roots.extend(versioned_node_modules(&Path::new(&directory).join("global"), ""));
        }
        // System prefixes. `/opt/homebrew` is Homebrew on Apple Silicon,
        // `/usr/local` is Homebrew on Intel and the nodejs.org installer,
        // `/opt/local` is MacPorts.
        for prefix in ["/opt/homebrew/lib", "/usr/local/lib", "/opt/local/lib", "/usr/lib"] {
            roots.push(Path::new(prefix).join("node_modules"));
        }
        // `NODE_PATH` is a list of `node_modules` directories by definition.
        if let Some(paths) = std::env::var_os("NODE_PATH") {
            roots.extend(std::env::split_paths(&paths));
        }
    }

    roots
}

/// `<base>/<version>/<middle>/lib/node_modules` for every version installed,
/// newest first.
///
/// Unix only, in the compiled binary: no version manager on Windows moves the
/// global prefix -- nvm-windows shares one, and it is the same APPDATA one npm
/// uses whatever runtime is in play. The tests run this everywhere regardless.
///
/// Order matters because a CLI installed under more than one runtime works from
/// any of them, and the one in use is the new one.
#[cfg(any(not(windows), test))]
fn versioned_node_modules(base: &Path, middle: &str) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(base) else {
        return Vec::new();
    };
    let mut versions: Vec<PathBuf> =
        entries.filter_map(Result::ok).map(|entry| entry.path()).filter(|path| path.is_dir()).collect();
    versions.sort_by(|left, right| version_key(right).cmp(&version_key(left)));
    versions
        .into_iter()
        .map(|version| if middle.is_empty() { version } else { version.join(middle) })
        .map(|version| version.join("lib").join("node_modules"))
        .collect()
}

/// `v20.11.0` -> `[20, 11, 0]`. Sorting by name puts `v9` above `v20`, which is
/// the one place the difference would show.
#[cfg(any(not(windows), test))]
fn version_key(path: &Path) -> Vec<u64> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.split(|c: char| !c.is_ascii_digit()).filter_map(|part| part.parse().ok()).collect())
        .unwrap_or_default()
}

/// npm's `bin` entries are scripts, not programs: `dbx` is a symlink to a `.js`
/// file beginning with `#!`. Spawning one means going through `node`, which the
/// child cannot find -- a process the GUI started has no shell PATH -- so a shim
/// is never used as the CLI. It is still worth reading, because where it lives
/// says where the package is.
fn is_script(path: &Path) -> bool {
    let Ok(mut file) = fs::File::open(path) else {
        return false;
    };
    let mut head = [0u8; 2];
    file.read_exact(&mut head).is_ok() && &head == b"#!"
}

/// The `node_modules` directories a shim could have got its package from,
/// nearest first.
///
/// The shim is resolved through any symlink first -- pnpm and `npm link` both
/// install by symlink, and it is the link target that names the real tree -- and
/// then every ancestor's `node_modules` is offered, because that is where a
/// hoisted dependency lands.
fn shim_node_modules(shim: &Path) -> Vec<PathBuf> {
    let Some(resolved) = fs::canonicalize(shim).ok() else {
        return Vec::new();
    };
    // `<package>/bin/<file>` -> `<package>`
    let Some(package) = resolved.parent().and_then(Path::parent) else {
        return Vec::new();
    };
    let mut roots = vec![package.join("node_modules")];
    // Bounded: a package directory is never deep, and an unbounded walk would end
    // up offering the filesystem root's `node_modules`.
    let mut ancestor = package.parent();
    for _ in 0..6 {
        let Some(current) = ancestor else {
            break;
        };
        roots.push(current.join("node_modules"));
        ancestor = current.parent();
    }
    roots
}

/// How long the login-shell probe is allowed to take. A shell slower than this is
/// stuck on something in an rc file, and finding the CLI is not worth waiting.
#[cfg(not(windows))]
const SHELL_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

#[cfg(not(windows))]
static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// The `dbx` the user's own login shell would run.
///
/// A process the desktop app started does not inherit the PATH a terminal has --
/// launchd hands it a minimal one -- and on macOS the usual way to get Node is a
/// version manager whose setup lives in a shell profile. So a machine where `dbx`
/// works perfectly in a terminal can look like a machine with no CLI at all from
/// in here. DBX itself resolves `node` the same way for its own MCP setup.
///
/// Last resort, and probed once per process -- including when it finds nothing,
/// so a shell that is slow to start is paid for once rather than on every query.
#[cfg(not(windows))]
fn shell_lookup(executable: &str) -> Option<PathBuf> {
    // `Some(None)` means "asked, and there was nothing".
    static CACHE: OnceLock<Mutex<Option<Option<PathBuf>>>> = OnceLock::new();
    let mut cache = CACHE.get_or_init(|| Mutex::new(None)).lock().ok()?;
    if let Some(found) = cache.as_ref() {
        return found.clone();
    }
    let found = probe_login_shell(executable);
    *cache = Some(found.clone());
    found
}

#[cfg(not(windows))]
fn probe_login_shell(executable: &str) -> Option<PathBuf> {
    // Marks our own line, so anything an rc file prints is ignored.
    const MARKER: &str = "__dbx_cli_lookup=";
    let script = format!("printf '{MARKER}%s\\n' \"$(command -v {executable} 2>/dev/null)\"");

    let shell = std::env::var("SHELL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(default_shell);
    let name = Path::new(&shell).file_name().and_then(|name| name.to_str()).unwrap_or_default();
    let args: Vec<String> = match name {
        "fish" => vec!["-l".into(), "-i".into(), "-c".into(), script],
        "sh" | "dash" => vec!["-ic".into(), script],
        // `-i` reads the rc file and `-l` the profile: a version manager writes
        // its setup to one or the other depending on which one it is and when it
        // was installed.
        _ => vec!["-ilc".into(), script],
    };

    // The shell writes to a file rather than a pipe. An rc file that leaves a
    // background process behind would hold a pipe open, and reading it would then
    // block past the timeout this is here to enforce.
    let scratch = std::env::temp_dir().join(format!(
        "dbx-cli-lookup-{}-{}.txt",
        std::process::id(),
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let sink = fs::File::create(&scratch).ok()?;

    let mut child = Command::new(&shell)
        .args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(sink))
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let deadline = Instant::now() + SHELL_PROBE_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                break;
            }
            Ok(None) => thread::sleep(POLL_INTERVAL),
            Err(_) => {
                let _ = child.kill();
                break;
            }
        }
    }

    let output = fs::read_to_string(&scratch).unwrap_or_default();
    let _ = fs::remove_file(&scratch);

    output
        .lines()
        .find_map(|line| line.trim().strip_prefix(MARKER))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        // An absolute path to a real file, or nothing. `command -v` also answers
        // for shell functions and aliases, and prints a bare name for those.
        .filter(|path| path.is_absolute() && path.is_file())
}

#[cfg(not(windows))]
fn default_shell() -> String {
    if Path::new("/bin/zsh").exists() {
        "/bin/zsh".to_string()
    } else {
        "/bin/sh".to_string()
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A throwaway directory tree. This crate has no dev-dependencies, so it is
    /// built by hand; the name carries the test's own name and the process id so
    /// two tests, or two runs, cannot collide.
    struct Tree(PathBuf);

    impl Tree {
        fn new(name: &str) -> Self {
            let root = std::env::temp_dir().join(format!("dbx-cli-{}-{}", std::process::id(), name));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("create the scratch tree");
            Self(root)
        }

        fn file(&self, relative: &str, content: &[u8]) -> PathBuf {
            let path = self.0.join(relative);
            fs::create_dir_all(path.parent().expect("a file has a parent")).expect("create the parent");
            fs::write(&path, content).expect("write the file");
            path
        }

        fn dir(&self, relative: &str) {
            fs::create_dir_all(self.0.join(relative)).expect("create the directory");
        }
    }

    impl Drop for Tree {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn arm64() -> Vec<String> {
        vec!["cli-darwin-arm64".to_string()]
    }

    /// Compare after canonicalising both sides: `shim_node_modules` resolves the
    /// shim first, and on Windows that prefixes the result with `\\?\` while the
    /// test's own path does not have it.
    fn assert_same_path(found: Option<PathBuf>, expected: &Path) {
        let found = fs::canonicalize(found.expect("a binary was found")).expect("canonicalize what was found");
        let expected = fs::canonicalize(expected).expect("canonicalize what was expected");
        assert_eq!(found, expected);
    }

    #[test]
    fn the_nested_layout_is_what_npm_produces() {
        let tree = Tree::new("nested");
        let expected =
            tree.file("node_modules/@dbx-app/cli/node_modules/@dbx-app/cli-darwin-arm64/bin/dbx", b"\x7fELF");
        assert_eq!(native_under(&tree.0.join("node_modules"), &arm64(), "dbx"), Some(expected));
    }

    #[test]
    fn the_hoisted_layout_is_what_a_pnpm_store_has() {
        let tree = Tree::new("hoisted");
        let expected = tree.file("node_modules/@dbx-app/cli-darwin-arm64/bin/dbx", b"\x7fELF");
        assert_eq!(native_under(&tree.0.join("node_modules"), &arm64(), "dbx"), Some(expected));
    }

    #[test]
    fn only_the_platform_that_was_asked_for_is_returned() {
        let tree = Tree::new("platform");
        tree.file("node_modules/@dbx-app/cli-darwin-arm64/bin/dbx", b"\x7fELF");
        let expected = tree.file("node_modules/@dbx-app/cli-darwin-x64/bin/dbx", b"\x7fELF");
        let found = native_under(&tree.0.join("node_modules"), &["cli-darwin-x64".to_string()], "dbx");
        assert_eq!(found, Some(expected));
    }

    #[test]
    fn a_shim_names_the_package_the_native_binary_is_in() {
        let tree = Tree::new("shim");
        let expected =
            tree.file("lib/node_modules/@dbx-app/cli/node_modules/@dbx-app/cli-darwin-arm64/bin/dbx", b"\x7fELF");
        let shim = tree.file("lib/node_modules/@dbx-app/cli/bin/dbx.js", b"#!/usr/bin/env node\n");
        assert!(is_script(&shim), "a `#!` file is a script, not a program");
        let found =
            shim_node_modules(&shim).iter().find_map(|modules| native_under(modules, &arm64(), "dbx"));
        assert_same_path(found, &expected);
    }

    #[test]
    fn a_shim_also_finds_a_hoisted_package_above_it() {
        let tree = Tree::new("shim-hoisted");
        let expected = tree.file("lib/node_modules/@dbx-app/cli-darwin-arm64/bin/dbx", b"\x7fELF");
        let shim = tree.file("lib/node_modules/@dbx-app/cli/bin/dbx.js", b"#!/usr/bin/env node\n");
        let found =
            shim_node_modules(&shim).iter().find_map(|modules| native_under(modules, &arm64(), "dbx"));
        assert_same_path(found, &expected);
    }

    #[test]
    fn a_binary_is_not_mistaken_for_a_shim() {
        let tree = Tree::new("binary");
        assert!(!is_script(&tree.file("bin/dbx", b"\x7fELF\x02\x01\x01")));
        assert!(!is_script(&tree.0.join("nothing-here")));
    }

    #[test]
    fn platform_package_names_use_the_clis_spelling() {
        assert_eq!(platform_package_names_for("macos", "aarch64"), vec!["cli-darwin-arm64", "cli-darwin-x64"]);
        assert_eq!(platform_package_names_for("macos", "x86_64"), vec!["cli-darwin-x64", "cli-darwin-arm64"]);
        assert_eq!(platform_package_names_for("windows", "x86_64"), vec!["cli-win32-x64"]);
        assert_eq!(
            platform_package_names_for("linux", "x86_64"),
            vec!["cli-linux-x64", "cli-linux-x64-gnu", "cli-linux-x64-musl"]
        );
    }

    #[test]
    fn the_executable_is_named_per_platform() {
        assert_eq!(native_name_for("windows"), "dbx.exe");
        assert_eq!(native_name_for("macos"), "dbx");
        assert_eq!(native_name_for("linux"), "dbx");
    }

    #[test]
    fn version_directories_are_listed_newest_first() {
        let tree = Tree::new("versions");
        tree.dir(".nvm/versions/node/v9.11.2");
        tree.dir(".nvm/versions/node/v20.11.0");
        tree.dir(".nvm/versions/node/v18.20.4");
        let found = versioned_node_modules(&tree.0.join(".nvm/versions/node"), "");
        let names: Vec<String> = found
            .iter()
            .filter_map(|path| Some(path.parent()?.parent()?.file_name()?.to_str()?.to_string()))
            .collect();
        assert_eq!(names, vec!["v20.11.0", "v18.20.4", "v9.11.2"]);
        assert!(found[0].ends_with("lib/node_modules"));
        assert!(versioned_node_modules(&tree.0.join(".nvm/versions/nowhere"), "").is_empty());
    }

    #[test]
    fn a_version_is_compared_as_numbers_not_text() {
        assert_eq!(version_key(Path::new("v20.11.0")), vec![20, 11, 0]);
        assert!(version_key(Path::new("v20.11.0")) > version_key(Path::new("v9.11.2")));
    }

    #[test]
    fn a_version_manager_can_hide_the_prefix_behind_an_extra_directory() {
        let tree = Tree::new("fnm");
        tree.dir("node-versions/v20.11.0/installation");
        let found = versioned_node_modules(&tree.0.join("node-versions"), "installation");
        assert_eq!(found, vec![tree.0.join("node-versions/v20.11.0/installation/lib/node_modules")]);
    }
}
