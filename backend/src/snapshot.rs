//! Snapshot collection, and the background-job state that `diff` also drives.
//!
//! Collection runs on a background thread. A single plugin invoke is capped at
//! 120s by the host (`pluginHostBridge.ts:578`), so the start methods return
//! immediately and the UI polls the job.
//!
//! The tables are fixed (`tables.rs`). The two filters come from the caller (the
//! input boxes on screen) and are **recorded in the snapshot** -- not to be
//! silently re-applied later, but so a diff has something to compare the current
//! inputs against and can ask before running with a different condition.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use chrono::{Local, SecondsFormat};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::cli;
use crate::config;
use crate::encode;
use crate::schema;
use crate::store::{self, SchemaMeta, SessionInfo, SnapshotMeta, TableMeta};
use crate::dialect::{self, Dialect};
use crate::tables::{self, ResolvedSpec};

const QUERY_TIMEOUT: Duration = Duration::from_secs(180);
const POLL_HINT_MS: u64 = 500;

#[derive(Debug)]
pub enum Abort {
    Cancelled,
    Failed(String),
}

impl From<String> for Abort {
    fn from(message: String) -> Self {
        Abort::Failed(message)
    }
}

impl From<cli::CliError> for Abort {
    fn from(error: cli::CliError) -> Self {
        if error.code == cli::CANCELLED {
            Abort::Cancelled
        } else {
            Abort::Failed(format!("[{}] {}", error.code, error.message))
        }
    }
}

// ---------------------------------------------------------------- job state

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobView {
    pub kind: String,
    pub phase: String,
    pub connection: String,
    pub database: String,
    pub detail: String,
    pub tables_done: u32,
    pub tables_total: u32,
    pub rows: u64,
    pub snapshot_id: Option<String>,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub started_at: String,
    pub poll_hint_ms: u64,
}

#[derive(Debug)]
pub struct Job {
    pub kind: String,
    pub phase: String,
    pub connection: String,
    pub database: String,
    pub detail: String,
    pub tables_done: u32,
    pub tables_total: u32,
    pub rows: u64,
    pub snapshot_id: Option<String>,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub started_at: String,
    cancel: Arc<AtomicBool>,
}

fn jobs() -> &'static Mutex<Option<Job>> {
    static JOBS: OnceLock<Mutex<Option<Job>>> = OnceLock::new();
    JOBS.get_or_init(|| Mutex::new(None))
}

pub fn with_job<R>(update: impl FnOnce(&mut Job) -> R) -> Option<R> {
    jobs().lock().ok().and_then(|mut guard| guard.as_mut().map(update))
}

/// Take the job slot and hand back a cancellation flag. Refuses if one is
/// already running -- two concurrent jobs would race on the same snapshot store.
pub fn begin_job(kind: &str, connection: &str, database: &str, total: u32) -> Result<Arc<AtomicBool>, Abort> {
    let mut guard = jobs().lock().map_err(|_| Abort::Failed("任务状态锁不可用".to_string()))?;
    if let Some(job) = guard.as_ref() {
        if job.phase == "running" {
            return Err(Abort::Failed(format!(
                "已有正在运行的任务（{} · {} · {}）—— 等它结束或取消后再试",
                job.kind, job.connection, job.database
            )));
        }
    }
    let cancel = Arc::new(AtomicBool::new(false));
    *guard = Some(Job {
        kind: kind.to_string(),
        phase: "running".to_string(),
        connection: connection.to_string(),
        database: database.to_string(),
        detail: "准备中".to_string(),
        tables_done: 0,
        tables_total: total,
        rows: 0,
        snapshot_id: None,
        result: None,
        error: None,
        started_at: Local::now().to_rfc3339_opts(SecondsFormat::Secs, false),
        cancel: cancel.clone(),
    });
    Ok(cancel)
}

/// `result` is the job's payload for the UI: the new snapshot id for a snapshot
/// run, the diff summary for a diff run.
pub fn settle(outcome: Result<Value, Abort>) {
    let _ = with_job(|job| match outcome {
        Ok(result) => {
            job.phase = "done".to_string();
            job.snapshot_id = result.get("snapshotId").and_then(Value::as_str).map(str::to_string);
            job.result = Some(result);
            job.detail = "完成".to_string();
        }
        Err(Abort::Cancelled) => {
            job.phase = "cancelled".to_string();
            job.error = Some("已取消".to_string());
        }
        Err(Abort::Failed(message)) => {
            job.phase = "failed".to_string();
            job.error = Some(message);
        }
    });
}

pub fn status() -> Value {
    match jobs().lock() {
        Ok(guard) => match guard.as_ref() {
            Some(job) => serde_json::to_value(JobView {
                kind: job.kind.clone(),
                phase: job.phase.clone(),
                connection: job.connection.clone(),
                database: job.database.clone(),
                detail: job.detail.clone(),
                tables_done: job.tables_done,
                tables_total: job.tables_total,
                rows: job.rows,
                snapshot_id: job.snapshot_id.clone(),
                result: job.result.clone(),
                error: job.error.clone(),
                started_at: job.started_at.clone(),
                poll_hint_ms: POLL_HINT_MS,
            })
            .unwrap_or_else(|_| json!({ "phase": "unknown" })),
            None => json!({ "phase": "idle" }),
        },
        Err(_) => json!({ "phase": "unknown", "error": "任务状态锁不可用" }),
    }
}

pub fn cancel() -> Value {
    let cancelled = with_job(|job| {
        if job.phase == "running" {
            job.cancel.store(true, Ordering::Relaxed);
            true
        } else {
            false
        }
    })
    .unwrap_or(false);
    json!({ "cancelRequested": cancelled })
}

pub fn cancelled() -> bool {
    with_job(|job| job.cancel.load(Ordering::Relaxed)).unwrap_or(false)
}

// ------------------------------------------------------------------ create

pub fn start_create(params: &Value) -> Result<Value, Abort> {
    let connection = required_str(params, "connection")?;
    // The dialect comes from the CLI's own record of the connection, so the
    // panel does not have to know about it and a test driving the sidecar
    // directly gets it for free.
    let dialect = dialect::of_connection(&connection).map_err(Abort::Failed)?;
    let database = resolve_database(&connection, dialect, optional_str(params, "database"))?;
    // The UI sends what is on screen so there is no dependence on it having saved
    // first; the config is the fallback for callers that do not.
    let saved = config::load();
    let filters: BTreeMap<String, String> = match params.get("filters") {
        Some(value) => serde_json::from_value(value.clone())
            .map_err(|error| Abort::Failed(format!("filters 解析失败: {error}")))?,
        None => saved.filters.clone(),
    };
    let schema_filter = optional_str(params, "schemaFilter").unwrap_or(saved.schema_filter);

    // Fail before the job starts rather than a third of the way through.
    if let Err(message) = schema::compile_filter(&schema_filter) {
        return Err(Abort::Failed(message));
    }

    let total = tables::TABLES.len() as u32 + 1;
    let cancel = begin_job("snapshot", &connection, &database, total)?;
    thread::spawn(move || {
        let outcome =
            run_snapshot(&connection, &database, dialect, &filters, &schema_filter, &cancel, None);
        settle(outcome)
    });
    Ok(json!({ "started": true, "pollHintMs": POLL_HINT_MS, "tables": tables::TABLES.len() }))
}

/// Collect a snapshot and write it to `into` (a fresh directory when `None`).
///
/// The diff calls this with a directory it has already prepared, so "compare"
/// also records what the database looked like at compare time without reading
/// everything twice.
pub fn run_snapshot(
    connection: &str,
    database: &str,
    dialect: Dialect,
    filters: &BTreeMap<String, String>,
    schema_filter: &str,
    cancel: &Arc<AtomicBool>,
    prepared: Option<(String, std::path::PathBuf)>,
) -> Result<Value, Abort> {
    // Read the table shapes before anything is created: a catalogue failure
    // should cost nothing, and it is the first thing that says whether these
    // tables even exist here.
    let specs = resolve_specs(connection, dialect, database)?;
    let now = Local::now();
    let (snapshot_id, dir) = match prepared {
        Some(pair) => pair,
        None => {
            let id = store::next_snapshot_id(connection, database, &now.format("%Y%m%d-%H%M%S").to_string());
            let dir = store::prepare_dir(connection, database, &id).map_err(Abort::Failed)?;
            (id, dir)
        }
    };
    let owns_directory = true;
    with_job(|job| job.snapshot_id = Some(snapshot_id.clone()));

    let mut written: Vec<TableMeta> = Vec::new();
    let mut rows_so_far: u64 = 0;
    for (index, spec) in specs.iter().enumerate() {
        if cancel.load(Ordering::Relaxed) {
            if owns_directory {
                cleanup(connection, database, &snapshot_id);
            }
            return Err(Abort::Cancelled);
        }
        let filter = effective_filter(spec, filters.get(&spec.table).map(String::as_str).unwrap_or(""));
        with_job(|job| {
            job.tables_done = index as u32;
            job.detail = format!("读取数据 {}/{} · {}", index + 1, specs.len(), spec.table);
        });

        let collected =
            collect_table_meta(connection, database, dialect, spec, &spec.columns, &filter, &dir, &mut |_| Ok(()));
        let table_meta = match collected {
            Ok(value) => value,
            Err(error) => {
                if owns_directory {
                    cleanup(connection, database, &snapshot_id);
                }
                return Err(error);
            }
        };
        rows_so_far += table_meta.row_count;
        written.push(table_meta);
        with_job(|job| {
            job.tables_done = index as u32 + 1;
            job.rows = rows_so_far;
        });
    }

    // Whole-database schema, last so the progress bar only reaches 100% when the
    // snapshot is actually complete.
    with_job(|job| job.detail = "读取全库表结构".to_string());
    let schema_meta = match write_schema(connection, database, dialect, schema_filter, &dir) {
        Ok(meta) => meta,
        Err(error) => {
            if owns_directory {
                cleanup(connection, database, &snapshot_id);
            }
            return Err(error);
        }
    };
    with_job(|job| job.tables_done = job.tables_total);

    let meta = SnapshotMeta {
        snapshot_id: snapshot_id.clone(),
        connection: connection.to_string(),
        database: database.to_string(),
        taken_at: now.to_rfc3339_opts(SecondsFormat::Secs, false),
        plugin_version: env!("CARGO_PKG_VERSION").to_string(),
        hash_algo_version: encode::HASH_ALGO_VERSION,
        session: probe_session(connection, dialect),
        schema_filter: schema_filter.to_string(),
        data_tables: written,
        schema: Some(schema_meta),
    };
    if let Err(error) = store::write_meta(&dir, &meta) {
        if owns_directory {
            cleanup(connection, database, &snapshot_id);
        }
        return Err(Abort::Failed(error));
    }
    let collapsed: u64 = meta.data_tables.iter().map(|table| table.collapsed).sum();
    Ok(json!({
        "snapshotId": snapshot_id,
        "directory": dir.display().to_string(),
        "dataTables": meta.data_tables,
        "collapsed": collapsed,
    }))
}

fn cleanup(connection: &str, database: &str, snapshot_id: &str) {
    // A cancelled or failed run must not leave a directory that looks like a
    // usable snapshot.
    let _ = store::delete(connection, database, snapshot_id);
}

/// One scanned row, in the shape both the snapshot writer and the diff need.
pub struct ScannedRow {
    /// Identity values. One column for these tables (`tables.rs`).
    pub primary_key: Vec<Option<String>>,
    /// Hash over `hash_columns`, in that order. This is what the comparison
    /// uses, and it is the only hash computed on an ordinary run.
    pub hash: String,
    /// A second hash, over `columns` instead of `hash_columns`.
    ///
    /// `Some` only when the two lists differ -- that is, only on the run where
    /// the baseline covered a different column set. The comparison has to hash
    /// the baseline's columns to be comparable at all, while the snapshot being
    /// taken has to hash its own columns or its `meta.json` would be describing
    /// hashes it does not contain. One of those is enough on every other run,
    /// which is why this is an `Option` and not a second hash.
    pub snapshot_hash: Option<String>,
    /// The table's own columns' values, in ordinal order -- the shape the
    /// snapshot file is written in.
    pub values: Vec<Option<String>>,
}

impl ScannedRow {
    /// The hash the snapshot file should record.
    pub fn for_snapshot(&self) -> &str {
        self.snapshot_hash.as_deref().unwrap_or(&self.hash)
    }
}

#[derive(Debug, Default)]
pub struct ScanSummary {
    /// Rows handed to the callback -- after collapsing.
    pub row_count: u64,
    /// Rows that shared an identity with an earlier row and were folded into it.
    /// Non-zero means the snapshot cannot represent the source exactly.
    pub collapsed: u64,
    pub collapsed_samples: Vec<String>,
}

/// Both filters for one table: the built-in one from `tables.rs` plus whatever
/// was typed in the UI. Kept as one string so the snapshot can record exactly
/// what produced its hashes.
///
/// The user's text is normalized before joining: a leading `where` / `and` /
/// `or` is stripped (the input boxes say not to write them, but typing one must
/// not produce `... AND and ...`), and the remainder is wrapped in parentheses
/// so `a = 1 OR b = 2` cannot escape the built-in condition through precedence.
pub fn effective_filter(spec: &ResolvedSpec, user_filter: &str) -> String {
    let user = strip_leading_keyword(user_filter.trim());
    match (spec.base_filter.as_deref(), user.is_empty()) {
        (Some(base), false) => format!("{base} AND ({user})"),
        (Some(base), true) => base.to_string(),
        (None, false) => user.to_string(),
        (None, true) => String::new(),
    }
}

fn strip_leading_keyword(mut text: &str) -> &str {
    loop {
        let lowered = text.to_ascii_lowercase();
        let keyword = ["where", "and", "or"].iter().find_map(|keyword| {
            let rest = lowered.strip_prefix(keyword)?;
            // A word boundary, not just a prefix: `android` is not `and`.
            if rest.is_empty() || !(rest.as_bytes()[0] as char).is_ascii_alphanumeric() && rest.as_bytes()[0] != b'_' {
                Some(keyword.len())
            } else {
                None
            }
        });
        match keyword {
            Some(len) => text = text[len..].trim_start(),
            None => return text,
        }
    }
}

/// Page through one table and hand every row to `on_row`.
///
/// The same function backs both the snapshot and the diff's live read, which is
/// what guarantees the two sides hash identically.
#[allow(clippy::too_many_arguments)]
pub fn scan_table(
    connection: &str,
    database: &str,
    dialect: Dialect,
    table: &str,
    columns: &[String],
    hash_columns: &[String],
    identity: &[String],
    order_by: &[String],
    filter: &str,
    on_row: &mut dyn FnMut(ScannedRow) -> Result<(), Abort>,
) -> Result<ScanSummary, Abort> {
    // Identity columns come first so the key is a positional slice, then the
    // table's own columns that are not part of it. `hash_columns` is always a
    // subset of `columns` -- see `diff::start` -- so the select list covers
    // both and one read serves either.
    let select_columns = encode::dedup(&[identity.to_vec(), columns.to_vec()].concat());
    let table_ref = dialect.qualify(database, table);
    let where_clause = if filter.trim().is_empty() { String::new() } else { format!(" WHERE {}", filter.trim()) };
    let positions: Vec<Option<usize>> =
        columns.iter().map(|name| select_columns.iter().position(|selected| selected == name)).collect();
    let hash_positions: Vec<Option<usize>> =
        hash_columns.iter().map(|name| select_columns.iter().position(|selected| selected == name)).collect();
    if positions.iter().any(Option::is_none) || hash_positions.iter().any(Option::is_none) {
        return Err(Abort::Failed(format!("表 '{table}' 的列不在选择列表里")));
    }

    let mut summary = ScanSummary::default();
    // Identity is not unique, so paging sorts on the database's own primary key
    // (`TableSpec::order_by`), which is unique: page boundaries are defined, the
    // sort is on two indexed short columns, and TEXT/BLOB columns never enter
    // the sort key at all.
    let order_by = dialect.quote_ident_list(order_by);
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();

    let mut page: u64 = 0;
    loop {
        if cancelled() {
            return Err(Abort::Cancelled);
        }
        let sql = format!(
            "SELECT {} FROM {}{} ORDER BY {}",
            dialect.quote_ident_list(&select_columns),
            table_ref,
            where_clause,
            order_by,
        );
        let sql = dialect.page(&sql, cli::PAGE_SIZE, page * cli::PAGE_SIZE as u64);
        // Cancellable: a page over a slow link can take minutes, and waiting for
        // it to return before honouring "cancel" is what made cancelling hang.
        let result = cli::query_with_cancel(connection, &sql, QUERY_TIMEOUT, &cancelled).map_err(|error| {
            Abort::from(cli::CliError::new(
                error.code.clone(),
                format!("读取表 '{table}' 第 {} 页失败: {}", page + 1, error.message),
            ))
        })?;
        let returned = cli::result_columns(&result);
        if returned.len() != select_columns.len() {
            return Err(Abort::Failed(format!(
                "表 '{table}' 返回了 {} 列，期望 {} 列",
                returned.len(),
                select_columns.len()
            )));
        }
        let rows = cli::result_rows(&result);
        let fetched = rows.len();

        for row in &rows {
            let values: Vec<Option<String>> =
                select_columns.iter().map(|name| row.get(name).and_then(encode::canonical)).collect();
            let primary_key_values = values[..identity.len()].to_vec();
            let key = encode::encode_values(&primary_key_values);
            // First occurrence wins, deterministically, because the ORDER BY is
            // total. Never silently: the count and a few ids reach the report.
            if !seen.insert(key) {
                summary.collapsed += 1;
                if summary.collapsed_samples.len() < 10 {
                    summary
                        .collapsed_samples
                        .push(primary_key_values.iter().map(|v| v.clone().unwrap_or_else(|| "NULL".into())).collect::<Vec<_>>().join(", "));
                }
                continue;
            }
            let configured: Vec<Option<String>> =
                positions.iter().map(|index| values[index.expect("checked above")].clone()).collect();
            // The steady state: one projection, one hash, exactly as before. The
            // second hash is only paid for on a run that has to reconcile two
            // different column sets.
            let (hash, snapshot_hash) = if hash_columns == columns {
                (encode::sha256_hex(&encode::encode_values(&configured)), None)
            } else {
                let comparable: Vec<Option<String>> =
                    hash_positions.iter().map(|index| values[index.expect("checked above")].clone()).collect();
                (
                    encode::sha256_hex(&encode::encode_values(&comparable)),
                    Some(encode::sha256_hex(&encode::encode_values(&configured))),
                )
            };
            on_row(ScannedRow {
                primary_key: primary_key_values,
                hash,
                snapshot_hash,
                values: configured,
            })?;
            summary.row_count += 1;
        }

        with_job(|job| job.rows = summary.row_count);
        page += 1;
        if fetched < cli::PAGE_SIZE {
            break;
        }
    }
    Ok(summary)
}

/// Scan one table, write its `data/<table>.jsonl` into `dir`, and hand every row
/// to `on_row` as well.
///
/// This is the single place a table is read. The snapshot uses it with a no-op
/// `on_row`; the diff uses it with one that fills the comparison map. Because
/// both go through here, the hashes on the two sides cannot drift apart -- and
/// the diff gets the "also record what things look like now" snapshot for free,
/// without a second read.
#[allow(clippy::too_many_arguments)]
pub fn collect_table_meta(
    connection: &str,
    database: &str,
    dialect: Dialect,
    spec: &ResolvedSpec,
    hash_columns: &[String],
    filter: &str,
    dir: &Path,
    on_row: &mut dyn FnMut(ScannedRow) -> Result<(), Abort>,
) -> Result<TableMeta, Abort> {
    let columns = spec.columns.clone();
    let identity = spec.identity.clone();
    let order_by = spec.order_by.clone();
    let file_name = format!("data/{}.jsonl", spec.table);
    let path = dir.join(&file_name);
    let file =
        File::create(&path).map_err(|error| Abort::Failed(format!("无法创建 {}: {error}", path.display())))?;
    let mut writer = BufWriter::new(file);
    let mut hasher = Sha256::new();

    let summary = scan_table(
        connection,
        database,
        dialect,
        &spec.table,
        &columns,
        hash_columns,
        &identity,
        &order_by,
        filter,
        &mut |row| {
            // The file records the hash over the snapshot's own columns, which
            // is what `TableMeta::columns` below claims it covers.
        let line = json!({ "p": row.primary_key, "h": row.for_snapshot() });
        let mut bytes = serde_json::to_vec(&line).map_err(|error| Abort::Failed(error.to_string()))?;
        bytes.push(b'\n');
        hasher.update(&bytes);
        writer.write_all(&bytes).map_err(|error| Abort::Failed(format!("写入失败: {error}")))?;
            on_row(row)
        },
    )?;
    writer.flush().map_err(|error| Abort::Failed(format!("写入 {} 失败: {error}", path.display())))?;
    drop(writer);

    // Page count is only ever reported, so "one page per 10k rows, at least one"
    // is enough -- the authoritative number is `row_count`.
    let pages = summary.row_count.div_ceil(cli::PAGE_SIZE as u64).max(1);
    let where_clause = if filter.trim().is_empty() { String::new() } else { format!(" WHERE {}", filter.trim()) };
    let table_ref = dialect.qualify(database, &spec.table);
    let count_star = count_rows(connection, &table_ref, &where_clause).ok();

    Ok(TableMeta {
        table: spec.table.to_string(),
        columns,
        primary_key: identity,
        filter: filter.to_string(),
        row_count: summary.row_count,
        count_star,
        collapsed: summary.collapsed,
        collapsed_samples: summary.collapsed_samples,
        pages,
        sha256: encode::hex(&hasher.finalize()),
        file: file_name,
    })
}

/// The whole-database schema for a snapshot, written to `dir/schema.jsonl`.
pub fn write_schema(
    connection: &str,
    database: &str,
    dialect: Dialect,
    schema_filter: &str,
    dir: &Path,
) -> Result<SchemaMeta, Abort> {
    let mut objects = schema::collect(connection, database, dialect)?;
    schema::filter_objects(&mut objects, schema_filter).map_err(Abort::Failed)?;
    let path = dir.join("schema.jsonl");
    schema::write_jsonl(&path, &objects).map_err(Abort::Failed)?;
    let bytes = std::fs::read(&path).map_err(|error| Abort::Failed(error.to_string()))?;
    Ok(SchemaMeta {
        object_count: objects.len(),
        table_count: objects.iter().filter(|object| object.o == "table").count(),
        sha256: encode::sha256_hex(&bytes),
    })
}

/// Counted with the **same filter** as the rows, so the two are comparable.
fn count_rows(connection: &str, table_ref: &str, where_clause: &str) -> Result<u64, Abort> {
    let sql = format!("SELECT COUNT(*) AS n FROM {table_ref}{where_clause}");
    let result = cli::query(connection, &sql, QUERY_TIMEOUT).map_err(Abort::from)?;
    let rows = cli::result_rows(&result);
    let value = rows.first().and_then(|row| row.get("n")).and_then(encode::canonical);
    match value.and_then(|text| text.parse::<u64>().ok()) {
        Some(count) => Ok(count),
        None => Err(Abort::Failed("COUNT(*) 没有返回可解析的数字".to_string())),
    }
}

/// Best effort -- only ever recorded, never acted on (PLAN.md §2.1).
fn probe_session(connection: &str, dialect: Dialect) -> SessionInfo {
    match cli::query(connection, dialect.session_probe_sql(), Duration::from_secs(30)) {
        Ok(result) => {
            let row = cli::result_rows(&result).into_iter().next().unwrap_or_else(|| json!({}));
            SessionInfo {
                time_zone: row.get("tz").and_then(encode::canonical),
                sql_mode: row.get("mode").and_then(encode::canonical),
                charset: row.get("charset").and_then(encode::canonical),
            }
        }
        Err(_) => SessionInfo::default(),
    }
}

// ----------------------------------------------------------------- helpers

pub fn resolve_database(
    connection: &str,
    dialect: Dialect,
    configured: Option<String>,
) -> Result<String, Abort> {
    if let Some(database) = configured.filter(|value| !value.trim().is_empty()) {
        return Ok(database);
    }
    let result = cli::query(connection, dialect.current_database_sql(), Duration::from_secs(60))
        .map_err(|error| Abort::Failed(format!("读取连接默认库失败 [{}]: {}", error.code, error.message)))?;
    let database = cli::result_rows(&result)
        .first()
        .and_then(|row| row.get("db"))
        .and_then(encode::canonical)
        .unwrap_or_default();
    if database.trim().is_empty() {
        return Err(Abort::Failed(format!(
            "连接 '{connection}' 没有默认数据库，请在连接后面选一个库"
        )));
    }
    Ok(database)
}

/// The six tables as this database has them: names folded for the dialect, and
/// **every column the table currently has**.
///
/// The column list is discovered rather than compiled in, because it is not the
/// same everywhere: `fnec_prod` has 36 of `dsfa_rm`'s columns and `eb169` has
/// 41. What a snapshot covers is recorded in it, and a diff hashes the live rows
/// over the *baseline's* list, so drift is visible rather than fatal. See
/// `tables::ResolvedSpec`.
pub fn resolve_specs(
    connection: &str,
    dialect: Dialect,
    database: &str,
) -> Result<Vec<ResolvedSpec>, Abort> {
    let names: Vec<String> =
        tables::TABLES.iter().map(|spec| format!("'{}'", dialect.fold(spec.table))).collect();
    let sql = format!(
        "SELECT table_name AS t, column_name AS c FROM information_schema.columns \
         WHERE table_schema = {} AND table_name IN ({}) \
         ORDER BY table_name, ordinal_position",
        dialect.schema_expr(database),
        names.join(", ")
    );
    let result = cli::query(connection, &sql, QUERY_TIMEOUT).map_err(|error| {
        Abort::from(cli::CliError::new(
            error.code.clone(),
            format!("读取 {} 的表结构失败: {}", dialect.label(), error.message),
        ))
    })?;

    let mut found: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in cli::result_rows(&result) {
        let (Some(table), Some(column)) =
            (row.get("t").and_then(encode::canonical), row.get("c").and_then(encode::canonical))
        else {
            continue;
        };
        found.entry(dialect.fold(&table)).or_default().push(dialect.fold(&column));
    }

    let specs = tables::resolve_all(dialect, &found);
    let missing: Vec<&str> =
        specs.iter().filter(|spec| spec.columns.is_empty()).map(|spec| spec.table.as_str()).collect();
    if !missing.is_empty() {
        return Err(Abort::Failed(format!(
            "在 {} 上找不到这些表：{}。检查连接指向的库 / schema 对不对。",
            dialect.label(),
            missing.join(", ")
        )));
    }
    Ok(specs)
}

fn required_str(params: &Value, key: &str) -> Result<String, Abort> {
    optional_str(params, key)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Abort::Failed(format!("缺少参数 {key}")))
}

fn optional_str(params: &Value, key: &str) -> Option<String> {
    params.get(key).and_then(Value::as_str).map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tables::ResolvedSpec;

    fn spec(base: Option<&'static str>) -> ResolvedSpec {
        ResolvedSpec {
            table: "t".to_string(),
            columns: vec!["id".to_string()],
            identity: vec!["id".to_string()],
            order_by: vec!["id".to_string()],
            base_filter: base.map(str::to_string),
        }
    }

    /// A row with no second hash hands the snapshot the comparison one. This is
    /// the branch every ordinary run takes -- the other one only exists for a
    /// run that had to reconcile two column sets.
    #[test]
    fn a_row_without_a_second_hash_snapshots_the_first() {
        let row = ScannedRow {
            primary_key: vec![Some("a".to_string())],
            hash: "comparison".to_string(),
            snapshot_hash: None,
            values: vec![Some("a".to_string())],
        };
        assert_eq!(row.for_snapshot(), "comparison");
    }

    /// And uses the second when there is one, because the snapshot's own
    /// `columns` describes *that* hash.
    #[test]
    fn a_row_with_a_second_hash_prefers_it() {
        let row = ScannedRow {
            primary_key: vec![Some("a".to_string())],
            hash: "comparison".to_string(),
            snapshot_hash: Some("snapshot".to_string()),
            values: vec![Some("a".to_string()), Some("b".to_string())],
        };
        assert_eq!(row.for_snapshot(), "snapshot");
    }

    #[test]
    fn strips_a_leading_and_or_where() {
        let with_base = spec(Some("ds_active = '1'"));
        // Exactly what a user typed in the bug report.
        assert_eq!(
            effective_filter(&with_base, "and id not like 'test%'"),
            "ds_active = '1' AND (id not like 'test%')"
        );
        assert_eq!(effective_filter(&with_base, "  AND id = 1  "), "ds_active = '1' AND (id = 1)");
        assert_eq!(effective_filter(&with_base, "where id = 1"), "ds_active = '1' AND (id = 1)");
        assert_eq!(effective_filter(&with_base, "OR id = 1"), "ds_active = '1' AND (id = 1)");
        assert_eq!(effective_filter(&with_base, ""), "ds_active = '1'");
        assert_eq!(effective_filter(&with_base, "and"), "ds_active = '1'");
    }

    #[test]
    fn does_not_eat_column_names_starting_with_keywords() {
        let with_base = spec(Some("ds_active = '1'"));
        assert_eq!(
            effective_filter(&with_base, "android = 'x'"),
            "ds_active = '1' AND (android = 'x')"
        );
        assert_eq!(effective_filter(&with_base, "whereabouts = 1"), "ds_active = '1' AND (whereabouts = 1)");
    }

    #[test]
    fn user_part_is_parenthesized() {
        // Without the parens, `OR` would escape the built-in condition.
        let with_base = spec(Some("ds_active = '1'"));
        assert_eq!(
            effective_filter(&with_base, "a = 1 OR b = 2"),
            "ds_active = '1' AND (a = 1 OR b = 2)"
        );
        let no_base = spec(None);
        assert_eq!(effective_filter(&no_base, "a = 1"), "a = 1");
        assert_eq!(effective_filter(&no_base, ""), "");
    }
}
