//! The diff job: read a baseline snapshot, read the live database, compare both
//! channels, and write the files a human takes to the other environment.
//!
//! Comparing also **records what the database looks like right now** as a new
//! snapshot. That costs nothing extra: each table is read once, and the same
//! scan feeds both the comparison and the new snapshot's data file.
//!
//! Nothing here executes anything. The plugin never connects to the target
//! environment and never writes to the source database (PLAN.md §6).
//!
//! Each table's filter comes from the **caller** -- the input boxes on screen.
//! The baseline's recorded filter is only the fallback for callers that do not
//! send one, and the comparison point for the confirmation: when the two differ,
//! `start` refuses to launch the job and returns the differences instead, so the
//! UI can ask "当前过滤条件与快照过滤条件不一致，是否继续" before anything runs.
//! Whatever is finally used is recorded in the new snapshot, so the next compare
//! against it needs no confirmation.
//!
//! The database is the caller's, not the baseline's: it is half the snapshot's
//! identity (`store.rs`), so the UI picks it before it can even list snapshots.
//! A baseline whose recorded database is not the one being read is refused
//! outright rather than confirmed -- "compare A's snapshot against B" is not a
//! question with a safe answer, it is a stale panel.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use chrono::{Local, SecondsFormat};
use serde_json::{json, Value};

use crate::config;
use crate::dialect::{self, Dialect};
use crate::encode::{self, quote_string};
use crate::schema::{self, SchemaDiff, SchemaObject};
use crate::snapshot::{self, Abort, ScannedRow};
use crate::store::{self, SchemaMeta, SnapshotMeta, TableMeta};
use crate::tables::{self, ResolvedSpec};

/// Identity values per DELETE / backup statement.
///
/// `IN` lists are bounded by memory, not by a count: with the default
/// `range_optimizer_max_mem_size` (8 MB) MySQL stops using the index at roughly
/// **10,000** elements for a single-column list and warns 3170, then falls back
/// to a full scan -- measured on 8.0.36, and it is a scan rather than an error.
/// 500 is twenty times inside that. Long ids move the threshold down
/// proportionally; the byte size of one statement here is what matters, and at
/// these ids it is about 18 KB.
const CHUNK: usize = 500;

/// `row key -> (identity values, hash, configured column values)`
type LiveMap = HashMap<Vec<u8>, (Vec<Option<String>>, String, Vec<Option<String>>)>;
/// `row key -> (identity values, hash)`
type BaselineMap = HashMap<Vec<u8>, (Vec<Option<String>>, String)>;

/// Layer the baseline's column lists over the live ones.
///
/// Pure, so the three rules can be tested without a database:
///
/// - **Gained a column** -- reconcile. Hash the live rows over the baseline's
///   columns (which all still exist), and let the new snapshot record the wider
///   set so the next comparison starts aligned.
/// - **Lost a column** -- refuse. The baseline's hash covers a value that is not
///   there any more, so there is nothing to recompute, and comparing anyway
///   would report every row as modified.
/// - **Identity changed** -- refuse. That redefines what a row is.
fn plan_comparisons(
    specs: &[ResolvedSpec],
    baseline: &SnapshotMeta,
) -> Result<Vec<Comparison>, Abort> {
    let mut comparisons = Vec::with_capacity(specs.len());
    for spec in specs {
        let Some(table) = baseline.data_tables.iter().find(|table| table.table == spec.table) else {
            return Err(Abort::Failed(format!("基准快照里没有表 {}，重新打一次", spec.table)));
        };
        // An identity that changed redefines what a row *is*. There is nothing
        // to reconcile, so this still refuses.
        if table.primary_key != spec.identity {
            return Err(Abort::Failed(format!(
                "表 {} 的标识列变了：快照是 [{}]，当前是 [{}] —— 重新打一次快照",
                spec.table,
                table.primary_key.join(", "),
                spec.identity.join(", ")
            )));
        }

        // A column the baseline hashed and this table no longer has cannot be
        // hashed at all -- there is no value to hash. Every row would come out
        // "modified", which is worse than saying so.
        let live: std::collections::HashSet<&String> = spec.columns.iter().collect();
        let dropped: Vec<String> =
            table.columns.iter().filter(|column| !live.contains(column)).cloned().collect();
        if !dropped.is_empty() {
            return Err(Abort::Failed(format!(
                "表 {} 的列 {} 在基准快照里参与 hash，但本库已经没有这些列了 —— 算不出可比的 hash，重新打一次快照",
                spec.table,
                dropped.join(", ")
            )));
        }

        // Gaining a column *is* reconcilable: the baseline's columns all still
        // exist, so hash the live rows over them and let the new snapshot pick
        // up the wider set.
        let known: std::collections::HashSet<&String> = table.columns.iter().collect();
        let added: Vec<String> =
            spec.columns.iter().filter(|column| !known.contains(column)).cloned().collect();
        comparisons.push(Comparison {
            hash_columns: table.columns.clone(),
            added_columns: added,
            spec: spec.clone(),
        });
    }
    Ok(comparisons)
}

/// The filters one compare actually runs with: the caller's input where given,
/// the baseline's record otherwise. Whatever ends up here is what the live read
/// uses and what the new snapshot records -- the two cannot disagree.
/// One table as this comparison runs it.
///
/// Two column lists, because they stop being the same list the moment a table
/// gains a column:
///
/// - `hash_columns` is what the rows are hashed over -- **the baseline's**, so
///   the two sides mean the same thing. Hashing the live list instead would be
///   comparing rows whose hash covers a different set of values.
/// - `spec.columns` is what the table has now, and what the new snapshot
///   records, so the next comparison starts from the current shape.
#[derive(Debug)]
struct Comparison {
    spec: ResolvedSpec,
    hash_columns: Vec<String>,
    /// Columns the table has now that the baseline did not. Non-empty means the
    /// comparison covers less than the table does, and the report says so.
    added_columns: Vec<String>,
}

struct ResolvedFilters {
    /// table -> effective WHERE (built-in plus user input), without `where`.
    tables: HashMap<String, String>,
    schema_filter: String,
    /// (table, baseline filter, used filter) for every table where they differ.
    changed: Vec<(String, String, String)>,
    /// (baseline schema filter, used schema filter) when they differ.
    schema_changed: Option<(String, String)>,
}

impl ResolvedFilters {
    fn changed(&self) -> bool {
        !self.changed.is_empty() || self.schema_changed.is_some()
    }
}

pub fn start(params: &Value) -> Result<Value, Abort> {
    let connection = required_str(params, "connection")?;
    let snapshot_id = required_str(params, "snapshotId")?;
    let dialect = dialect::of_connection(&connection).map_err(Abort::Failed)?;

    // Required, not inferred. The database is half the snapshot's identity, so
    // it is what says *which* baseline this is; guessing it from DATABASE()
    // would silently pick a different history on a connection that has moved.
    let database = required_str(params, "database")?;
    let dir = store::snapshot_dir(&connection, &database, &snapshot_id);
    if !dir.is_dir() {
        // The usual cause is a panel showing a list built for another database,
        // which is a stale list rather than a wrong question.
        return Err(Abort::Failed(format!(
            "快照 {snapshot_id} 不在库 `{database}` 下 —— 刷新一下快照列表再点"
        )));
    }
    let baseline = store::read_meta(&dir).map_err(Abort::Failed)?;
    // Same id can exist under two databases (the id is a timestamp). This is
    // the check that keeps that from being read as "the baseline changed".
    if baseline.database != database {
        return Err(Abort::Failed(format!(
            "快照 {snapshot_id} 记录的是库 `{}`，当前选的是 `{database}` —— 刷新一下快照列表",
            baseline.database
        )));
    }
    if baseline.hash_algo_version != encode::HASH_ALGO_VERSION {
        return Err(Abort::Failed(format!(
            "快照的 hash 算法版本是 {}，当前插件是 {} —— 两者不可比",
            baseline.hash_algo_version,
            encode::HASH_ALGO_VERSION
        )));
    }
    if baseline.schema.is_none() {
        return Err(Abort::Failed(format!(
            "快照 {snapshot_id} 里没有结构数据 —— 它是更早版本打的快照。重新打一次即可。"
        )));
    }
    // The live table shapes. The column list is discovered, so this is where a
    // database that has drifted from the baseline shows up.
    let specs = snapshot::resolve_specs(&connection, dialect, &database)?;

    let comparisons = plan_comparisons(&specs, &baseline)?;

    // One step for the live schema, one per data table.
    let resolved = resolve_filters(params, &baseline, &specs)?;
    if resolved.changed() && !params.get("force").and_then(Value::as_bool).unwrap_or(false) {
        // Do not launch the job. The UI shows the differences and asks whether
        // to continue; a second call with `force: true` goes through.
        return Ok(json!({
            "needsConfirm": true,
            "baseline": snapshot_id,
            "filterDiffs": resolved.changed.iter().map(|(table, old, new)| json!({
                "table": table, "baseline": old, "current": new,
            })).collect::<Vec<_>>(),
            "schemaFilterDiff": resolved.schema_changed.as_ref().map(|(old, new)| json!({
                "baseline": old, "current": new,
            })),
        }));
    }

    let total = 1 + comparisons.len() as u32;
    let cancel = snapshot::begin_job("diff", &connection, &database, total)?;
    thread::spawn(move || {
        let outcome = run(
            &connection, &database, dialect, &snapshot_id, &dir, &baseline, &comparisons, &resolved,
            &cancel,
        );
        snapshot::settle(outcome)
    });
    Ok(json!({ "started": true, "pollHintMs": 500 }))
}

/// Effective filters for this run: the caller's input where given, the
/// baseline's record where not. Anything absent from the input falls back
/// table-by-table, so a caller that sends nothing gets the old behavior.
fn resolve_filters(
    params: &Value,
    baseline: &SnapshotMeta,
    specs: &[ResolvedSpec],
) -> Result<ResolvedFilters, Abort> {
    let input: Option<BTreeMap<String, String>> = match params.get("filters") {
        Some(value) => Some(
            serde_json::from_value(value.clone()).map_err(|error| Abort::Failed(format!("filters 解析失败: {error}")))?,
        ),
        None => None,
    };
    let input_schema: Option<String> = params.get("schemaFilter").and_then(Value::as_str).map(str::trim).map(str::to_string);

    let mut tables = HashMap::new();
    let mut changed = Vec::new();
    for spec in specs {
        let baseline_table = baseline
            .data_tables
            .iter()
            .find(|table| table.table == spec.table)
            .ok_or_else(|| Abort::Failed(format!("基准快照里没有表 {}", spec.table)))?;
        let effective = match &input {
            Some(filters) => {
                snapshot::effective_filter(spec, filters.get(&spec.table).map(String::as_str).unwrap_or(""))
            }
            None => baseline_table.filter.clone(),
        };
        if effective.trim() != baseline_table.filter.trim() {
            changed.push((spec.table.to_string(), baseline_table.filter.clone(), effective.clone()));
        }
        tables.insert(spec.table.to_string(), effective);
    }

    let schema_filter = input_schema.unwrap_or_else(|| baseline.schema_filter.clone());
    let schema_changed = if schema_filter.trim() != baseline.schema_filter.trim() {
        Some((baseline.schema_filter.clone(), schema_filter.clone()))
    } else {
        None
    };

    Ok(ResolvedFilters { tables, schema_filter, changed, schema_changed })
}

/// Open the folder the generated files landed in. Restricted to the configured
/// output root, so the UI cannot ask the sidecar to launch a shell on an
/// arbitrary path.
pub fn reveal(params: &Value) -> Result<Value, Abort> {
    let requested = required_str(params, "path")?;
    let root = config::load().resolved_output_dir();
    let root = root.canonicalize().unwrap_or(root);
    let path = PathBuf::from(&requested);
    let resolved = path.canonicalize().unwrap_or_else(|_| path.clone());
    if !resolved.starts_with(&root) {
        return Err(Abort::Failed(format!("拒绝打开输出目录之外的路径: {}", resolved.display())));
    }
    if !resolved.is_dir() {
        return Err(Abort::Failed(format!("目录不存在: {}", resolved.display())));
    }
    open_in_file_manager(&resolved)?;
    Ok(json!({ "opened": resolved.display().to_string() }))
}

fn open_in_file_manager(path: &Path) -> Result<(), Abort> {
    #[cfg(windows)]
    let result = std::process::Command::new("explorer").arg(path).spawn();
    #[cfg(target_os = "macos")]
    let result = std::process::Command::new("open").arg(path).spawn();
    #[cfg(all(unix, not(target_os = "macos")))]
    let result = std::process::Command::new("xdg-open").arg(path).spawn();
    result.map(|_| ()).map_err(|error| Abort::Failed(format!("无法打开 {}: {error}", path.display())))
}

// --------------------------------------------------------------------- run

#[allow(clippy::too_many_arguments)]
fn run(
    connection: &str,
    database: &str,
    dialect: Dialect,
    baseline_id: &str,
    baseline_dir: &Path,
    baseline: &SnapshotMeta,
    comparisons: &[Comparison],
    filters: &ResolvedFilters,
    cancel: &AtomicBool,
) -> Result<Value, Abort> {
    // The snapshot this run also takes. A failure anywhere below removes it
    // again, so a partial directory can never masquerade as a usable baseline.
    let now_id = store::next_snapshot_id(connection, database, &Local::now().format("%Y%m%d-%H%M%S").to_string());
    let now_dir = store::prepare_dir(connection, database, &now_id).map_err(Abort::Failed)?;
    let outcome = collect_and_compare(
        connection, database, dialect, baseline_id, baseline_dir, baseline, comparisons, filters,
        cancel, &now_id, &now_dir,
    );
    if outcome.is_err() {
        let _ = store::delete(connection, database, &now_id);
    }
    outcome
}

#[allow(clippy::too_many_arguments)]
fn collect_and_compare(
    connection: &str,
    database: &str,
    dialect: Dialect,
    baseline_id: &str,
    baseline_dir: &Path,
    baseline: &SnapshotMeta,
    comparisons: &[Comparison],
    filters: &ResolvedFilters,
    cancel: &AtomicBool,
    now_id: &str,
    now_dir: &Path,
) -> Result<Value, Abort> {
    snapshot::with_job(|job| job.detail = "读取基准快照".to_string());
    let baseline_schema = schema::read_jsonl(&baseline_dir.join("schema.jsonl")).map_err(Abort::Failed)?;
    let baseline_rows = read_baseline_rows(baseline_dir, &baseline.data_tables)?;
    check_cancel(cancel)?;

    snapshot::with_job(|job| {
        job.detail = "读取当前结构".to_string();
        job.tables_done = 1;
    });
    // Read unfiltered: the schema filter narrows what the *schema channel*
    // compares, but the six data tables still need their column types for
    // literal rendering whether or not the filter keeps them.
    let live_all = schema::collect(connection, database, dialect)?;
    let column_types = column_types(&live_all);
    let mut live_filtered = live_all;
    schema::filter_objects(&mut live_filtered, &filters.schema_filter).map_err(Abort::Failed)?;
    check_cancel(cancel)?;

    let schema_diff = schema::diff(&baseline_schema, &live_filtered, dialect);

    let mut deltas: Vec<TableDelta> = Vec::new();
    let mut now_tables: Vec<TableMeta> = Vec::new();
    for (index, comparison) in comparisons.iter().enumerate() {
        let spec = &comparison.spec;
        check_cancel(cancel)?;
        let baseline_table = baseline
            .data_tables
            .iter()
            .find(|table| table.table == spec.table)
            .ok_or_else(|| Abort::Failed(format!("基准快照里没有表 {}", spec.table)))?;
        snapshot::with_job(|job| {
            job.detail = format!("读取当前数据 {}/{} · {}", index + 1, tables::TABLES.len(), spec.table);
        });

        // One scan. It writes the new snapshot's data file and fills the
        // comparison map with the very same rows -- the two cannot disagree.
        // The filter is the resolved one for this run, and the new snapshot
        // records it, so comparing against the new snapshot needs no confirm.
        let used_filter = filters
            .tables
            .get(&spec.table)
            .cloned()
            .unwrap_or_else(|| baseline_table.filter.clone());
        let mut live: LiveMap = HashMap::new();
        let table_meta = snapshot::collect_table_meta(
            connection,
            database,
            dialect,
            spec,
            &comparison.hash_columns,
            &used_filter,
            now_dir,
            &mut |row: ScannedRow| {
                live.insert(encode::encode_values(&row.primary_key), (row.primary_key, row.hash, row.values));
                Ok(())
            },
        )?;
        let baseline_map = baseline_rows.get(&spec.table).cloned().unwrap_or_default();
        // The columns this table gained, as positions in each row's values.
        let added = positions_of(&comparison.added_columns, &spec.columns);
        deltas.push(compare(&spec.table, &baseline_map, &live, &added));
        now_tables.push(table_meta);
        snapshot::with_job(|job| job.tables_done = index as u32 + 2);
    }

    // Record the "as of this compare" snapshot: the schema set just compared
    // and the filters just used.
    snapshot::with_job(|job| job.detail = "写出当前快照".to_string());
    let schema_path = now_dir.join("schema.jsonl");
    schema::write_jsonl(&schema_path, &live_filtered).map_err(Abort::Failed)?;
    let schema_bytes = fs::read(&schema_path).map_err(|error| Abort::Failed(error.to_string()))?;
    let now_meta = SnapshotMeta {
        snapshot_id: now_id.to_string(),
        connection: connection.to_string(),
        database: database.to_string(),
        taken_at: Local::now().to_rfc3339_opts(SecondsFormat::Secs, false),
        plugin_version: env!("CARGO_PKG_VERSION").to_string(),
        hash_algo_version: encode::HASH_ALGO_VERSION,
        session: baseline.session.clone(),
        schema_filter: filters.schema_filter.clone(),
        data_tables: now_tables,
        schema: Some(SchemaMeta {
            object_count: live_filtered.len(),
            table_count: live_filtered.iter().filter(|object| object.o == "table").count(),
            sha256: encode::sha256_hex(&schema_bytes),
        }),
    };
    store::write_meta(now_dir, &now_meta).map_err(Abort::Failed)?;

    snapshot::with_job(|job| job.detail = "写出文件".to_string());
    write_outputs(
        connection, database, dialect, comparisons, baseline, &now_meta.data_tables, baseline_id,
        now_id,
        filters, &schema_diff, &deltas, &column_types,
    )
}

fn check_cancel(cancel: &AtomicBool) -> Result<(), Abort> {
    if cancel.load(Ordering::Relaxed) {
        Err(Abort::Cancelled)
    } else {
        Ok(())
    }
}

fn read_baseline_rows(dir: &Path, tables: &[TableMeta]) -> Result<HashMap<String, BaselineMap>, Abort> {
    let mut all = HashMap::new();
    for table in tables {
        let path = dir.join(&table.file);
        let raw =
            fs::read_to_string(&path).map_err(|error| Abort::Failed(format!("无法读取 {}: {error}", path.display())))?;
        let mut rows = BaselineMap::new();
        for (number, line) in raw.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let parsed: Value = serde_json::from_str(line)
                .map_err(|error| Abort::Failed(format!("{} 第 {} 行解析失败: {error}", path.display(), number + 1)))?;
            let primary_key: Vec<Option<String>> = parsed
                .get("p")
                .and_then(Value::as_array)
                .map(|values| values.iter().map(|value| value.as_str().map(str::to_string)).collect())
                .unwrap_or_default();
            let hash = parsed.get("h").and_then(Value::as_str).unwrap_or_default().to_string();
            if primary_key.len() != table.primary_key.len() {
                return Err(Abort::Failed(format!(
                    "{} 第 {} 行的标识有 {} 列，期望 {} 列 —— 快照与表定义不匹配，重新打一次快照",
                    path.display(),
                    number + 1,
                    primary_key.len(),
                    table.primary_key.len()
                )));
            }
            rows.insert(encode::encode_values(&primary_key), (primary_key, hash));
        }
        all.insert(table.table.clone(), rows);
    }
    Ok(all)
}

/// `(table, column) -> COLUMN_TYPE`, so literals are rendered by the column's own
/// type rather than by guessing from the text.
fn column_types(objects: &[SchemaObject]) -> HashMap<(String, String), String> {
    objects
        .iter()
        .filter(|object| object.o == "column")
        .map(|object| {
            let column_type = object.d.get("columnType").and_then(Value::as_str).unwrap_or("").to_string();
            ((object.t.clone(), object.n.clone()), column_type)
        })
        .collect()
}

#[derive(Debug, Default)]
struct TableDelta {
    /// Carried so every consumer can look the table up **by name** in whichever
    /// snapshot it is reporting on. Never pair a delta with a `TableMeta` by
    /// position: the array order comes from `tables.rs` when the snapshot was
    /// taken, which is not necessarily the order in force now.
    table: String,
    inserts: usize,
    updates: usize,
    /// Of `updates`, the rows whose *old* columns were unchanged and which are
    /// here only because a column the table gained carries a value. Counted
    /// inside `updates`; kept separately so the report can say why they are
    /// there rather than leaving the number unexplained.
    added_column_updates: usize,
    deletes: usize,
    unchanged: usize,
    live_rows: u64,
    /// The ids 03 is about to write over: **every insert, and the new half of
    /// every update**.
    ///
    /// The inserts are on the list because the target may already hold a row
    /// with that id -- one the source never had. Deleting it first is what keeps
    /// the insert from dying on the unique key, and it costs nothing when the
    /// row is not there.
    arriving_keys: Vec<Vec<Option<String>>>,
    /// The ids that are simply gone: in the baseline, not in the live read.
    /// Nothing writes these back, so 04 is the one file that only ever removes.
    removed_keys: Vec<Vec<Option<String>>>,
    /// Rows arriving: inserts plus the new half of every update.
    arriving_values: Vec<Vec<Option<String>>>,
}

/// NULL or an empty string means the column says nothing. A zero-length string
/// is not NULL, but it is the same absence of information for these columns --
/// a `NOT NULL` column cannot hold NULL, so `''` is what "no value" looks like
/// there.
fn carries_a_value(value: &Option<String>) -> bool {
    matches!(value, Some(text) if !text.is_empty())
}

/// Where each of `names` sits in `columns`. Missing names are skipped: a column
/// the table does not have cannot be read out of a row.
fn positions_of(names: &[String], columns: &[String]) -> Vec<usize> {
    names.iter().filter_map(|name| columns.iter().position(|column| column == name)).collect()
}

/// `added` is where, within each row's values, a column sits that the baseline
/// never had.
///
/// Those columns are deliberately outside the hash -- that is what keeps the two
/// sides comparable at all -- so a row that differs *only* in them looks
/// unchanged. It is not: the target does not have that column's value, and the
/// whole point of the comparison is to tell the target what it is missing. So a
/// row carrying something in one of them is written like any other update.
///
/// "Carrying something" is the test, not "the column exists": a table can gain a
/// column that is empty on every row, and writing the whole table for that would
/// be a large amount of SQL to say nothing.
fn compare(table: &str, baseline: &BaselineMap, live: &LiveMap, added: &[usize]) -> TableDelta {
    let mut delta =
        TableDelta { table: table.to_string(), live_rows: live.len() as u64, ..TableDelta::default() };

    for (key, (primary_key, hash, values)) in live {
        match baseline.get(key) {
            None => {
                delta.inserts += 1;
                delta.arriving_keys.push(primary_key.clone());
                delta.arriving_values.push(values.clone());
            }
            Some((_, old_hash)) if old_hash != hash => {
                // An update is a delete plus an insert. The old row carries the
                // same identity -- they are equal by construction, so the live
                // key is also the baseline key.
                delta.updates += 1;
                delta.arriving_keys.push(primary_key.clone());
                delta.arriving_values.push(values.clone());
            }
            Some(_) => {
                if added.iter().any(|index| carries_a_value(&values[*index])) {
                    delta.updates += 1;
                    delta.added_column_updates += 1;
                    delta.arriving_keys.push(primary_key.clone());
                    delta.arriving_values.push(values.clone());
                } else {
                    delta.unchanged += 1;
                }
            }
        }
    }
    // Nothing here overlaps the list above: an identity is in exactly one of
    // these three buckets, which is why 03 and 04 can be run in either order.
    for (key, (primary_key, _)) in baseline {
        if !live.contains_key(key) {
            delta.deletes += 1;
            delta.removed_keys.push(primary_key.clone());
        }
    }
    delta
}

// ---------------------------------------------------------------- emitting

#[allow(clippy::too_many_arguments)]
fn write_outputs(
    connection: &str,
    database: &str,
    dialect: Dialect,
    comparisons: &[Comparison],
    meta: &SnapshotMeta,
    now_tables: &[TableMeta],
    baseline_id: &str,
    now_id: &str,
    filters: &ResolvedFilters,
    schema_diff: &SchemaDiff,
    deltas: &[TableDelta],
    column_types: &HashMap<(String, String), String>,
) -> Result<Value, Abort> {
    let now = Local::now();
    let stamp = now.format("%Y%m%d-%H%M%S").to_string();
    // `<table>_bak_<stamp>`, built per table below -- a prefix would put the
    // timestamp first and file every backup of a run together, which is not how
    // anyone looks for them.
    //
    // Two suffixes, because 03 and 04 each back up what they remove and each
    // has to be runnable on its own. Same stamp, different marker, so a glance
    // at a backup table says which file made it.
    let backup_stamp = now.format("%Y%m%d_%H%M%S");
    let backup_suffix = format!("_bak_{backup_stamp}");
    let delete_backup_suffix = format!("_delbak_{backup_stamp}");
    let root = config::load().resolved_output_dir();
    let dir = root.join(store::slug(connection)).join(store::slug(database)).join(&stamp);
    fs::create_dir_all(&dir).map_err(|error| format!("无法创建输出目录 {}: {error}", dir.display()))?;

    let origin = format!(
        "基准快照 {baseline_id}（{}）· 本次快照 {now_id} · 连接 {} · 库 {database}",
        meta.taken_at, connection
    );
    let precheck = render_precheck(meta, deltas, dialect, filters);
    let auto_header = [
        "DB Diff — 01 结构变更（可自动执行）".to_string(),
        origin.clone(),
        "按编号顺序执行：00 → 01 → 02 → 03 → 04。缺哪个编号就是没有那类变更。".to_string(),
        "本文件里的语句重复执行是安全的：已经生效的再跑一次不会有副作用。".to_string(),
        "MySQL 的 DDL 会隐式提交、无法回滚。中途失败就把失败的那条跳过，修好后整个文件重跑。".to_string(),
        "本文件不含任何 DROP。".to_string(),
    ];
    let review_header = [
        "DB Diff — 02 结构变更（需要人工判断）".to_string(),
        origin.clone(),
        "这些语句要么会改写数据，要么可能失败。逐条看完再决定执行哪些。".to_string(),
        "类型收窄（例如 varchar(1000) -> varchar(100)）会静默截断数据；类型转换可能直接失败。".to_string(),
        "类型放宽（varchar(100) -> varchar(1000)）不在这里，它们不会丢数据，已经放进 01。".to_string(),
        "本文件不含任何 DROP。".to_string(),
    ];
    let insert =
        render_insert(meta, now_tables, deltas, dialect, filters, column_types, &backup_suffix, &origin);
    let delete = render_delete(
        meta, now_tables, deltas, dialect, filters, column_types, &delete_backup_suffix, &origin,
    );

    // A file with nothing in it is not written at all. An empty
    // 02-schema-review.sql reads as "there is something to review, it is just not
    // in here" -- and four files where two are empty is two files too many to
    // open. What was written is reported back, so nothing is silently missing.
    let mut files: Vec<(&str, String)> = vec![("00-precheck.sql", precheck)];
    if !schema_diff.auto.is_empty() {
        files.push(("01-schema-auto.sql", schema::render_file(&auto_header, &schema_diff.auto)));
    }
    if !schema_diff.review.is_empty() {
        files.push(("02-schema-review.sql", schema::render_file(&review_header, &schema_diff.review)));
    }
    if let Some(insert) = insert {
        files.push(("03-data.sql", insert));
    }
    if let Some(delete) = delete {
        files.push(("04-delete.sql", delete));
    }
    let written: Vec<&str> = files.iter().map(|(name, _)| *name).collect();
    for (name, content) in &files {
        write(&dir.join(name), content)?;
    }

    let report = render_report(meta, baseline_id, now_id, filters, schema_diff, deltas, comparisons, &written);
    write(&dir.join("report.md"), &report)?;

    let manifest = json!({
        "generatedAt": now.to_rfc3339_opts(SecondsFormat::Secs, false),
        "pluginVersion": env!("CARGO_PKG_VERSION"),
        "hashAlgoVersion": encode::HASH_ALGO_VERSION,
        "connection": connection,
        "database": database,
        "baselineSnapshot": baseline_id,
        "baselineTakenAt": meta.taken_at,
        "currentSnapshot": now_id,
        "backupTableSuffix": backup_suffix,
        "deleteBackupTableSuffix": delete_backup_suffix,
        // Only what was actually written -- a file that had no statements is not
        // in here, which is how a consumer tells "no changes" from "not run".
        "files": files.iter()
            .map(|(name, content)| (name.to_string(), json!(encode::sha256_hex(content.as_bytes()))))
            .collect::<serde_json::Map<String, Value>>(),
    });
    write(&dir.join("manifest.json"), &serde_json::to_string_pretty(&manifest).unwrap_or_default())?;

    Ok(json!({
        "outputDir": dir.display().to_string(),
        "baseline": baseline_id,
        "snapshotId": now_id,
        "report": report,
        "files": written,
        // The tables whose columns moved since the baseline. The panel does not
        // read this yet; the report is where a human sees it.
        "columnDrift": comparisons.iter()
            .filter(|comparison| !comparison.added_columns.is_empty())
            .map(|comparison| json!({
                "table": comparison.spec.table,
                "baselineColumns": comparison.hash_columns.len(),
                "liveColumns": comparison.spec.columns.len(),
                "added": comparison.added_columns,
            }))
            .collect::<Vec<_>>(),
        "backupTableSuffix": backup_suffix,
        "deleteBackupTableSuffix": delete_backup_suffix,
        "addedTables": schema_diff.added_tables,
        "addedColumns": schema_diff.added_columns,
        "changedColumns": schema_diff.changed_columns,
        "addedIndexes": schema_diff.added_indexes,
        "autoStatements": schema_diff.auto.len(),
        "reviewStatements": schema_diff.review.len(),
        "removedObjects": schema_diff.removed.len(),
        "collapsed": meta.data_tables.iter().map(|table| table.collapsed).sum::<u64>(),
        "tables": deltas.iter().map(|delta| {
            let meta_table = meta.table(&delta.table);
            json!({
            "table": delta.table,
            // The filter the live side actually ran with. When the run used the
            // baseline's own filters (the common case) this equals the baseline's.
            "filter": filters.tables.get(&delta.table).cloned()
                .or_else(|| meta_table.map(|table| table.filter.clone()))
                .unwrap_or_default(),
            "baselineRows": meta_table.map(|table| table.row_count).unwrap_or(0),
            "liveRows": delta.live_rows,
            "inserts": delta.inserts,
            "updates": delta.updates,
            "deletes": delta.deletes,
            "unchanged": delta.unchanged,
            "collapsed": meta_table.map(|table| table.collapsed).unwrap_or(0),
        })}).collect::<Vec<_>>(),
        "totalInserts": deltas.iter().map(|d| d.inserts).sum::<usize>(),
        "totalUpdates": deltas.iter().map(|d| d.updates).sum::<usize>(),
        "totalDeletes": deltas.iter().map(|d| d.deletes).sum::<usize>(),
    }))
}

/// Row counts on the target, compared against the baseline -- and where the
/// identity filter is applied, so the number means the same rows the diff meant.
/// It is the only thing standing between "the target drifted" and deleting the
/// wrong rows silently.
///
/// One statement for all six tables, glued with `UNION ALL`, so the precheck is
/// a single thing to run and a single thing to read: either every `verdict` is
/// `ok`, or the target is not the baseline. `UNION ALL` and not `UNION` --
/// `UNION` also merges rows that come out identical, which is exactly what two
/// of these rows look like for tables that are both empty.
///
/// A table's notes cannot sit next to its `SELECT` any more, so they are all
/// collected above the statement, where the reader is still deciding whether to
/// run it.
/// The filter a table's statements must carry: the one this run actually ran
/// with, falling back to the baseline record only for callers that sent none.
///
/// Never read `TableMeta::filter` directly when emitting SQL. That is the filter
/// as of the snapshot, and it is a different thing the moment someone confirms a
/// change -- which is exactly when getting it wrong matters, because the
/// precheck's `WHERE` and the diff's `WHERE` would be asking about different
/// rows.
fn used_filter(filters: &ResolvedFilters, meta: &SnapshotMeta, table: &str) -> String {
    filters
        .tables
        .get(table)
        .cloned()
        .or_else(|| meta.table(table).map(|table| table.filter.clone()))
        .unwrap_or_default()
}

/// The table as this run's **new** snapshot records it.
///
/// The data files have to be written from this one, not from the baseline: the
/// rows going to the target are in the live shape, and on a column drift that
/// shape is wider than the baseline's. Reading the baseline's column list while
/// zipping live values silently drops whatever the table gained -- and would put
/// every value against the wrong column if the two orders ever differed.
fn emitted_table<'a>(
    now: &'a [TableMeta],
    baseline: &'a SnapshotMeta,
    name: &str,
) -> Option<&'a TableMeta> {
    now.iter().find(|table| table.table == name).or_else(|| baseline.table(name))
}

fn render_precheck(
    meta: &SnapshotMeta,
    deltas: &[TableDelta],
    dialect: Dialect,
    filters: &ResolvedFilters,
) -> String {
    let mut out = String::new();
    out.push_str("-- DB Diff — 00 前置校验\n");
    out.push_str("-- 只读，不修改任何数据。\n");
    out.push_str("-- 一条语句查完全部表：verdict 全是 ok 才继续。\n");
    out.push_str("-- 任何一个 MISMATCH -- 数量不一致 都说明目标环境不等于基准快照，后面的语句不要执行。\n\n");

    for delta in deltas {
        let Some(table) = meta.table(&delta.table) else { continue };
        let filter = used_filter(filters, meta, &table.table);
        out.push_str(&format!("-- {}  过滤: {}\n", table.table, display_filter(&filter)));
        if table.collapsed > 0 {
            out.push_str(&format!(
                "--   注意：基准快照里这张表有 {} 行因为标识重复被折叠，写入快照的是 {} 行。\n--   下面的 expected 用的是目标库原始 COUNT(*)，不受折叠影响。\n",
                table.collapsed, table.row_count
            ));
        }
        if table.count_star.is_none() {
            // The expected value below falls back to the written row count, which
            // is not the same number when identities collapsed. Say so rather
            // than let the weaker check pass silently.
            out.push_str(
                "--   注意：采集时这张表的 COUNT(*) 没取到（查询失败），下面的 expected 用的是写入行数。\n--   如果目标库这里有重复标识，这个值会偏小，请人工核对。\n",
            );
        }
    }
    out.push('\n');

    // A table the baseline does not know is skipped, so counting by position
    // would put `UNION ALL` after a row that was never emitted.
    let mut emitted = 0usize;
    for delta in deltas {
        let Some(table) = meta.table(&delta.table) else { continue };
        // The target's raw COUNT(*) against the baseline's raw count -- not
        // against the number of rows written. Those differ when identities
        // collapsed, and the target will have the raw number too.
        let expected = table.count_star.unwrap_or(table.row_count);
        let filter = used_filter(filters, meta, &table.table);
        let where_clause =
            if filter.trim().is_empty() { String::new() } else { format!(" WHERE {}", filter.trim()) };
        if emitted > 0 {
            out.push_str("UNION ALL\n");
        }
        emitted += 1;
        let verdict = dialect.conditional(
            &format!("COUNT(*) = {expected}"),
            "'ok'",
            "'MISMATCH -- 数量不一致'",
        );
        out.push_str(&format!(
            "SELECT {} AS table_name, {} AS expected, COUNT(*) AS actual,\n  {} AS verdict\n  FROM {}{}\n",
            quote_string(&table.table),
            expected,
            verdict,
            dialect.quote_ident(&table.table),
            where_clause
        ));
    }
    out.push_str(";\n");
    out
}

/// `03-data.sql` -- everything the comparison found as **new or modified**.
///
/// Per table: back up what is about to be overwritten, delete it, then insert.
/// The inserts are on the delete list too, because the target may already hold a
/// row with that id -- one the source never had -- and the insert would die on
/// the unique key without it. Backing up and deleting the same id list is what
/// makes that safe to do.
///
/// One INSERT per row: a failure costs one row instead of a chunk of five
/// hundred, and a row gets a line of its own, which a multi-row `VALUES` list
/// stops doing as soon as the columns get wide.
///
/// `None` when nothing is arriving, so no empty file is written.
#[allow(clippy::too_many_arguments)]
fn render_insert(
    meta: &SnapshotMeta,
    now_tables: &[TableMeta],
    deltas: &[TableDelta],
    dialect: Dialect,
    filters: &ResolvedFilters,
    column_types: &HashMap<(String, String), String>,
    backup_suffix: &str,
    origin: &str,
) -> Option<String> {
    let column_type = |table: &str, column: &str| -> String {
        column_types.get(&(table.to_string(), column.to_string())).cloned().unwrap_or_default()
    };

    let mut out = String::new();
    for line in [
        "DB Diff — 03 数据变更（新增 + 修改）".to_string(),
        origin.to_string(),
        "本文件只处理对比结果是**新增和修改**的行；对比结果是**删除**的行在 04-delete.sql。".to_string(),
        "两边的 id 集合不相交，先跑哪个都行（按编号顺序跑就是）。".to_string(),
        "每张表的顺序：建备份表 → 备份 → 删除 → 插入。".to_string(),
        "CREATE TABLE ... LIKE 建的是**空表**，只复制结构，不复制数据 —— 备份的数据在下面那条".to_string(),
        "INSERT ... SELECT 里，而且只有要删掉的那些标识。".to_string(),
        "删除的标识包含新增的那些：目标库如果已经有同 id 的行，不先删掉，插入会撞唯一键。".to_string(),
        "目标库没有的标识，备份的 SELECT 自然查不到东西，不会凭空多出行来。".to_string(),
        "一行一条 INSERT：哪一条失败就跳过哪一条，其余照跑。整个重跑就整个文件重跑一遍。".to_string(),
        format!("备份表名 <表名>{backup_suffix}，留在目标库同一个 schema 里，不自动删除。"),
    ] {
        out.push_str(&format!("-- {line}\n"));
    }
    out.push('\n');

    let mut any = false;
    for delta in deltas {
        let Some(table) = emitted_table(now_tables, meta, &delta.table) else { continue };
        // Gate on the values, not on the counters: those are the same thing in
        // practice, and this way the file cannot come out claiming a table it
        // then emits nothing for.
        if delta.arriving_values.is_empty() {
            continue;
        }
        let arriving = delta.inserts + delta.updates;
        any = true;
        out.push_str(&format!(
            "-- ===== {}：写入 {} 行（新增 {} · 修改 {}）=====\n",
            table.table, arriving, delta.inserts, delta.updates
        ));
        let filter = used_filter(filters, meta, &table.table);
        out.push_str(&format!("-- 过滤: {}\n\n", if filter.is_empty() { "（无）".to_string() } else { filter }));

        let backup_table = format!("{}{}", table.table, backup_suffix);
        let id_types: Vec<String> =
            table.primary_key.iter().map(|column| column_type(&table.table, column)).collect();

        if !delta.arriving_keys.is_empty() {
            out.push_str(&dialect.create_table_like(&backup_table, &table.table));
            out.push_str("\n\n");
            for chunk in delta.arriving_keys.chunks(CHUNK) {
                out.push_str(&format!(
                    "INSERT INTO {} SELECT * FROM {} WHERE {};\n",
                    dialect.quote_ident(&backup_table),
                    dialect.quote_ident(&table.table),
                    key_predicate(dialect, &table.primary_key, &id_types, chunk)
                ));
            }
            out.push('\n');
            for chunk in delta.arriving_keys.chunks(CHUNK) {
                out.push_str(&format!(
                    "DELETE FROM {} WHERE {};\n",
                    dialect.quote_ident(&table.table),
                    key_predicate(dialect, &table.primary_key, &id_types, chunk)
                ));
            }
            out.push('\n');
        }

        let types: Vec<String> = table.columns.iter().map(|column| column_type(&table.table, column)).collect();
        let columns = table.columns.iter().map(|column| dialect.quote_ident(column)).collect::<Vec<_>>().join(", ");
        for values in &delta.arriving_values {
            let rendered: Vec<String> = values
                .iter()
                .zip(types.iter())
                .map(|(value, column_type)| schema::render_value(value.as_deref(), column_type))
                .collect();
            out.push_str(&format!(
                "INSERT INTO {} ({columns}) VALUES ({});\n",
                dialect.quote_ident(&table.table),
                rendered.join(", ")
            ));
        }
        out.push('\n');
    }

    if !any {
        return None;
    }
    Some(out)
}

/// `04-delete.sql` -- the rows the comparison found as **deleted**: present in
/// the baseline, gone from the live database.
///
/// Only removals. Nothing here writes a row back, and nothing here is needed by
/// 03: an identity is either arriving or removed, never both.
///
/// The backup table is separate from the one in 03 (`_delbak_` rather than
/// `_bak_`) so the two files stay independent -- run either alone and what it
/// removed is still recoverable from its own table.
///
/// `None` when nothing was deleted, so no empty file is written.
#[allow(clippy::too_many_arguments)]
fn render_delete(
    meta: &SnapshotMeta,
    now_tables: &[TableMeta],
    deltas: &[TableDelta],
    dialect: Dialect,
    filters: &ResolvedFilters,
    column_types: &HashMap<(String, String), String>,
    backup_suffix: &str,
    origin: &str,
) -> Option<String> {
    let column_type = |table: &str, column: &str| -> String {
        column_types.get(&(table.to_string(), column.to_string())).cloned().unwrap_or_default()
    };

    let mut out = String::new();
    for line in [
        "DB Diff — 04 数据删除".to_string(),
        origin.to_string(),
        "**本文件只删不写。** 对比结果是删除的行——快照里有、现在没有了——全在这里。".to_string(),
        "新增和修改的行在 03-data.sql，两边的 id 集合不相交，先跑哪个都行。".to_string(),
        "每张表的顺序：建备份表 → 备份 → 删除。备份用的是本文件自己的表，不是 03 那张。".to_string(),
        "CREATE TABLE ... LIKE 建的是**空表**，只复制结构，不复制数据 —— 备份的数据在下面那条".to_string(),
        "INSERT ... SELECT 里，只覆盖要删掉的这些标识。".to_string(),
        "目标库没有的标识，备份的 SELECT 自然查不到东西。".to_string(),
        format!("备份表名 <表名>{backup_suffix}，留在目标库同一个 schema 里，不自动删除。"),
        "备份表是 LIKE 复制的、带着同样的主键：重跑本文件时，已经备份过的那几条会因为主键".to_string(),
        "重复而失败，跳过即可 —— 那说明它们已经备份过了。".to_string(),
    ] {
        out.push_str(&format!("-- {line}\n"));
    }
    out.push('\n');

    let mut any = false;
    for delta in deltas {
        let Some(table) = emitted_table(now_tables, meta, &delta.table) else { continue };
        if delta.removed_keys.is_empty() {
            continue;
        }
        any = true;
        out.push_str(&format!(
            "-- ===== {}：删除 {} 行 =====\n",
            table.table,
            delta.removed_keys.len()
        ));
        let filter = used_filter(filters, meta, &table.table);
        out.push_str(&format!("-- 过滤: {}\n\n", if filter.is_empty() { "（无）".to_string() } else { filter }));

        let backup_table = format!("{}{}", table.table, backup_suffix);
        let id_types: Vec<String> =
            table.primary_key.iter().map(|column| column_type(&table.table, column)).collect();
        out.push_str(&dialect.create_table_like(&backup_table, &table.table));
        out.push_str("\n\n");
        for chunk in delta.removed_keys.chunks(CHUNK) {
            out.push_str(&format!(
                "INSERT INTO {} SELECT * FROM {} WHERE {};\n",
                dialect.quote_ident(&backup_table),
                dialect.quote_ident(&table.table),
                key_predicate(dialect, &table.primary_key, &id_types, chunk)
            ));
        }
        out.push('\n');
        for chunk in delta.removed_keys.chunks(CHUNK) {
            out.push_str(&format!(
                "DELETE FROM {} WHERE {};\n",
                dialect.quote_ident(&table.table),
                key_predicate(dialect, &table.primary_key, &id_types, chunk)
            ));
        }
        out.push('\n');
    }

    if !any {
        return None;
    }
    Some(out)
}

/// `` (`a`, `b`) IN ((1, 'x'), (2, 'y')) `` -- valid for a single-column key too.
fn key_predicate(
    dialect: Dialect,
    primary_key: &[String],
    primary_key_types: &[String],
    keys: &[Vec<Option<String>>],
) -> String {
    let columns = dialect.quote_ident_list(primary_key);
    let tuples: Vec<String> = keys
        .iter()
        .map(|key| {
            let rendered: Vec<String> = key
                .iter()
                .zip(primary_key_types.iter())
                .map(|(value, column_type)| schema::render_value(value.as_deref(), column_type))
                .collect();
            format!("({})", rendered.join(", "))
        })
        .collect();
    format!("({columns}) IN ({})", tuples.join(", "))
}

fn render_report(
    meta: &SnapshotMeta,
    baseline_id: &str,
    now_id: &str,
    filters: &ResolvedFilters,
    schema_diff: &SchemaDiff,
    deltas: &[TableDelta],
    comparisons: &[Comparison],
    // The files actually written. A category with no statements in it is not
    // generated at all, so the report has to say which ones exist rather than
    // point at all four by name.
    files: &[&str],
) -> String {
    let mut out = String::new();
    out.push_str("# DB Diff 报告\n\n");
    out.push_str(&format!("- 连接：`{}`\n", meta.connection));
    out.push_str(&format!("- 库：`{}`\n", meta.database));
    out.push_str(&format!("- 基准快照：`{baseline_id}`（{}）\n", meta.taken_at));
    out.push_str(&format!("- 本次快照：`{now_id}`（对比时同时保存）\n"));
    if filters.changed() {
        out.push_str("- **本次对比使用的过滤条件与基准快照不一致（已确认继续）**\n");
        for (table, old, new) in &filters.changed {
            out.push_str(&format!(
                "  - `{table}`：基准 `{}` → 本次 `{}`\n",
                display_filter(old),
                display_filter(new)
            ));
        }
        if let Some((old, new)) = &filters.schema_changed {
            out.push_str(&format!(
                "  - 表结构过滤：基准 `{}` → 本次 `{}`\n",
                display_filter(old),
                display_filter(new)
            ));
        }
    }
    out.push_str(&format!("- 生成时间：{}\n\n", Local::now().to_rfc3339_opts(SecondsFormat::Secs, false)));

    out.push_str("## 数据差异\n\n");
    out.push_str("| 表 | 快照 | 当前 | 新增 | 修改 | 删除 | 未变 |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for delta in deltas {
        let Some(table) = meta.table(&delta.table) else { continue };
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} |\n",
            table.table, table.row_count, delta.live_rows, delta.inserts, delta.updates, delta.deletes, delta.unchanged
        ));
    }
    out.push_str(&format!(
        "\n合计：新增 {} · 修改 {} · 删除 {}（修改按「备份 + 删 + 插」处理）\n\n",
        deltas.iter().map(|d| d.inserts).sum::<usize>(),
        deltas.iter().map(|d| d.updates).sum::<usize>(),
        deltas.iter().map(|d| d.deletes).sum::<usize>(),
    ));

    out.push_str("## 每张表的过滤条件\n\n");
    for table in &meta.data_tables {
        let filter = used_filter(filters, meta, &table.table);
        out.push_str(&format!(
            "- `{}`：{}\n",
            table.table,
            if filter.is_empty() { "（无）".to_string() } else { format!("`{filter}`") }
        ));
    }
    out.push('\n');

    let collapsed: Vec<&TableMeta> = meta.data_tables.iter().filter(|table| table.collapsed > 0).collect();
    let no_count: Vec<&TableMeta> = meta.data_tables.iter().filter(|table| table.count_star.is_none()).collect();
    if !no_count.is_empty() {
        out.push_str("## ⚠ 采集时 COUNT(*) 没取到\n\n");
        out.push_str("这些表在打快照时 `COUNT(*)` 查询失败（通常是网络抖动），所以：\n\n");
        out.push_str("- `00-precheck.sql` 里这几张表的 `expected` 用的是**写入行数**，不是真实行数\n");
        out.push_str("- 目标库如果有重复标识，这个期望值会偏小，执行前请人工核对\n\n");
        for table in no_count {
            out.push_str(&format!("- ⚠ `{}`：写入 {} 行\n", table.table, table.row_count));
        }
        out.push('\n');
    }
    if !collapsed.is_empty() {
        // The leading ⚠ is what the panel keys the red heading off.
        out.push_str("## ⚠ 标识重复，有行被折叠\n\n");
        out.push_str("这些表里有多个行共用一个标识，而快照每个标识只能保留一条，**其余的被丢弃了**。\n");
        out.push_str("下面这些行不会进快照，也就不会被同步过去。要保留哪一条，用该表的过滤条件指定。\n\n");
        for table in collapsed {
            out.push_str(&format!(
                "- ⚠ `{}`：折叠 {} 行（写入 {} 行，`COUNT(*)` 是 {} 行）\n",
                table.table,
                table.collapsed,
                table.row_count,
                table.count_star.map(|count| count.to_string()).unwrap_or_else(|| "?".into())
            ));
            if !table.collapsed_samples.is_empty() {
                out.push_str(&format!("  - 例如：{}\n", table.collapsed_samples.join(" / ")));
            }
            out.push_str(&format!("  - 当前过滤条件：`{}`\n", used_filter(filters, meta, &table.table)));
        }
        out.push('\n');
    }

    let drifted: Vec<&Comparison> =
        comparisons.iter().filter(|comparison| !comparison.added_columns.is_empty()).collect();
    if !drifted.is_empty() {
        out.push_str("## ⚠ 列明细漂移\n\n");
        out.push_str("这些表现在的列比基准快照多。本次对比是按**基准快照的列**算的 hash —— 不比这一趟，\n");
        out.push_str("两边的 hash 含义就不同；新快照记的是**本库当前的列**，所以下一趟就对齐了。\n\n");
        out.push_str("多出来的列怎么处理：\n\n");
        out.push_str("- 旧列 hash **不一样** → 正常走修改（备份 + 删 + 插）\n");
        out.push_str("- 旧列 hash 一样、新列**全是空**（NULL 或空串）→ 这行不动\n");
        out.push_str("- 旧列 hash 一样、新列**有一个有值** → 也走修改，把新列的值带过去\n\n");
        out.push_str("| 表 | 基准列数 | 本库列数 | 新增 | 因此写入 |\n");
        out.push_str("| --- | ---: | ---: | --- | ---: |\n");
        for comparison in &drifted {
            let promoted = deltas
                .iter()
                .find(|delta| delta.table == comparison.spec.table)
                .map(|delta| delta.added_column_updates)
                .unwrap_or(0);
            out.push_str(&format!(
                "| `{}` | {} | {} | {} | {} |\n",
                comparison.spec.table,
                comparison.hash_columns.len(),
                comparison.spec.columns.len(),
                comparison.added_columns.iter().map(|name| format!("`{name}`")).collect::<Vec<_>>().join(", "),
                promoted
            ));
        }
        out.push('\n');
    }

    // Where a statement landed, or "—" when that file was not generated at all.
    let into = |name: &str| if files.contains(&name) { format!("`{name}`") } else { "—".to_string() };
    out.push_str("## 结构差异\n\n");
    out.push_str("| 项 | 数量 | 落点 |\n| --- | --- | --- |\n");
    out.push_str(&format!("| 新增表 | {} | {} |\n", schema_diff.added_tables, into("01-schema-auto.sql")));
    out.push_str(&format!("| 新增列 | {} | {} |\n", schema_diff.added_columns, into("01-schema-auto.sql")));
    out.push_str(&format!("| 新增索引 | {} | {} |\n", schema_diff.added_indexes, into("01-schema-auto.sql")));
    out.push_str(&format!("| 变更列 | {} | 见下 |\n", schema_diff.changed_columns));
    out.push_str(&format!("| 需人工判断 | {} | {} |\n", schema_diff.review.len(), into("02-schema-review.sql")));
    out.push_str(&format!("| 已删除（不生成语句） | {} | — |\n\n", schema_diff.removed.len()));

    if !schema_diff.removed.is_empty() {
        out.push_str("## 已删除（只报告，不生成语句）\n\n");
        for item in &schema_diff.removed {
            out.push_str(&format!("- {item}\n"));
        }
        out.push('\n');
    }
    if !schema_diff.notes.is_empty() {
        out.push_str("## 其它差异（比出来了但不生成语句）\n\n");
        for item in &schema_diff.notes {
            out.push_str(&format!("- {item}\n"));
        }
        out.push('\n');
    }

    out.push_str("## 已忽略\n\n");
    out.push_str("- 列注释 / 表注释\n- `AUTO_INCREMENT`\n- 分区、视图、触发器、存储过程、事件\n");
    out.push_str("- 表 / 列 / 索引 / 外键的**删除**（见上）\n\n");

    // Only the files that exist, plus an explicit line for the ones that do not.
    // A missing number would otherwise read as an oversight rather than as "this
    // run had no changes of that kind".
    const STEPS: &[(&str, &str)] = &[
        ("00-precheck.sql", "只读，verdict 全是 ok 才继续。"),
        ("01-schema-auto.sql", "建表 / 加列 / 加索引 / 类型放宽。"),
        ("02-schema-review.sql", "人工逐条判断。"),
        ("03-data.sql", "备份 + 删除 + 插入。对比出来是**新增和修改**的行。"),
        ("04-delete.sql", "备份 + 删除。对比出来是**删除**的行。"),
    ];
    out.push_str("## 生成的文件\n\n");
    for (name, note) in STEPS {
        if files.contains(name) {
            out.push_str(&format!("- `{name}` —— {note}\n"));
        } else {
            out.push_str(&format!("- ~~`{name}`~~ —— 没有这类变更，**本次没有生成**。\n"));
        }
    }
    out.push('\n');
    // 03 and 04 work on disjoint id sets -- an identity is either being written
    // or being removed, never both -- so the order between them is free. The
    // order between 01 and 03 is not.
    out.push_str("**执行顺序：按编号 00 → 01 → 02 → 03 → 04。** 03 和 04 处理的 id 集合不相交，\n");
    out.push_str("先后无所谓；01 必须在 03/04 之前（先有列再有值）。\n");
    out
}

fn display_filter(filter: &str) -> String {
    if filter.trim().is_empty() {
        "（无）".to_string()
    } else {
        filter.trim().to_string()
    }
}

fn write(path: &Path, content: &str) -> Result<(), Abort> {
    fs::write(path, content).map_err(|error| Abort::Failed(format!("无法写入 {}: {error}", path.display())))
}

fn required_str(params: &Value, key: &str) -> Result<String, Abort> {
    params
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| Abort::Failed(format!("缺少参数 {key}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::SessionInfo;

    fn table(name: &str, filter: &str, rows: u64) -> TableMeta {
        TableMeta {
            table: name.to_string(),
            columns: vec!["id".to_string()],
            primary_key: vec!["id".to_string()],
            filter: filter.to_string(),
            row_count: rows,
            count_star: Some(rows),
            collapsed: 0,
            collapsed_samples: Vec::new(),
            pages: 1,
            sha256: String::new(),
            file: format!("{name}.jsonl"),
        }
    }

    fn meta(tables: Vec<TableMeta>) -> SnapshotMeta {
        SnapshotMeta {
            snapshot_id: "20260920-100000".to_string(),
            connection: "c".to_string(),
            database: "d".to_string(),
            taken_at: "2026-09-20T10:00:00+08:00".to_string(),
            plugin_version: "0.1.0".to_string(),
            hash_algo_version: 1,
            session: SessionInfo::default(),
            schema_filter: String::new(),
            data_tables: tables,
            schema: None,
        }
    }

    fn delta(name: &str) -> TableDelta {
        TableDelta { table: name.to_string(), ..TableDelta::default() }
    }

    /// The resolved filter set, as `resolve_filters` would build it.
    fn resolved(pairs: &[(&str, &str)]) -> ResolvedFilters {
        let mut filters = ResolvedFilters {
            tables: HashMap::new(),
            schema_filter: String::new(),
            changed: Vec::new(),
            schema_changed: None,
        };
        for (table, filter) in pairs {
            filters.tables.insert(table.to_string(), filter.to_string());
        }
        filters
    }

    #[test]
    fn precheck_is_one_statement_over_every_table() {
        let meta = meta(vec![
            table("dsfa_rm", "ds_active = '1'", 1237),
            table("dsfa_route_version", "", 90),
        ]);
        let deltas = vec![delta("dsfa_rm"), delta("dsfa_route_version")];
        let filters = resolved(&[("dsfa_rm", "ds_active = '1'"), ("dsfa_route_version", "")]);
        let sql = render_precheck(&meta, &deltas, Dialect::MySql, &filters);
        println!("---8<---\n{sql}---8<---");

        // Each table keeps its own filter, and no filter means no WHERE at all.
        assert!(sql.contains("FROM `dsfa_rm` WHERE ds_active = '1'"), "{sql}");
        assert!(sql.contains("FROM `dsfa_route_version`\n"), "{sql}");
        // One statement: one UNION ALL between two tables, one terminator.
        assert_eq!(sql.matches("UNION ALL").count(), 1, "{sql}");
        assert_eq!(sql.matches(';').count(), 1, "{sql}");
        assert!(sql.contains("'ok', 'MISMATCH -- 数量不一致'"), "{sql}");
    }

    /// 03 handles new and modified rows (backup + delete + insert), 04 handles
    /// the deleted ones (backup + delete) with a backup table of its own.
    #[test]
    fn data_files_split_writes_from_removals() {
        let meta = meta(vec![table("dsfa_mm", "ds_active = '1'", 4)]);
        let mut types = HashMap::new();
        types.insert(("dsfa_mm".to_string(), "id".to_string()), "varchar(32)".to_string());

        let mut delta = delta("dsfa_mm");
        delta.inserts = 1;
        delta.updates = 1;
        delta.deletes = 1;
        // Arriving: one modified row (an old row gets overwritten) and one new
        // row the target may or may not already have.
        delta.arriving_keys =
            vec![vec![Some("being-updated".into())], vec![Some("brand-new".into())]];
        delta.arriving_values =
            vec![vec![Some("being-updated".into())], vec![Some("brand-new".into())]];
        delta.removed_keys = vec![vec![Some("going-away".into())]];

        let deltas = vec![delta];
        let filters = resolved(&[("dsfa_mm", "ds_active = '1'")]);
        let insert = render_insert(&meta, &meta.data_tables, &deltas, Dialect::MySql, &filters, &types, "_bak_20260920_094436", "ORIGIN")
            .expect("rows are arriving");
        let delete = render_delete(&meta, &meta.data_tables, &deltas, Dialect::MySql, &filters, &types, "_delbak_20260920_094436", "ORIGIN")
            .expect("rows are gone");
        println!("---8<--- 03-data.sql\n{insert}---8<--- 04-delete.sql\n{delete}---8<---");

        // 03: back up, delete, then one INSERT per row.
        assert!(insert.contains("CREATE TABLE IF NOT EXISTS `dsfa_mm_bak_20260920_094436`"), "{insert}");
        assert_eq!(insert.matches("INSERT INTO `dsfa_mm_bak_20260920_094436`").count(), 1, "{insert}");
        assert_eq!(insert.matches("DELETE FROM `dsfa_mm`").count(), 1, "{insert}");
        assert_eq!(insert.matches("INSERT INTO `dsfa_mm` (").count(), 2, "{insert}");
        assert!(insert.contains("being-updated") && insert.contains("brand-new"), "{insert}");
        assert!(!insert.contains("going-away"), "{}", insert);

        // 04: its own backup table, and nothing written back.
        assert!(delete.contains("CREATE TABLE IF NOT EXISTS `dsfa_mm_delbak_20260920_094436`"), "{delete}");
        assert_eq!(delete.matches("INSERT INTO `dsfa_mm_delbak_20260920_094436`").count(), 1, "{delete}");
        assert_eq!(delete.matches("DELETE FROM `dsfa_mm`").count(), 1, "{delete}");
        assert!(delete.contains("going-away"), "{delete}");
        assert!(!delete.contains("`dsfa_mm_bak_"), "must not reuse the backup table 03 made",);
        assert!(!delete.contains("INSERT INTO `dsfa_mm` ("), "04 never writes a row back");
    }

    /// Everything emitted has to carry the filter this run *ran with*: the
    /// precheck's `WHERE`, and the `-- 过滤:` comment in 03 and 04. Reading them
    /// off the baseline was the bug -- the precheck is SQL that runs on the
    /// target, so the old filter meant validating the wrong rows.
    #[test]
    fn emitted_sql_uses_the_filter_this_run_ran_with() {
        let meta = meta(vec![table("dsfa_rm", "ds_active = '1'", 10)]);
        let used = "ds_active = '1' AND (ds_note <> 'x')";
        let filters = resolved(&[("dsfa_rm", used)]);

        let precheck = render_precheck(&meta, &[delta("dsfa_rm")], Dialect::MySql, &filters);
        println!("---8<--- 00-precheck.sql\n{precheck}---8<---");
        assert!(precheck.contains(&format!("WHERE {used}")), "{precheck}");
        assert!(precheck.contains(&format!("-- dsfa_rm  过滤: {used}")), "{precheck}");

        let mut touched = delta("dsfa_rm");
        touched.inserts = 1;
        touched.deletes = 1;
        touched.arriving_keys = vec![vec![Some("a".into())]];
        touched.arriving_values = vec![vec![Some("a".into())]];
        touched.removed_keys = vec![vec![Some("b".into())]];
        let deltas = vec![touched];
        let types: HashMap<(String, String), String> = HashMap::new();

        let insert = render_insert(&meta, &meta.data_tables, &deltas, Dialect::MySql, &filters, &types, "_bak_x", "ORIGIN").unwrap();
        let delete = render_delete(&meta, &meta.data_tables, &deltas, Dialect::MySql, &filters, &types, "_delbak_x", "ORIGIN").unwrap();
        for sql in [&insert, &delete] {
            assert!(sql.contains(&format!("-- 过滤: {used}")), "{sql}");
            assert!(!sql.contains("-- 过滤: ds_active = '1'\n"), "{sql}");
        }
    }

    fn spec_of(name: &str, columns: &[&str]) -> ResolvedSpec {
        ResolvedSpec {
            table: name.to_string(),
            columns: columns.iter().map(|column| column.to_string()).collect(),
            identity: vec!["id".to_string()],
            order_by: vec!["id".to_string()],
            base_filter: None,
        }
    }

    /// A baseline whose table covers `columns`.
    fn baseline_with(name: &str, columns: &[&str]) -> SnapshotMeta {
        let mut recorded = table(name, "", 3);
        recorded.columns = columns.iter().map(|column| column.to_string()).collect();
        meta(vec![recorded])
    }

    fn key(name: &str) -> Vec<u8> {
        encode::encode_values(&[Some(name.to_string())])
    }

    /// A row whose old columns are unchanged but which carries something in a
    /// column the table gained. The hash cannot see it -- that column is outside
    /// the hash on purpose -- so the rule has to, or the target never learns the
    /// value.
    #[test]
    fn a_gained_column_with_a_value_makes_the_row_an_update() {
        let mut baseline = BaselineMap::new();
        baseline.insert(key("a"), (vec![Some("a".to_string())], "same".to_string()));
        let mut live = LiveMap::new();
        live.insert(
            key("a"),
            (
                vec![Some("a".to_string())],
                "same".to_string(),
                vec![Some("a".to_string()), Some("filled".to_string())],
            ),
        );

        let delta = compare("t", &baseline, &live, &[1]);
        assert_eq!(delta.unchanged, 0);
        assert_eq!(delta.updates, 1);
        assert_eq!(delta.added_column_updates, 1, "counted so the report can explain the number");
        assert_eq!(delta.arriving_values.len(), 1, "and written, so the target gets the value");
        assert_eq!(delta.arriving_keys.len(), 1);
    }

    /// The same row with nothing in the gained column: a table can gain a column
    /// that is empty everywhere, and writing the whole table to say nothing
    /// would be a lot of SQL for no reason.
    #[test]
    fn a_gained_column_that_is_empty_leaves_the_row_alone() {
        for empty in [None, Some(String::new())] {
            let mut baseline = BaselineMap::new();
            baseline.insert(key("a"), (vec![Some("a".to_string())], "same".to_string()));
            let mut live = LiveMap::new();
            live.insert(
                key("a"),
                (
                    vec![Some("a".to_string())],
                    "same".to_string(),
                    vec![Some("a".to_string()), empty.clone()],
                ),
            );

            let delta = compare("t", &baseline, &live, &[1]);
            assert_eq!(delta.unchanged, 1, "empty was {empty:?}");
            assert_eq!(delta.updates, 0, "empty was {empty:?}");
            assert!(delta.arriving_values.is_empty());
        }
    }

    /// With no gained columns the check must not fire at all -- this is the
    /// ordinary run, and it has to behave exactly as it always did.
    #[test]
    fn without_a_gained_column_nothing_is_promoted() {
        let mut baseline = BaselineMap::new();
        baseline.insert(key("a"), (vec![Some("a".to_string())], "same".to_string()));
        let mut live = LiveMap::new();
        live.insert(
            key("a"),
            (
                vec![Some("a".to_string())],
                "same".to_string(),
                vec![Some("a".to_string()), Some("filled".to_string())],
            ),
        );

        let delta = compare("t", &baseline, &live, &[]);
        assert_eq!(delta.unchanged, 1);
        assert_eq!(delta.updates, 0);
    }

    /// A name the table does not have cannot be read out of a row.
    #[test]
    fn positions_skip_columns_the_table_does_not_have() {
        let names = vec!["a".to_string(), "not_there".to_string(), "c".to_string()];
        let columns = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(positions_of(&names, &columns), vec![0, 2]);
    }

    /// Steady state: one list, so the row pipeline stays at one hash per row.
    /// This is the case that must not get slower.
    #[test]
    fn unchanged_columns_plan_a_single_hash_list() {
        let planned = plan_comparisons(&[spec_of("t", &["id"])], &baseline_with("t", &["id"]))
            .expect("same shape");
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].hash_columns, planned[0].spec.columns);
        assert!(planned[0].added_columns.is_empty(), "{:?}", planned[0].added_columns);
    }

    /// Gaining a column is reconciled rather than refused: the hash follows the
    /// baseline, the snapshot follows the table, and the difference is reported.
    #[test]
    fn a_gained_column_is_reconciled_and_reported() {
        let planned = plan_comparisons(&[spec_of("t", &["id", "new"])], &baseline_with("t", &["id"]))
            .expect("gaining a column can be reconciled");
        assert_eq!(planned[0].hash_columns, vec!["id".to_string()], "hash follows the baseline");
        assert_eq!(planned[0].spec.columns, vec!["id".to_string(), "new".to_string()]);
        assert_eq!(planned[0].added_columns, vec!["new".to_string()]);
    }

    /// Losing one cannot be: the baseline's hash covers a value that is gone, so
    /// there is nothing to recompute and every row would read as modified.
    #[test]
    fn a_lost_column_is_refused() {
        let error = plan_comparisons(&[spec_of("t", &["id"])], &baseline_with("t", &["id", "gone"]))
            .expect_err("a lost column cannot be reconciled");
        let Abort::Failed(message) = error else { panic!("expected a failure") };
        assert!(message.contains("gone"), "{message}");
    }

    /// The identity is what a row *is*, so a change to it is not a drift to
    /// reconcile.
    #[test]
    fn a_changed_identity_is_refused() {
        let mut baseline = baseline_with("t", &["id"]);
        baseline.data_tables[0].primary_key = vec!["other_id".to_string()];
        let error = plan_comparisons(&[spec_of("t", &["id"])], &baseline)
            .expect_err("a changed identity cannot be reconciled");
        let Abort::Failed(message) = error else { panic!("expected a failure") };
        assert!(message.contains("标识列"), "{message}");
    }

    /// The section has to show the filters this run used, not the ones the
    /// baseline recorded. Those differ exactly when someone confirmed a filter
    /// change -- which is when the reader most needs the right answer.
    #[test]
    fn report_shows_the_filters_this_run_used() {
        let meta = meta(vec![table("dsfa_rm", "ds_active = '1'", 10)]);
        let mut filters = ResolvedFilters {
            tables: HashMap::new(),
            schema_filter: String::new(),
            changed: Vec::new(),
            schema_changed: None,
        };
        filters
            .tables
            .insert("dsfa_rm".to_string(), "ds_active = '1' AND (ds_note <> 'x')".to_string());

        let report = render_report(
            &meta,
            "20260920-100000",
            "20260920-110000",
            &filters,
            &SchemaDiff::default(),
            &[delta("dsfa_rm")],
            &[],
            &["00-precheck.sql", "03-data.sql"],
        );
        println!("---8<---\n{report}---8<---");

        assert!(report.contains("ds_active = '1' AND (ds_note <> 'x')"), "{report}");
        // The baseline's shorter filter must not be what the section shows.
        assert!(!report.contains("：`ds_active = '1'`\n"), "{report}");
        assert!(!report.contains("隐式提交"), "{report}");
    }

    /// Two tables with no rows produce two identical rows, which is exactly what
    /// `UNION` would collapse into one -- hence `UNION ALL`.
    #[test]
    fn precheck_does_not_use_plain_union() {
        let meta = meta(vec![table("a", "", 0), table("b", "", 0)]);
        let deltas = [delta("a"), delta("b")];
        let sql = render_precheck(&meta, &deltas, Dialect::MySql, &resolved(&[("a", ""), ("b", "")]));
        assert_eq!(sql.matches("UNION").count(), sql.matches("UNION ALL").count(), "{sql}");
    }
}
