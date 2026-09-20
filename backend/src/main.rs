//! DB Diff sidecar.
//!
//! Read-only. Every database access is a `dbx query` process; the plugin never
//! writes to any connection and never contacts the target environment.
//!
//! Surface: list connections and their databases, take snapshots, and diff a
//! snapshot against the live database. The compared tables and columns are fixed
//! in `tables.rs`.
//!
//! A snapshot belongs to a **(connection, database)** pair and every method that
//! touches one is given both -- see `store.rs` for why the pair, not the
//! connection alone, is the identity.

mod cli;
mod config;
mod dialect;
mod diff;
mod encode;
mod schema;
mod schema_pg;
mod snapshot;
mod store;
mod tables;

use std::time::Duration;

use dbx_plugin_sdk::{PluginEmitter, PluginError, PluginHandler, PluginMetadata, PluginServer, RequestContext};
use serde_json::{json, Value};

use snapshot::Abort;

const PLUGIN_ID: &str = "dbx.demo.dbdiff";

struct DbDiff;

impl PluginHandler for DbDiff {
    fn handle(
        &self,
        _context: RequestContext,
        method: &str,
        params: Value,
        _emitter: &PluginEmitter,
    ) -> Result<Value, PluginError> {
        match method {
            "host/info" => Ok(host_info()),
            "connections/list" => finish(list_connections()),
            "databases/list" => finish(list_databases(&params)),
            "config/get" => Ok(config_get()),
            "config/save" => finish(save_config(&params)),
            "snapshot/list" => finish(list_snapshots(&params)),
            "snapshot/inspect" => finish(inspect_snapshot(&params)),
            "snapshot/create" => finish(snapshot::start_create(&params)),
            "snapshot/status" => Ok(snapshot::status()),
            "snapshot/cancel" => Ok(snapshot::cancel()),
            "snapshot/delete" => finish(delete_snapshot(&params)),
            "diff/run" => finish(diff::start(&params)),
            "diff/reveal" => finish(diff::reveal(&params)),
            _ => Err(PluginError::method_not_found(method)),
        }
    }
}

fn host_info() -> Value {
    json!({
        "pluginId": PLUGIN_ID,
        "pluginVersion": env!("CARGO_PKG_VERSION"),
        "hashAlgoVersion": encode::HASH_ALGO_VERSION,
        "pageSize": cli::PAGE_SIZE,
        "dataDir": config::data_dir().display().to_string(),
        "outputDir": config::load().resolved_output_dir().display().to_string(),
        "tables": tables::TABLES.iter().map(|spec| json!({
            "table": spec.table,
            "columns": spec.columns,
            "primaryKey": spec.identity,
        })).collect::<Vec<_>>(),
        "cli": cli::describe_cli(),
    })
}

/// The saved per-table filters and schema regex, so reopening the panel shows
/// what was last used.
fn config_get() -> Value {
    let config = config::load();
    json!({
        "config": config,
        "outputDir": config.resolved_output_dir().display().to_string(),
        "configPath": config::config_path().display().to_string(),
    })
}

fn save_config(params: &Value) -> Result<Value, Abort> {
    let raw = params.get("config").cloned().ok_or_else(|| Abort::Failed("缺少 config".to_string()))?;
    let config: config::Config =
        serde_json::from_value(raw).map_err(|error| Abort::Failed(format!("配置解析失败: {error}")))?;
    config::save(&config).map_err(Abort::Failed)?;
    Ok(json!({ "saved": true }))
}

fn list_connections() -> Result<Value, Abort> {
    let result = cli::connections().map_err(Abort::from)?;
    let connections = result.get("connections").cloned().unwrap_or_else(|| json!([]));
    Ok(json!({ "connections": connections, "cli": cli::describe_cli() }))
}

/// The databases on a connection, for the picker next to it.
///
/// What "database" means differs by family -- see `Dialect::list_databases_sql`
/// -- so the query comes from the dialect while the shape of the answer does
/// not. Everything readable is listed, including databases the user cannot open:
/// picking one fails with the server's own message, which says more than
/// silently hiding it would.
fn list_databases(params: &Value) -> Result<Value, Abort> {
    let connection = required_str(params, "connection")?;
    let dialect = dialect::of_connection(&connection).map_err(Abort::Failed)?;
    let result =
        cli::query(&connection, dialect.list_databases_sql(), Duration::from_secs(20)).map_err(Abort::from)?;
    let databases: Vec<String> = cli::result_rows(&result)
        .iter()
        .filter_map(|row| row.get("name").and_then(encode::canonical))
        .collect();
    Ok(json!({ "connection": connection, "dialect": dialect.label(), "databases": databases }))
}

fn list_snapshots(params: &Value) -> Result<Value, Abort> {
    let connection = required_str(params, "connection")?;
    let database = required_str(params, "database")?;
    let snapshots = store::list(&connection, &database).map_err(Abort::Failed)?;
    Ok(json!({
        "connection": connection,
        "database": database,
        "snapshots": snapshots,
        "directory": store::database_dir(&connection, &database).display().to_string(),
    }))
}

fn inspect_snapshot(params: &Value) -> Result<Value, Abort> {
    let connection = required_str(params, "connection")?;
    let database = required_str(params, "database")?;
    let snapshot_id = required_str(params, "snapshotId")?;
    let dir = store::snapshot_dir(&connection, &database, &snapshot_id);
    let meta = store::read_meta(&dir).map_err(Abort::Failed)?;
    Ok(json!({ "snapshot": meta, "directory": dir.display().to_string() }))
}

fn delete_snapshot(params: &Value) -> Result<Value, Abort> {
    let connection = required_str(params, "connection")?;
    let database = required_str(params, "database")?;
    let snapshot_id = required_str(params, "snapshotId")?;
    store::delete(&connection, &database, &snapshot_id).map_err(Abort::Failed)?;
    Ok(json!({ "deleted": snapshot_id }))
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

fn finish(result: Result<Value, Abort>) -> Result<Value, PluginError> {
    result.map_err(|abort| match abort {
        Abort::Cancelled => PluginError::new(-32020, "已取消"),
        Abort::Failed(message) => PluginError::new(-32021, message),
    })
}

fn main() -> std::io::Result<()> {
    let metadata = PluginMetadata::new(PLUGIN_ID, env!("CARGO_PKG_VERSION"))
        .with_capability("snapshot")
        .with_capability("cli");
    PluginServer::new(metadata, DbDiff).serve()
}
