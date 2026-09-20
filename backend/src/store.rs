//! Snapshot storage.
//!
//! One directory per snapshot, plain text files, no database (PLAN.md §2):
//!
//! ```text
//! <data>/snapshots/<connection>/<database>/<snapshot-id>/
//!   meta.json
//!   data/<table>.jsonl
//! ```
//!
//! **A snapshot's identity is (connection, database).** Two databases behind one
//! connection are two unrelated histories: a baseline taken against `timeuse`
//! says nothing about `timeuse_bak`, and comparing the two would emit SQL that
//! rewrites the wrong rows. The database is a path component, not a field to
//! filter on afterwards, so the two can never end up in one listing.
//!
//! The access pattern is always "read the whole thing into a map", so there is
//! no index to build and SQLite would only add a dependency. Sorted output also
//! makes each file's sha256 a stable change/integrity signal.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::config;
use crate::encode;

/// Recorded for diagnosis only -- nothing reads these to make a decision.
/// Time zone and `sql_mode` are enforced identical across environments at the
/// project level (PLAN.md §8); if that ever stops being true, this is the
/// record that will say so.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    pub time_zone: Option<String>,
    pub sql_mode: Option<String>,
    pub charset: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableMeta {
    pub table: String,
    /// The columns the row hash covered, in order. Stored **in the snapshot** so
    /// an old snapshot can still prove what it hashed after the config changes.
    pub columns: Vec<String>,
    /// Row identity. Compared before any diff: a snapshot taken under a
    /// different identity cannot be compared against the current one.
    pub primary_key: Vec<String>,
    /// The effective `WHERE` condition -- the table's built-in one combined with
    /// the user's filter, without the `where` keyword. Stored per table so a diff
    /// re-applies exactly what produced these hashes.
    #[serde(default)]
    pub filter: String,
    /// Rows actually written.
    pub row_count: u64,
    /// `SELECT COUNT(*)` on the same table. A mismatch is reported, not fatal --
    /// the table can legitimately change while pages are being read.
    pub count_star: Option<u64>,
    /// Rows that share an identity with another row and were therefore folded
    /// into one. Non-zero means the snapshot is lossy for this table and the
    /// report says so.
    #[serde(default)]
    pub collapsed: u64,
    /// Identity values that collapsed, for the report. Capped so a badly
    /// duplicated table cannot blow up `meta.json`.
    #[serde(default)]
    pub collapsed_samples: Vec<String>,
    pub pages: u64,
    pub sha256: String,
    pub file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaMeta {
    pub object_count: usize,
    pub table_count: usize,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotMeta {
    pub snapshot_id: String,
    pub connection: String,
    pub database: String,
    pub taken_at: String,
    pub plugin_version: String,
    pub hash_algo_version: u32,
    pub session: SessionInfo,
    /// Regex the schema channel kept table names by. Same reasoning as the
    /// per-table filters: a table excluded here but present live would otherwise
    /// show up as a whole new table to create.
    #[serde(default)]
    pub schema_filter: String,
    pub data_tables: Vec<TableMeta>,
    /// Absent on snapshots taken before the schema channel existed, which is why
    /// the diff refuses to use one as a schema baseline rather than treating the
    /// missing file as "no tables".
    #[serde(default)]
    pub schema: Option<SchemaMeta>,
}

impl SnapshotMeta {
    /// Look a table up **by name**. Never index `data_tables` positionally
    /// against anything else: the array's order comes from `tables.rs` at the
    /// time the snapshot was taken, so a snapshot written before the order
    /// changed would pair the wrong rows with the wrong table.
    pub fn table(&self, name: &str) -> Option<&TableMeta> {
        self.data_tables.iter().find(|table| table.table == name)
    }
}

pub fn snapshots_root() -> PathBuf {
    config::data_dir().join("snapshots")
}

/// Connection and database names can contain characters that are illegal in
/// filenames (`192.168.0.195:6397 | gjj:1` has two), so each path component gets
/// a sanitized name plus a hash suffix to keep it collision-free.
///
/// One function for both components on purpose: `a/b` and `a_b` sanitize to the
/// same text, but their digests differ, so two different databases stay two
/// different directories.
pub fn slug(value: &str) -> String {
    let mut safe: String = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect();
    safe.truncate(48);
    let digest = encode::sha256_hex(value.as_bytes());
    format!("{safe}-{}", &digest[..8])
}

pub fn connection_dir(connection: &str) -> PathBuf {
    snapshots_root().join(slug(connection))
}

pub fn database_dir(connection: &str, database: &str) -> PathBuf {
    connection_dir(connection).join(slug(database))
}

pub fn snapshot_dir(connection: &str, database: &str, snapshot_id: &str) -> PathBuf {
    database_dir(connection, database).join(snapshot_id)
}

/// `20260917-143000`, with a numeric suffix if that second is already taken.
pub fn next_snapshot_id(connection: &str, database: &str, base: &str) -> String {
    let root = database_dir(connection, database);
    if !root.join(base).exists() {
        return base.to_string();
    }
    for suffix in 2..1000 {
        let candidate = format!("{base}-{suffix}");
        if !root.join(&candidate).exists() {
            return candidate;
        }
    }
    format!("{base}-{}", encode::sha256_hex(base.as_bytes())[..6].to_string())
}

pub fn prepare_dir(connection: &str, database: &str, snapshot_id: &str) -> Result<PathBuf, String> {
    let dir = snapshot_dir(connection, database, snapshot_id);
    fs::create_dir_all(dir.join("data")).map_err(|error| format!("无法创建快照目录 {}: {error}", dir.display()))?;
    Ok(dir)
}

/// Snapshots used to sit directly under the connection directory, from before
/// the database became part of the identity. Anything still at that depth is
/// moved into `<connection>/<slug(database)>/` using the database its own
/// `meta.json` records.
///
/// Every move is a `rename` inside one tree, so it is atomic: a crash can leave
/// a snapshot at the old depth, never in neither place. A directory with no
/// `meta.json` is a database directory and is skipped, which is also what stops
/// this from doing anything on an already-migrated store.
pub fn migrate_legacy(connection: &str) {
    let root = connection_dir(connection);
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() || !path.join("meta.json").is_file() {
            continue;
        }
        let meta = match read_meta(&path) {
            Ok(meta) => meta,
            Err(_) => continue,
        };
        let name = match path.file_name() {
            Some(name) => name.to_owned(),
            None => continue,
        };
        let target = database_dir(connection, &meta.database);
        if fs::create_dir_all(&target).is_err() {
            continue;
        }
        let _ = fs::rename(&path, target.join(name));
    }
}

pub fn write_meta(dir: &Path, meta: &SnapshotMeta) -> Result<(), String> {
    let raw = serde_json::to_string_pretty(meta).map_err(|error| error.to_string())?;
    let path = dir.join("meta.json");
    fs::write(&path, format!("{raw}\n")).map_err(|error| format!("无法写入 {}: {error}", path.display()))
}

pub fn read_meta(dir: &Path) -> Result<SnapshotMeta, String> {
    let path = dir.join("meta.json");
    let raw = fs::read_to_string(&path).map_err(|error| format!("无法读取 {}: {error}", path.display()))?;
    serde_json::from_str(&raw).map_err(|error| format!("{} 解析失败: {error}", path.display()))
}

/// Newest first. Only this (connection, database) pair's snapshots.
pub fn list(connection: &str, database: &str) -> Result<Vec<SnapshotMeta>, String> {
    migrate_legacy(connection);
    if database.trim().is_empty() {
        return Ok(Vec::new());
    }
    let root = database_dir(connection, database);
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("无法读取 {}: {error}", root.display())),
    };
    let mut snapshots = Vec::new();
    for entry in entries.flatten() {
        if !entry.path().is_dir() {
            continue;
        }
        if let Ok(meta) = read_meta(&entry.path()) {
            snapshots.push(meta);
        }
    }
    snapshots.sort_by(|a, b| b.snapshot_id.cmp(&a.snapshot_id));
    Ok(snapshots)
}

pub fn delete(connection: &str, database: &str, snapshot_id: &str) -> Result<(), String> {
    // Guard against a crafted id escaping the database directory.
    if snapshot_id.is_empty() || snapshot_id.contains(['/', '\\']) || snapshot_id.contains("..") {
        return Err(format!("非法的快照 id: {snapshot_id}"));
    }
    let dir = snapshot_dir(connection, database, snapshot_id);
    if !dir.exists() {
        return Err(format!("快照不存在: {snapshot_id}"));
    }
    fs::remove_dir_all(&dir).map_err(|error| format!("无法删除 {}: {error}", dir.display()))
}
