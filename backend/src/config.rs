//! Settings that outlive a session, and where the plugin is allowed to write.
//!
//! The compared tables and their columns are fixed in `tables.rs`. What is
//! configurable is the per-table filter and the schema regex, and those are saved
//! here so reopening the panel shows what was last used.
//!
//! A diff runs with the **input boxes' current values**, not with this file and
//! not with the snapshot's record. Those saved values exist to be compared
//! against the snapshot's: when they differ, the diff asks first.
//!
//! The sidecar's working directory is the installed plugin version
//! (`<data>/plugins/<id>/versions/<version>`), which is what DBX's own
//! `PluginRegistry::plugin_data_dir` is derived from -- keeping this there means
//! it follows a portable DBX data directory instead of a fixed per-user path.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const PLUGIN_ID: &str = "dbx.demo.dbdiff";
const DATA_DIR_ENV: &str = "DBX_DBDIFF_DATA_DIR";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Config {
    /// Where the generated SQL and report are written. Empty means
    /// `<data dir>/output`.
    #[serde(default, alias = "output_dir")]
    pub output_dir: String,
    /// Extra `WHERE` conditions keyed by table name, without the `where`
    /// keyword -- `{"dsfa_rm": "ds_version = 'project'"}`.
    #[serde(default)]
    pub filters: BTreeMap<String, String>,
    /// Regex the schema channel keeps table names by.
    #[serde(default, alias = "schema_filter")]
    pub schema_filter: String,
}

impl Config {
    pub fn resolved_output_dir(&self) -> PathBuf {
        if self.output_dir.trim().is_empty() {
            data_dir().join("output")
        } else {
            PathBuf::from(self.output_dir.trim())
        }
    }
}

pub fn data_dir() -> PathBuf {
    if let Some(explicit) = std::env::var_os(DATA_DIR_ENV) {
        return PathBuf::from(explicit);
    }
    derive_plugin_data_dir().unwrap_or_else(fallback_data_dir)
}

/// `<data>/plugins/<id>/versions/<version>` -> `<data>/plugin-data/<id>`
fn derive_plugin_data_dir() -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    let versions = cwd.parent()?;
    let plugin = versions.parent()?;
    let plugins = plugin.parent()?;
    // Validate the shape before trusting it: a plugin run from a source tree
    // (or a debugger) must not silently write somewhere unrelated.
    if versions.file_name()?.to_str()? != "versions" {
        return None;
    }
    if plugin.file_name()?.to_str()? != PLUGIN_ID {
        return None;
    }
    if plugins.file_name()?.to_str()? != "plugins" {
        return None;
    }
    Some(plugins.parent()?.join("plugin-data").join(PLUGIN_ID))
}

fn fallback_data_dir() -> PathBuf {
    #[cfg(windows)]
    if let Some(appdata) = std::env::var_os("APPDATA") {
        return Path::new(&appdata).join("dbx-plugin-dbdiff");
    }
    if let Some(home) = std::env::var_os("HOME") {
        return Path::new(&home).join(".local").join("share").join("dbx-plugin-dbdiff");
    }
    PathBuf::from(".dbx-plugin-dbdiff")
}

pub fn config_path() -> PathBuf {
    data_dir().join("config.json")
}

pub fn load() -> Config {
    match fs::read_to_string(config_path()) {
        Ok(raw) => serde_json::from_str(&raw).unwrap_or_default(),
        Err(_) => Config::default(),
    }
}

pub fn save(config: &Config) -> Result<(), String> {
    let path = config_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| format!("无法创建数据目录 {}: {error}", parent.display()))?;
    }
    let raw = serde_json::to_string_pretty(config).map_err(|error| error.to_string())?;
    fs::write(&path, format!("{raw}\n")).map_err(|error| format!("无法写入 {}: {error}", path.display()))
}
