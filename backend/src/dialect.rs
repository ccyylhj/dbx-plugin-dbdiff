//! Everything that differs between database families, in one place.
//!
//! The plugin speaks to a server through `dbx query`, and the CLI already
//! flattens the *data* side: values come back as JSON strings whatever the
//! driver, `COUNT(*)`/`ORDER BY`/`LIMIT OFFSET` mean the same thing, and a row
//! constructor `(a, b) IN ((..), (..))` works on both. What does not flatten is
//! syntax, and that is what this module holds -- identifier quoting, how to ask
//! for the current database, how a conditional is written, how to make an empty
//! copy of a table.
//!
//! **PostgreSQL here means the PostgreSQL protocol family**, not one product:
//! KingbaseES, GaussDB, highgo, vastbase and the rest speak the same wire
//! protocol and the same `information_schema`, and `dbx connections list`
//! reports all of them as `postgres`. Anything that turns out to be
//! product-specific belongs in a new variant rather than a flag on this one.
//!
//! The rule for names: **a name from `tables.rs` is folded through the dialect
//! before it is used anywhere else**. MySQL is case-insensitive for column
//! names, so the list keeps the spelling the MySQL database happens to have
//! (`dsfa_mm_valueAttributes_id`); PostgreSQL folds unquoted identifiers to
//! lower case, so the same name has to be spelled lower there or a quoted
//! reference misses the column. Folding at the one boundary keeps a single
//! column list instead of two that would drift apart.

use crate::cli;
use crate::encode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    MySql,
    Postgres,
}

/// `None` for a connection type this plugin has no syntax for. The caller
/// reports that rather than guessing -- issuing MySQL syntax at a MongoDB would
/// produce a mysterious server error instead of a clear one.
pub fn for_connection_type(kind: &str) -> Option<Dialect> {
    match kind.trim().to_ascii_lowercase().as_str() {
        "mysql" | "mariadb" | "doris" | "starrocks" => Some(Dialect::MySql),
        "postgres" | "postgresql" | "kingbase" | "gaussdb" | "highgo" | "vastbase" | "uxdb"
        | "yashandb" | "greenplum" | "redshift" => Some(Dialect::Postgres),
        _ => None,
    }
}

impl Dialect {
    pub fn label(&self) -> &'static str {
        match self {
            Dialect::MySql => "MySQL",
            Dialect::Postgres => "PostgreSQL",
        }
    }

    /// The spelling this server uses for a name that came out of `tables.rs`.
    pub fn fold(&self, name: &str) -> String {
        match self {
            // Case-insensitive for column names, and the list already uses the
            // spelling the server reports.
            Dialect::MySql => name.to_string(),
            // Unquoted identifiers fold to lower case; quoting the mixed-case
            // spelling would make it case-sensitive and miss.
            Dialect::Postgres => name.to_ascii_lowercase(),
        }
    }

    /// Quote an identifier for a statement that runs on this server. The name is
    /// taken as final -- fold it first if it came from `tables.rs`.
    pub fn quote_ident(&self, name: &str) -> String {
        match self {
            Dialect::MySql => encode::quote_ident(name),
            Dialect::Postgres => format!("\"{}\"", name.replace('"', "\"\"")),
        }
    }

    pub fn quote_ident_list(&self, names: &[String]) -> String {
        names.iter().map(|name| self.quote_ident(name)).collect::<Vec<_>>().join(", ")
    }

    /// One row with the current database, so `resolve_database` has something to
    /// fall back on when the caller does not name one.
    pub fn current_database_sql(&self) -> &'static str {
        match self {
            Dialect::MySql => "SELECT DATABASE() AS db",
            Dialect::Postgres => "SELECT current_database() AS db",
        }
    }

    /// A SQL expression for "the schema whose objects the structure channel
    /// compares", given the database the user picked.
    ///
    /// The two families scope structure completely differently. MySQL has one
    /// namespace per database, so `information_schema` rows are filtered by the
    /// database name. PostgreSQL separates database from schema inside it: the
    /// connection is already bound to one database, and `information_schema`
    /// covers *every* schema of it, so the filter has to name a schema instead.
    /// `current_schema()` is the first entry of `search_path` that exists, which
    /// is what an unqualified table name resolves to -- the same thing these
    /// queries mean on MySQL. A machine whose tables live in a schema that is
    /// not on `search_path` needs this to become configurable.
    pub fn schema_expr(&self, database: &str) -> String {
        match self {
            Dialect::MySql => encode::quote_string(database),
            Dialect::Postgres => "current_schema()".to_string(),
        }
    }

    /// How a table is named in a statement. MySQL can reach across databases
    /// from one connection, so it qualifies. PostgreSQL cannot -- a
    /// cross-database reference is an error there -- so the table is left
    /// unqualified and resolves through `search_path`.
    pub fn qualify(&self, database: &str, table: &str) -> String {
        match self {
            Dialect::MySql => format!(
                "{}.{}",
                encode::quote_ident(database),
                encode::quote_ident(table)
            ),
            Dialect::Postgres => self.quote_ident(table),
        }
    }

    /// What the database picker offers.
    ///
    /// One connection sees many databases on MySQL, so they are all listed. On
    /// PostgreSQL a connection *is* a database -- `information_schema.schemata`
    /// lists schemas inside it, and nothing can reach another database without
    /// an extension -- so the only honest answer is the one it is already
    /// connected to.
    pub fn list_databases_sql(&self) -> &'static str {
        match self {
            Dialect::MySql => concat!(
                "SELECT SCHEMA_NAME AS name FROM information_schema.SCHEMATA ",
                "WHERE SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys') ",
                "ORDER BY SCHEMA_NAME",
            ),
            Dialect::Postgres => "SELECT current_database() AS name",
        }
    }

    /// `IF(c, a, b)` is MySQL; PostgreSQL spells it `CASE WHEN`.
    pub fn conditional(&self, condition: &str, then: &str, otherwise: &str) -> String {
        match self {
            Dialect::MySql => format!("IF({condition}, {then}, {otherwise})"),
            Dialect::Postgres => format!("CASE WHEN {condition} THEN {then} ELSE {otherwise} END"),
        }
    }

    /// An empty table with the same shape as `existing`.
    ///
    /// `LIKE` is not portable: MySQL copies indexes and all, PostgreSQL needs
    /// `INCLUDING ALL` to do the same. Without it the backup table would come
    /// out with no primary key -- and the report leans on that key to explain
    /// why a second run's backup is a no-op.
    pub fn create_table_like(&self, new: &str, existing: &str) -> String {
        match self {
            Dialect::MySql => format!(
                "CREATE TABLE IF NOT EXISTS {} LIKE {};",
                self.quote_ident(new),
                self.quote_ident(existing)
            ),
            Dialect::Postgres => format!(
                "CREATE TABLE IF NOT EXISTS {} (LIKE {} INCLUDING ALL);",
                self.quote_ident(new),
                self.quote_ident(existing)
            ),
        }
    }

    /// One page of a scan. Both servers accept `LIMIT n OFFSET m`; they disagree
    /// on `LIMIT m, n`, which is why it is written this way.
    pub fn page(&self, select_sql: &str, limit: usize, offset: u64) -> String {
        format!("{select_sql} LIMIT {limit} OFFSET {offset}")
    }

    /// Session facts recorded in the snapshot for diagnosis only. The column
    /// names are the same on both sides so one reader handles either.
    pub fn session_probe_sql(&self) -> &'static str {
        match self {
            Dialect::MySql => concat!(
                "SELECT @@session.time_zone AS tz, @@session.sql_mode AS mode, ",
                "@@session.character_set_connection AS charset",
            ),
            Dialect::Postgres => concat!(
                "SELECT current_setting('TimeZone') AS tz, ",
                "current_setting('search_path') AS mode, ",
                "current_setting('server_encoding') AS charset",
            ),
        }
    }

}

/// The dialect a saved connection speaks, from the CLI's own connection record.
///
/// Errors rather than defaulting: issuing MySQL syntax at a MongoDB produces a
/// confusing server error, and the message here can just say what is wrong.
pub fn of_connection(connection: &str) -> Result<Dialect, String> {
    let kind = cli::connection_kind(connection)
        .map_err(|error| format!("读取连接 '{connection}' 的类型失败：{}", error.message))?;
    for_connection_type(&kind).ok_or_else(|| {
        format!(
            "连接 '{connection}' 的类型是 '{kind}'，本插件还没有这个方言的语法支持。             目前支持 MySQL 和 PostgreSQL 两族（含金仓 / GaussDB 等 PG 协议系）。"
        )
    })
}
