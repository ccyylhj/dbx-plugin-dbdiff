//! Whole-database schema collection, comparison, and DDL emission.
//!
//! Source is `information_schema`, never `SHOW CREATE TABLE` -- the latter drifts
//! its formatting between MySQL versions, which shows up as thousands of false
//! differences (PLAN.md §3.2).
//!
//! Direction matters: the baseline is the **old** snapshot and `live` is the
//! database now. Anything present in `live` but not the baseline is new in this
//! environment and gets a statement. Anything present in the baseline but not in
//! `live` was removed here, so the target environment has it too -- that is
//! reported, never turned into a DROP (PLAN.md §0.3).

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::cli;
use crate::dialect::Dialect;
use crate::schema_pg;
use crate::encode::{quote_ident, quote_string};
use crate::snapshot::Abort;

/// One structural fact. Short keys keep `schema.jsonl` compact and greppable.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaObject {
    /// table name
    pub t: String,
    /// kind: `table` | `column` | `index` | `foreign_key`
    pub o: String,
    /// object name within the table (`""` for the table itself)
    pub n: String,
    pub d: Value,
}

// ------------------------------------------------------------------ collect

pub fn compile_filter(pattern: &str) -> Result<Option<fancy_regex::Regex>, String> {
    let pattern = pattern.trim();
    if pattern.is_empty() {
        return Ok(None);
    }
    fancy_regex::Regex::new(pattern)
        .map(Some)
        .map_err(|error| format!("表结构过滤正则无效: {error}"))
}

/// Keep only objects whose table matches the filter.
///
/// Applied *after* collection rather than inside it, so the same unfiltered read
/// can also serve as the source of column types for the data channel -- those
/// six tables have to be typed correctly whether or not the schema filter keeps
/// them.
pub fn filter_objects(objects: &mut Vec<SchemaObject>, pattern: &str) -> Result<(), String> {
    let Some(regex) = compile_filter(pattern)? else {
        return Ok(());
    };
    // A regex that errors at match time (the backtracking guard firing, say)
    // drops the table rather than taking the whole collection down.
    objects.retain(|object| regex.is_match(&object.t).unwrap_or(false));
    Ok(())
}

/// The database's structure. Every query is paginated for the same reason the
/// data channel is: `dbx query` silently caps at 10000 rows otherwise.
///
/// Only MySQL has an implementation so far. The PostgreSQL collection is the
/// remaining piece of the dialect work -- `information_schema` there has no
/// `COLUMN_TYPE`, no `ENGINE` and no `STATISTICS`, so it is a separate set of
/// queries rather than a variation on these.
pub fn collect(connection: &str, database: &str, dialect: Dialect) -> Result<Vec<SchemaObject>, Abort> {
    match dialect {
        Dialect::MySql => collect_mysql(connection, database),
        Dialect::Postgres => crate::schema_pg::collect(connection),
    }
}

fn collect_mysql(connection: &str, database: &str) -> Result<Vec<SchemaObject>, Abort> {
    let db = quote_string(database);
    let mut objects = Vec::new();

    let tables_sql = format!(
        "SELECT tb.TABLE_NAME AS table_name, tb.ENGINE AS engine, tb.TABLE_COLLATION AS table_collation \
         FROM information_schema.TABLES tb \
         WHERE tb.TABLE_SCHEMA = {db} AND tb.TABLE_TYPE = 'BASE TABLE' \
         ORDER BY tb.TABLE_NAME"
    );
    cli::paginate(connection, &tables_sql, &mut |rows| {
        for row in rows {
            objects.push(SchemaObject {
                t: text(row, "table_name"),
                o: "table".to_string(),
                n: String::new(),
                d: json!({
                    "engine": nullable_text(row, "engine"),
                    "collation": nullable_text(row, "table_collation"),
                }),
            });
        }
        Ok(())
    })?;

    let columns_sql = format!(
        "SELECT c.TABLE_NAME AS table_name, c.COLUMN_NAME AS column_name, c.COLUMN_TYPE AS column_type, \
                c.IS_NULLABLE AS is_nullable, c.COLUMN_DEFAULT AS column_default, c.EXTRA AS extra, \
                c.CHARACTER_SET_NAME AS charset, c.COLLATION_NAME AS collation, c.ORDINAL_POSITION AS ordinal \
         FROM information_schema.COLUMNS c \
         JOIN information_schema.TABLES tb \
           ON tb.TABLE_SCHEMA = c.TABLE_SCHEMA AND tb.TABLE_NAME = c.TABLE_NAME \
         WHERE c.TABLE_SCHEMA = {db} AND tb.TABLE_TYPE = 'BASE TABLE' \
         ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION"
    );
    cli::paginate(connection, &columns_sql, &mut |rows| {
        for row in rows {
            objects.push(SchemaObject {
                t: text(row, "table_name"),
                o: "column".to_string(),
                n: text(row, "column_name"),
                d: json!({
                    "columnType": text(row, "column_type"),
                    "nullable": text(row, "is_nullable").eq_ignore_ascii_case("YES"),
                    "default": nullable_text(row, "column_default"),
                    "extra": text(row, "extra"),
                    "charset": nullable_text(row, "charset"),
                    "collation": nullable_text(row, "collation"),
                    "ordinal": text(row, "ordinal").parse::<u32>().unwrap_or(0),
                }),
            });
        }
        Ok(())
    })?;

    // One row per index column; folded into a single object per index below.
    let indexes_sql = format!(
        "SELECT s.TABLE_NAME AS table_name, s.INDEX_NAME AS index_name, s.NON_UNIQUE AS non_unique, \
                s.SEQ_IN_INDEX AS seq, s.COLUMN_NAME AS column_name, s.SUB_PART AS sub_part, \
                s.INDEX_TYPE AS index_type \
         FROM information_schema.STATISTICS s \
         JOIN information_schema.TABLES tb \
           ON tb.TABLE_SCHEMA = s.TABLE_SCHEMA AND tb.TABLE_NAME = s.TABLE_NAME \
         WHERE s.TABLE_SCHEMA = {db} AND tb.TABLE_TYPE = 'BASE TABLE' \
         ORDER BY s.TABLE_NAME, s.INDEX_NAME, s.SEQ_IN_INDEX"
    );
    let mut pending_index: Option<(String, String, Value, Vec<Value>)> = None;
    let flush_index = |pending: Option<(String, String, Value, Vec<Value>)>, objects: &mut Vec<SchemaObject>| {
        if let Some((table, name, mut head, columns)) = pending {
            head["columns"] = Value::Array(columns);
            objects.push(SchemaObject { t: table, o: "index".to_string(), n: name, d: head });
        }
    };
    cli::paginate(connection, &indexes_sql, &mut |rows| {
        for row in rows {
            let table = text(row, "table_name");
            let name = text(row, "index_name");
            let column = json!({
                "name": text(row, "column_name"),
                "prefix": nullable_text(row, "sub_part"),
            });
            match &mut pending_index {
                Some((pending_table, pending_name, _, columns)) if *pending_table == table && *pending_name == name => {
                    columns.push(column);
                }
                _ => {
                    flush_index(pending_index.take(), &mut objects);
                    let head = json!({
                        "unique": text(row, "non_unique") == "0",
                        "type": text(row, "index_type"),
                    });
                    pending_index = Some((table, name, head, vec![column]));
                }
            }
        }
        Ok(())
    })?;
    flush_index(pending_index.take(), &mut objects);

    let foreign_keys_sql = format!(
        "SELECT k.TABLE_NAME AS table_name, k.CONSTRAINT_NAME AS constraint_name, k.COLUMN_NAME AS column_name, \
                k.REFERENCED_TABLE_NAME AS ref_table, k.REFERENCED_COLUMN_NAME AS ref_column, \
                r.UPDATE_RULE AS update_rule, r.DELETE_RULE AS delete_rule \
         FROM information_schema.KEY_COLUMN_USAGE k \
         JOIN information_schema.REFERENTIAL_CONSTRAINTS r \
           ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME \
         WHERE k.TABLE_SCHEMA = {db} AND k.REFERENCED_TABLE_NAME IS NOT NULL \
         ORDER BY k.TABLE_NAME, k.CONSTRAINT_NAME, k.ORDINAL_POSITION"
    );
    let mut pending_fk: Option<(String, String, Value, Vec<String>, Vec<String>)> = None;
    let flush_fk = |pending: Option<(String, String, Value, Vec<String>, Vec<String>)>,
                        objects: &mut Vec<SchemaObject>| {
        if let Some((table, name, mut head, columns, referenced)) = pending {
            head["columns"] = json!(columns);
            head["referencedColumns"] = json!(referenced);
            objects.push(SchemaObject { t: table, o: "foreign_key".to_string(), n: name, d: head });
        }
    };
    cli::paginate(connection, &foreign_keys_sql, &mut |rows| {
        for row in rows {
            let table = text(row, "table_name");
            let name = text(row, "constraint_name");
            match &mut pending_fk {
                Some((pending_table, pending_name, _, columns, referenced))
                    if *pending_table == table && *pending_name == name =>
                {
                    columns.push(text(row, "column_name"));
                    referenced.push(text(row, "ref_column"));
                }
                _ => {
                    flush_fk(pending_fk.take(), &mut objects);
                    let head = json!({
                        "referencedTable": text(row, "ref_table"),
                        "updateRule": nullable_text(row, "update_rule"),
                        "deleteRule": nullable_text(row, "delete_rule"),
                    });
                    pending_fk = Some((table, name, head, vec![text(row, "column_name")], vec![text(row, "ref_column")]));
                }
            }
        }
        Ok(())
    })?;
    flush_fk(pending_fk.take(), &mut objects);

    objects.sort_by(|a, b| (&a.t, &a.o, &a.n).cmp(&(&b.t, &b.o, &b.n)));
    Ok(objects)
}

// ------------------------------------------------------------------ storage

pub fn write_jsonl(path: &Path, objects: &[SchemaObject]) -> Result<(), String> {
    let mut out = String::new();
    for object in objects {
        out.push_str(&serde_json::to_string(object).map_err(|error| error.to_string())?);
        out.push('\n');
    }
    fs::write(path, out).map_err(|error| format!("无法写入 {}: {error}", path.display()))
}

pub fn read_jsonl(path: &Path) -> Result<Vec<SchemaObject>, String> {
    let raw = fs::read_to_string(path).map_err(|error| format!("无法读取 {}: {error}", path.display()))?;
    let mut objects = Vec::new();
    for line in raw.lines() {
        if line.trim().is_empty() {
            continue;
        }
        objects.push(serde_json::from_str(line).map_err(|error| format!("{} 解析失败: {error}", path.display()))?);
    }
    Ok(objects)
}

// --------------------------------------------------------------------- diff

/// One generated statement, with enough context to identify it in the report.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Statement {
    pub table: String,
    /// `table` | `column:<name>` | `index:<name>` | `foreign_key:<name>` | `engine` | `collation`
    pub object: String,
    /// Rendered above the statement in the generated file.
    pub comment: Option<String>,
    pub sql: String,
}

impl Statement {
    fn plain(table: &str, object: impl Into<String>, sql: impl Into<String>) -> Self {
        Self { table: table.to_string(), object: object.into(), comment: None, sql: sql.into() }
    }

    fn noted(table: &str, object: impl Into<String>, comment: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            table: table.to_string(),
            object: object.into(),
            comment: Some(comment.into()),
            sql: sql.into(),
        }
    }
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaDiff {
    /// Safe to run unattended: new tables, columns and indexes, **widening**
    /// type changes (`varchar(100) -> varchar(1000)`), defaults.
    pub auto: Vec<Statement>,
    /// Needs a human: narrowing or converting type changes, `EXTRA` changes,
    /// foreign keys.
    pub review: Vec<Statement>,
    /// Present in the baseline but gone now. Never turned into a statement.
    pub removed: Vec<String>,
    /// Objects that exist on both sides but differ in ways we do not act on.
    pub notes: Vec<String>,
    pub added_tables: usize,
    pub added_columns: usize,
    pub changed_columns: usize,
    pub added_indexes: usize,
}

/// A column type split into the three things that decide whether a change can
/// lose data: what family it is, the capacity numbers in parentheses, and the
/// trailing modifiers (`unsigned`, `zerofill`, ...).
struct ColumnType {
    base: String,
    args: Vec<i64>,
    modifiers: Vec<String>,
}

/// `None` when the type is not a plain `base(args) modifiers` -- `enum('a','b')`
/// and `set(...)` carry quoted members that are not capacities, so they are
/// refused rather than guessed at.
fn parse_column_type(raw: &str) -> Option<ColumnType> {
    let text = raw.trim().to_ascii_lowercase();
    if text.is_empty() {
        return None;
    }
    let (head, tail) = match text.split_once('(') {
        Some((head, tail)) => (head, Some(tail)),
        None => (text.as_str(), None),
    };
    let mut words = head.split_whitespace();
    let base = words.next()?.to_string();
    let mut modifiers: Vec<String> = words.map(str::to_string).collect();
    let mut args: Vec<i64> = Vec::new();
    if let Some(tail) = tail {
        let (inside, after) = tail.split_once(')')?;
        for part in inside.split(',') {
            args.push(part.trim().parse::<i64>().ok()?);
        }
        modifiers.extend(after.split_whitespace().map(str::to_string));
    }
    Some(ColumnType { base, args, modifiers })
}

/// Integer types, narrowest first. `integer` is `int` under another name.
const INTEGER_FAMILY: &[&str] = &["tinyint", "smallint", "mediumint", "int", "bigint"];
/// Text and blob types, narrowest first. Each is its own family: a `text` has no
/// business being compared against a `blob`.
const TEXT_FAMILY: &[&str] = &["tinytext", "text", "mediumtext", "longtext"];
const BLOB_FAMILY: &[&str] = &["tinyblob", "blob", "mediumblob", "longblob"];

/// Whether `new` can hold everything `old` could. Answers one question only --
/// *can this lose data* -- and says no whenever it is unsure, because "no" is
/// what sends a change to the file a human reads. Getting this wrong in the
/// other direction silently truncates a production column.
fn widens(old: &str, new: &str) -> bool {
    let (Some(old), Some(new)) = (parse_column_type(old), parse_column_type(new)) else {
        return false;
    };
    // `int(11) -> int(11) unsigned` is not a widening: the column changed kind,
    // and every value may have to be rewritten. A differing modifier list is
    // never a widening.
    if old.modifiers != new.modifiers {
        return false;
    }

    if old.base == new.base {
        if old.base == "decimal" {
            // Precision alone says nothing: decimal(10,2) -> decimal(10,3) keeps
            // the precision but gives up an integer digit, so it can overflow.
            // The digits left of the point are what has to grow.
            return match (old.args.as_slice(), new.args.as_slice()) {
                ([op, os], [np, ns]) => np - ns >= op - os && ns >= os,
                _ => false,
            };
        }
        // varchar/char/binary/varbinary and the temporal fractional-second forms
        // all take capacity numbers that only grow in one direction.
        return !old.args.is_empty()
            && old.args.len() == new.args.len()
            && old.args.iter().zip(new.args.iter()).all(|(old, new)| new >= old);
    }

    // Different base: only upward within one family. `int` and `integer` are the
    // same type spelled two ways, and MySQL reports the short one.
    let normalize = |base: &str| if base == "integer" { "int".to_string() } else { base.to_string() };
    let (old_base, new_base) = (normalize(&old.base), normalize(&new.base));
    for family in [INTEGER_FAMILY, TEXT_FAMILY, BLOB_FAMILY] {
        let rank = |base: &str| family.iter().position(|item| *item == base);
        if let (Some(old_rank), Some(new_rank)) = (rank(&old_base), rank(&new_base)) {
            return new_rank > old_rank;
        }
    }
    // float -> double is the one widening that crosses two bases.
    old_base == "float" && new_base == "double"
}

fn group(objects: &[SchemaObject]) -> BTreeMap<(String, String), BTreeMap<String, Value>> {
    let mut grouped: BTreeMap<(String, String), BTreeMap<String, Value>> = BTreeMap::new();
    for object in objects {
        grouped.entry((object.t.clone(), object.o.clone())).or_default().insert(object.n.clone(), object.d.clone());
    }
    grouped
}

pub fn diff(baseline: &[SchemaObject], live: &[SchemaObject], dialect: Dialect) -> SchemaDiff {
    let old = group(baseline);
    let new = group(live);
    let mut result = SchemaDiff::default();

    let empty: BTreeMap<String, Value> = BTreeMap::new();
    let old_tables = table_names(baseline);
    let new_tables = table_names(live);

    // ---- tables
    for table in &new_tables {
        if !old_tables.contains(table) {
            result.added_tables += 1;
            result.auto.push(Statement::noted(
                table,
                "table",
                "整表新增 —— 目标库还没有这张表。已经建过再跑一次，CREATE TABLE 会直接报错跳过。",
                create_table(dialect, table, &new),
            ));
            // PostgreSQL cannot declare a plain index inside `CREATE TABLE`, and
            // PostgreSQL indexes are not in the index diff below for a table the
            // baseline does not have -- so a new table's indexes would go missing.
            // MySQL puts them inline, which is why this is one-sided.
            if dialect == Dialect::Postgres {
                let indexes = new.get(&(table.clone(), "index".to_string())).unwrap_or(&empty);
                for (name, detail) in indexes {
                    if !schema_pg::is_primary(detail) {
                        result.added_indexes += 1;
                        result.auto.push(Statement::plain(
                            table,
                            format!("index:{name}"),
                            schema_pg::index_statement(table, name, detail),
                        ));
                    }
                }
            }
        }
    }
    for table in &old_tables {
        if !new_tables.contains(table) {
            result.removed.push(format!("表 `{table}` 已删除 —— 不生成 DROP"));
        }
    }

    // ---- columns
    for table in new_tables.intersection(&old_tables) {
        let old_columns = old.get(&(table.clone(), "column".to_string())).unwrap_or(&empty);
        let new_columns = new.get(&(table.clone(), "column".to_string())).unwrap_or(&empty);
        let table_detail = new.get(&(table.clone(), "table".to_string())).and_then(|tables| tables.get(""));

        for (name, detail) in new_columns {
            match old_columns.get(name) {
                None => {
                    result.added_columns += 1;
                    result.auto.push(Statement::plain(
                        table,
                        format!("column:{name}"),
                        add_column_statement(dialect, table, name, detail, table_detail),
                    ));
                }
                Some(previous) if previous != detail => {
                    result.changed_columns += 1;
                    let definition = match dialect {
                        Dialect::MySql => format!(
                            "ALTER TABLE {} MODIFY COLUMN {};",
                            quote_ident(table),
                            column_definition(name, detail, table_detail)
                        ),
                        Dialect::Postgres => schema_pg::change_column_statement(table, name, detail),
                    };
                    let column_type_changed = previous.get("columnType") != detail.get("columnType");
                    let extra_changed = previous.get("extra") != detail.get("extra");
                    let collation_changed = previous.get("collation") != detail.get("collation");
                    if column_type_changed {
                        let old_type = previous.get("columnType").and_then(Value::as_str).unwrap_or("?");
                        let new_type = detail.get("columnType").and_then(Value::as_str).unwrap_or("?");
                        let widening = match dialect {
                            Dialect::MySql => widens(old_type, new_type),
                            Dialect::Postgres => schema_pg::widens(old_type, new_type),
                        };
                        if widening {
                            // `varchar(100) -> varchar(1000)` cannot lose a row,
                            // and it is the most common type change there is. It
                            // has no business in the file a human has to read
                            // through line by line.
                            result.auto.push(Statement::noted(
                                table,
                                format!("column:{name}"),
                                format!("类型放宽 {old_type} -> {new_type}（不会丢数据）"),
                                definition,
                            ));
                        } else {
                            result.review.push(Statement::noted(
                                table,
                                format!("column:{name}"),
                                format!("类型变化 {old_type} -> {new_type}。收窄或转换都可能丢数据，确认后再执行。"),
                                definition,
                            ));
                        }
                    } else if extra_changed || collation_changed {
                        result.review.push(Statement::noted(
                            table,
                            format!("column:{name}"),
                            "列定义变化（EXTRA / COLLATE）—— 会重写该列，确认后再执行。",
                            definition,
                        ));
                    } else {
                        result.auto.push(Statement::plain(table, format!("column:{name}"), definition));
                    }
                }
                Some(_) => {}
            }
        }
        for name in old_columns.keys() {
            if !new_columns.contains_key(name) {
                result.removed.push(format!("列 `{table}`.`{name}` 已删除 —— 不生成 DROP COLUMN"));
            }
        }
    }

    // ---- indexes
    for table in new_tables.intersection(&old_tables) {
        let old_indexes = old.get(&(table.clone(), "index".to_string())).unwrap_or(&empty);
        let new_indexes = new.get(&(table.clone(), "index".to_string())).unwrap_or(&empty);
        for (name, detail) in new_indexes {
            match old_indexes.get(name) {
                None => {
                    result.added_indexes += 1;
                    let sql = match dialect {
                        Dialect::MySql => format!(
                            "ALTER TABLE {} ADD {};",
                            quote_ident(table),
                            index_definition(name, detail)
                        ),
                        Dialect::Postgres => schema_pg::index_statement(table, name, detail),
                    };
                    result.auto.push(Statement::plain(table, format!("index:{name}"), sql));
                }
                Some(previous) if previous != detail => {
                    // Changing an index in place means DROP + ADD, and DROP is off
                    // the table for this plugin.
                    result.notes.push(format!(
                        "索引 `{table}`.`{name}` 定义不同（{} vs {}）—— 只能 DROP+ADD 改，本插件不生成",
                        summarize_index(previous),
                        summarize_index(detail)
                    ));
                }
                Some(_) => {}
            }
        }
        for name in old_indexes.keys() {
            if !new_indexes.contains_key(name) {
                result.removed.push(format!("索引 `{table}`.`{name}` 已删除 —— 不生成 DROP INDEX"));
            }
        }
    }

    // ---- foreign keys
    for table in new_tables.intersection(&old_tables) {
        let old_keys = old.get(&(table.clone(), "foreign_key".to_string())).unwrap_or(&empty);
        let new_keys = new.get(&(table.clone(), "foreign_key".to_string())).unwrap_or(&empty);
        for (name, detail) in new_keys {
            if !old_keys.contains_key(name) {
                result.review.push(Statement::noted(
                    table,
                    format!("foreign_key:{name}"),
                    "新增外键 —— 两边数据不一致时会失败，而且会锁表，确认后再执行。",
                    match dialect {
                        Dialect::MySql => format!(
                            "ALTER TABLE {} ADD {};",
                            quote_ident(table),
                            foreign_key_definition(name, detail)
                        ),
                        Dialect::Postgres => schema_pg::foreign_key_statement(table, name, detail),
                    },
                ));
            }
        }
        for name in old_keys.keys() {
            if !new_keys.contains_key(name) {
                result.removed.push(format!("外键 `{table}`.`{name}` 已删除 —— 不生成 DROP FOREIGN KEY"));
            }
        }
    }

    // ---- table options
    for table in new_tables.intersection(&old_tables) {
        let Some(current) = new.get(&(table.clone(), "table".to_string())).and_then(|tables| tables.get("")) else {
            continue;
        };
        let Some(previous) = old.get(&(table.clone(), "table".to_string())).and_then(|tables| tables.get("")) else {
            continue;
        };
        if previous.get("engine") != current.get("engine") {
            let engine = current.get("engine").and_then(Value::as_str).unwrap_or("InnoDB");
            result.auto.push(
                Statement::noted(
                    table,
                    "engine",
                    format!(
                        "引擎变化 {} -> {}。会重建整张表，大表上很慢。",
                        previous.get("engine").and_then(Value::as_str).unwrap_or("?"),
                        engine
                    ),
                    format!("ALTER TABLE {} ENGINE={};", quote_ident(table), engine),
                ),
            );
        }
        if previous.get("collation") != current.get("collation") {
            // Only the table *default*. `CONVERT TO CHARACTER SET` would rewrite
            // every text column's data, which is a different and much heavier
            // operation -- column-level collation changes go through MODIFY.
            let collation = current.get("collation").and_then(Value::as_str).unwrap_or("");
            let charset = collation.split('_').next().unwrap_or("");
            result.auto.push(
                Statement::noted(
                    table,
                    "collation",
                    format!(
                        "表默认字符集变化 {} -> {}（只改默认值，不动已有列的数据）。",
                        previous.get("collation").and_then(Value::as_str).unwrap_or("?"),
                        collation
                    ),
                    format!(
                        "ALTER TABLE {} DEFAULT CHARACTER SET {} COLLATE {};",
                        quote_ident(table),
                        charset,
                        collation
                    ),
                ),
            );
        }
    }

    result
}

// ----------------------------------------------------------------- emitting

pub fn render_file(header: &[String], statements: &[Statement]) -> String {
    let mut out = String::new();
    for line in header {
        out.push_str("-- ");
        out.push_str(line);
        out.push('\n');
    }
    out.push('\n');
    if statements.is_empty() {
        out.push_str("-- （本次没有需要执行的语句）\n");
        return out;
    }
    for statement in statements {
        if let Some(comment) = &statement.comment {
            out.push_str(&format!("-- {comment}\n"));
        }
        out.push_str(&statement.sql);
        out.push_str("\n\n");
    }
    out
}

// -------------------------------------------------------------- definitions

fn table_names(objects: &[SchemaObject]) -> BTreeSet<String> {
    objects.iter().filter(|object| object.o == "table").map(|object| object.t.clone()).collect()
}

fn create_table(
    dialect: Dialect,
    table: &str,
    live: &BTreeMap<(String, String), BTreeMap<String, Value>>,
) -> String {
    if dialect == Dialect::Postgres {
        return schema_pg::create_table(table, live);
    }
    let empty = BTreeMap::new();
    let detail = live.get(&(table.to_string(), "table".to_string())).and_then(|tables| tables.get(""));
    let columns = live.get(&(table.to_string(), "column".to_string())).unwrap_or(&empty);
    let indexes = live.get(&(table.to_string(), "index".to_string())).unwrap_or(&empty);
    let foreign_keys = live.get(&(table.to_string(), "foreign_key".to_string())).unwrap_or(&empty);

    let mut definitions: Vec<String> = Vec::new();
    // ORDER BY ORDINAL_POSITION was applied during collection, and BTreeMap over
    // the column name would scramble it -- so re-sort explicitly.
    let mut ordered: Vec<(&String, &Value)> = columns.iter().collect();
    ordered.sort_by_key(|(_, value)| value.get("ordinal").and_then(Value::as_u64).unwrap_or(0));
    for (name, column) in ordered {
        definitions.push(column_definition(name, column, detail));
    }
    for (name, index) in indexes {
        definitions.push(index_definition(name, index));
    }
    for (name, key) in foreign_keys {
        definitions.push(foreign_key_definition(name, key));
    }

    let mut options = String::new();
    if let Some(engine) = detail.and_then(|value| value.get("engine")).and_then(Value::as_str) {
        options.push_str(&format!(" ENGINE={engine}"));
    }
    if let Some(collation) = detail.and_then(|value| value.get("collation")).and_then(Value::as_str) {
        let charset = collation.split('_').next().unwrap_or("");
        options.push_str(&format!(" DEFAULT CHARSET={charset} COLLATE={collation}"));
    }

    let mut out = format!("CREATE TABLE {} (\n", quote_ident(table));
    out.push_str(&definitions.join(",\n"));
    out.push_str(&format!("\n){options};"));
    out
}

/// `ADD COLUMN` is the one column statement the two families agree on in shape;
/// only the quoting and the definition text differ.
fn add_column_statement(
    dialect: Dialect,
    table: &str,
    name: &str,
    detail: &Value,
    table_detail: Option<&Value>,
) -> String {
    match dialect {
        Dialect::MySql => format!(
            "ALTER TABLE {} ADD COLUMN {};",
            quote_ident(table),
            column_definition(name, detail, table_detail)
        ),
        Dialect::Postgres => format!(
            "ALTER TABLE {} ADD COLUMN {};",
            schema_pg::quote_ident(table),
            schema_pg::column_definition(name, detail)
        ),
    }
}

fn column_definition(name: &str, detail: &Value, table_detail: Option<&Value>) -> String {
    let column_type = detail.get("columnType").and_then(Value::as_str).unwrap_or("text");
    let nullable = detail.get("nullable").and_then(Value::as_bool).unwrap_or(true);
    let extra = detail.get("extra").and_then(Value::as_str).unwrap_or("");
    let charset = detail.get("charset").and_then(Value::as_str);
    let collation = detail.get("collation").and_then(Value::as_str);

    let mut out = format!("{} {}", quote_ident(name), column_type);

    // Only spell out a character set when it differs from the table default.
    let table_collation = table_detail.and_then(|value| value.get("collation")).and_then(Value::as_str);
    if let Some(collation) = collation {
        if Some(collation) != table_collation {
            if let Some(charset) = charset {
                out.push_str(&format!(" CHARACTER SET {charset}"));
            }
            out.push_str(&format!(" COLLATE {collation}"));
        }
    }

    if !nullable {
        out.push_str(" NOT NULL");
    }

    // information_schema returns SQL NULL when there is no default, and a string
    // when there is one. The one ambiguity: MySQL also hands back the *string*
    // "NULL" for a nullable column in places, so treat that as SQL NULL unless
    // the column is a string type -- where "NULL" really is a four-character
    // default.
    let default = detail.get("default");
    let no_default = match default {
        None | Some(Value::Null) => true,
        Some(Value::String(text)) => text.eq_ignore_ascii_case("NULL") && !is_string_type(column_type),
        _ => false,
    };
    if no_default {
        if nullable {
            out.push_str(" DEFAULT NULL");
        }
    } else if let Some(Value::String(text)) = default {
        out.push_str(&format!(" DEFAULT {}", render_default(text, column_type, extra)));
    }

    let extra = extra
        .split_whitespace()
        .filter(|part| *part != "DEFAULT_GENERATED")
        .collect::<Vec<_>>()
        .join(" ");
    if !extra.is_empty() {
        out.push(' ');
        out.push_str(&extra);
    }
    out
}

fn is_string_type(column_type: &str) -> bool {
    let lower = column_type.to_ascii_lowercase();
    ["char", "varchar", "text", "enum", "set", "blob", "binary"]
        .iter()
        .any(|prefix| lower.starts_with(prefix))
}

pub(crate) fn is_numeric_type(column_type: &str) -> bool {
    let lower = column_type.to_ascii_lowercase();
    // The two families' spellings in one list: `real` and `double precision`
    // are PostgreSQL, the rest are MySQL or shared.
    [
        "int", "bigint", "smallint", "tinyint", "mediumint", "decimal", "numeric", "float", "double",
        "bit", "year", "real", "serial", "money",
    ]
    .iter()
    .any(|prefix| lower.starts_with(prefix))
}

/// Render one value as a SQL literal, using the column's own type rather than
/// guessing from the text.
///
/// Only the types these six tables actually use are handled specially
/// (`varchar`, `text`, `longtext`, `datetime`, `decimal`, `int`): there is no
/// binary or `TIMESTAMP` column among them, so there is no hex form and no
/// session-time-zone conversion to get wrong.
pub fn render_value(value: Option<&str>, column_type: &str) -> String {
    let Some(text) = value else {
        return "NULL".to_string();
    };
    // A numeric column whose value does not parse stays quoted rather than being
    // emitted bare -- an unparseable bare token would be a syntax error.
    if is_numeric_type(column_type) && !text.is_empty() && text.parse::<f64>().is_ok() {
        return text.to_string();
    }
    quote_string(text)
}

fn render_default(default: &str, column_type: &str, extra: &str) -> String {
    if extra.split_whitespace().any(|part| part == "DEFAULT_GENERATED") {
        return default.to_string();
    }
    let lower = column_type.to_ascii_lowercase();
    if is_numeric_type(column_type) && default.parse::<f64>().is_ok() {
        return default.to_string();
    }
    if (lower.starts_with("timestamp") || lower.starts_with("datetime"))
        && default.to_ascii_uppercase().starts_with("CURRENT_TIMESTAMP")
    {
        return default.to_string();
    }
    quote_string(default)
}

fn index_definition(name: &str, detail: &Value) -> String {
    let columns = detail
        .get("columns")
        .and_then(Value::as_array)
        .map(|columns| {
            columns
                .iter()
                .map(|column| {
                    let name = quote_ident(column.get("name").and_then(Value::as_str).unwrap_or(""));
                    match column.get("prefix").and_then(Value::as_str) {
                        Some(prefix) if !prefix.is_empty() => format!("{name}({prefix})"),
                        _ => name,
                    }
                })
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();

    let index_type = detail.get("type").and_then(Value::as_str).unwrap_or("BTREE").to_ascii_uppercase();
    if index_type == "FULLTEXT" {
        return format!("FULLTEXT KEY {} ({columns})", quote_ident(name));
    }
    if index_type == "SPATIAL" {
        return format!("SPATIAL KEY {} ({columns})", quote_ident(name));
    }
    if name == "PRIMARY" {
        return format!("PRIMARY KEY ({columns})");
    }
    if detail.get("unique").and_then(Value::as_bool).unwrap_or(false) {
        return format!("UNIQUE KEY {} ({columns})", quote_ident(name));
    }
    format!("KEY {} ({columns})", quote_ident(name))
}

fn foreign_key_definition(name: &str, detail: &Value) -> String {
    let columns = plain_list(detail.get("columns"));
    let referenced = plain_list(detail.get("referencedColumns"));
    let table = detail.get("referencedTable").and_then(Value::as_str).unwrap_or("");
    let mut out = format!(
        "CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})",
        quote_ident(name),
        columns.iter().map(|column| quote_ident(column)).collect::<Vec<_>>().join(", "),
        quote_ident(table),
        referenced.iter().map(|column| quote_ident(column)).collect::<Vec<_>>().join(", "),
    );
    if let Some(rule) = detail.get("deleteRule").and_then(Value::as_str) {
        if !rule.is_empty() && rule != "NO ACTION" && rule != "RESTRICT" {
            out.push_str(&format!(" ON DELETE {rule}"));
        }
    }
    if let Some(rule) = detail.get("updateRule").and_then(Value::as_str) {
        if !rule.is_empty() && rule != "NO ACTION" && rule != "RESTRICT" {
            out.push_str(&format!(" ON UPDATE {rule}"));
        }
    }
    out
}

fn summarize_index(detail: &Value) -> String {
    let columns = named_list(detail.get("columns"));
    format!("unique={} [{columns:?}]", detail.get("unique").and_then(Value::as_bool).unwrap_or(false))
}

/// Index columns come back as objects carrying a prefix length.
fn named_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| items.iter().map(|item| item.get("name").and_then(Value::as_str).unwrap_or("").to_string()).collect())
        .unwrap_or_default()
}

/// Foreign key columns come back as plain strings.
pub(crate) fn plain_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).map(str::to_string).collect())
        .unwrap_or_default()
}

pub(crate) fn text(row: &Value, key: &str) -> String {
    row.get(key).and_then(crate::encode::canonical).unwrap_or_default()
}

pub(crate) fn nullable_text(row: &Value, key: &str) -> Option<String> {
    row.get(key).and_then(crate::encode::canonical)
}

#[cfg(test)]
mod tests {
    use super::widens;

    /// The case that has to land in the auto file: it is the most common type
    /// change there is, and it cannot lose a row.
    #[test]
    fn widening_is_auto() {
        assert!(widens("varchar(100)", "varchar(1000)"));
        assert!(widens("int(11)", "bigint(20)"));
        assert!(widens("tinytext", "mediumtext"));
        assert!(widens("decimal(10,2)", "decimal(12,2)"));
        assert!(widens("float", "double"));
        assert!(widens("datetime(3)", "datetime(6)"));
    }

    /// The case that must never be automatic: it truncates silently.
    #[test]
    fn narrowing_is_not_a_widening() {
        assert!(!widens("varchar(1000)", "varchar(100)"));
        assert!(!widens("bigint(20)", "int(11)"));
        assert!(!widens("longtext", "text"));
        assert!(!widens("decimal(12,2)", "decimal(10,2)"));
        assert!(!widens("double", "float"));
        assert!(!widens("datetime(6)", "datetime(3)"));
    }

    /// Precision is not capacity: the digits left of the point are.
    #[test]
    fn decimal_scale_is_not_capacity() {
        assert!(!widens("decimal(10,2)", "decimal(10,3)"));
        assert!(widens("decimal(10,2)", "decimal(11,3)"));
    }

    /// Changing signedness or family is a conversion, not a widening -- every
    /// value may be rewritten, so a human decides.
    #[test]
    fn kind_changes_are_not_widenings() {
        assert!(!widens("int(11)", "int(11) unsigned"));
        assert!(!widens("bigint(20) unsigned", "bigint(20)"));
        assert!(!widens("varchar(100)", "text"));
        assert!(!widens("int(11)", "varchar(100)"));
        assert!(!widens("datetime", "date"));
        assert!(!widens("enum('a','b')", "enum('a','b','c')"));
    }

    /// Unparseable input answers "no", because "no" is what sends the statement
    /// to the file a human reads.
    #[test]
    fn unknown_types_are_not_widenings() {
        assert!(!widens("", "varchar(100)"));
        assert!(!widens("varchar(100)", "geometry"));
        assert!(!widens("varchar(100)", "varchar"));
        assert!(!widens("decimal(10,2)", "decimal(10)"));
    }
}
