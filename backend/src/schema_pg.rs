//! The structure channel on PostgreSQL.
//!
//! A separate implementation rather than a variation on the MySQL one, because
//! the catalogues are genuinely different: PostgreSQL's `information_schema` has
//! no `COLUMN_TYPE`, no `ENGINE` and no `STATISTICS`, indexes live in
//! `pg_index` rather than in `information_schema`, and `MODIFY COLUMN` does not
//! exist -- a type change is `ALTER COLUMN ... TYPE`.
//!
//! What it produces is deliberately the **same object shape** as the MySQL side
//! (`SchemaObject` with kinds `table` / `column` / `index` / `foreign_key` and
//! the same detail keys), so `schema::diff` -- the part that decides what counts
//! as a difference -- is written once and works for both.

use std::collections::{BTreeSet, HashMap};

use serde_json::{json, Value};

use crate::cli;
use crate::encode::quote_string_pg;
use crate::schema::{is_numeric_type, nullable_text, plain_list, text, SchemaObject};
use crate::snapshot::Abort;

/// Rows of one `IN (...)` shard; mirrors `schema::FILTER_CHUNK` in spirit.
/// 500 quoted table names are ~10KB of SQL. PostgreSQL quoting: backslashes
/// are literal under standard_conforming_strings=on.
const FILTER_CHUNK: usize = 500;

fn in_list(names: &[String]) -> String {
    names.iter().map(|name| quote_string_pg(name)).collect::<Vec<_>>().join(", ")
}

fn report_phase(detail: String) {
    crate::snapshot::with_job(|job| job.detail = detail);
}

/// The schema expression every query below is scoped by. PostgreSQL separates
/// database from schema, and these project tables live in the schema the
/// connection resolves unqualified names to.
const SCHEMA: &str = "current_schema()";

/// Shared column-read shape: straight off `pg_catalog`, not
/// `information_schema.columns`. That view is a multi-catalog JOIN with
/// per-row privilege checks whose predicates do not push down: on a 3000-table
/// schema even a 500-table `IN` shard recomputes the whole schema per shard
/// (observed: one shard > 10 min). This resolves the same OIDs the view itself
/// reads, so every alias carries the same value the old query returned:
///
/// - `udt_name` = `pg_type.typname`, lengths from `atttypmod` exactly as the
///   view decodes them (`varchar`/`bpchar`: `typmod - 4`; `numeric`: high/low
///   16 bits; timestamp precision: `typmod`, `-1` meaning "unspecified" just
///   like the view's NULL)
/// - `column_default` = `pg_get_expr(adbin, adrelid)`, the view's own
///   expression; the `LEFT JOIN` keeps it NULL (and cheap) for the common
///   no-default case instead of deparsing per column
/// - `collation_name` = `pg_collation.collname` off `attcollation`, the view's
///   own join; non-collatable types have `attcollation = 0` -> NULL
/// - `is_nullable` / `is_identity` spelled `YES`/`NO` as the view spells them
/// - `has_table_privilege(..., 'SELECT, ...')` mirrors the view's row
///   visibility so the table set cannot drift from the table list above
///
/// Both the full collect and the data channel's tiny type lookup build on it,
/// so the two can never decode a type differently.
fn columns_base_sql() -> String {
    format!(
        "SELECT c.relname AS table_name, a.attname AS column_name, t.typname AS udt_name, \
                CASE WHEN t.typname IN ('varchar', 'bpchar') AND a.atttypmod > 0 \
                     THEN (a.atttypmod - 4)::text END AS char_len, \
                CASE WHEN t.typname = 'numeric' AND a.atttypmod > 0 \
                     THEN (((a.atttypmod - 4) >> 16) & 65535)::text END AS num_precision, \
                CASE WHEN t.typname = 'numeric' AND a.atttypmod > 0 \
                     THEN ((a.atttypmod - 4) & 65535)::text END AS num_scale, \
                CASE WHEN t.typname IN ('timestamp', 'timestamptz', 'time', 'timetz') AND a.atttypmod >= 0 \
                     THEN a.atttypmod::text END AS dt_precision, \
                CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable, \
                pg_get_expr(d.adbin, d.adrelid) AS column_default, \
                co.collname AS collation_name, a.attnum::text AS ordinal, \
                CASE WHEN a.attidentity <> '' THEN 'YES' ELSE 'NO' END AS is_identity \
         FROM pg_attribute a \
         JOIN pg_class c ON c.oid = a.attrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         JOIN pg_type t ON t.oid = a.atttypid \
         LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
         LEFT JOIN pg_collation co ON co.oid = a.attcollation \
         WHERE n.nspname = {SCHEMA} AND c.relkind IN ('r', 'p') \
           AND a.attnum > 0 AND NOT a.attisdropped \
           AND has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')"
    )
}

pub fn collect(
    connection: &str,
    schema_filter: &str,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<Vec<SchemaObject>, Abort> {
    let mut objects = Vec::new();

    // Phase 1: cheap table list. The keep set is computed from it with the
    // same `fancy_regex` match as the old post-filter, so the result is
    // identical -- the SQL `IN` below is an exact table-name match, never a
    // regex translation (PostgreSQL's `~` is POSIX and cannot express
    // `(?!...)` anyway).
    let tables_sql = format!(
        "SELECT t.table_name AS table_name \
         FROM information_schema.tables t \
         WHERE t.table_schema = {SCHEMA} AND t.table_type = 'BASE TABLE' \
         ORDER BY t.table_name"
    );
    let mut all_tables: Vec<String> = Vec::new();
    cli::paginate(connection, &tables_sql, is_cancelled, &mut |rows| {
        for row in rows {
            let table = text(row, "table_name");
            all_tables.push(table.clone());
            objects.push(SchemaObject {
                t: table,
                o: "table".to_string(),
                n: String::new(),
                // PostgreSQL has neither a table engine nor a table-level
                // collation, so both keys are null and the table-options branch
                // of the diff finds nothing to say. Same keys, so `diff` does not
                // need to know which family it is looking at.
                d: json!({ "engine": Value::Null, "collation": Value::Null }),
            });
        }
        Ok(())
    })?;

    let keep: Option<BTreeSet<String>> = match crate::schema::compile_filter(schema_filter) {
        Ok(None) => None,
        Ok(Some(regex)) => {
            let mut keep = BTreeSet::new();
            for table in &all_tables {
                if regex.is_match(table).unwrap_or(false) {
                    keep.insert(table.clone());
                }
            }
            Some(keep)
        }
        Err(message) => return Err(Abort::Failed(message)),
    };
    if let Some(keep) = &keep {
        report_phase(format!("表清单 {} 张，过滤后保留 {} 张", all_tables.len(), keep.len()));
        objects.retain(|object| keep.contains(&object.t));
        if keep.is_empty() {
            return Ok(objects);
        }
    }
    // Sorted shards keep every shard's ORDER BY globally ordered, which is
    // what the foreign-key folding below assumes across shards.
    let shards: Vec<Vec<String>> = match &keep {
        None => vec![Vec::new()],
        Some(keep) => {
            let mut sorted: Vec<String> = keep.iter().cloned().collect();
            sorted.sort();
            sorted.chunks(FILTER_CHUNK).map(<[String]>::to_vec).collect()
        }
    };
    let shard_total = shards.len();

    // Straight off `pg_catalog` -- see `columns_base_sql`.
    let columns_base = columns_base_sql();
    let push_columns = |rows: &[Value], objects: &mut Vec<SchemaObject>| -> Result<(), String> {
        for row in rows {
            objects.push(SchemaObject {
                t: text(row, "table_name"),
                o: "column".to_string(),
                n: text(row, "column_name"),
                d: json!({
                    "columnType": column_type(row),
                    "nullable": text(row, "is_nullable").eq_ignore_ascii_case("YES"),
                    "default": nullable_text(row, "column_default"),
                    "extra": extra(row),
                    "charset": Value::Null,
                    // Recorded because it is a real difference when two
                    // environments disagree, and because it is what a text
                    // column's comparison semantics depend on.
                    "collation": nullable_text(row, "collation_name"),
                    "ordinal": text(row, "ordinal").parse::<u32>().unwrap_or(0),
                }),
            });
        }
        Ok(())
    };
    if keep.is_none() {
        let columns_sql = format!("{columns_base} ORDER BY table_name, ordinal");
        cli::paginate(connection, &columns_sql, is_cancelled, &mut |rows| push_columns(rows, &mut objects))?;
    } else {
        for (index, shard) in shards.iter().enumerate() {
            report_phase(format!("读取列 {index}/{shard_total}（{} 张表）", shard.len()));
            let columns_sql = format!(
                "{columns_base} AND c.relname IN ({}) ORDER BY table_name, ordinal",
                in_list(shard)
            );
            cli::paginate(connection, &columns_sql, is_cancelled, &mut |rows| push_columns(rows, &mut objects))?;
        }
    }

    // `pg_index` rather than `information_schema`: the standard view has no
    // index information at all. One row per index, with the key columns already
    // in key order. This is the most expensive of the four queries
    // (`pg_get_expr` per index), and the one the table whitelist saves most on.
    let indexes_base = format!(
        "SELECT tb.relname AS table_name, ix.relname AS index_name, \
                i.indisprimary AS is_primary, i.indisunique AS is_unique, am.amname AS method, \
                (SELECT string_agg(a.attname, ',' ORDER BY k.ord) \
                   FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) \
                   JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum) AS cols, \
                pg_get_expr(i.indpred, i.indrelid) AS predicate \
         FROM pg_index i \
         JOIN pg_class ix ON ix.oid = i.indexrelid \
         JOIN pg_class tb ON tb.oid = i.indrelid \
         JOIN pg_am am ON am.oid = ix.relam \
         JOIN pg_namespace n ON n.oid = tb.relnamespace \
         WHERE n.nspname = {SCHEMA} AND tb.relkind = 'r'"
    );
    let push_index_rows = |rows: &[Value], objects: &mut Vec<SchemaObject>| -> Result<(), String> {
        for row in rows {
            let columns: Vec<Value> = text(row, "cols")
                .split(',')
                .filter(|name| !name.is_empty())
                .map(|name| json!({ "name": name, "prefix": Value::Null }))
                .collect();
            objects.push(SchemaObject {
                t: text(row, "table_name"),
                o: "index".to_string(),
                n: text(row, "index_name"),
                d: json!({
                    // The primary key is not a separate object in PostgreSQL --
                    // it is an index marked `indisprimary`. `name` is what the
                    // MySQL side keys off, so it is recorded here instead.
                    "primary": matches!(row.get("is_primary"), Some(Value::Bool(true))),
                    "unique": matches!(row.get("is_unique"), Some(Value::Bool(true))),
                    "type": text(row, "method").to_ascii_uppercase(),
                    "predicate": nullable_text(row, "predicate"),
                    "columns": columns,
                }),
            });
        }
        Ok(())
    };
    if keep.is_none() {
        let indexes_sql = format!("{indexes_base} ORDER BY tb.relname, ix.relname");
        cli::paginate(connection, &indexes_sql, is_cancelled, &mut |rows| push_index_rows(rows, &mut objects))?;
    } else {
        for (index, shard) in shards.iter().enumerate() {
            report_phase(format!("读取索引 {index}/{shard_total}（{} 张表）", shard.len()));
            let indexes_sql = format!(
                "{indexes_base} AND tb.relname IN ({}) ORDER BY tb.relname, ix.relname",
                in_list(shard)
            );
            cli::paginate(connection, &indexes_sql, is_cancelled, &mut |rows| push_index_rows(rows, &mut objects))?;
        }
    }

    let foreign_keys_base = format!(
        "SELECT tc.table_name AS table_name, tc.constraint_name AS constraint_name, \
                kcu.column_name AS column_name, ccu.table_name AS ref_table, \
                ccu.column_name AS ref_column, rc.update_rule AS update_rule, \
                rc.delete_rule AS delete_rule \
         FROM information_schema.table_constraints tc \
         JOIN information_schema.key_column_usage kcu \
           ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema \
         JOIN information_schema.constraint_column_usage ccu \
           ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema \
         LEFT JOIN information_schema.referential_constraints rc \
           ON rc.constraint_name = tc.constraint_name AND rc.constraint_schema = tc.table_schema \
         WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = {SCHEMA}"
    );
    let mut pending: Option<(String, String, Value, Vec<String>, Vec<String>)> = None;
    let flush = |pending: Option<(String, String, Value, Vec<String>, Vec<String>)>,
                 objects: &mut Vec<SchemaObject>| {
        if let Some((table, name, mut head, columns, referenced)) = pending {
            head["columns"] = json!(columns);
            head["referencedColumns"] = json!(referenced);
            objects.push(SchemaObject { t: table, o: "foreign_key".to_string(), n: name, d: head });
        }
    };
    let push_fk_page = |rows: &[Value], pending: &mut Option<(String, String, Value, Vec<String>, Vec<String>)>, objects: &mut Vec<SchemaObject>| {
        for row in rows {
            let table = text(row, "table_name");
            let name = text(row, "constraint_name");
            let column = text(row, "column_name");
            let referenced = text(row, "ref_column");
            match pending {
                Some((pending_table, pending_name, _, columns, referenced_columns))
                    if *pending_table == table && *pending_name == name =>
                {
                    columns.push(column);
                    referenced_columns.push(referenced);
                }
                _ => {
                    flush(pending.take(), objects);
                    *pending = Some((
                        table,
                        name,
                        json!({
                            "referencedTable": text(row, "ref_table"),
                            "updateRule": nullable_text(row, "update_rule"),
                            "deleteRule": nullable_text(row, "delete_rule"),
                        }),
                        vec![column],
                        vec![referenced],
                    ));
                }
            }
        }
    };
    if keep.is_none() {
        let foreign_keys_sql = format!(
            "{foreign_keys_base} ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position"
        );
        cli::paginate(connection, &foreign_keys_sql, is_cancelled, &mut |rows| {
            push_fk_page(rows, &mut pending, &mut objects);
            Ok(())
        })?;
    } else {
        for (index, shard) in shards.iter().enumerate() {
            report_phase(format!("读取外键 {index}/{shard_total}（{} 张表）", shard.len()));
            let foreign_keys_sql = format!(
                "{foreign_keys_base} AND tc.table_name IN ({}) ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position",
                in_list(shard)
            );
            cli::paginate(connection, &foreign_keys_sql, is_cancelled, &mut |rows| {
                push_fk_page(rows, &mut pending, &mut objects);
                Ok(())
            })?;
        }
    }
    flush(pending.take(), &mut objects);

    objects.sort_by(|a, b| (&a.t, &a.o, &a.n).cmp(&(&b.t, &b.o, &b.n)));
    Ok(objects)
}

/// Column types for exactly `tables`, regardless of the schema filter.
///
/// The filtered `collect` above no longer sees excluded tables, but the data
/// channel still renders literals for its own tables with their own types.
/// One tiny query instead of the old "read the whole schema unfiltered".
pub fn column_types_for_tables(
    connection: &str,
    tables: &[String],
    is_cancelled: &dyn Fn() -> bool,
) -> Result<HashMap<(String, String), String>, Abort> {
    let mut out = HashMap::new();
    if tables.is_empty() {
        return Ok(out);
    }
    let mut sorted: Vec<String> = tables.to_vec();
    sorted.sort();
    let base = columns_base_sql();
    for shard in sorted.chunks(FILTER_CHUNK) {
        let sql = format!(
            "{base} AND c.relname IN ({}) ORDER BY table_name, ordinal",
            in_list(shard)
        );
        cli::paginate(connection, &sql, is_cancelled, &mut |rows| {
            for row in rows {
                out.insert((text(row, "table_name"), text(row, "column_name")), column_type(row));
            }
            Ok(())
        })?;
    }
    Ok(out)
}

/// The column's type, spelled the way it would be written in DDL.
///
/// Built from `udt_name` rather than `data_type`: the standard name for a
/// timestamp is the phrase `timestamp without time zone`, which is neither what
/// anyone writes nor a stable thing to compare, while `udt_name` is the
/// canonical short name on every server in this family.
pub fn column_type(row: &Value) -> String {
    let udt = text(row, "udt_name");
    let length = || text(row, "char_len").parse::<i64>().ok();
    let precision = || text(row, "num_precision").parse::<i64>().ok();
    let scale = || text(row, "num_scale").parse::<i64>().ok();
    let datetime_precision = || text(row, "dt_precision").parse::<i64>().ok();

    match udt.as_str() {
        "varchar" => length().map(|n| format!("varchar({n})")).unwrap_or_else(|| "varchar".to_string()),
        "bpchar" => length().map(|n| format!("char({n})")).unwrap_or_else(|| "char".to_string()),
        "numeric" => match (precision(), scale()) {
            (Some(p), Some(s)) => format!("numeric({p},{s})"),
            (Some(p), None) => format!("numeric({p})"),
            _ => "numeric".to_string(),
        },
        "int2" => "smallint".to_string(),
        "int4" => "integer".to_string(),
        "int8" => "bigint".to_string(),
        "float4" => "real".to_string(),
        "float8" => "double precision".to_string(),
        "bool" => "boolean".to_string(),
        "timestamptz" => "timestamptz".to_string(),
        "timestamp" => match datetime_precision() {
            // 6 is what an unqualified `timestamp` already means.
            Some(p) if p < 6 => format!("timestamp({p})"),
            _ => "timestamp".to_string(),
        },
        "timetz" => "timetz".to_string(),
        "time" => "time".to_string(),
        _ => udt,
    }
}

/// PostgreSQL's `EXTRA` equivalent, kept under the same key so the diff's
/// `extra` comparison works unchanged. Identity columns matter -- a row's id is
/// generated server-side and must not be written by hand.
fn extra(row: &Value) -> String {
    if text(row, "is_identity").eq_ignore_ascii_case("YES") {
        "identity".to_string()
    } else {
        String::new()
    }
}

/// One column as it appears in `CREATE TABLE` or `ADD COLUMN`.
///
/// No `CHARACTER SET` / `COLLATE`: PostgreSQL has no per-database character set
/// to differ from, so spelling the collation out on every text column would be
/// noise on every statement rather than a difference.
pub fn column_definition(name: &str, detail: &Value) -> String {
    let column_type = detail.get("columnType").and_then(Value::as_str).unwrap_or("text");
    let nullable = detail.get("nullable").and_then(Value::as_bool).unwrap_or(true);
    let mut out = format!("{} {}", quote_ident(name), column_type);

    if let Some(default) = detail.get("default").and_then(Value::as_str) {
        out.push_str(&format!(" DEFAULT {}", render_default(default, column_type)));
    }
    if !nullable {
        out.push_str(" NOT NULL");
    }
    out
}

/// PostgreSQL normalizes a default to an expression (`'x'::character varying`),
/// so most of them go out as written. The two that must not be quoted are the
/// ones that are already expressions -- `now()`, `nextval(...)`, a cast.
fn render_default(default: &str, column_type: &str) -> String {
    let lowered = default.trim().to_ascii_lowercase();
    if lowered.contains("::")
        || lowered.ends_with("()")
        || lowered.starts_with("nextval")
        || lowered == "true"
        || lowered == "false"
        || lowered == "null"
    {
        return default.trim().to_string();
    }
    if is_numeric_type(column_type) && default.trim().parse::<f64>().is_ok() {
        return default.trim().to_string();
    }
    quote_string_pg(default.trim())
}

pub fn create_table(table: &str, live: &std::collections::BTreeMap<(String, String), std::collections::BTreeMap<String, Value>>) -> String {
    let empty = std::collections::BTreeMap::new();
    let columns = live.get(&(table.to_string(), "column".to_string())).unwrap_or(&empty);
    let indexes = live.get(&(table.to_string(), "index".to_string())).unwrap_or(&empty);

    let mut ordered: Vec<(&String, &Value)> = columns.iter().collect();
    ordered.sort_by_key(|(_, value)| value.get("ordinal").and_then(Value::as_u64).unwrap_or(0));

    let mut definitions: Vec<String> = ordered
        .iter()
        .map(|(name, detail)| format!("  {}", column_definition(name, detail)))
        .collect();

    // The primary key has to be inline; PostgreSQL cannot add one afterwards
    // without dropping and recreating, and a table created without it would be
    // the wrong shape for the rows the data channel writes.
    for (_name, detail) in indexes {
        if is_primary(detail) {
            definitions.push(format!("  PRIMARY KEY ({})", index_columns(detail)));
            break;
        }
    }
    // Non-primary indexes are separate statements -- see `index_statement`.
    format!("CREATE TABLE {} (\n{}\n);", quote_ident(table), definitions.join(",\n"))
}

/// A whole `CREATE INDEX`, because PostgreSQL has no `ALTER TABLE ... ADD KEY`
/// and a plain index cannot be declared inside `CREATE TABLE`. Unique ones
/// could be written as a constraint, but keeping both shapes as one statement
/// type is simpler to read and behaves the same.
pub fn index_statement(table: &str, name: &str, detail: &Value) -> String {
    let unique = if detail.get("unique").and_then(Value::as_bool).unwrap_or(false) { "UNIQUE " } else { "" };
    let method = detail.get("type").and_then(Value::as_str).unwrap_or("BTREE").to_ascii_lowercase();
    let using = if method.is_empty() || method == "btree" { String::new() } else { format!(" USING {method}") };
    let predicate = match detail.get("predicate").and_then(Value::as_str) {
        Some(predicate) if !predicate.is_empty() => format!(" WHERE {predicate}"),
        _ => String::new(),
    };
    format!(
        "CREATE {unique}INDEX {} ON {}{using} ({}){predicate};",
        quote_ident(name),
        quote_ident(table),
        index_columns(detail)
    )
}

/// The full statement: PostgreSQL uses `ALTER COLUMN ... TYPE`, where MySQL has
/// `MODIFY COLUMN`. Everything else about the column (default, nullability) is
/// carried along so the result matches the baseline exactly.
pub fn change_column_statement(table: &str, name: &str, detail: &Value) -> String {
    let column_type = detail.get("columnType").and_then(Value::as_str).unwrap_or("text");
    let mut out = format!(
        "ALTER TABLE {} ALTER COLUMN {} TYPE {column_type}",
        quote_ident(table),
        quote_ident(name)
    );
    // `USING` is not emitted: a cast that needs one is a conversion, and those
    // land in the review file where a human writes it.
    if let Some(default) = detail.get("default").and_then(Value::as_str) {
        out.push_str(&format!(
            ";\nALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
            quote_ident(table),
            quote_ident(name),
            render_default(default, column_type)
        ));
    }
    if detail.get("nullable").and_then(Value::as_bool).unwrap_or(true) {
        out.push_str(&format!(
            ";\nALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
            quote_ident(table),
            quote_ident(name)
        ));
    } else {
        out.push_str(&format!(
            ";\nALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
            quote_ident(table),
            quote_ident(name)
        ));
    }
    out.push(';');
    out
}

pub fn foreign_key_statement(table: &str, name: &str, detail: &Value) -> String {
    let columns = plain_list(detail.get("columns"));
    let referenced = plain_list(detail.get("referencedColumns"));
    let referenced_table = detail.get("referencedTable").and_then(Value::as_str).unwrap_or("");
    let mut out = format!(
        "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})",
        quote_ident(table),
        quote_ident(name),
        quote_list(&columns),
        quote_ident(referenced_table),
        quote_list(&referenced)
    );
    if let Some(rule) = detail.get("deleteRule").and_then(Value::as_str) {
        if !rule.eq_ignore_ascii_case("NO ACTION") {
            out.push_str(&format!(" ON DELETE {rule}"));
        }
    }
    if let Some(rule) = detail.get("updateRule").and_then(Value::as_str) {
        if !rule.eq_ignore_ascii_case("NO ACTION") {
            out.push_str(&format!(" ON UPDATE {rule}"));
        }
    }
    out.push(';');
    out
}

/// PostgreSQL reports the primary key as an index flag, not as a name.
pub fn is_primary(detail: &Value) -> bool {
    detail.get("primary").and_then(Value::as_bool).unwrap_or(false)
}

fn index_columns(detail: &Value) -> String {
    let names: Vec<String> = detail
        .get("columns")
        .and_then(Value::as_array)
        .map(|columns| {
            columns
                .iter()
                .map(|column| column.get("name").and_then(Value::as_str).unwrap_or("").to_string())
                .collect()
        })
        .unwrap_or_default();
    quote_list(&names)
}

pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn quote_list(names: &[String]) -> String {
    names.iter().map(|name| quote_ident(name)).collect::<Vec<_>>().join(", ")
}

/// Every place a statement can be built from. Kept as a function so the diff can
/// ask one question -- "is this type change a widening" -- without caring which
/// family answered.
pub fn widens(old: &str, new: &str) -> bool {
    let (Some(old), Some(new)) = (parse_type(old), parse_type(new)) else {
        return false;
    };
    if old.modifiers != new.modifiers {
        return false;
    }
    if old.base == new.base {
        return match (old.args.as_slice(), new.args.as_slice()) {
            // numeric(10,2) -> numeric(10,3) keeps the precision but gives up an
            // integer digit, so it can overflow. The digits left of the point
            // are what has to grow.
            ([op, os], [np, ns]) if old.base == "numeric" => np - ns >= op - os && ns >= os,
            ([op], [np]) => np >= op,
            ([], []) => true,
            _ => false,
        };
    }
    let rank = |base: &str| INTEGER_FAMILY.iter().position(|item| *item == base);
    if let (Some(o), Some(n)) = (rank(&old.base), rank(&new.base)) {
        return n > o;
    }
    old.base == "real" && new.base == "double precision"
}

struct PgType {
    base: String,
    args: Vec<i64>,
    modifiers: Vec<String>,
}

/// `None` for anything that is not `base(args)` -- a user-defined type, an
/// array, an enum. Refusing to classify sends it to the review file, which is
/// the safe direction.
fn parse_type(raw: &str) -> Option<PgType> {
    let text = raw.trim().to_ascii_lowercase();
    if text.is_empty() || text.contains('[') {
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
    Some(PgType { base, args, modifiers })
}

/// Integer types, narrowest first.
const INTEGER_FAMILY: &[&str] = &["smallint", "integer", "bigint"];
