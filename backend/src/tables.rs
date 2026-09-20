//! The six tables the data channel compares.
//!
//! Generated from `information_schema`, not hand-written: column names are the
//! database's own spelling, including `ID`, `Type_text` and
//! `dsfa_mm_valueAttributes_id`.
//!
//! **`identity` is the `<table>_id` column alone**, not the `(id, ds_version)` pair
//! the database's own primary key uses. The intent is that the target keeps one
//! row per id, so `ds_version` is deliberately not part of the key, and
//! `base_filter` pins `ds_active = '1'` so that row is the active version.
//!
//! That is not airtight in the source data: `dsfa_rm` currently has three ids
//! where both `ds_version='1'` and `ds_version='project'` are active. The scan
//! counts those collapses and the report names them -- it never drops a row
//! silently. Use the per-table filter (`ds_version='project'`) to disambiguate.
//!
//! The array order is the order everything is presented in: collection progress,
//! the report, the generated files. A diff looks tables up **by name**, never by
//! position, so an old snapshot taken under a different order still compares
//! correctly.
//!
//! **`order_by` is the database's own primary key** (`(id, ds_version)`, single
//! column where that is the whole key). It is unique, so paging on it has defined
//! page boundaries; it is two indexed short columns, so the sort is cheap and
//! cannot hit error 1038. For an id with duplicates, the survivor is the one that
//! sorts first on this key -- deterministic, but which one that is depends on the
//! data, which is exactly the case the report already flags via `collapsed`.

use std::collections::BTreeMap;

use crate::dialect::Dialect;

pub struct TableSpec {
    pub table: &'static str,
    pub columns: &'static [&'static str],
    /// Row identity. One column, not the database's primary key.
    pub identity: &'static [&'static str],
    /// `ORDER BY` for paging: the database's own primary key, in ordinal order.
    pub order_by: &'static [&'static str],
    /// Applied on every read, in addition to whatever the user types. Kept here
    /// rather than in the UI so it cannot be edited away.
    pub base_filter: Option<&'static str>,
}

pub const TABLES: &[TableSpec] = &[
    TableSpec {
            table: "dsfa_rm",
            identity: &["dsfa_rm_id"],
            order_by: &[
                "dsfa_rm_id",
                "ds_version",
            ],
            base_filter: Some("ds_active = '1'"),
            columns: &[
                "dsfa_rm_id",
                "ds_create_time",
                "ds_create_user_id",
                "ds_update_user_id",
                "ds_deleted",
                "ds_update_user_name",
                "ds_create_user_name",
                "ds_update_time",
                "code",
                "path",
                "treeinfo_pid",
                "treeinfo_icon",
                "treeinfo_globalid",
                "treeinfo_level",
                "name",
                "type_text",
                "type_value",
                "ID",
                "ds_order",
                "ds_unit_id",
                "ds_dept_id",
                "note",
                "treeinfo_type",
                "remark",
                "templateid",
                "desbuttons",
                "exinfo",
                "content",
                "adapter_value",
                "adapter_text",
                "ds_version",
                "ds_active",
                "dict_type_value",
                "dict_type_text",
                "db_use_value",
                "db_use_text",
                "pinyin_value",
                "ds_security_level_value",
                "ds_security_level_text",
                "py_value",
                "ds_security_level_expirydate",
            ],
        },
    TableSpec {
            table: "dsfa_mm",
            identity: &["dsfa_mm_id"],
            order_by: &[
                "dsfa_mm_id",
                "ds_version",
            ],
            base_filter: Some("ds_active = '1'"),
            columns: &[
                "dsfa_mm_id",
                "ds_create_time",
                "ds_create_user_id",
                "ds_update_user_id",
                "ds_deleted",
                "ds_update_user_name",
                "ds_create_user_name",
                "ds_update_time",
                "ds_order",
                "ID",
                "code",
                "Type_text",
                "Type_value",
                "controls_text",
                "controls_value",
                "level",
                "Name",
                "defaultValue",
                "at",
                "dataSource",
                "ds_unit_id",
                "ds_dept_id",
                "realname",
                "ds_version",
                "ds_active",
                "content",
            ],
        },
    TableSpec {
            table: "dsfa_mm_valueattributes",
            identity: &["dsfa_mm_valueAttributes_id"],
            order_by: &[
                "dsfa_mm_valueAttributes_id",
                "ds_version",
            ],
            base_filter: Some("ds_active = '1'"),
            columns: &[
                "dsfa_mm_valueAttributes_id",
                "ds_create_time",
                "ds_create_user_id",
                "ds_update_user_id",
                "ds_deleted",
                "ds_update_user_name",
                "ds_create_user_name",
                "ds_update_time",
                "ds_order",
                "dsfa_mm_id",
                "unit_type",
                "length",
                "unit_value",
                "code",
                "name",
                "defaultValue",
                "type_text",
                "type_value",
                "ds_unit_id",
                "ds_dept_id",
                "encrypt",
                "ds_version",
                "ds_active",
            ],
        },
    TableSpec {
            table: "dsfa_dbsource_meta",
            identity: &["dsfa_dbsource_meta_id"],
            order_by: &[
                "dsfa_dbsource_meta_id",
                "ds_version",
            ],
            base_filter: Some("ds_active = '1'"),
            columns: &[
                "dsfa_dbsource_meta_id",
                "ds_create_time",
                "ds_create_user_id",
                "ds_update_user_id",
                "ds_order",
                "ds_deleted",
                "ds_update_user_name",
                "ds_create_user_name",
                "ds_update_time",
                "ds_unit_id",
                "dsfa_rm_id",
                "colname",
                "name",
                "type",
                "ds_dept_id",
                "ds_version",
                "ds_active",
            ],
        },
    TableSpec {
            table: "dsfa_rm_dict_list",
            identity: &["dsfa_rm_dict_list_id"],
            order_by: &[
                "dsfa_rm_dict_list_id",
                "ds_version",
            ],
            base_filter: Some("ds_active = '1'"),
            columns: &[
                "dsfa_rm_dict_list_id",
                "ds_create_time",
                "ds_create_user_id",
                "ds_update_user_id",
                "ds_order",
                "ds_deleted",
                "ds_update_user_name",
                "ds_create_user_name",
                "ds_update_time",
                "dsfa_rm_id",
                "code",
                "class",
                "value",
                "ds_unit_id",
                "ds_dept_id",
                "ds_version",
                "ds_active",
                "treeinfo_pid",
                "treeinfo_icon",
                "treeinfo_globalid",
                "alias",
                "treeinfo_type",
                "treeinfo_level",
                "pinyin_value",
                "py_value",
                "stat_dict_id",
                "stat_standard_code",
                "stat_desc",
                "stat_standby_field",
                "stat_unit_value",
                "stat_unit_text",
                "stat_um",
            ],
        },
    TableSpec {
            table: "dsfa_route_version",
            identity: &["dsfa_route_version_id"],
            order_by: &[
                "dsfa_route_version_id",
            ],
            base_filter: None,
            columns: &[
                "dsfa_route_version_id",
                "ds_update_user_id",
                "ds_dept_id",
                "ds_create_user_name",
                "ds_create_user_id",
                "ds_update_user_name",
                "ds_deleted",
                "ds_order",
                "ds_unit_id",
                "ds_create_time",
                "ds_update_time",
                "namespace",
                "version_name",
                "version_order",
                "page_name",
                "req_path",
                "real_path",
                "type_value",
                "type_text",
            ],
        },
];

// ------------------------------------------------------------- per environment

/// A `TableSpec` as one particular database can honour it.
///
/// **The column list is discovered, not compiled in.** A snapshot covers every
/// column the table has *right now*, read from `information_schema` in ordinal
/// order. A compiled list cannot do that: `fnec_prod`'s `dsfa_rm` has 36 columns
/// and `eb169`'s has 41, so any fixed list is wrong for one of them, and reading
/// a column a database does not have is an error rather than an empty result.
/// The compiled list in `TABLES` is kept only as the fallback for a catalogue
/// read that failed.
///
/// What stays compiled is everything that defines *meaning* rather than shape,
/// and must not drift with the data:
///
/// - `identity` -- the `<table>_id` column a row is keyed by
/// - `order_by` -- the unique ordering paging is built on
/// - `base_filter` -- `ds_active = '1'`, the "one active row per id" rule
///
/// Drift in the column list is handled at compare time rather than refused: the
/// live rows are hashed over the **baseline's** columns so the two sides are
/// comparable, the new snapshot records the **current** columns, and the report
/// says both. See `diff::collect_and_compare`.
#[derive(Debug, Clone)]
pub struct ResolvedSpec {
    pub table: String,
    /// Every column the table has, folded for the dialect, in ordinal order.
    pub columns: Vec<String>,
    pub identity: Vec<String>,
    pub order_by: Vec<String>,
    pub base_filter: Option<String>,
}

impl TableSpec {
    /// `actual` is the column names this database reports for the table, in
    /// ordinal order, already folded by `Dialect::fold`. `None` means the
    /// catalogue could not be read, and the compiled list is used instead.
    pub fn resolve(&self, dialect: Dialect, actual: Option<&[String]>) -> ResolvedSpec {
        let fold = |name: &str| dialect.fold(name);
        let have = |name: &str| match actual {
            None => true,
            Some(names) => names.iter().any(|candidate| candidate == &fold(name)),
        };

        let columns: Vec<String> = match actual {
            Some(names) => names.to_vec(),
            None => self.columns.iter().map(|name| fold(name)).collect(),
        };

        // The identity and the sort key have to be columns that exist, or the
        // read itself would fail with a message about a missing column rather
        // than about the table having changed shape.
        ResolvedSpec {
            table: fold(self.table),
            identity: self.identity.iter().filter(|name| have(name)).map(|name| fold(name)).collect(),
            order_by: self.order_by.iter().filter(|name| have(name)).map(|name| fold(name)).collect(),
            base_filter: self.base_filter.map(str::to_string),
            columns,
        }
    }
}

/// Every configured table, resolved against what the database reports.
///
/// `actual` is keyed by the folded table name, and a table missing from it is
/// one the database does not have at all -- resolved with `None`, which falls
/// back to the compiled list so the read fails with the server's own words.
pub fn resolve_all(dialect: Dialect, actual: &BTreeMap<String, Vec<String>>) -> Vec<ResolvedSpec> {
    TABLES
        .iter()
        .map(|spec| spec.resolve(dialect, actual.get(&dialect.fold(spec.table)).map(Vec::as_slice)))
        .collect()
}

