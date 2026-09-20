//! Row encoding and SQL text helpers.
//!
//! The row hash deliberately covers **only the configured columns' values** --
//! not their types, not their names. A column's type changing is a schema
//! difference, and folding it in here would flag every row in the table as
//! modified (see PLAN.md §0.2). The column list that a hash was computed
//! against is stored in `meta.json` and checked before any diff.

use serde_json::Value;
use sha2::{Digest, Sha256};

/// Bump when the encoding below changes. Snapshots record the version they were
/// written with; a mismatch means the two hashes are not comparable.
pub const HASH_ALGO_VERSION: u32 = 1;

/// A CLI value normalized to the bytes that go into the hash. `None` = SQL NULL,
/// which is kept distinct from the empty string.
///
/// The CLI's value representation depends on the driver behind the connection:
/// MySQL hands back everything as strings, SQLite hands back typed JSON. Both
/// have to land on the same canonical bytes.
pub fn canonical(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        // Kept distinct from the string "1" / "0" that a MySQL tinyint produces.
        Value::Bool(flag) => Some(if *flag { "true".to_string() } else { "false".to_string() }),
        Value::Number(number) => Some(number.to_string()),
        // Arrays and objects only appear for document databases; their JSON text
        // is deterministic given the same input.
        other => Some(other.to_string()),
    }
}

/// Length-prefixed, self-delimiting encoding of one row's values.
///
/// Used for both the content hash and the primary-key map key. Length prefixes
/// mean no separator character can be ambiguous -- `["ab","c"]` and `["a","bc"]`
/// encode differently.
pub fn encode_values(values: &[Option<String>]) -> Vec<u8> {
    let mut out = Vec::new();
    for value in values {
        match value {
            None => out.push(0),
            Some(text) => {
                out.push(1);
                push_len(&mut out, text.len());
                out.extend_from_slice(text.as_bytes());
            }
        }
    }
    out
}

fn push_len(out: &mut Vec<u8>, mut value: usize) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex(&hasher.finalize())
}

pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

/// `` `name` `` with embedded backticks doubled.
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// `'text'`. Escaping is by the standard rules: `sql_mode` is enforced to be
/// identical across environments at the project level, so there is no
/// `NO_BACKSLASH_ESCAPES` variant to handle (PLAN.md §8).
pub fn quote_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for character in value.chars() {
        match character {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\0' => out.push_str("\\0"),
            other => out.push(other),
        }
    }
    out.push('\'');
    out
}

/// Deduplicate while preserving the first occurrence's position.
pub fn dedup(names: &[String]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    names.iter().filter(|name| seen.insert((*name).clone())).cloned().collect()
}
