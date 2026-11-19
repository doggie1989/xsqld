use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender};
use std::time::Instant;

use anyhow::{Result, ensure};
use duckdb::core::LogicalTypeId;
use duckdb::{Connection, appender_params_from_iter, params_from_iter, types::Value as DuckValue};
use parking_lot::{Condvar, Mutex, RwLock};
use serde::{Deserialize, Serialize};
use sqlparser::{ast::Statement, dialect::DuckDbDialect, parser::Parser};

use crate::config::Config;
use crate::conn::Conn;
use crate::metrics::DbMetrics;
use crate::proto::{Col, StmtResult, Value};
use crate::reader::ReadGate;
use crate::writer::Writer;

pub const INSTALL_SQL: &str = include_str!("../htap-p0/schema.sql");
const SOURCE_POSITION: &str = "SELECT source_id,max(pruned_seq,COALESCE((SELECT MAX(seq) FROM xsqld_changelog),0)),pruned_seq,schema_fingerprint FROM xsqld_htap_meta WHERE id=1";
const MIRROR_FORMAT: i64 = 2;
pub const CHANGE_TRIGGER_PREFIX: &str = "xsqld_capture_";
const INTERNAL_OBJECTS: [&str; 2] = ["xsqld_changelog", "xsqld_htap_meta"];

#[derive(Clone, Debug)]
struct ColumnSchema {
    name: String,
    declared_type: String,
    primary_key_order: i64,
}

#[derive(Clone, Debug)]
enum RowKey {
    Rowid(String),
    Columns(Vec<usize>),
}

#[derive(Clone, Debug)]
struct TableSchema {
    name: String,
    sql: String,
    mirror_name: String,
    physical_name: String,
    key_name: String,
    columns: Vec<ColumnSchema>,
    key: RowKey,
}

#[derive(Deserialize)]
struct EncodedCell {
    t: String,
    v: Option<String>,
}

#[derive(Deserialize)]
struct ChangePayload {
    values: Vec<EncodedCell>,
}

#[derive(Clone)]
struct ChangeRecord {
    seq: i64,
    table: String,
    operation: String,
    key: String,
    payload: Option<String>,
}

/// Whether a schema object belongs to the capture (its tables and generated triggers)
pub fn is_capture_trigger(name: &str) -> bool {
    name.get(..CHANGE_TRIGGER_PREFIX.len())
        .is_some_and(|p| p.eq_ignore_ascii_case(CHANGE_TRIGGER_PREFIX))
        || ["orders_ai", "orders_au", "orders_ad"]
            .iter()
            .any(|n| name.eq_ignore_ascii_case(n))
}

pub fn is_internal_object(name: &str) -> bool {
    INTERNAL_OBJECTS.iter().any(|n| name.eq_ignore_ascii_case(n)) || is_capture_trigger(name)
}

pub fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('\"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn hash_name(name: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in name.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn table_schemas(conn: &rusqlite::Connection) -> Result<Vec<TableSchema>> {
    let mut kinds = HashMap::new();
    {
        let mut statement = conn.prepare("SELECT name,type,wr FROM pragma_table_list WHERE schema='main'")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            kinds.insert(
                row.get::<_, String>(0)?.to_ascii_lowercase(),
                (row.get::<_, String>(1)?, row.get::<_, bool>(2)?),
            );
        }
    }
    let mut statement =
        conn.prepare("SELECT name,sql FROM sqlite_schema WHERE type='table' AND sql IS NOT NULL ORDER BY name")?;
    let mut rows = statement.query([])?;
    let mut tables = Vec::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let sql: String = row.get(1)?;
        if name.to_ascii_lowercase().starts_with("sqlite_") || is_internal_object(&name) {
            continue;
        }
        let (table_kind, without_rowid) = kinds
            .get(&name.to_ascii_lowercase())
            .map(|(kind, wr)| (Some(kind.as_str()), *wr))
            .unwrap_or((None, false));
        match table_kind {
            Some("shadow") => continue,
            // SQLite allows no triggers on a virtual table, so there is nothing to capture incrementally
            Some("virtual") => continue,
            _ if sql
                .trim_start()
                .to_ascii_uppercase()
                .starts_with("CREATE VIRTUAL TABLE") =>
            {
                continue;
            }
            _ => {}
        }
        let mut info = conn.prepare("SELECT name,type,pk FROM pragma_table_xinfo(?1) ORDER BY cid")?;
        let mut info_rows = info.query([&name])?;
        let mut columns = Vec::new();
        while let Some(col) = info_rows.next()? {
            columns.push(ColumnSchema {
                name: col.get(0)?,
                declared_type: col.get::<_, Option<String>>(1)?.unwrap_or_default(),
                primary_key_order: col.get(2)?,
            });
        }
        ensure!(!columns.is_empty(), "table `{name}` has no columns");
        let primary = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key_order > 0)
            .map(|(i, c)| (c.primary_key_order, i))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect::<Vec<_>>();
        let key = if without_rowid {
            ensure!(!primary.is_empty(), "WITHOUT ROWID table `{name}` has no primary key");
            RowKey::Columns(primary)
        } else {
            let shadowed = |candidate: &str| columns.iter().any(|c| c.name.eq_ignore_ascii_case(candidate));
            match ["rowid", "_rowid_", "oid"].into_iter().find(|n| !shadowed(n)) {
                Some(alias) => RowKey::Rowid(alias.to_owned()),
                None if !primary.is_empty() => RowKey::Columns(primary),
                None => anyhow::bail!("table `{name}` shadows all rowid aliases and has no primary key"),
            }
        };
        let mirror_name = format!("{name}_native");
        let physical_name = format!("__xsqld_data_{:016x}", hash_name(&name.to_ascii_lowercase()));
        let mut key_name = "__xsqld_key".to_owned();
        while columns.iter().any(|c| c.name.eq_ignore_ascii_case(&key_name)) {
            key_name.push('_');
        }
        tables.push(TableSchema {
            name,
            sql,
            mirror_name,
            physical_name,
            key_name,
            columns,
            key,
        });
    }
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    let mut targets = HashSet::new();
    for table in &tables {
        ensure!(
            targets.insert(table.mirror_name.to_ascii_lowercase()),
            "mirror name collision for `{}`",
            table.mirror_name
        );
    }
    Ok(tables)
}

fn fnv1a<'a>(parts: impl Iterator<Item = &'a [u8]>) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for bytes in parts {
        for byte in bytes.iter().copied().chain(std::iter::once(0xff)) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    hash
}

/// The CREATE text carries the primary key, its column order and WITHOUT ROWID, so a key change shows up here
fn table_parts(table: &TableSchema) -> impl Iterator<Item = &[u8]> {
    std::iter::once(table.name.as_bytes())
        .chain(std::iter::once(table.sql.as_bytes()))
        .chain(
            table
                .columns
                .iter()
                .flat_map(|col| [col.name.as_bytes(), col.declared_type.as_bytes()]),
        )
}

fn schema_fingerprint(tables: &[TableSchema]) -> String {
    format!("{:016x}", fnv1a(tables.iter().flat_map(table_parts)))
}

/// One table's shape on its own, so a schema change rebuilds that table's mirror and leaves the others alone
fn table_fingerprint(table: &TableSchema) -> String {
    format!("{:016x}", fnv1a(table_parts(table)))
}

fn cell_json_expr(alias: &str, column: &str) -> String {
    let value = format!("{alias}.{}", quote_identifier(column));
    format!(
        "json_object('t',typeof({value}),'v',CASE typeof({value}) WHEN 'blob' THEN hex({value}) WHEN 'text' THEN hex(CAST({value} AS BLOB)) WHEN 'real' THEN printf('%!.17g',{value}) WHEN 'null' THEN NULL ELSE CAST({value} AS TEXT) END)"
    )
}

fn key_json_expr(table: &TableSchema, alias: &str) -> String {
    let cells = match &table.key {
        RowKey::Rowid(rowid) => cell_json_expr(alias, rowid),
        RowKey::Columns(indices) => indices
            .iter()
            .map(|i| cell_json_expr(alias, &table.columns[*i].name))
            .collect::<Vec<_>>()
            .join(","),
    };
    format!("json_array({cells})")
}

fn payload_json_expr(table: &TableSchema, alias: &str) -> String {
    let values = table
        .columns
        .iter()
        .map(|c| cell_json_expr(alias, &c.name))
        .collect::<Vec<_>>()
        .join(",");
    format!("json_object('values',json_array({values}))")
}

fn key_changed_expr(table: &TableSchema) -> String {
    let names: Vec<&str> = match &table.key {
        RowKey::Rowid(rowid) => vec![rowid.as_str()],
        RowKey::Columns(indices) => indices.iter().map(|i| table.columns[*i].name.as_str()).collect(),
    };
    names
        .iter()
        .map(|name| {
            let quoted = quote_identifier(name);
            format!("OLD.{quoted} IS NOT NEW.{quoted}")
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn capture_trigger_sql(table: &TableSchema) -> String {
    let hash = format!("{:016x}", hash_name(&table.name.to_ascii_lowercase()));
    let table_q = quote_identifier(&table.name);
    let table_l = quote_literal(&table.name);
    let trigger = |suffix: &str| quote_identifier(&format!("{CHANGE_TRIGGER_PREFIX}{hash}_{suffix}"));
    let insert = |op: &str, alias: &str| {
        format!(
            "INSERT INTO xsqld_changelog(tbl,op,pk,payload,ts) VALUES({table_l},{},{},{},unixepoch());",
            quote_literal(op),
            key_json_expr(table, alias),
            payload_json_expr(table, alias)
        )
    };
    // An update that leaves the key alone is one upsert; only a key change also has to retire the old key
    let retire_old_key = format!(
        "INSERT INTO xsqld_changelog(tbl,op,pk,payload,ts) SELECT {table_l},'D',{},{},unixepoch() WHERE {};",
        key_json_expr(table, "OLD"),
        payload_json_expr(table, "OLD"),
        key_changed_expr(table)
    );
    format!(
        "CREATE TRIGGER {} AFTER INSERT ON {table_q} BEGIN {} END; \
         CREATE TRIGGER {} AFTER UPDATE ON {table_q} BEGIN {} {} END; \
         CREATE TRIGGER {} AFTER DELETE ON {table_q} BEGIN {} END;",
        trigger("i"),
        insert("I", "NEW"),
        trigger("u"),
        retire_old_key,
        insert("I", "NEW"),
        trigger("d"),
        insert("D", "OLD")
    )
}

fn ensure_schema_fingerprint_column(conn: &rusqlite::Connection) -> Result<()> {
    let mut statement = conn.prepare("SELECT name FROM pragma_table_info('xsqld_htap_meta')")?;
    let mut rows = statement.query([])?;
    let mut found = false;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        found |= name.eq_ignore_ascii_case("schema_fingerprint");
    }
    drop(rows);
    if !found {
        conn.execute_batch("ALTER TABLE xsqld_htap_meta ADD COLUMN schema_fingerprint TEXT NOT NULL DEFAULT ''")?;
    }
    Ok(())
}

/// Installs the outbox and capture triggers for every ordinary user table.
pub fn install_capture(conn: &rusqlite::Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE")?;
    let result = (|| -> Result<()> {
        let legacy = changelog_uses_legacy_integer_key(conn)?;
        if legacy {
            drop_capture_triggers(conn)?;
            conn.execute_batch("DROP TABLE xsqld_changelog")?;
        }
        conn.execute_batch(INSTALL_SQL)?;
        ensure_schema_fingerprint_column(conn)?;
        if legacy {
            conn.execute(
                "UPDATE xsqld_htap_meta SET source_id=lower(hex(randomblob(16))),pruned_seq=0,schema_fingerprint='' WHERE id=1",
                [],
            )?;
        }
        reconcile_capture_in_transaction(conn, true)?;
        Ok(())
    })();
    match result {
        Ok(()) => {
            conn.execute_batch("COMMIT")?;
            Ok(())
        }
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

fn changelog_uses_legacy_integer_key(conn: &rusqlite::Connection) -> Result<bool> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='xsqld_changelog')",
        [],
        |row| row.get(0),
    )?;
    if !exists {
        return Ok(false);
    }
    let declared: String = conn.query_row(
        "SELECT type FROM pragma_table_info('xsqld_changelog') WHERE name='pk'",
        [],
        |row| row.get(0),
    )?;
    Ok(!declared.eq_ignore_ascii_case("TEXT"))
}

fn drop_capture_triggers(conn: &rusqlite::Connection) -> Result<()> {
    let mut statement = conn.prepare("SELECT name FROM sqlite_schema WHERE type='trigger' ORDER BY name")?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    for name in names {
        if is_capture_trigger(&name) {
            conn.execute_batch(&format!("DROP TRIGGER {}", quote_identifier(&name)))?;
        }
    }
    Ok(())
}

/// Refreshes capture inside the writer's current statement savepoint after a schema change.
pub fn refresh_capture_in_transaction(conn: &rusqlite::Connection) -> Result<bool> {
    ensure_schema_fingerprint_column(conn)?;
    reconcile_capture_in_transaction(conn, false)
}

fn reconcile_capture_in_transaction(conn: &rusqlite::Connection, force: bool) -> Result<bool> {
    let tables = table_schemas(conn)?;
    let fingerprint = schema_fingerprint(&tables);
    let previous: String = conn.query_row("SELECT schema_fingerprint FROM xsqld_htap_meta WHERE id=1", [], |row| {
        row.get(0)
    })?;
    let changed = previous != fingerprint;
    if !changed && !force {
        return Ok(false);
    }
    drop_capture_triggers(conn)?;
    for table in &tables {
        conn.execute_batch(&capture_trigger_sql(table))?;
    }
    conn.execute(
        "UPDATE xsqld_htap_meta SET schema_fingerprint=? WHERE id=1",
        [&fingerprint],
    )?;
    if changed {
        // The source generation stays: only the tables whose own shape changed are rebuilt, so a schema change
        // must not invalidate the mirrors of the tables it did not touch. The marker moves the source position so
        // the sync wakes even when no business row changed.
        conn.execute(
            "INSERT INTO xsqld_changelog(tbl,op,pk,payload,ts) VALUES('__xsqld_schema__','S','[]',?,unixepoch())",
            [&fingerprint],
        )?;
    }
    Ok(changed)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub sql: String,
    #[serde(default)]
    pub args: Vec<Value>,
}

#[derive(Serialize, Debug)]
pub struct Response {
    pub watermark: i64,
    pub result: StmtResult,
}

#[derive(Default)]
struct Status {
    watermark: i64,
    error: Option<String>,
}

#[derive(Default)]
struct Progress {
    status: Mutex<Status>,
    changed: Condvar,
    stopped: AtomicBool,
    scheduled: AtomicBool,
    pending: AtomicBool,
    running: Mutex<bool>,
}

struct WorkerState {
    mirror: Connection,
    receive: mpsc::Receiver<()>,
    writer: Writer,
    idle_since: Option<Instant>,
}

pub struct Htap {
    root: Mutex<Option<Connection>>,
    queries: RwLock<()>,
    progress: Arc<Progress>,
    wake: SyncSender<()>,
    worker: Mutex<Option<WorkerState>>,
    interrupt: Arc<duckdb::InterruptHandle>,
    path: PathBuf,
    cfg: Arc<Config>,
    /// The database's read gate: the capture's reads pass it like any other read, so a checkpoint that must restart
    /// the WAL can wait for them (README §3)
    gate: Arc<ReadGate>,
    metrics: Arc<DbMetrics>,
    snapshot: Arc<RwLock<Option<Arc<MirrorSnapshot>>>>,
}

struct Deadline(tokio::task::JoinHandle<()>);

impl Deadline {
    fn new(conn: &Connection, deadline: Instant) -> Self {
        let handle = conn.interrupt_handle();
        Self(tokio::runtime::Handle::current().spawn(async move {
            tokio::time::sleep_until(deadline.into()).await;
            handle.interrupt();
        }))
    }
}

impl Drop for Deadline {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub fn query_sql(sql: &str) -> Result<String> {
    let statements = Parser::parse_sql(&DuckDbDialect {}, sql)?;
    ensure!(statements.len() == 1, "analytics requires exactly one SELECT query");
    ensure!(
        matches!(&statements[0], Statement::Query(_)),
        "analytics accepts SELECT queries only"
    );
    // DuckDB prepare executes preceding statements; only pass a canonical, single query.
    Ok(format!("SELECT * FROM ({}) AS xsqld_analysis", statements[0]))
}

fn normalize_variant_outputs(sql: &str, variants: &[(usize, String)]) -> String {
    let replacements = variants
        .iter()
        .map(|(_, name)| {
            let quoted = quote_identifier(name);
            format!(
                "CASE WHEN {quoted} IS NULL THEN 'VARIANT_NULL:' WHEN variant_typeof({quoted})='BLOB' THEN 'BLOB:' || hex(CAST({quoted} AS BLOB)) ELSE variant_typeof({quoted}) || ':' || CAST({quoted} AS VARCHAR) END AS {quoted}"
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("SELECT * REPLACE ({replacements}) FROM ({sql}) AS xsqld_variant_output")
}

/// duckdb-rs refuses to decode VARIANT result columns, so the projection has to be rewritten before the query runs.
/// `DESCRIBE` binds the same query without producing a result, which is the only way to learn the output names early.
fn result_column_names(conn: &Connection, sql: &str, args: &[DuckValue]) -> Result<Vec<String>> {
    let mut statement = conn.prepare(&format!("DESCRIBE {sql}"))?;
    let mut rows = statement.query(params_from_iter(args.iter()))?;
    let mut names = Vec::new();
    while let Some(row) = rows.next()? {
        names.push(row.get::<_, String>(0)?);
    }
    Ok(names)
}

fn variant_protocol_value(value: String) -> Result<Value> {
    let (kind, value) = value
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("invalid DuckDB VARIANT result"))?;
    Ok(match kind {
        "VARIANT_NULL" | "NULL" => Value::Null,
        "BOOL_TRUE" | "BOOL_FALSE" | "BOOLEAN" => Value::Integer(i64::from(value.eq_ignore_ascii_case("true"))),
        "INT8" | "INT16" | "INT32" | "INT64" | "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" => {
            Value::Integer(value.parse()?)
        }
        "UINT8" | "UINT16" | "UINT32" | "UINT64" | "UTINYINT" | "USMALLINT" | "UINTEGER" | "UBIGINT" => value
            .parse::<u64>()
            .ok()
            .and_then(|n| i64::try_from(n).ok())
            .map(Value::Integer)
            .unwrap_or_else(|| Value::Text(value.to_owned())),
        "FLOAT" | "DOUBLE" => Value::Float(value.parse()?),
        "VARCHAR" => Value::Text(value.to_owned()),
        "BLOB" => Value::Blob(decode_hex(value)?),
        other if other.starts_with("DECIMAL") => Value::Float(value.parse()?),
        _ => Value::Text(value.to_owned()),
    })
}

pub fn mirror_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".duckdb");
    name.into()
}

fn position(conn: &rusqlite::Connection) -> Result<(String, i64, i64, String)> {
    Ok(conn.query_row(SOURCE_POSITION, [], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
    })?)
}

fn watermark(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row("SELECT watermark FROM xsqld_meta WHERE id=1", [], |r| r.get(0))?)
}

fn mirror_state(conn: &Connection) -> Result<(String, i64, i64, String)> {
    match conn.query_row(
        "SELECT source_id,watermark,format,schema_fingerprint FROM xsqld_meta WHERE id=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    ) {
        Ok(state) => Ok(state),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok((String::new(), -1, -1, String::new())),
        Err(error) => Err(error.into()),
    }
}

/// What the mirror holds, as seen without touching DuckDB: the router needs this on every read
#[derive(Clone, Debug)]
pub struct TableState {
    pub watermark: i64,
    /// Per column, the storage classes ever seen (`i` integer, `r` real, `t` text, `b` blob, `n` null)
    pub purity: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct MirrorSnapshot {
    pub watermark: i64,
    /// Keyed by the source table name in lower case
    pub tables: BTreeMap<String, TableState>,
}

#[derive(Clone, Debug)]
struct CatalogEntry {
    source_name: String,
    mirror_name: String,
    physical_name: String,
    /// `None` on a mirror written before per-table shape was recorded
    fingerprint: Option<String>,
    watermark: Option<i64>,
    /// Per column, the storage classes ever seen, as sorted letters (i r t b n). A column that only ever held
    /// integers and NULLs is "in"; text that was not UTF-8 decodes to a blob, so such a column reads "bt".
    purity: Option<String>,
}

/// What the mirror holds, keyed by the source table name in lower case
fn read_catalog(conn: &Connection) -> Result<BTreeMap<String, CatalogEntry>> {
    let mut statement =
        conn.prepare("SELECT source_name,mirror_name,physical_name,fingerprint,watermark,purity FROM xsqld_tables")?;
    let mut rows = statement.query([])?;
    let mut out = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        out.insert(
            name.to_ascii_lowercase(),
            CatalogEntry {
                source_name: name.clone(),
                mirror_name: row.get(1)?,
                physical_name: row.get(2)?,
                fingerprint: row.get(3)?,
                watermark: row.get(4)?,
                purity: row.get(5)?,
            },
        );
    }
    Ok(out)
}

fn duck_object_type(conn: &Connection, name: &str) -> Result<Option<String>> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='main' AND table_name=?)",
        [name],
        |r| r.get(0),
    )?;
    if !exists {
        return Ok(None);
    }
    Ok(Some(conn.query_row(
        "SELECT table_type FROM information_schema.tables WHERE table_schema='main' AND table_name=?",
        [name],
        |r| r.get(0),
    )?))
}

fn drop_duck_object(conn: &Connection, name: &str) -> Result<()> {
    match duck_object_type(conn, name)?.as_deref() {
        Some("VIEW") => conn.execute_batch(&format!("DROP VIEW {}", quote_identifier(name)))?,
        Some(_) => conn.execute_batch(&format!("DROP TABLE {}", quote_identifier(name)))?,
        None => {}
    }
    Ok(())
}

fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='main' AND table_name=? AND column_name=?)",
        [table, column],
        |r| r.get(0),
    )?)
}

fn ensure_duck_schema(conn: &Connection) -> Result<()> {
    let has_meta = duck_object_type(conn, "xsqld_meta")?.is_some();
    if has_meta && !has_column(conn, "xsqld_meta", "format")? {
        drop_duck_object(conn, "orders_native")?;
        drop_duck_object(conn, "xsqld_meta")?;
        drop_duck_object(conn, "xsqld_tables")?;
    }
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS xsqld_meta(id INTEGER PRIMARY KEY,source_id VARCHAR NOT NULL,watermark BIGINT NOT NULL,format BIGINT NOT NULL,schema_fingerprint VARCHAR NOT NULL); \
         CREATE TABLE IF NOT EXISTS xsqld_tables(source_name VARCHAR PRIMARY KEY,mirror_name VARCHAR NOT NULL,physical_name VARCHAR NOT NULL);",
    )?;
    // Per-table shape and position, added to mirrors that predate them. The format stays the same: a mirror
    // without these columns is adopted as it stands when its recorded schema still matches (see `adopt_legacy`).
    for (column, decl) in [
        ("fingerprint", "VARCHAR"),
        ("watermark", "BIGINT"),
        ("purity", "VARCHAR"),
    ] {
        if !has_column(conn, "xsqld_tables", column)? {
            conn.execute_batch(&format!("ALTER TABLE xsqld_tables ADD COLUMN {column} {decl}"))?;
        }
    }
    Ok(())
}

fn duck_table_ddl(table: &TableSchema) -> String {
    let mut columns = vec![format!("{} VARCHAR PRIMARY KEY", quote_identifier(&table.key_name))];
    columns.extend(
        table
            .columns
            .iter()
            .map(|column| format!("{} VARIANT", quote_identifier(&column.name))),
    );
    format!(
        "CREATE TABLE {}({})",
        quote_identifier(&table.physical_name),
        columns.join(",")
    )
}

fn duck_view_ddl(table: &TableSchema) -> String {
    let columns = table
        .columns
        .iter()
        .map(|c| quote_identifier(&c.name))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "CREATE VIEW {} AS SELECT {columns} FROM {}",
        quote_identifier(&table.mirror_name),
        quote_identifier(&table.physical_name)
    )
}

fn decode_hex(hex: &str) -> Result<Vec<u8>> {
    ensure!(hex.len().is_multiple_of(2), "invalid hex payload");
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().as_chunks::<2>().0 {
        let digit = |b: u8| -> Result<u8> {
            match b {
                b'0'..=b'9' => Ok(b - b'0'),
                b'a'..=b'f' => Ok(b - b'a' + 10),
                b'A'..=b'F' => Ok(b - b'A' + 10),
                _ => anyhow::bail!("invalid hex payload"),
            }
        };
        bytes.push((digit(pair[0])? << 4) | digit(pair[1])?);
    }
    Ok(bytes)
}

fn decode_cell(cell: EncodedCell) -> Result<DuckValue> {
    Ok(match cell.t.as_str() {
        "null" => DuckValue::Null,
        "integer" => DuckValue::BigInt(
            cell.v
                .ok_or_else(|| anyhow::anyhow!("missing integer payload"))?
                .parse()?,
        ),
        "real" => DuckValue::Double(cell.v.ok_or_else(|| anyhow::anyhow!("missing real payload"))?.parse()?),
        "text" => {
            let bytes = decode_hex(cell.v.as_deref().unwrap_or_default())?;
            match String::from_utf8(bytes) {
                Ok(text) => DuckValue::Text(text),
                Err(error) => DuckValue::Blob(error.into_bytes()),
            }
        }
        "blob" => DuckValue::Blob(decode_hex(cell.v.as_deref().unwrap_or_default())?),
        other => anyhow::bail!("unsupported SQLite storage class `{other}`"),
    })
}

impl Htap {
    pub fn start(
        path: &Path,
        cfg: Arc<Config>,
        writer: Writer,
        gate: Arc<ReadGate>,
        metrics: Arc<DbMetrics>,
        deadline: Instant,
    ) -> Result<Arc<Self>> {
        let (wake, receive) = writer.subscribe_htap();
        let threads = std::thread::available_parallelism()
            .map_or(1, |n| n.get())
            .min(cfg.parallel_max_ranges);
        let memory_limit = format!("{}MB", cfg.htap_memory_limit_mb);
        let settings = duckdb::Config::default()
            .enable_external_access(false)?
            .enable_autoload_extension(false)?
            .with("allow_unsigned_extensions", "false")?
            .with("memory_limit", &memory_limit)?
            .with("preserve_insertion_order", "false")?
            .threads(threads as i64)?;
        let mut root = Connection::open_with_flags(mirror_path(path), settings)?;
        let timer = Deadline::new(&root, deadline);
        root.execute_batch("SET lock_configuration=true")?;
        ensure_duck_schema(&root)?;
        let Some(pass) = gate.enter(deadline) else {
            anyhow::bail!("the analytical snapshot could not start: the read gate stayed closed past the deadline");
        };
        let source = Conn::open_reader(path, &cfg)?;
        source.set_deadline(Some(deadline));
        source.c.execute_batch("BEGIN")?;
        let target = position(&source.c)?.1;
        // The same planning and batch path the sync worker uses, so opening a mirror and syncing it cannot disagree
        apply_snapshot(&source.c, &mut root, &metrics, deadline)?;
        drop(source);
        drop(pass);
        let initial = watermark(&root)?;
        metrics.htap_watermark.store(initial, Ordering::Relaxed);
        metrics.htap_source_position.store(target, Ordering::Relaxed);
        drop(timer);
        let mirror = root.try_clone()?;
        let interrupt = mirror.interrupt_handle();
        let progress = Arc::new(Progress::default());
        progress.status.lock().watermark = initial;
        let htap = Arc::new(Self {
            root: Mutex::new(Some(root)),
            queries: RwLock::new(()),
            progress,
            wake,
            worker: Mutex::new(Some(WorkerState {
                mirror,
                receive,
                writer: writer.clone(),
                idle_since: None,
            })),
            interrupt,
            snapshot: Arc::new(RwLock::new(None)),
            path: path.to_owned(),
            cfg,
            gate,
            metrics,
        });
        // A commit signals the sync and starts its worker: a worker that had gone idle would leave the signal
        // unread, and the mirror would stay behind until someone ran an analytical query.
        writer.set_htap_wake(Arc::new({
            let weak = Arc::downgrade(&htap);
            move || {
                if let Some(htap) = weak.upgrade() {
                    htap.progress.pending.store(true, Ordering::Release);
                    let _ = htap.wake.try_send(());
                    htap.spawn_worker();
                }
            }
        }));
        htap.refresh_snapshot()?;
        htap.spawn_worker();
        Ok(htap)
    }

    /// Prepares and reads one query, rewriting VARIANT result columns first because duckdb-rs refuses to decode
    /// them. Shared by the analytics endpoint and the router so both produce values the same way.
    fn read_result(
        &self,
        conn: &Connection,
        sql: &str,
        args: &[DuckValue],
        deadline: Instant,
        strict: bool,
    ) -> Result<(Vec<Col>, Vec<Vec<Value>>)> {
        let mut sql = sql.to_owned();
        let mut statement = conn.prepare(&sql)?;
        let variants: Vec<usize> = (0..statement.column_count())
            .filter(|index| statement.column_logical_type(*index).id() == LogicalTypeId::Variant)
            .collect();
        let mut variant_outputs = HashSet::new();
        if !variants.is_empty() {
            let names = result_column_names(conn, &sql, args)?;
            ensure!(
                names.len() == statement.column_count(),
                "analytics result schema changed"
            );
            let tagged = variants
                .iter()
                .map(|index| (*index, names[*index].clone()))
                .collect::<Vec<_>>();
            drop(statement);
            sql = normalize_variant_outputs(&sql, &tagged);
            statement = conn.prepare(&sql)?;
            variant_outputs.extend(variants);
        }
        let mut records = statement.query(params_from_iter(args.iter()))?;
        let metadata = records
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing query metadata"))?;
        let cols = metadata
            .column_names()
            .into_iter()
            .map(|name| Col {
                name: Some(name),
                decltype: None,
            })
            .collect::<Vec<_>>();
        let mut budget = self.cfg.max_response_bytes;
        for col in &cols {
            let bytes = serde_json::to_vec(col)?.len() as u64 + 1;
            ensure!(bytes <= budget, "RESPONSE_TOO_LARGE");
            budget -= bytes;
        }
        let mut rows = Vec::new();
        while let Some(row) = records.next()? {
            ensure!(Instant::now() < deadline, "analytics request timed out");
            let mut values = Vec::with_capacity(cols.len());
            for i in 0..cols.len() {
                let value = if variant_outputs.contains(&i) {
                    let tagged = row.get::<_, String>(i)?;
                    // Same rule on the tagged path: the gate admits integer and text columns only, so any other
                    // storage class means DuckDB produced something SQLite would not (a wider SUM, a decimal).
                    if strict && !admitted_variant_tag(&tagged) {
                        anyhow::bail!("the mirror's result is outside the admitted storage classes");
                    }
                    variant_protocol_value(tagged)?
                } else {
                    let raw = row.get::<_, DuckValue>(i)?;
                    // A routed read must not hand back an answer SQLite would not give: SQLite raises an overflow
                    // error where DuckDB widens the integer, so refuse here and let SQLite answer instead.
                    if strict && widens_beyond_i64(&raw) {
                        anyhow::bail!("the mirror's result leaves the signed 64-bit range");
                    }
                    protocol_value(raw)?
                };
                let bytes = serde_json::to_vec(&value)?.len() as u64 + 3;
                ensure!(bytes <= budget, "RESPONSE_TOO_LARGE");
                budget -= bytes;
                values.push(value);
            }
            ensure!(budget >= 2, "RESPONSE_TOO_LARGE");
            budget -= 2;
            rows.push(values);
        }
        drop(records);
        drop(statement);
        Ok((cols, rows))
    }

    /// Answers a query the router already proved equivalent and fresh. It does not wait for the watermark and
    /// does not enter the read gate: the caller checked freshness and released its pass before calling, so a long
    /// analytical read cannot hold up a checkpoint.
    pub fn run_routed(self: &Arc<Self>, sql: &str, args: Vec<Value>, deadline: Instant) -> Result<StmtResult> {
        let _query = self.queries.read();
        ensure!(!self.progress.stopped.load(Ordering::Acquire), "HTAP is shutting down");
        let conn = self
            .root
            .lock()
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("HTAP is unavailable"))?
            .try_clone()?;
        let _timer = Deadline::new(&conn, deadline);
        let started = Instant::now();
        let args: Vec<DuckValue> = args
            .into_iter()
            .map(|v| match v {
                Value::Null => DuckValue::Null,
                Value::Integer(n) => DuckValue::BigInt(n),
                Value::Float(n) => DuckValue::Double(n),
                Value::Text(s) => DuckValue::Text(s),
                Value::Blob(b) => DuckValue::Blob(b),
            })
            .collect();
        conn.execute_batch("BEGIN TRANSACTION READ ONLY")?;
        let (cols, rows) = self.read_result(&conn, sql, &args, deadline, true)?;
        conn.execute_batch("COMMIT")?;
        let rows_read = rows.len() as u64;
        self.metrics.htap_queries.fetch_add(1, Ordering::Relaxed);
        self.metrics.htap_query.record(started.elapsed());
        Ok(StmtResult {
            cols,
            rows,
            rows_read,
            query_duration_ms: started.elapsed().as_secs_f64() * 1000.0,
            ..Default::default()
        })
    }

    pub fn query(self: &Arc<Self>, request: Request, deadline: Instant) -> Result<Response> {
        let sql = query_sql(&request.sql)?;
        let _query = self.queries.read();
        ensure!(!self.progress.stopped.load(Ordering::Acquire), "HTAP is shutting down");
        let conn = self
            .root
            .lock()
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("HTAP is unavailable"))?
            .try_clone()?;
        let _timer = Deadline::new(&conn, deadline);
        let target = {
            let Some(_pass) = self.gate.enter(deadline) else {
                anyhow::bail!(
                    "the analytical query could not read the source position: the read gate stayed closed past the deadline"
                );
            };
            let source = Conn::open_reader(&self.path, &self.cfg)?;
            source.set_deadline(Some(deadline));
            let (_, target, _, _) = position(&source.c)?;
            target
        };
        self.metrics.htap_source_position.store(target, Ordering::Relaxed);
        self.progress.pending.store(true, Ordering::Release);
        let _ = self.wake.try_send(());
        self.spawn_worker();
        let mut status = self.progress.status.lock();
        while status.watermark < target {
            ensure!(!self.progress.stopped.load(Ordering::Acquire), "HTAP is shutting down");
            if let Some(error) = &status.error {
                anyhow::bail!("HTAP synchronization failed: {error}");
            }
            ensure!(Instant::now() < deadline, "analytics synchronization timed out");
            self.progress.changed.wait_until(&mut status, deadline);
        }
        drop(status);
        ensure!(Instant::now() < deadline, "analytics request timed out");
        conn.execute_batch("BEGIN TRANSACTION READ ONLY")?;
        let started = Instant::now();
        let args: Vec<_> = request
            .args
            .into_iter()
            .map(|v| match v {
                Value::Null => DuckValue::Null,
                Value::Integer(n) => DuckValue::BigInt(n),
                Value::Float(n) => DuckValue::Double(n),
                Value::Text(s) => DuckValue::Text(s),
                Value::Blob(b) => DuckValue::Blob(b),
            })
            .collect();
        let (cols, rows) = self.read_result(&conn, &sql, &args, deadline, false)?;
        conn.execute_batch("COMMIT")?;
        let wm = watermark(
            self.root
                .lock()
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("HTAP is unavailable"))?,
        )?;
        let rows_read = rows.len() as u64;
        self.metrics.htap_queries.fetch_add(1, Ordering::Relaxed);
        self.metrics.htap_query.record(started.elapsed());
        Ok(Response {
            watermark: wm,
            result: StmtResult {
                cols,
                rows,
                rows_read,
                query_duration_ms: started.elapsed().as_secs_f64() * 1000.0,
                ..Default::default()
            },
        })
    }

    pub fn stop(self: &Arc<Self>) {
        self.progress.stopped.store(true, Ordering::Release);
        self.progress.changed.notify_all();
        self.interrupt.interrupt();
        self.progress.pending.store(true, Ordering::Release);
        let _ = self.wake.try_send(());
        self.spawn_worker();
        let _queries = self.queries.write();
        let mut running = self.progress.running.lock();
        while *running {
            self.progress.changed.wait(&mut running);
        }
        drop(running);
        self.worker.lock().take();
        self.root.lock().take();
    }

    /// What the mirror currently holds, or `None` when it must not be read: no mirror yet, a failed sync, or
    /// shutting down. Reads only cached state — it never opens DuckDB, never installs, never waits.
    pub fn probe(&self) -> Option<Arc<MirrorSnapshot>> {
        if self.progress.stopped.load(Ordering::Acquire) {
            return None;
        }
        if self.progress.status.lock().error.is_some() {
            return None;
        }
        self.snapshot.read().clone()
    }

    /// Refreshes the cached view after a batch committed. Called with the mirror at rest.
    fn refresh_snapshot(&self) -> Result<()> {
        let guard = self.root.lock();
        let Some(conn) = guard.as_ref() else {
            return Ok(());
        };
        let watermark = watermark(conn)?;
        let mut tables = BTreeMap::new();
        for (name, entry) in read_catalog(conn)? {
            if entry.fingerprint.is_none() {
                continue;
            }
            tables.insert(
                name,
                TableState {
                    watermark: entry.watermark.unwrap_or(watermark),
                    purity: entry.purity.unwrap_or_default().split(',').map(str::to_owned).collect(),
                },
            );
        }
        *self.snapshot.write() = Some(Arc::new(MirrorSnapshot { watermark, tables }));
        Ok(())
    }

    fn spawn_worker(self: &Arc<Self>) {
        let shared = self.progress.clone();
        if shared.stopped.load(Ordering::Acquire) || shared.scheduled.swap(true, Ordering::AcqRel) {
            return;
        }
        shared.pending.store(false, Ordering::Release);
        let Some(mut state) = self.worker.lock().take() else {
            shared.scheduled.store(false, Ordering::Release);
            return;
        };
        let htap = Arc::downgrade(self);
        let path = self.path.clone();
        let cfg = self.cfg.clone();
        let gate = self.gate.clone();
        let metrics = self.metrics.clone();
        crate::pool::spawn(move || {
            *shared.running.lock() = true;
            let mut run = || -> Result<()> {
                let mut source = Conn::open_reader(&path, &cfg)?;
                let cancelled = shared.clone();
                source
                    .c
                    .progress_handler(1000, Some(move || cancelled.stopped.load(Ordering::Acquire)))?;
                loop {
                    if shared.stopped.load(Ordering::Acquire) {
                        break;
                    }
                    // The sync's read transaction passes the read gate like any other read: a checkpoint that has to
                    // restart the WAL waits for it instead of failing behind it, and the WAL does not grow without
                    // bound while the capture reads (README §3, checkpoint starvation).
                    let result = match gate.enter(Instant::now() + cfg.request_timeout()) {
                        Some(_pass) => {
                            let started = Instant::now();
                            let applied = apply_pending(
                                &source.c,
                                &mut state.mirror,
                                &metrics,
                                Instant::now() + cfg.request_timeout(),
                            );
                            metrics.htap_apply.record(started.elapsed());
                            applied
                        }
                        None => {
                            // a checkpoint holds the gate: leave this batch for the next wake
                            shared.changed.notify_all();
                            if shared.stopped.load(Ordering::Acquire) {
                                break;
                            }
                            match state.receive.recv_timeout(cfg.htap_worker_idle()) {
                                Ok(()) => {
                                    state.idle_since = None;
                                    continue;
                                }
                                Err(_) => break,
                            }
                        }
                    };
                    let mut status = shared.status.lock();
                    match result {
                        Ok(wm) => {
                            if wm > status.watermark {
                                state.writer.prune_htap(wm, Instant::now() + cfg.request_timeout());
                            }
                            status.watermark = wm;
                            status.error = None;
                            metrics.htap_watermark.store(wm, Ordering::Relaxed);
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "HTAP synchronization failed");
                            status.error = Some(e.to_string());
                            metrics.htap_sync_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    shared.changed.notify_all();
                    drop(status);
                    if let Some(htap) = htap.upgrade()
                        && let Err(e) = htap.refresh_snapshot()
                    {
                        tracing::warn!(error = %e, "could not refresh the mirror view");
                    }
                    if shared.stopped.load(Ordering::Acquire) {
                        break;
                    }
                    if shared.pending.swap(false, Ordering::AcqRel) {
                        state.idle_since = None;
                        continue;
                    }
                    let idle_since = *state.idle_since.get_or_insert_with(Instant::now);
                    let remaining = cfg.htap_worker_idle().saturating_sub(idle_since.elapsed());
                    if remaining.is_zero() {
                        break;
                    }
                    match state.receive.recv_timeout(remaining) {
                        Ok(()) => {
                            state.idle_since = None;
                            continue;
                        }
                        Err(RecvTimeoutError::Timeout) => break,
                        Err(RecvTimeoutError::Disconnected) => break,
                    }
                }
                Ok(())
            };
            if let Err(e) = run() {
                shared.status.lock().error = Some(e.to_string());
            }
            *shared.running.lock() = false;
            shared.changed.notify_all();
            if let Some(htap) = htap.upgrade() {
                state.idle_since = None;
                *htap.worker.lock() = Some(state);
            }
            shared.scheduled.store(false, Ordering::Release);
            if !shared.stopped.load(Ordering::Acquire)
                && shared.pending.load(Ordering::Acquire)
                && let Some(htap) = htap.upgrade()
            {
                htap.spawn_worker();
            }
        });
    }
}

impl Drop for Htap {
    fn drop(&mut self) {
        self.progress.stopped.store(true, Ordering::Release);
        self.progress.changed.notify_all();
        self.interrupt.interrupt();
        let _ = self.wake.try_send(());
        // The worker holds only a Weak, so it does not keep this alive: it can still be inside a batch, using the
        // writer it was handed, while these fields go away. Wait for it as `stop` does.
        let mut running = self.progress.running.lock();
        while *running {
            self.progress.changed.wait(&mut running);
        }
    }
}

/// One table's mirror, replaced inside the caller's transaction together with its catalog row, so a reader never
/// sees new data under an old shape or a shape without its data
fn rebuild_table(
    source: &rusqlite::Connection,
    tx: &duckdb::Transaction<'_>,
    table: &TableSchema,
    fingerprint: &str,
    watermark: i64,
    deadline: Instant,
) -> Result<String> {
    let mut purity: Vec<BTreeSet<char>> = vec![BTreeSet::new(); table.columns.len()];
    drop_duck_object(tx, &table.mirror_name)?;
    drop_duck_object(tx, &table.physical_name)?;
    tx.execute_batch(&duck_table_ddl(table))?;
    {
        let mut append = tx.appender(&table.physical_name)?;
        let sql = format!(
            "SELECT {},{} FROM {} AS xsqld_source",
            key_json_expr(table, "xsqld_source"),
            payload_json_expr(table, "xsqld_source"),
            quote_identifier(&table.name)
        );
        let mut statement = source.prepare(&sql)?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            ensure!(Instant::now() < deadline, "HTAP initial snapshot timed out");
            let key: String = row.get(0)?;
            let payload: String = row.get(1)?;
            let payload: ChangePayload = serde_json::from_str(&payload)?;
            let mut values = Vec::with_capacity(table.columns.len() + 1);
            values.push(DuckValue::Text(key));
            values.extend(
                payload
                    .values
                    .into_iter()
                    .map(decode_cell)
                    .collect::<Result<Vec<_>>>()?,
            );
            note_classes(&mut purity, &values, 1);
            append.append_row(appender_params_from_iter(values.iter()))?;
        }
        append.flush()?;
    }
    tx.execute_batch(&duck_view_ddl(table))?;
    tx.execute(
        "INSERT OR REPLACE INTO xsqld_tables(source_name,mirror_name,physical_name,fingerprint,watermark,purity) VALUES(?,?,?,?,?,?)",
        duckdb::params![
            table.name,
            table.mirror_name,
            table.physical_name,
            fingerprint,
            watermark,
            purity_string(&purity)
        ],
    )?;
    Ok(purity_string(&purity))
}

/// The storage class a decoded cell belongs to. Non-UTF-8 text arrives here as a blob (see `decode_cell`), which
/// is what makes such a column mixed and therefore unfit for routing.
fn class_of(value: &DuckValue) -> char {
    match value {
        DuckValue::Null => 'n',
        DuckValue::BigInt(_) => 'i',
        DuckValue::Double(_) => 'r',
        DuckValue::Text(_) => 't',
        _ => 'b',
    }
}

fn note_classes(acc: &mut [BTreeSet<char>], values: &[DuckValue], skip: usize) {
    for (slot, value) in acc.iter_mut().zip(values.iter().skip(skip)) {
        slot.insert(class_of(value));
    }
}

fn purity_string(acc: &[BTreeSet<char>]) -> String {
    acc.iter()
        .map(|set| set.iter().collect::<String>())
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_purity(text: &str, columns: usize) -> Vec<BTreeSet<char>> {
    let mut out: Vec<BTreeSet<char>> = text.split(',').map(|part| part.chars().collect()).collect();
    out.resize(columns, BTreeSet::new());
    out
}

fn drop_mirror_table(tx: &duckdb::Transaction<'_>, entry: &CatalogEntry) -> Result<()> {
    drop_duck_object(tx, &entry.mirror_name)?;
    drop_duck_object(tx, &entry.physical_name)?;
    Ok(())
}

fn rebuild_all(
    source: &rusqlite::Connection,
    mirror: &mut Connection,
    id: &str,
    watermark: i64,
    fingerprint: &str,
    tables: &[TableSchema],
    deadline: Instant,
) -> Result<()> {
    let tx = mirror.transaction()?;
    for entry in read_catalog(&tx)?.values() {
        drop_mirror_table(&tx, entry)?;
    }
    drop_duck_object(&tx, "orders_native")?;
    tx.execute("DELETE FROM xsqld_tables", [])?;
    for table in tables {
        rebuild_table(source, &tx, table, &table_fingerprint(table), watermark, deadline)?;
    }
    tx.execute(
        "INSERT OR REPLACE INTO xsqld_meta(id,source_id,watermark,format,schema_fingerprint) VALUES(1,?,?,?,?)",
        duckdb::params![id, watermark, MIRROR_FORMAT, fingerprint],
    )?;
    tx.commit()?;
    Ok(())
}

fn apply_pending(
    source: &rusqlite::Connection,
    mirror: &mut Connection,
    metrics: &DbMetrics,
    deadline: Instant,
) -> Result<i64> {
    source.execute_batch("BEGIN")?;
    let result = apply_snapshot(source, mirror, metrics, deadline);
    source.execute_batch("ROLLBACK")?;
    result
}

fn apply_table_changes(
    tx: &duckdb::Transaction<'_>,
    table: &TableSchema,
    changes: &[ChangeRecord],
    purity: &mut [BTreeSet<char>],
) -> Result<u64> {
    if changes.is_empty() {
        return Ok(0);
    }
    let hash = format!("{:016x}", hash_name(&table.name.to_ascii_lowercase()));
    let stage = format!("__xsqld_stage_{hash}");
    let latest = format!("__xsqld_latest_{hash}");
    let seq_col = "__xsqld_seq";
    let key_col = "__xsqld_key";
    let deleted_col = "__xsqld_deleted";
    let value_cols = (0..table.columns.len())
        .map(|i| format!("__xsqld_v{i}"))
        .collect::<Vec<_>>();
    let mut stage_columns = vec![
        format!("{} BIGINT", quote_identifier(seq_col)),
        format!("{} VARCHAR", quote_identifier(key_col)),
        format!("{} BOOLEAN", quote_identifier(deleted_col)),
    ];
    stage_columns.extend(
        value_cols
            .iter()
            .map(|column| format!("{} VARIANT", quote_identifier(column))),
    );
    tx.execute_batch(&format!(
        "CREATE TEMP TABLE {}({})",
        quote_identifier(&stage),
        stage_columns.join(",")
    ))?;
    let carried = changes.len() as u64;
    {
        let mut append = tx.appender(&stage)?;
        for change in changes {
            let deleted = change.operation == "D";
            ensure!(
                deleted || change.operation == "I" || change.operation == "U",
                "invalid changelog operation"
            );
            let mut values = vec![
                DuckValue::BigInt(change.seq),
                DuckValue::Text(change.key.clone()),
                DuckValue::Boolean(deleted),
            ];
            if deleted {
                values.extend((0..table.columns.len()).map(|_| DuckValue::Null));
            } else {
                let payload: ChangePayload = serde_json::from_str(
                    change
                        .payload
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("missing changelog payload"))?,
                )?;
                ensure!(
                    payload.values.len() == table.columns.len(),
                    "changelog column count mismatch for `{}`",
                    table.name
                );
                values.extend(
                    payload
                        .values
                        .into_iter()
                        .map(decode_cell)
                        .collect::<Result<Vec<_>>>()?,
                );
                note_classes(purity, &values, 3);
            }
            append.append_row(appender_params_from_iter(values.iter()))?;
        }
        append.flush()?;
    }
    let insert_columns = std::iter::once(quote_identifier(&table.key_name))
        .chain(table.columns.iter().map(|c| quote_identifier(&c.name)))
        .collect::<Vec<_>>()
        .join(",");
    let select_columns = std::iter::once(format!("s.{}", quote_identifier(key_col)))
        .chain(value_cols.iter().map(|c| format!("s.{}", quote_identifier(c))))
        .collect::<Vec<_>>()
        .join(",");
    let updates = table
        .columns
        .iter()
        .map(|c| {
            let column = quote_identifier(&c.name);
            format!("{column}=excluded.{column}")
        })
        .collect::<Vec<_>>()
        .join(",");
    tx.execute_batch(&format!(
        "CREATE TEMP TABLE {latest_q} AS SELECT * FROM {stage_q} QUALIFY row_number() OVER(PARTITION BY {key_q} ORDER BY {seq_q} DESC)=1; \
         DELETE FROM {physical_q} AS p USING {latest_q} AS s WHERE p.{physical_key_q}=s.{latest_key_q} AND s.{deleted_q}; \
         INSERT INTO {physical_q}({insert_columns}) SELECT {select_columns} FROM {latest_q} AS s WHERE NOT s.{deleted_q} \
         ON CONFLICT({physical_key_q}) DO UPDATE SET {updates}; \
         DROP TABLE {latest_q}; DROP TABLE {stage_q};",
        latest_q=quote_identifier(&latest),
        stage_q=quote_identifier(&stage),
        key_q=quote_identifier(key_col),
        seq_q=quote_identifier(seq_col),
        physical_q=quote_identifier(&table.physical_name),
        physical_key_q=quote_identifier(&table.key_name),
        latest_key_q=quote_identifier(key_col),
        deleted_q=quote_identifier(deleted_col),
    ))?;
    Ok(carried)
}

/// What this round has to do. Deciding it in one place keeps `Htap::start` and the sync worker from drifting apart.
struct SyncPlan {
    /// The mirror as a whole cannot be trusted: another source, another format, or ahead of the source
    rebuild_all: bool,
    /// Indices into `tables` that need a fresh snapshot: new, reshaped, or behind the retained log
    snapshot: Vec<usize>,
    /// Indices into `tables` that only need the log, each with the position it already holds
    incremental: Vec<(usize, i64)>,
    /// Mirrors whose source table is gone (a drop, or the old name of a rename)
    drop: Vec<CatalogEntry>,
    /// The oldest position among the tables that stay, so one read of the log covers them all
    read_from: i64,
    /// Per table (parallel to `tables`): the position it holds, or `target` for one being snapshotted
    held: Vec<i64>,
}

impl SyncPlan {
    fn is_noop(&self) -> bool {
        self.snapshot.is_empty() && self.incremental.is_empty() && self.drop.is_empty()
    }
}

fn plan_sync(
    catalog: &BTreeMap<String, CatalogEntry>,
    tables: &[TableSchema],
    mirror: &(String, i64, i64, String),
    source_id: &str,
    fingerprint: &str,
    target: i64,
    pruned: i64,
) -> SyncPlan {
    let (mirror_id, wm, format, mirror_fingerprint) = mirror;
    // A mirror written before per-table shape was recorded can only be taken as it stands if its recorded
    // schema still matches: that is the one proof that nothing changed shape since it was built.
    let legacy = catalog.values().any(|e| e.fingerprint.is_none());
    let mut plan = SyncPlan {
        rebuild_all: *format != MIRROR_FORMAT
            || mirror_id != source_id
            || *wm > target
            || (legacy && mirror_fingerprint != fingerprint),
        snapshot: Vec::new(),
        incremental: Vec::new(),
        drop: Vec::new(),
        read_from: target,
        held: Vec::new(),
    };
    if plan.rebuild_all {
        return plan;
    }
    let kept: HashSet<String> = tables.iter().map(|t| t.name.to_ascii_lowercase()).collect();
    for (index, table) in tables.iter().enumerate() {
        let key = table.name.to_ascii_lowercase();
        let shape = table_fingerprint(table);
        let entry = catalog.get(&key);
        // A legacy row stands for the current shape at the mirror's own position (the gate above proved it)
        let held = match entry {
            Some(e) if e.fingerprint.is_some() => e.watermark.unwrap_or(*wm),
            Some(_) => *wm,
            None => {
                plan.snapshot.push(index);
                plan.held.push(target);
                continue;
            }
        };
        let same_shape = entry.is_some_and(|e| e.fingerprint.as_deref().is_none_or(|f| f == shape));
        if !same_shape || held < pruned {
            plan.snapshot.push(index);
            plan.held.push(target);
        } else {
            if held < target {
                plan.incremental.push((index, held));
                plan.read_from = plan.read_from.min(held);
            }
            plan.held.push(held);
        }
    }
    for (key, entry) in catalog {
        if !kept.contains(key) {
            plan.drop.push(entry.clone());
        }
    }
    plan
}

fn apply_snapshot(
    source: &rusqlite::Connection,
    mirror: &mut Connection,
    metrics: &DbMetrics,
    deadline: Instant,
) -> Result<i64> {
    let (source_id, target, pruned, source_fingerprint) = position(source)?;
    metrics.htap_source_position.store(target, Ordering::Relaxed);
    let tables = table_schemas(source)?;
    let fingerprint = schema_fingerprint(&tables);
    ensure!(
        source_fingerprint == fingerprint,
        "SQLite capture schema is out of date"
    );
    let state = mirror_state(mirror)?;
    let catalog = read_catalog(mirror)?;
    let plan = plan_sync(&catalog, &tables, &state, &source_id, &fingerprint, target, pruned);
    if plan.rebuild_all {
        rebuild_all(source, mirror, &source_id, target, &fingerprint, &tables, deadline)?;
        metrics.htap_rebuilds.fetch_add(tables.len() as u64, Ordering::Relaxed);
        return Ok(target);
    }
    // Planning comes first: a table created while the mirror was already caught up still needs its snapshot.
    // The recorded position is derived from the tables, so it is also written when it disagrees with them.
    let expected = plan.held.iter().copied().min().unwrap_or(target);
    if plan.is_noop() && state.1 == expected {
        return Ok(state.1);
    }
    let mut by_table: BTreeMap<String, Vec<ChangeRecord>> = BTreeMap::new();
    if !plan.incremental.is_empty() {
        let mut statement =
            source.prepare("SELECT seq,tbl,op,pk,payload FROM xsqld_changelog WHERE seq>? AND seq<=? ORDER BY seq")?;
        let mut rows = statement.query(rusqlite::params![plan.read_from, target])?;
        while let Some(row) = rows.next()? {
            let record = ChangeRecord {
                seq: row.get(0)?,
                table: row.get(1)?,
                operation: row.get(2)?,
                key: row.get(3)?,
                payload: row.get(4)?,
            };
            by_table
                .entry(record.table.to_ascii_lowercase())
                .or_default()
                .push(record);
        }
    }
    let tx = mirror.transaction()?;
    for entry in &plan.drop {
        drop_mirror_table(&tx, entry)?;
    }
    for entry in &plan.drop {
        tx.execute("DELETE FROM xsqld_tables WHERE source_name=?", [&entry.source_name])?;
    }
    let mut applied = 0u64;
    let mut purity: BTreeMap<String, String> = BTreeMap::new();
    for index in &plan.snapshot {
        let table = &tables[*index];
        let seen = rebuild_table(source, &tx, table, &table_fingerprint(table), target, deadline)?;
        purity.insert(table.name.to_ascii_lowercase(), seen);
    }
    for (index, held) in &plan.incremental {
        let table = &tables[*index];
        let key = table.name.to_ascii_lowercase();
        let mut classes = parse_purity(
            catalog.get(&key).and_then(|e| e.purity.as_deref()).unwrap_or(""),
            table.columns.len(),
        );
        let changes = by_table
            .remove(&key)
            .unwrap_or_default()
            .into_iter()
            .filter(|c| c.seq > *held)
            .collect::<Vec<_>>();
        applied += apply_table_changes(&tx, table, &changes, &mut classes)?;
        purity.insert(key, purity_string(&classes));
    }
    // Each table advances to `target` without ever going backwards; the mirror promises only the oldest of them
    let mut final_wm = target;
    for (table, held) in tables.iter().zip(&plan.held) {
        let reached = (*held).max(target);
        let key = table.name.to_ascii_lowercase();
        let seen = purity.get(&key).cloned().or_else(|| {
            catalog
                .get(&key)
                .and_then(|e| e.purity.clone())
                .or_else(|| Some(purity_string(&vec![BTreeSet::new(); table.columns.len()])))
        });
        tx.execute(
            "UPDATE xsqld_tables SET fingerprint=?,watermark=?,purity=? WHERE source_name=?",
            duckdb::params![table_fingerprint(table), reached, seen, table.name],
        )?;
        final_wm = final_wm.min(reached);
    }
    tx.execute("UPDATE xsqld_meta SET watermark=? WHERE id=1", [final_wm])?;
    tx.execute("UPDATE xsqld_meta SET schema_fingerprint=? WHERE id=1", [&fingerprint])?;
    tx.commit()?;
    metrics.htap_batches.fetch_add(1, Ordering::Relaxed);
    metrics.htap_rows_applied.fetch_add(applied, Ordering::Relaxed);
    metrics
        .htap_rebuilds
        .fetch_add(plan.snapshot.len() as u64, Ordering::Relaxed);
    Ok(final_wm)
}

/// The tags a routed read may see: NULL, the signed integer widths, and text. Everything else (a widened SUM,
/// a decimal, a float, a blob) means the two engines would not answer alike.
fn admitted_variant_tag(tagged: &str) -> bool {
    matches!(
        tagged.split_once(':').map(|(kind, _)| kind),
        Some("VARIANT_NULL" | "INT8" | "INT16" | "INT32" | "INT64" | "VARCHAR")
    )
}

/// Whether a value is one SQLite could not represent: it raises an overflow error where DuckDB widens the type.
fn widens_beyond_i64(value: &DuckValue) -> bool {
    match value {
        DuckValue::HugeInt(v) => i64::try_from(*v).is_err(),
        DuckValue::UHugeInt(v) => i64::try_from(*v).is_err(),
        DuckValue::UBigInt(v) => i64::try_from(*v).is_err(),
        DuckValue::Decimal(_) => true,
        _ => false,
    }
}

fn protocol_value(value: DuckValue) -> Result<Value> {
    Ok(match value {
        DuckValue::Null => Value::Null,
        DuckValue::Boolean(v) => Value::Integer(i64::from(v)),
        DuckValue::TinyInt(v) => Value::Integer(v.into()),
        DuckValue::SmallInt(v) => Value::Integer(v.into()),
        DuckValue::Int(v) => Value::Integer(v.into()),
        DuckValue::BigInt(v) => Value::Integer(v),
        DuckValue::UTinyInt(v) => Value::Integer(v.into()),
        DuckValue::USmallInt(v) => Value::Integer(v.into()),
        DuckValue::UInt(v) => Value::Integer(v.into()),
        DuckValue::UBigInt(v) => i64::try_from(v)
            .map(Value::Integer)
            .unwrap_or_else(|_| Value::Text(v.to_string())),
        DuckValue::HugeInt(v) => i64::try_from(v)
            .map(Value::Integer)
            .unwrap_or_else(|_| Value::Text(v.to_string())),
        DuckValue::UHugeInt(v) => i64::try_from(v)
            .map(Value::Integer)
            .unwrap_or_else(|_| Value::Text(v.to_string())),
        DuckValue::Float(v) => Value::Float(v.into()),
        DuckValue::Double(v) => Value::Float(v),
        DuckValue::Decimal(v) => Value::Text(v.to_string()),
        DuckValue::Text(v) | DuckValue::Enum(v) => Value::Text(v),
        DuckValue::Blob(v) => Value::Blob(v),
        other => anyhow::bail!("unsupported analytics result type {other:?}; cast it to VARCHAR"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Db, Registry};
    use crate::proto::{StreamRequest, StreamResult};
    use crate::writer::{Job, Outcome};

    fn test_deadline() -> Instant {
        Instant::now() + std::time::Duration::from_secs(60)
    }

    async fn write(db: &Db, sql: &[&str]) -> Result<()> {
        let requests = sql
            .iter()
            .map(|sql| {
                serde_json::from_value::<StreamRequest>(serde_json::json!({"type":"execute","stmt":{"sql":sql}}))
                    .unwrap()
            })
            .collect();
        let (job, response) = Job::new(
            requests,
            123,
            false,
            Instant::now() + std::time::Duration::from_secs(10),
        );
        db.writer.submit(job);
        let Outcome::Done { results, .. } = response.await? else {
            anyhow::bail!("writer unavailable");
        };
        for result in results {
            if let StreamResult::Error { error } = result {
                anyhow::bail!("{}", error.message);
            }
        }
        Ok(())
    }

    async fn fixture() -> (tempfile::TempDir, Arc<Registry>, Arc<Db>) {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry::new(Arc::new(Config::for_tests(dir.path())));
        registry.create("tenant", None).await.unwrap();
        let db = registry.get("tenant").await.unwrap().unwrap();
        write(&db, &["CREATE TABLE orders(id INTEGER PRIMARY KEY,user_id INTEGER NOT NULL,amount_cents INTEGER NOT NULL,status INTEGER NOT NULL,channel INTEGER NOT NULL,created_at INTEGER NOT NULL)",
            "INSERT INTO orders VALUES(1,10,100,0,1,1000),(2,20,200,1,2,2000)"]).await.unwrap();
        (dir, registry, db)
    }

    async fn query(db: &Arc<Db>, registry: &Arc<Registry>, sql: &str) -> Result<Response> {
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        let htap = db.analytics(registry.cfg.clone(), deadline).await?;
        let request = Request {
            sql: sql.to_owned(),
            args: vec![],
        };
        tokio::task::spawn_blocking(move || htap.query(request, deadline)).await?
    }

    async fn shutdown(registry: Arc<Registry>) {
        tokio::task::spawn_blocking(move || registry.shutdown()).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mirror_tracks_all_fields_primary_key_changes_rollback_and_restart() {
        let (_dir, registry, db) = fixture().await;
        let first = query(&db, &registry, "SELECT * FROM orders_native ORDER BY id")
            .await
            .unwrap();
        assert_eq!(first.result.rows.len(), 2);
        assert_eq!(
            first.watermark, 1,
            "initial schema generation is recorded in the outbox"
        );
        write(
            &db,
            &[
                "BEGIN",
                "UPDATE orders SET id=3,user_id=99,amount_cents=101,status=4,channel=8,created_at=5555 WHERE id=1",
                "DELETE FROM orders WHERE id=2",
                "INSERT INTO orders VALUES(4,40,400,2,4,4000)",
                "COMMIT",
            ],
        )
        .await
        .unwrap();
        let result = query(&db, &registry, "SELECT * FROM orders_native ORDER BY id")
            .await
            .unwrap();
        let ints = |values: &[i64]| values.iter().copied().map(Value::Integer).collect::<Vec<_>>();
        assert_eq!(
            result.result.rows,
            vec![ints(&[3, 99, 101, 4, 8, 5555]), ints(&[4, 40, 400, 2, 4, 4000])]
        );
        assert_eq!(result.watermark, 5);
        write(
            &db,
            &["BEGIN", "UPDATE orders SET user_id=0,amount_cents=0", "ROLLBACK"],
        )
        .await
        .unwrap();
        let after = query(&db, &registry, "SELECT * FROM orders_native ORDER BY id")
            .await
            .unwrap();
        assert_eq!(after.result.rows, result.result.rows);
        assert_eq!(after.watermark, result.watermark);
        let cfg = registry.cfg.clone();
        shutdown(registry).await;
        let registry = Registry::new(cfg);
        let reopened = registry.get("tenant").await.unwrap().unwrap();
        write(&reopened, &["UPDATE orders SET user_id=777 WHERE id=4"])
            .await
            .unwrap();
        let result = query(&reopened, &registry, "SELECT user_id FROM orders_native WHERE id=4")
            .await
            .unwrap();
        assert_eq!(result.result.rows, vec![ints(&[777])]);
        assert_eq!(result.watermark, 6);
        shutdown(registry).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn analysis_is_read_only_bounded_and_can_run_concurrently() {
        let (_dir, registry, db) = fixture().await;
        query(&db, &registry, "SELECT count(*) FROM orders_native")
            .await
            .unwrap();
        for sql in [
            "DELETE FROM orders_native",
            "SELECT 1; ROLLBACK; DELETE FROM orders_native",
            "COPY orders_native TO '/tmp/xsqld-forbidden'",
            "SET enable_external_access=true",
            "SELECT * FROM read_text('/etc/passwd')",
            "SELECT * FROM query('DELETE FROM orders_native')",
        ] {
            assert!(query(&db, &registry, sql).await.is_err(), "accepted {sql}");
        }
        let mut requests = Vec::new();
        for _ in 0..8 {
            let (db, registry) = (db.clone(), registry.clone());
            requests.push(tokio::spawn(async move {
                query(
                    &db,
                    &registry,
                    "WITH x AS (SELECT * FROM orders_native) SELECT sum(amount_cents) FROM x",
                )
                .await
                .unwrap()
            }));
        }
        for request in requests {
            assert_eq!(request.await.unwrap().result.rows, vec![vec![Value::Integer(300)]]);
        }
        let capture = format!("{}{:016x}_u", CHANGE_TRIGGER_PREFIX, hash_name("orders"));
        assert!(write(&db, &[&format!("DROP TRIGGER {capture}")]).await.is_err());
        assert!(write(&db, &["DELETE FROM xsqld_changelog"]).await.is_err());
        assert!(write(&db, &["DROP TABLE xsqld_htap_meta"]).await.is_err());
        shutdown(registry).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_and_watermark_share_one_source_transaction() {
        let (_dir, registry, db) = fixture().await;
        db.writer
            .install_htap(Instant::now() + std::time::Duration::from_secs(10))
            .await
            .unwrap();
        let cfg = registry.cfg.clone();
        let path = cfg.db_file("tenant");
        tokio::task::spawn_blocking(move || {
            let source = Conn::open_reader(&path, &cfg).unwrap();
            source.c.execute_batch("BEGIN").unwrap();
            let (id, wm, _, fingerprint) = position(&source.c).unwrap();
            let tables = table_schemas(&source.c).unwrap();
            let concurrent = rusqlite::Connection::open(&path).unwrap();
            concurrent
                .execute("INSERT INTO orders VALUES(3,30,300,3,3,3000)", [])
                .unwrap();
            let mut mirror = Connection::open_in_memory().unwrap();
            ensure_duck_schema(&mirror).unwrap();
            rebuild_all(
                &source.c,
                &mut mirror,
                &id,
                wm,
                &fingerprint,
                &tables,
                Instant::now() + std::time::Duration::from_secs(10),
            )
            .unwrap();
            assert_eq!(watermark(&mirror).unwrap(), 1);
            assert_eq!(
                mirror
                    .query_row("SELECT count(*) FROM orders_native", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                2
            );
            source.c.execute_batch("COMMIT").unwrap();
            assert_eq!(
                apply_pending(&source.c, &mut mirror, &DbMetrics::default(), test_deadline()).unwrap(),
                2
            );
            assert_eq!(
                mirror
                    .query_row("SELECT count(*) FROM orders_native", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                3
            );
        })
        .await
        .unwrap();
        shutdown(registry).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deletion_closes_mirror_and_recreation_starts_fresh() {
        let (_dir, registry, db) = fixture().await;
        query(&db, &registry, "SELECT * FROM orders_native").await.unwrap();
        let path = mirror_path(&registry.cfg.db_file("tenant"));
        assert!(path.exists());
        registry.delete("tenant").await.unwrap();
        assert!(!path.exists());
        assert!(
            db.analytics(registry.cfg.clone(), Instant::now() + std::time::Duration::from_secs(1))
                .await
                .is_err()
        );
        registry.create("tenant", None).await.unwrap();
        let fresh = registry.get("tenant").await.unwrap().unwrap();
        write(&fresh, &["CREATE TABLE orders(id INTEGER PRIMARY KEY,user_id INTEGER,amount_cents INTEGER,status INTEGER,channel INTEGER,created_at INTEGER)", "INSERT INTO orders VALUES(9,9,9,9,9,9)"]).await.unwrap();
        assert_eq!(
            query(&fresh, &registry, "SELECT id FROM orders_native")
                .await
                .unwrap()
                .result
                .rows,
            vec![vec![Value::Integer(9)]]
        );
        shutdown(registry).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn queries_bind_values_enforce_size_and_interrupt_only_their_connection() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::for_tests(dir.path());
        cfg.max_response_bytes = 1024;
        let registry = Registry::new(Arc::new(cfg));
        registry.create("tenant", None).await.unwrap();
        let db = registry.get("tenant").await.unwrap().unwrap();
        write(&db, &["CREATE TABLE orders(id INTEGER PRIMARY KEY,user_id INTEGER,amount_cents INTEGER,status INTEGER,channel INTEGER,created_at INTEGER)", "INSERT INTO orders VALUES(1,10,100,0,1,1000)"]).await.unwrap();
        let htap = db
            .analytics(
                registry.cfg.clone(),
                Instant::now() + std::time::Duration::from_secs(10),
            )
            .await
            .unwrap();
        let active = htap.clone();
        let result = tokio::task::spawn_blocking(move || {
            active.query(
                Request {
                    sql: "SELECT id FROM orders_native WHERE user_id=?".into(),
                    args: vec![Value::Integer(10)],
                },
                Instant::now() + std::time::Duration::from_secs(5),
            )
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.result.rows, vec![vec![Value::Integer(1)]]);
        assert!(
            query(&db, &registry, "SELECT repeat('x',4096)")
                .await
                .unwrap_err()
                .to_string()
                .contains("RESPONSE_TOO_LARGE")
        );
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            htap.query(
                Request {
                    sql: "SELECT sum(sin(i)) FROM range(1000000000) t(i)".into(),
                    args: vec![],
                },
                Instant::now() + std::time::Duration::from_millis(50),
            )
        })
        .await
        .unwrap();
        assert!(result.is_err());
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
        assert_eq!(
            query(&db, &registry, "SELECT count(*) FROM orders_native")
                .await
                .unwrap()
                .result
                .rows,
            vec![vec![Value::Integer(1)]]
        );
        shutdown(registry).await;
    }

    /// A routed read runs outside the read gate, so a checkpoint that has closed the gate and is draining readers
    /// is not held up by a long analytical query. With the gate closed, an ordinary read cannot even start, while
    /// the mirror read still completes.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_routed_read_does_not_hold_the_read_gate() {
        let (_dir, registry, db) = fixture().await;
        query(&db, &registry, "SELECT COUNT(*) AS n FROM orders_native")
            .await
            .unwrap();
        let htap = db.mirror().expect("mirror is up");
        let closed = db.readers.gate.close();
        // an ordinary read waits for the gate and cannot finish while it is closed
        let blocked = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            db.readers.run(
                vec![StreamRequest::Execute {
                    stmt: serde_json::from_value(serde_json::json!({"sql":"SELECT COUNT(*) AS n FROM orders"}))
                        .unwrap(),
                }],
                Instant::now() + std::time::Duration::from_secs(5),
            ),
        )
        .await;
        assert!(blocked.is_err(), "an ordinary read started behind a closed gate");
        // the mirror read does not touch the gate at all
        let result = htap
            .run_routed(
                "SELECT COUNT(*) AS n FROM orders_native",
                vec![],
                Instant::now() + std::time::Duration::from_secs(5),
            )
            .expect("the mirror read waited on the read gate");
        assert_eq!(result.rows, vec![vec![Value::Integer(2)]]);
        assert!(
            closed.drain(std::time::Duration::from_millis(200)),
            "the gate could not drain"
        );
        drop(closed);
        shutdown(registry).await;
    }

    /// A routed read must never miss a committed write. The router checks the mirror's position against the
    /// source live on every request, so a write the sync has not applied yet keeps the query on SQLite.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_routed_read_never_returns_a_stale_row() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::for_tests(dir.path());
        cfg.route_enabled = true;
        cfg.route_min_rows = 1;
        let registry = Registry::new(Arc::new(cfg));
        registry.create("tenant", None).await.unwrap();
        let db = registry.get("tenant").await.unwrap().unwrap();
        write(
            &db,
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES(1,10),(2,20)",
            ],
        )
        .await
        .unwrap();
        // build the mirror, then make the read pool aware of it
        query(&db, &registry, "SELECT COUNT(*) AS n FROM t_native")
            .await
            .unwrap();
        // COUNT(*) over a full scan: a shape the router admits. (MIN/MAX would not do — SQLite answers it with a
        // `SEARCH` plan, which the benefit gate correctly keeps on the row store.)
        let sql = "SELECT COUNT(*) AS n FROM t";
        for round in 0..6 {
            let want = 3 + round;
            write(&db, &[&format!("INSERT INTO t VALUES({},10)", 10 + round)])
                .await
                .unwrap();
            // first read straight after the commit: the mirror may still be behind, and the router must then
            // keep the query on SQLite rather than answer from a stale mirror
            for pass in 0..2 {
                if pass == 1 {
                    // let the sync catch up, so this pass is answered by the mirror
                    query(&db, &registry, "SELECT COUNT(*) AS n FROM t_native")
                        .await
                        .unwrap();
                }
                let requests = vec![StreamRequest::Execute {
                    stmt: serde_json::from_value(serde_json::json!({"sql": sql})).unwrap(),
                }];
                let outcome = db
                    .readers
                    .run(requests, Instant::now() + std::time::Duration::from_secs(10))
                    .await
                    .unwrap();
                let crate::reader::ReadOutcome::Done { results, .. } = outcome else {
                    panic!("read did not complete");
                };
                let StreamResult::Ok {
                    response: crate::proto::StreamResponse::Execute { result },
                } = &results[0]
                else {
                    panic!("unexpected result: {:?}", results[0]);
                };
                assert_eq!(
                    result.rows,
                    vec![vec![Value::Integer(want)]],
                    "round {round} pass {pass} read a stale count"
                );
            }
        }
        assert!(
            db.metrics.route_runs.load(Ordering::Relaxed) > 0,
            "routing never fired, so this test proved nothing"
        );
        shutdown(registry).await;
    }

    /// The router may only read columns whose storage class is uniform; this is what tells it so. Non-UTF-8 text
    /// decodes to a blob, which makes such a column mixed without needing a separate flag.
    #[test]
    fn purity_records_the_storage_classes_each_column_held() {
        let source = rusqlite::Connection::open_in_memory().unwrap();
        source
            .execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, pure INTEGER, mixy, words TEXT); \
                 INSERT INTO t VALUES(1,10,20,'a'),(2,NULL,'text','b')",
            )
            .unwrap();
        install_capture(&source).unwrap();
        let mut mirror = Connection::open_in_memory().unwrap();
        ensure_duck_schema(&mirror).unwrap();
        let metrics = DbMetrics::default();
        apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        let catalog = read_catalog(&mirror).unwrap();
        let purity = catalog["t"].purity.clone().unwrap();
        let cols: Vec<&str> = purity.split(',').collect();
        assert_eq!(cols[0], "i", "id");
        assert_eq!(cols[1], "in", "pure integer with a null");
        assert_eq!(cols[2], "it", "mixed column");
        assert_eq!(cols[3], "t", "text");
        // a real value arriving later makes the pure column mixed, and it never goes back
        source.execute_batch("INSERT INTO t VALUES(3,1.5,1,'c')").unwrap();
        apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        let after = read_catalog(&mirror).unwrap()["t"].purity.clone().unwrap();
        assert_eq!(after.split(',').nth(1).unwrap(), "inr");
        source.execute_batch("DELETE FROM t WHERE id=3").unwrap();
        apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        let kept = read_catalog(&mirror).unwrap()["t"].purity.clone().unwrap();
        assert_eq!(
            kept.split(',').nth(1).unwrap(),
            "inr",
            "purity must not shrink on delete"
        );
    }

    /// The point of per-table planning: a schema change rebuilds the table it touched and leaves the others'
    /// data alone. A sentinel row that only exists in the mirror proves a table was not re-snapshotted.
    #[test]
    fn ddl_rebuilds_only_the_changed_table() {
        let source = rusqlite::Connection::open_in_memory().unwrap();
        source
            .execute_batch(
                "CREATE TABLE big(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO big VALUES(1,10),(2,20),(3,30); \
                 CREATE TABLE small(id INTEGER PRIMARY KEY, note TEXT); INSERT INTO small VALUES(1,'x')",
            )
            .unwrap();
        install_capture(&source).unwrap();
        let mut mirror = Connection::open_in_memory().unwrap();
        ensure_duck_schema(&mirror).unwrap();
        let metrics = DbMetrics::default();
        apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        let sentinel = |mirror: &Connection, table: &str| {
            let physical: String = mirror
                .query_row(
                    "SELECT physical_name FROM xsqld_tables WHERE source_name=?",
                    [table],
                    |r| r.get(0),
                )
                .unwrap();
            mirror
                .execute_batch(&format!(
                    "INSERT INTO {}(\"__xsqld_key\") VALUES('[999]')",
                    quote_identifier(&physical)
                ))
                .unwrap();
        };
        let holds = |mirror: &Connection, table: &str| -> bool {
            let physical: String = mirror
                .query_row(
                    "SELECT physical_name FROM xsqld_tables WHERE source_name=?",
                    [table],
                    |r| r.get(0),
                )
                .unwrap();
            mirror
                .query_row(
                    &format!(
                        "SELECT count(*) FROM {} WHERE {}='[999]'",
                        quote_identifier(&physical),
                        "__xsqld_key"
                    ),
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap()
                == 1
        };
        sentinel(&mirror, "big");
        sentinel(&mirror, "small");
        let before = metrics.htap_rebuilds.load(Ordering::Relaxed);

        // a new table arrives: only it is snapshotted
        source
            .execute_batch("CREATE TABLE extra(id INTEGER PRIMARY KEY, z INTEGER); INSERT INTO extra VALUES(1,5)")
            .unwrap();
        refresh_capture_in_transaction(&source).unwrap();
        apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        assert_eq!(
            metrics.htap_rebuilds.load(Ordering::Relaxed) - before,
            1,
            "only the new table"
        );
        assert!(holds(&mirror, "big"), "big was re-snapshotted");
        assert!(holds(&mirror, "small"), "small was re-snapshotted");
        assert_eq!(
            mirror
                .query_row("SELECT CAST(z AS BIGINT) FROM extra_native", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            5
        );

        // reshaping one table rebuilds that one only
        let before = metrics.htap_rebuilds.load(Ordering::Relaxed);
        sentinel(&mirror, "extra");
        source.execute_batch("ALTER TABLE big ADD COLUMN more TEXT").unwrap();
        refresh_capture_in_transaction(&source).unwrap();
        apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        assert_eq!(metrics.htap_rebuilds.load(Ordering::Relaxed) - before, 1, "only big");
        assert!(!holds(&mirror, "big"), "big should have been rebuilt");
        assert!(holds(&mirror, "small"), "small was re-snapshotted");
        assert!(holds(&mirror, "extra"), "extra was re-snapshotted");

        // a primary key change is carried by the CREATE text, so it is noticed too
        let before = metrics.htap_rebuilds.load(Ordering::Relaxed);
        source
            .execute_batch("CREATE TABLE keyed(a INTEGER, b INTEGER, PRIMARY KEY(a,b)) WITHOUT ROWID; INSERT INTO keyed VALUES(1,2)")
            .unwrap();
        refresh_capture_in_transaction(&source).unwrap();
        apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        source
            .execute_batch("DROP TABLE keyed; CREATE TABLE keyed(a INTEGER, b INTEGER, PRIMARY KEY(b,a)) WITHOUT ROWID; INSERT INTO keyed VALUES(1,2)")
            .unwrap();
        refresh_capture_in_transaction(&source).unwrap();
        apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        assert!(
            metrics.htap_rebuilds.load(Ordering::Relaxed) - before >= 2,
            "keyed rebuilt for its new key"
        );
        assert!(holds(&mirror, "small"), "small survives a key change elsewhere");

        // dropping a table removes its mirror and its catalog row
        source.execute_batch("DROP TABLE extra").unwrap();
        refresh_capture_in_transaction(&source).unwrap();
        apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        assert!(!read_catalog(&mirror).unwrap().contains_key("extra"));
        assert!(duck_object_type(&mirror, "extra_native").unwrap().is_none());
        assert!(holds(&mirror, "small"));
    }

    /// A mirror written before per-table shape was recorded keeps its data: the columns are added and the rows
    /// adopted, with no rebuild, as long as the recorded schema still matches
    #[test]
    fn a_mirror_without_per_table_columns_is_adopted() {
        let source = rusqlite::Connection::open_in_memory().unwrap();
        source
            .execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO t VALUES(1,10),(2,20)")
            .unwrap();
        install_capture(&source).unwrap();
        let mut mirror = Connection::open_in_memory().unwrap();
        ensure_duck_schema(&mirror).unwrap();
        let metrics = DbMetrics::default();
        apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        // take the mirror back to the older catalog shape
        mirror
            .execute_batch(
                "ALTER TABLE xsqld_tables DROP COLUMN fingerprint; ALTER TABLE xsqld_tables DROP COLUMN watermark",
            )
            .unwrap();
        ensure_duck_schema(&mirror).unwrap();
        assert!(read_catalog(&mirror).unwrap()["t"].fingerprint.is_none());
        let before = metrics.htap_rebuilds.load(Ordering::Relaxed);
        source.execute_batch("INSERT INTO t VALUES(3,30)").unwrap();
        let wm = apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        assert_eq!(
            metrics.htap_rebuilds.load(Ordering::Relaxed),
            before,
            "adopted without a rebuild"
        );
        assert_eq!(
            mirror
                .query_row("SELECT count(*) FROM t_native", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            3
        );
        assert_eq!(read_catalog(&mirror).unwrap()["t"].watermark, Some(wm));
    }

    /// One table behind the others must still get its own log rows: reading from the mirror's global position
    /// would skip them, and pruning past them would lose them for good
    #[test]
    fn a_lagging_table_gets_its_own_log_rows() {
        let source = rusqlite::Connection::open_in_memory().unwrap();
        source
            .execute_batch(
                "CREATE TABLE a(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO a VALUES(1,10); \
                 CREATE TABLE b(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO b VALUES(1,10)",
            )
            .unwrap();
        install_capture(&source).unwrap();
        let mut mirror = Connection::open_in_memory().unwrap();
        ensure_duck_schema(&mirror).unwrap();
        let metrics = DbMetrics::default();
        let start = apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        source.execute_batch("UPDATE a SET v=99 WHERE id=1").unwrap();
        let target = apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        assert!(target > start);
        // put `a` back behind the retained log while `b` stays current
        mirror
            .execute("UPDATE xsqld_tables SET watermark=? WHERE source_name='a'", [start])
            .unwrap();
        mirror
            .execute("UPDATE xsqld_meta SET watermark=? WHERE id=1", [target])
            .unwrap();
        source.execute_batch("UPDATE b SET v=77 WHERE id=1").unwrap();
        let wm = apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        assert_eq!(
            mirror
                .query_row(
                    "SELECT CAST(v AS BIGINT) FROM a_native WHERE CAST(id AS BIGINT)=1",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            99,
            "a's own rows were skipped"
        );
        assert_eq!(
            mirror
                .query_row(
                    "SELECT CAST(v AS BIGINT) FROM b_native WHERE CAST(id AS BIGINT)=1",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            77
        );
        assert_eq!(read_catalog(&mirror).unwrap()["a"].watermark, Some(wm));
    }

    /// Adopting an old mirror must not hand back data whose log is already gone: a position behind the retained
    /// log still forces a snapshot, even though the catalog row was adopted without one
    #[test]
    fn a_legacy_mirror_behind_the_retained_log_is_rebuilt() {
        let source = rusqlite::Connection::open_in_memory().unwrap();
        source
            .execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO t VALUES(1,10)")
            .unwrap();
        install_capture(&source).unwrap();
        let mut mirror = Connection::open_in_memory().unwrap();
        ensure_duck_schema(&mirror).unwrap();
        let metrics = DbMetrics::default();
        let held = apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        source.execute_batch("INSERT INTO t VALUES(2,20),(3,30)").unwrap();
        let target = position(&source).unwrap().1;
        // the log up to the new position has been pruned, and the mirror is back at the older shape and position
        source
            .execute("DELETE FROM xsqld_changelog WHERE seq<=?", [target])
            .unwrap();
        source
            .execute("UPDATE xsqld_htap_meta SET pruned_seq=? WHERE id=1", [target])
            .unwrap();
        mirror
            .execute_batch(
                "ALTER TABLE xsqld_tables DROP COLUMN fingerprint; ALTER TABLE xsqld_tables DROP COLUMN watermark",
            )
            .unwrap();
        ensure_duck_schema(&mirror).unwrap();
        mirror
            .execute("UPDATE xsqld_meta SET watermark=? WHERE id=1", [held])
            .unwrap();
        let before = metrics.htap_rebuilds.load(Ordering::Relaxed);
        apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        assert!(
            metrics.htap_rebuilds.load(Ordering::Relaxed) > before,
            "adopted stale data instead of rebuilding"
        );
        assert_eq!(
            mirror
                .query_row("SELECT count(*) FROM t_native", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            3
        );
    }

    /// A mirror built from another database (a dump restored under a new name) is not this source's mirror
    #[test]
    fn a_mirror_from_another_source_is_rebuilt_whole() {
        let make = || {
            let source = rusqlite::Connection::open_in_memory().unwrap();
            source
                .execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO t VALUES(1,10)")
                .unwrap();
            install_capture(&source).unwrap();
            source
        };
        let first = make();
        let mut mirror = Connection::open_in_memory().unwrap();
        ensure_duck_schema(&mirror).unwrap();
        let metrics = DbMetrics::default();
        apply_pending(&first, &mut mirror, &metrics, test_deadline()).unwrap();
        let second = make();
        second.execute_batch("INSERT INTO t VALUES(2,99)").unwrap();
        assert_ne!(position(&first).unwrap().0, position(&second).unwrap().0);
        let before = metrics.htap_rebuilds.load(Ordering::Relaxed);
        apply_pending(&second, &mut mirror, &metrics, test_deadline()).unwrap();
        assert_eq!(
            metrics.htap_rebuilds.load(Ordering::Relaxed) - before,
            1,
            "one table, rebuilt whole"
        );
        assert_eq!(
            mirror
                .query_row("SELECT count(*) FROM t_native", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(mirror_state(&mirror).unwrap().0, position(&second).unwrap().0);
    }

    /// A table snapshot that cannot be recorded takes the whole batch with it: no half-built mirror, no moved position
    #[test]
    fn a_failed_table_rebuild_rolls_the_whole_batch_back() {
        let source = rusqlite::Connection::open_in_memory().unwrap();
        source
            .execute_batch("CREATE TABLE keep(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO keep VALUES(1,10)")
            .unwrap();
        install_capture(&source).unwrap();
        let mut mirror = Connection::open_in_memory().unwrap();
        ensure_duck_schema(&mirror).unwrap();
        let metrics = DbMetrics::default();
        let held = apply_pending(&source, &mut mirror, &metrics, test_deadline()).unwrap();
        source
            .execute_batch("CREATE TABLE added(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO added VALUES(1,5)")
            .unwrap();
        refresh_capture_in_transaction(&source).unwrap();
        // the catalog refuses the position this batch would record, after the snapshot has already appended
        mirror
            .execute_batch(&format!(
                "CREATE TABLE xsqld_copy AS SELECT * FROM xsqld_tables; DROP TABLE xsqld_tables; \
                 CREATE TABLE xsqld_tables(source_name VARCHAR PRIMARY KEY,mirror_name VARCHAR NOT NULL,physical_name VARCHAR NOT NULL,fingerprint VARCHAR,watermark BIGINT,purity VARCHAR,CHECK(watermark<={held})); \
                 INSERT INTO xsqld_tables SELECT * FROM xsqld_copy; DROP TABLE xsqld_copy;"
            ))
            .unwrap();
        assert!(apply_pending(&source, &mut mirror, &metrics, test_deadline()).is_err());
        assert!(
            !read_catalog(&mirror).unwrap().contains_key("added"),
            "a half-built mirror was left behind"
        );
        assert!(duck_object_type(&mirror, "added_native").unwrap().is_none());
        assert_eq!(watermark(&mirror).unwrap(), held);
        assert_eq!(
            mirror
                .query_row("SELECT count(*) FROM keep_native", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    /// A rebuild the sync runs takes the caller's deadline, not a fixed cap of its own: on a slow machine mirroring
    /// a large database legitimately takes longer than any constant, and a cap that is too short leaves the mirror
    /// failing every batch after a schema change
    #[test]
    fn a_sync_rebuild_uses_the_deadline_it_is_given() {
        let source = rusqlite::Connection::open_in_memory().unwrap();
        source
            .execute_batch(
                "CREATE TABLE orders(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO orders VALUES(1,10),(2,20)",
            )
            .unwrap();
        install_capture(&source).unwrap();
        let mut mirror = Connection::open_in_memory().unwrap();
        ensure_duck_schema(&mirror).unwrap();
        let error = apply_pending(&source, &mut mirror, &DbMetrics::default(), Instant::now()).unwrap_err();
        assert!(error.to_string().contains("timed out"), "{error}");
        // with a deadline that stands, the same rebuild goes through
        assert_eq!(
            apply_pending(&source, &mut mirror, &DbMetrics::default(), test_deadline()).unwrap(),
            1
        );
    }

    #[test]
    fn a_failed_batch_rolls_back_all_mirror_tables_and_watermark() {
        let source = rusqlite::Connection::open_in_memory().unwrap();
        source.execute_batch("CREATE TABLE orders(id INTEGER PRIMARY KEY,user_id INTEGER,amount_cents INTEGER,status INTEGER,channel INTEGER,created_at INTEGER); INSERT INTO orders VALUES(1,10,100,0,1,1000)").unwrap();
        install_capture(&source).unwrap();
        let (id, target, _, fingerprint) = position(&source).unwrap();
        let tables = table_schemas(&source).unwrap();
        let mut mirror = Connection::open_in_memory().unwrap();
        ensure_duck_schema(&mirror).unwrap();
        rebuild_all(
            &source,
            &mut mirror,
            &id,
            target,
            &fingerprint,
            &tables,
            Instant::now() + std::time::Duration::from_secs(10),
        )
        .unwrap();
        source
            .execute("INSERT INTO orders VALUES(2,20,200,1,2,2000)", [])
            .unwrap();
        let (id, wm, format, fingerprint) = mirror_state(&mirror).unwrap();
        // the mirror can take its own watermark back, but not the one the next batch has to move it to
        mirror.execute_batch(&format!("DROP TABLE xsqld_meta; CREATE TABLE xsqld_meta(id INTEGER PRIMARY KEY,source_id VARCHAR,watermark BIGINT,format BIGINT,schema_fingerprint VARCHAR,CHECK(watermark={wm}))")).unwrap();
        mirror
            .execute(
                "INSERT INTO xsqld_meta VALUES(1,?,?,?,?)",
                duckdb::params![id, wm, format, fingerprint],
            )
            .unwrap();
        assert!(apply_pending(&source, &mut mirror, &DbMetrics::default(), test_deadline()).is_err());
        assert_eq!(watermark(&mirror).unwrap(), wm);
        assert_eq!(
            mirror
                .query_row("SELECT count(*) FROM orders_native", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        mirror.execute_batch("DROP TABLE xsqld_meta; CREATE TABLE xsqld_meta(id INTEGER PRIMARY KEY,source_id VARCHAR,watermark BIGINT,format BIGINT,schema_fingerprint VARCHAR)").unwrap();
        mirror
            .execute(
                "INSERT INTO xsqld_meta VALUES(1,?,?,?,?)",
                duckdb::params![id, wm, format, fingerprint],
            )
            .unwrap();
        assert_eq!(
            apply_pending(&source, &mut mirror, &DbMetrics::default(), test_deadline()).unwrap(),
            2
        );
        assert_eq!(
            apply_pending(&source, &mut mirror, &DbMetrics::default(), test_deadline()).unwrap(),
            2
        );
        assert_eq!(
            mirror
                .query_row("SELECT count(*) FROM orders_native", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stale_mirror_rebuilds_when_required_log_was_pruned() {
        let (_dir, registry, db) = fixture().await;
        query(&db, &registry, "SELECT * FROM orders_native").await.unwrap();
        write(&db, &["UPDATE orders SET user_id=999 WHERE id=1"]).await.unwrap();
        assert_eq!(
            query(&db, &registry, "SELECT user_id FROM orders_native WHERE id=1")
                .await
                .unwrap()
                .watermark,
            2
        );
        write(&db, &["SELECT pruned_seq FROM xsqld_htap_meta"]).await.unwrap();
        assert!(
            write(&db, &["DELETE FROM sqlite_sequence WHERE name='xsqld_changelog'"])
                .await
                .is_err()
        );
        assert!(
            write(&db, &["UPDATE sqlite_sequence SET seq=0 WHERE name='xsqld_changelog'"])
                .await
                .is_err()
        );
        assert!(write(&db, &["CREATE TRIGGER poison AFTER UPDATE ON xsqld_htap_meta BEGIN UPDATE xsqld_htap_meta SET pruned_seq=999999; END"]).await.is_err());
        let cfg = registry.cfg.clone();
        let path = mirror_path(&cfg.db_file("tenant"));
        shutdown(registry).await;
        tokio::task::spawn_blocking(move || {
            let old = Connection::open(path).unwrap();
            let physical: String = old
                .query_row(
                    "SELECT physical_name FROM xsqld_tables WHERE source_name='orders'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            // an older mirror: its own position is behind the retained log, and its data is wrong
            old.execute_batch(&format!(
                "UPDATE xsqld_meta SET watermark=0; UPDATE xsqld_tables SET watermark=0; \
                 UPDATE {} SET user_id=CAST(-1 AS VARIANT)",
                quote_identifier(&physical)
            ))
            .unwrap();
        })
        .await
        .unwrap();
        let registry = Registry::new(cfg);
        let reopened = registry.get("tenant").await.unwrap().unwrap();
        let result = query(&reopened, &registry, "SELECT user_id FROM orders_native ORDER BY id")
            .await
            .unwrap();
        assert_eq!(result.watermark, 2);
        assert_eq!(
            result.result.rows,
            vec![vec![Value::Integer(999)], vec![Value::Integer(20)]]
        );
        shutdown(registry).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn htap_worker_releases_when_idle_and_resumes_on_query() {
        let (_dir, registry, db) = fixture().await;
        let htap = db
            .analytics(
                registry.cfg.clone(),
                Instant::now() + std::time::Duration::from_secs(10),
            )
            .await
            .unwrap();
        let until = Instant::now() + std::time::Duration::from_secs(5);
        while htap.progress.scheduled.load(Ordering::Acquire) && Instant::now() < until {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(!htap.progress.scheduled.load(Ordering::Acquire));
        assert!(htap.worker.lock().is_some());
        write(&db, &["INSERT INTO orders VALUES(3,30,300,3,3,3000)"])
            .await
            .unwrap();
        let result = query(&db, &registry, "SELECT count(*) FROM orders_native")
            .await
            .unwrap();
        assert_eq!(result.result.rows, vec![vec![Value::Integer(3)]]);
        assert_eq!(result.watermark, 2);
        shutdown(registry).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn idle_close_releases_htap_and_next_request_resumes_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::for_tests(dir.path());
        cfg.idle_close_ms = 300;
        let registry = Registry::new(Arc::new(cfg));
        registry.create("tenant", None).await.unwrap();
        let db = registry.get("tenant").await.unwrap().unwrap();
        write(&db, &["CREATE TABLE orders(id INTEGER PRIMARY KEY,user_id INTEGER,amount_cents INTEGER,status INTEGER,channel INTEGER,created_at INTEGER)", "INSERT INTO orders VALUES(1,10,100,0,1,1000)"]).await.unwrap();
        let mirror = mirror_path(&registry.cfg.db_file("tenant"));
        let result = query(&db, &registry, "SELECT count(*) FROM orders_native")
            .await
            .unwrap();
        assert_eq!(result.result.rows, vec![vec![Value::Integer(1)]]);
        drop(db);
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        assert_eq!(registry.close_idle().await, 1);
        assert!(mirror.exists());
        assert!(registry.open_dbs().is_empty());
        let reopened = registry.get("tenant").await.unwrap().unwrap();
        write(&reopened, &["INSERT INTO orders VALUES(2,20,200,1,2,2000)"])
            .await
            .unwrap();
        let source = Conn::open_reader(&registry.cfg.db_file("tenant"), &registry.cfg).unwrap();
        let (_, expected_watermark, _, _) = position(&source.c).unwrap();
        drop(source);
        let result = query(&reopened, &registry, "SELECT count(*) FROM orders_native")
            .await
            .unwrap();
        assert_eq!(result.result.rows, vec![vec![Value::Integer(2)]]);
        assert_eq!(result.watermark, expected_watermark);
        shutdown(registry).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_analytics_leaves_the_namespace_writable() {
        // The writer is cloned for the mirror; dropping that clone must not stop the write thread, or a failed
        // analytics call would take the whole namespace down.
        let (_dir, registry, db) = fixture().await;
        let result = query(&db, &registry, "SELECT count(*) FROM orders_native").await;
        assert!(result.is_ok());
        write(&db, &["INSERT INTO orders VALUES(3,30,300,3,3,3000)"])
            .await
            .unwrap();
        shutdown(registry).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn arbitrary_orders_schemas_are_mirrored() {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry::new(Arc::new(Config::for_tests(dir.path())));
        registry.create("tenant", None).await.unwrap();
        let db = registry.get("tenant").await.unwrap().unwrap();
        write(&db, &["CREATE TABLE orders(id TEXT PRIMARY KEY, name TEXT)"])
            .await
            .unwrap();
        let empty = query(&db, &registry, "SELECT count(*) FROM orders_native")
            .await
            .unwrap();
        assert_eq!(empty.result.rows, vec![vec![Value::Integer(0)]]);
        write(&db, &["INSERT INTO orders VALUES('a','b')"]).await.unwrap();
        let row = query(&db, &registry, "SELECT id,name FROM orders_native")
            .await
            .unwrap();
        assert_eq!(
            row.result.rows,
            vec![vec![Value::Text("a".into()), Value::Text("b".into())]]
        );
        shutdown(registry).await;
    }

    #[test]
    fn a_dump_leaves_the_capture_out() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path());
        let conn = Conn::open_writer(&dir.path().join("data"), &cfg).unwrap();
        conn.c
            .execute_batch("CREATE TABLE orders(id INTEGER PRIMARY KEY,user_id INTEGER,amount_cents INTEGER,status INTEGER,channel INTEGER,created_at INTEGER); \
             INSERT INTO orders VALUES(1,10,100,0,1,1000);")
            .unwrap();
        // the capture's own objects: installed as the server does it (a client may not create them)
        conn.internal.store(true, Ordering::Relaxed);
        install_capture(&conn.c).unwrap();
        conn.internal.store(false, Ordering::Relaxed);
        let mut out = Vec::new();
        crate::dump::export(&conn.c, &mut out, false).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("CREATE TABLE IF NOT EXISTS orders"), "{text}");
        let hash = format!("{:016x}", hash_name("orders"));
        for internal in [
            "xsqld_changelog".to_owned(),
            "xsqld_htap_meta".to_owned(),
            format!("{CHANGE_TRIGGER_PREFIX}{hash}_i"),
            format!("{CHANGE_TRIGGER_PREFIX}{hash}_u"),
            format!("{CHANGE_TRIGGER_PREFIX}{hash}_d"),
        ] {
            assert!(!text.contains(&internal), "{internal} is in the dump: {text}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn htap_reads_pass_the_read_gate() {
        // The capture's reads must be visible to the gate, or a checkpoint that closes it and drains finds nothing
        // to wait for and the WAL keeps growing behind the capture's read transaction (checkpoint starvation).
        let (_dir, registry, db) = fixture().await;
        query(&db, &registry, "SELECT count(*) FROM orders_native")
            .await
            .unwrap();
        let gate = db.readers.gate.clone();
        let closed = gate.close();
        write(&db, &["INSERT INTO orders VALUES(3,30,300,3,3,3000)"])
            .await
            .unwrap();
        // with the gate closed the query cannot read the source position, so it fails at its own deadline
        let deadline = Instant::now() + std::time::Duration::from_millis(300);
        let htap = db.analytics(registry.cfg.clone(), deadline).await.unwrap();
        let blocked = tokio::task::spawn_blocking(move || {
            htap.query(
                Request {
                    sql: "SELECT count(*) FROM orders_native".into(),
                    args: vec![],
                },
                deadline,
            )
        })
        .await
        .unwrap();
        assert!(blocked.is_err(), "the capture read behind a closed gate");
        drop(closed);
        let result = query(&db, &registry, "SELECT count(*) FROM orders_native")
            .await
            .unwrap();
        assert_eq!(result.result.rows, vec![vec![Value::Integer(3)]]);
        shutdown(registry).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn all_tables_follow_runtime_ddl_and_composite_keys() {
        let (_dir, registry, db) = fixture().await;
        query(&db, &registry, "SELECT count(*) FROM orders_native")
            .await
            .unwrap();
        write(
            &db,
            &[
                "CREATE TABLE events(tenant TEXT, event_id INTEGER, body BLOB, amount NUMERIC, PRIMARY KEY(tenant,event_id)) WITHOUT ROWID",
                "INSERT INTO events VALUES('a',1,X'00ff',12.25),('a',2,NULL,NULL)",
                "CREATE TABLE loose(label TEXT, value)",
                "INSERT INTO loose VALUES('int',1),('text','two'),('blob',X'0102'),('null',NULL)",
            ],
        )
        .await
        .unwrap();
        let events = query(
            &db,
            &registry,
            "SELECT tenant,event_id,body,amount FROM events_native ORDER BY tenant,event_id",
        )
        .await
        .unwrap();
        assert_eq!(events.result.rows.len(), 2);
        assert_eq!(events.result.rows[0][0], Value::Text("a".into()));
        assert_eq!(events.result.rows[0][1], Value::Integer(1));
        assert_eq!(events.result.rows[0][2], Value::Blob(vec![0, 255]));
        assert_eq!(events.result.rows[0][3], Value::Float(12.25));
        assert_eq!(events.result.rows[1][2], Value::Null);
        assert_eq!(events.result.rows[1][3], Value::Null);
        let loose = query(&db, &registry, "SELECT label,value FROM loose_native ORDER BY label")
            .await
            .unwrap();
        assert_eq!(loose.result.rows.len(), 4);
        assert_eq!(
            loose.result.rows,
            vec![
                vec![Value::Text("blob".into()), Value::Blob(vec![1, 2])],
                vec![Value::Text("int".into()), Value::Integer(1)],
                vec![Value::Text("null".into()), Value::Null],
                vec![Value::Text("text".into()), Value::Text("two".into())],
            ]
        );

        write(
            &db,
            &[
                "ALTER TABLE events ADD COLUMN note TEXT",
                "INSERT INTO events(tenant,event_id,body,amount,note) VALUES('b',1,X'ff',9.5,'after-alter')",
            ],
        )
        .await
        .unwrap();
        let after_alter = query(&db, &registry, "SELECT note FROM events_native WHERE tenant='b'")
            .await
            .unwrap();
        assert_eq!(after_alter.result.rows, vec![vec![Value::Text("after-alter".into())]]);
        assert_eq!(after_alter.result.cols.len(), 1);

        write(&db, &["DROP TABLE loose"]).await.unwrap();
        assert!(query(&db, &registry, "SELECT * FROM loose_native").await.is_err());
        assert_eq!(
            query(&db, &registry, "SELECT count(*) FROM events_native")
                .await
                .unwrap()
                .result
                .rows,
            vec![vec![Value::Integer(3)]]
        );
        shutdown(registry).await;
    }

    /// SQLite maintains `sqlite_sequence` itself while a table DDL runs, which the capture's protection refuses for
    /// client SQL: dropping and renaming an AUTOINCREMENT table must still work, and still be followed
    #[tokio::test(flavor = "multi_thread")]
    async fn autoincrement_tables_survive_rename_and_drop() {
        let (_dir, registry, db) = fixture().await;
        write(
            &db,
            &[
                "CREATE TABLE seq(id INTEGER PRIMARY KEY AUTOINCREMENT, note TEXT)",
                "INSERT INTO seq(note) VALUES('one'),('two')",
            ],
        )
        .await
        .unwrap();
        let before = query(&db, &registry, "SELECT id,note FROM seq_native ORDER BY id")
            .await
            .unwrap();
        assert_eq!(
            before.result.rows,
            vec![
                vec![Value::Integer(1), Value::Text("one".into())],
                vec![Value::Integer(2), Value::Text("two".into())],
            ]
        );
        // an ordinary update is one outbox row, not a delete and an insert
        write(&db, &["UPDATE seq SET note='renamed' WHERE id=2"]).await.unwrap();
        let updated = query(&db, &registry, "SELECT note FROM seq_native WHERE id=2")
            .await
            .unwrap();
        assert_eq!(updated.result.rows, vec![vec![Value::Text("renamed".into())]]);
        assert_eq!(updated.watermark, before.watermark + 1);

        write(&db, &["ALTER TABLE seq RENAME TO seq2"]).await.unwrap();
        assert!(query(&db, &registry, "SELECT * FROM seq_native").await.is_err());
        write(&db, &["INSERT INTO seq2(note) VALUES('three')"]).await.unwrap();
        assert_eq!(
            query(&db, &registry, "SELECT count(*) FROM seq2_native")
                .await
                .unwrap()
                .result
                .rows,
            vec![vec![Value::Integer(3)]]
        );

        write(&db, &["DROP TABLE seq2"]).await.unwrap();
        assert!(query(&db, &registry, "SELECT * FROM seq2_native").await.is_err());
        assert_eq!(
            query(&db, &registry, "SELECT count(*) FROM orders_native")
                .await
                .unwrap()
                .result
                .rows,
            vec![vec![Value::Integer(2)]]
        );
        // the allowance is the statement's, not the connection's: naming the internals directly is still refused
        assert!(
            write(&db, &["DELETE FROM sqlite_sequence WHERE name='xsqld_changelog'"])
                .await
                .is_err()
        );
        let capture = format!("{}{:016x}_i", CHANGE_TRIGGER_PREFIX, hash_name("orders"));
        assert!(write(&db, &[&format!("DROP TRIGGER {capture}")]).await.is_err());
        shutdown(registry).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn htap_metrics_report_lag_batches_and_queries() {
        let (_dir, registry, db) = fixture().await;
        let mut out = String::new();
        db.metrics.render("tenant", &mut out);
        assert!(
            !out.contains("htap_"),
            "a database without a mirror reports HTAP metrics: {out}"
        );
        query(&db, &registry, "SELECT count(*) FROM orders_native")
            .await
            .unwrap();
        write(&db, &["INSERT INTO orders VALUES(3,30,300,3,3,3000)"])
            .await
            .unwrap();
        let result = query(&db, &registry, "SELECT count(*) FROM orders_native")
            .await
            .unwrap();
        assert_eq!(result.watermark, 2);
        let mut out = String::new();
        db.metrics.render("tenant", &mut out);
        assert!(out.contains("xsqld_htap_watermark{ns=\"tenant\"} 2\n"), "{out}");
        assert!(out.contains("xsqld_htap_lag{ns=\"tenant\"} 0\n"), "{out}");
        assert!(out.contains("xsqld_htap_batches_total{ns=\"tenant\"} 1\n"), "{out}");
        assert!(
            out.contains("xsqld_htap_rows_applied_total{ns=\"tenant\"} 1\n"),
            "{out}"
        );
        assert!(out.contains("xsqld_htap_rebuilds_total{ns=\"tenant\"} 1\n"), "{out}");
        assert!(out.contains("xsqld_htap_queries_total{ns=\"tenant\"} 2\n"), "{out}");
        assert!(out.contains("xsqld_htap_sync_errors_total{ns=\"tenant\"} 0\n"), "{out}");
        shutdown(registry).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_write_resumes_the_sync_after_the_worker_went_idle() {
        let (_dir, registry, db) = fixture().await;
        query(&db, &registry, "SELECT count(*) FROM orders_native")
            .await
            .unwrap();
        // The test config uses a short HTAP worker idle timeout: it exits and leaves its wake channel unread
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        write(&db, &["INSERT INTO orders VALUES(3,30,300,3,3,3000)"])
            .await
            .unwrap();
        // the commit must resume the sync on its own, with no analytical query to prompt it
        let until = Instant::now() + std::time::Duration::from_secs(5);
        while db.metrics.htap_watermark.load(Ordering::Relaxed) < 1 && Instant::now() < until {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(
            db.metrics.htap_watermark.load(Ordering::Relaxed),
            1,
            "the mirror stayed behind after the worker went idle"
        );
        shutdown(registry).await;
    }

    #[test]
    fn query_validation_canonicalizes_before_duckdb_prepare() {
        for sql in [
            "SELECT 1; DELETE FROM orders_native",
            "SELECT 1) ; DROP TABLE orders_native; --",
            "CREATE TABLE t(a INTEGER)",
            "BEGIN; SELECT 1",
        ] {
            assert!(query_sql(sql).is_err(), "accepted {sql}");
        }
        assert!(query_sql("SELECT '; DROP TABLE orders_native' AS s; -- comment\r\n").is_ok());
        assert!(query_sql("WITH t AS (SELECT 1 a) SELECT * FROM t").is_ok());
    }
}
