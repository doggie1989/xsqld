//! Incremental sync: read the outbox prefix at a commit SCN, re-read row state, and apply it to the mirror.
//!
//! The outbox carries no payload, so row values are re-read from SeekDB at the same SCN. That makes the
//! apply order-independent: an identity present at the SCN is upserted, an absent one is deleted, regardless
//! of how many times it changed inside the batch. Replaying a batch after a crash therefore converges.
//!
//! Pruning must delete the exact seq set visible at the SCN, never a `seq <= max` range: SeekDB allocates
//! AUTO_INCREMENT values in a different order than transactions commit, so a range delete would drop rows
//! that were allocated early but committed after the snapshot and have not been applied yet.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use serde_json::Value;

use crate::capture::{self, TableSchema, outbox_read_sql, quote_ident, quote_literal};
use crate::mirror::{SourceColumn, mirror_name, snapshot_sql, sql_string};

/// One touched row identity, with the newest outbox seq that mentioned it.
#[derive(Debug, Clone, PartialEq)]
pub struct Identity {
    pub seq: i64,
    pub pk: Vec<Value>,
}

#[derive(Debug, Default, PartialEq)]
pub struct CycleStats {
    pub scn: u64,
    pub changes: usize,
    pub identities: usize,
    pub tables: usize,
    pub upserts: usize,
    pub deletes: usize,
    /// Tables rebuilt inside the apply path because the source schema changed mid-cycle.
    pub schema_rebuilds: usize,
    /// Applied changes per table, so the worker can reclaim DuckDB row versions by rebuilding.
    pub per_table: BTreeMap<String, usize>,
}

pub fn parse_pk(raw: &str) -> Result<Vec<Value>> {
    let v: Value = serde_json::from_str(raw).with_context(|| format!("outbox pk is not JSON: {raw}"))?;
    match v {
        Value::Array(items) => Ok(items),
        other => bail!("outbox pk must be a JSON array, got {other}"),
    }
}

/// Collapse the changelog to one identity per key, keeping the newest seq.
///
/// The JSON text is the identity, so grouping is exact and does not depend on number formatting: both the
/// trigger and the re-read produce it with SeekDB's own `JSON_ARRAY`.
pub fn group_identities(rows: &[(i64, String, String, String)]) -> BTreeMap<String, Vec<Identity>> {
    let mut per_table: BTreeMap<String, BTreeMap<String, Identity>> = BTreeMap::new();
    for (seq, tbl, _op, pk) in rows {
        let entry = per_table.entry(tbl.clone()).or_default();
        match entry.get(pk) {
            Some(prev) if prev.seq >= *seq => {}
            _ => {
                if let Ok(parsed) = parse_pk(pk) {
                    entry.insert(pk.clone(), Identity { seq: *seq, pk: parsed });
                }
            }
        }
    }
    per_table
        .into_iter()
        .map(|(t, m)| (t, m.into_values().collect()))
        .collect()
}

/// Render a JSON key component as a SQL literal. Strings are escaped; numbers stay numeric so that
/// `CAST`-free comparison against an INTEGER key column works.
pub fn sql_literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Bool(b) => if *b { "1" } else { "0" }.into(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => quote_literal(s),
        // A JSON array/object cannot be a key component; refuse rather than silently stringify.
        other => format!("/* unsupported key component: {other} */ NULL"),
    }
}

/// Null-safe identity predicate. `=` would silently drop every identity with a NULL key component, which
/// is exactly what keyless tables produce.
pub fn identity_predicate(key_cols: &[String], ids: &[Identity]) -> String {
    if ids.is_empty() {
        return "1=0".into();
    }
    ids.iter()
        .map(|id| {
            let conj = key_cols
                .iter()
                .zip(id.pk.iter())
                .map(|(c, v)| format!("{} <=> {}", quote_ident(c), sql_literal(v)))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("({conj})")
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

pub fn ids_ddl(table: &str, key_cols: &[String], key_types: &[String]) -> String {
    let cols = key_cols
        .iter()
        .zip(key_types.iter())
        .map(|(c, t)| format!("{} {t}", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "CREATE OR REPLACE TEMP TABLE {}({cols}, seq BIGINT)",
        quote_ident(&ids_name(table))
    )
}

pub fn ids_name(table: &str) -> String {
    format!("ids_{}", table.to_ascii_lowercase())
}

pub fn present_name(table: &str) -> String {
    format!("present_{}", table.to_ascii_lowercase())
}

pub fn staging_name(table: &str) -> String {
    format!("stg_{}", table.to_ascii_lowercase())
}

pub fn dedup_name(table: &str) -> String {
    format!("dedup_{}", table.to_ascii_lowercase())
}

fn ids_insert(table: &str, key_cols: &[String], ids: &[Identity]) -> String {
    let cols = key_cols
        .iter()
        .map(|c| quote_ident(c))
        .chain(std::iter::once("seq".to_string()))
        .collect::<Vec<_>>()
        .join(",");
    let tuples = ids
        .iter()
        .map(|id| {
            let vals = id.pk.iter().map(sql_literal).collect::<Vec<_>>().join(",");
            format!("({vals},{})", id.seq)
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("INSERT INTO {}({cols}) VALUES {tuples}", quote_ident(&ids_name(table)))
}

/// Non-nullable marker added to the re-read so "row absent at this SCN" can be distinguished from
/// "row present whose key happens to be NULL". Keyless tables are keyed on the whole row, so a key column
/// really can be NULL and must not be used as the presence indicator.
pub const PRESENT_MARKER: &str = "__xsqld_present";

/// Re-read only the touched rows, at the batch SCN, through the mysql extension so DuckDB keeps native
/// types. The projection applies the same lossy-mapping guards as the full snapshot.
pub fn present_sql(
    alias: &str,
    table: &str,
    columns: &[SourceColumn],
    key_cols: &[String],
    ids: &[Identity],
    scn: u64,
) -> String {
    let proj = columns
        .iter()
        .map(|c| {
            let q = quote_ident(&c.name);
            if c.data_type.eq_ignore_ascii_case("bit") {
                format!("CAST({q} AS UNSIGNED) AS {q}")
            } else if c.needs_text_cast() {
                format!("CAST({q} AS CHAR) AS {q}")
            } else {
                q
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    let inner = format!(
        "SELECT {proj}, 1 AS {PRESENT_MARKER} FROM {} AS OF SNAPSHOT {scn} WHERE {}",
        quote_ident(table),
        identity_predicate(key_cols, ids)
    );
    format!(
        "CREATE OR REPLACE TEMP TABLE {} AS SELECT * FROM mysql_query({}, {})",
        quote_ident(&present_name(table)),
        sql_string(alias),
        sql_string(&inner)
    )
}

/// Left join identities against the re-read rows: absent means deleted at this SCN.
pub fn staging_sql(table: &str, key_cols: &[String], value_cols: &[String]) -> String {
    let on = key_cols
        .iter()
        .map(|c| {
            let q = quote_ident(c);
            format!("i.{q} IS NOT DISTINCT FROM p.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let keys = key_cols
        .iter()
        .map(|c| format!("i.{}", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(",");
    let vals = value_cols
        .iter()
        .map(|c| format!("p.{}", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(",");
    let select_vals = if vals.is_empty() {
        String::new()
    } else {
        format!(",{vals}")
    };
    format!(
        "CREATE OR REPLACE TEMP TABLE {} AS SELECT i.seq,{keys}{select_vals},\
         CASE WHEN p.{PRESENT_MARKER} IS NULL THEN 'D' ELSE 'I' END AS op \
         FROM {} i LEFT JOIN {} p ON {on}",
        quote_ident(&staging_name(table)),
        quote_ident(&ids_name(table)),
        quote_ident(&present_name(table))
    )
}

/// The watermark lives in DuckDB, not SeekDB, so it commits in the same transaction as the apply.
/// A watermark written to the source could not be atomic with the mirror and would report freshness the
/// mirror does not actually have.
pub const DUCK_META: &str = "xsqld_mirror_meta";

pub fn duck_meta_ddl() -> String {
    format!("CREATE TABLE IF NOT EXISTS {DUCK_META}(id INTEGER PRIMARY KEY, watermark_scn BIGINT)")
}

pub fn duck_meta_seed() -> String {
    format!("INSERT OR IGNORE INTO {DUCK_META}(id, watermark_scn) VALUES(1, 0)")
}

pub fn watermark_read_sql() -> String {
    format!("SELECT watermark_scn FROM {DUCK_META} WHERE id=1")
}

pub fn watermark_write_sql(scn: u64) -> String {
    format!("UPDATE {DUCK_META} SET watermark_scn={scn} WHERE id=1")
}

/// Discover user tables and their key columns from SeekDB metadata. Internal tables are excluded because
/// `ATTACH` exposes them and does not scope them to the attached database.
pub async fn discover_tables(c: &mut Conn, db: &str) -> Result<Vec<TableSchema>> {
    let rows: Vec<(String, String, String, Option<String>)> = c
        .query(format!(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_KEY \
             FROM information_schema.columns \
             WHERE TABLE_SCHEMA={} ORDER BY TABLE_NAME, ORDINAL_POSITION",
            quote_literal(db)
        ))
        .await?;
    let mut order: Vec<String> = Vec::new();
    let mut cols: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut keys: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (table, column, _dt, key) in rows {
        if capture::is_internal_table(&table) {
            continue;
        }
        if !cols.contains_key(&table) {
            order.push(table.clone());
        }
        cols.entry(table.clone()).or_default().push(column.clone());
        if key.as_deref() == Some("PRI") {
            keys.entry(table.clone()).or_default().push(column);
        }
    }
    Ok(order
        .into_iter()
        .map(|name| TableSchema {
            key: keys.get(&name).cloned().unwrap_or_default(),
            columns: cols.get(&name).cloned().unwrap_or_default(),
            name,
        })
        .collect())
}

pub async fn discover_columns(c: &mut Conn, db: &str, table: &str) -> Result<Vec<SourceColumn>> {
    let rows: Vec<(String, String, Option<u32>)> = c
        .query(format!(
            "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION \
             FROM information_schema.columns WHERE TABLE_SCHEMA={} AND TABLE_NAME={} \
             ORDER BY ORDINAL_POSITION",
            quote_literal(db),
            quote_literal(table)
        ))
        .await?;
    Ok(rows
        .into_iter()
        .map(|(name, data_type, numeric_precision)| SourceColumn {
            name,
            data_type,
            numeric_precision,
        })
        .collect())
}

/// Full snapshot, one independent DuckDB transaction per table so a large table cannot roll back the rest.
pub fn snapshot_table(
    duck: &duckdb::Connection,
    alias: &str,
    table: &TableSchema,
    columns: &[SourceColumn],
    timeout_us: u64,
) -> Result<()> {
    let sql = snapshot_sql(alias, &table.name, columns, &mirror_name(&table.name), timeout_us);
    duck.execute_batch(&sql)
        .with_context(|| format!("snapshot {}", table.name))?;
    Ok(())
}

/// One sync cycle. Every merge and the watermark advance commit in a single DuckDB transaction; the outbox
/// is pruned only afterwards, so a crash in between replays the batch idempotently.
/// A direct client's DDL can land between `discover_columns` and the source-side re-read. The first
/// attempt then fails and DuckDB aborts the cycle's transaction, so the retry must restart the whole
/// cycle (fresh SCN, fresh outbox read, new transaction) — retrying inside the aborted transaction
/// only yields "Current transaction is aborted".
pub async fn run_cycle(
    c: &mut Conn,
    duck: &duckdb::Connection,
    alias: &str,
    db: &str,
    tables: &[TableSchema],
    batch: u64,
    timeout_us: u64,
) -> Result<CycleStats> {
    match run_cycle_once(c, duck, alias, db, tables, batch, timeout_us).await {
        Ok(stats) => Ok(stats),
        Err(e) if is_schema_race(&e) => {
            tracing::warn!(
                error = %format!("{e:#}"),
                "schema changed mid-apply, retrying the whole cycle once"
            );
            run_cycle_once(c, duck, alias, db, tables, batch, timeout_us).await
        }
        Err(e) => Err(e),
    }
}

async fn run_cycle_once(
    c: &mut Conn,
    duck: &duckdb::Connection,
    alias: &str,
    db: &str,
    tables: &[TableSchema],
    batch: u64,
    timeout_us: u64,
) -> Result<CycleStats> {
    let scn: i64 = c
        .query_first(capture::current_scn_sql())
        .await?
        .context("CURRENT_SCN() returned no row")?;
    let scn = scn as u64;
    let rows: Vec<(i64, String, String, String)> = c.query(outbox_read_sql(scn, batch)).await?;
    let mut stats = CycleStats {
        scn,
        changes: rows.len(),
        ..Default::default()
    };
    let grouped = group_identities(&rows);
    stats.identities = grouped.values().map(Vec::len).sum();
    stats.tables = grouped.len();

    duck.execute_batch(&duck_meta_ddl())?;
    duck.execute_batch(&duck_meta_seed())?;
    duck.execute_batch("BEGIN TRANSACTION")?;
    let applied = apply_tables(c, duck, (alias, db, timeout_us), tables, &grouped, scn, &mut stats).await;
    if let Err(e) = applied {
        duck.execute_batch("ROLLBACK").ok();
        return Err(e);
    }
    // Only claim the SCN once the whole prefix was consumed. With a truncated batch the mirror is behind
    // the SCN, so advancing the watermark would report freshness it does not have.
    let drained = (rows.len() as u64) < batch;
    if drained {
        duck.execute_batch(&watermark_write_sql(scn))?;
    }
    duck.execute_batch("COMMIT")?;

    // Prune exactly the applied rows. Pruning the whole snapshot prefix would delete rows beyond the
    // batch limit that were never applied, losing them permanently.
    let applied: Vec<i64> = rows.iter().map(|r| r.0).collect();
    for sql in capture::outbox_prune_sql(&applied) {
        c.query_drop(sql).await?;
    }
    Ok(stats)
}

/// A direct client's DDL can land between `discover_columns` and the source-side re-read, so the
/// re-read references a column that no longer exists (or the mirror binder rejects a new one).
/// One retry with a fresh schema resolves it; anything else is a real failure.
fn is_schema_race(e: &anyhow::Error) -> bool {
    let s = format!("{e:#}");
    s.contains("Unknown column")
        || s.contains("Binder Error")
        || s.contains("does not have a column")
        || s.contains("has no column")
}

async fn apply_tables(
    c: &mut Conn,
    duck: &duckdb::Connection,
    src: (&str, &str, u64),
    tables: &[TableSchema],
    grouped: &BTreeMap<String, Vec<Identity>>,
    scn: u64,
    stats: &mut CycleStats,
) -> Result<()> {
    for table in tables {
        let Some(ids) = grouped.get(&table.name) else { continue };
        if ids.is_empty() {
            continue;
        }
        let (up, del, rebuilt) = apply_one_table(c, duck, src, table, ids, scn).await?;
        stats.upserts += up;
        stats.deletes += del;
        *stats.per_table.entry(table.name.clone()).or_insert(0) += up + del;
        if rebuilt {
            stats.schema_rebuilds += 1;
        }
    }
    Ok(())
}

/// Returns (upserts, deletes, mirror_rebuilt). Counters are applied by the caller only on success so
/// a retry cannot double-count a partially applied attempt.
async fn apply_one_table(
    c: &mut Conn,
    duck: &duckdb::Connection,
    src: (&str, &str, u64),
    table: &TableSchema,
    ids: &[Identity],
    scn: u64,
) -> Result<(usize, usize, bool)> {
    let (alias, db, timeout_us) = src;
    let columns = discover_columns(c, db, &table.name).await?;
    // The source schema can change between reconcile and here (a direct client's ALTER lands in that
    // window). Applying new-schema SQL against the old mirror fails with a binder error and costs a
    // whole cycle, so detect the divergence and rebuild this table first.
    let mirror = mirror_name(&table.name);
    let mut rebuilt = false;
    if mirror_columns(duck, &mirror)? != columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>() {
        snapshot_table(duck, alias, table, &columns, timeout_us)?;
        rebuilt = true;
    }
    let key_cols = table.identity_columns().to_vec();
    let value_cols: Vec<String> = columns
        .iter()
        .map(|col| col.name.clone())
        .filter(|n| !key_cols.contains(n))
        .collect();
    let key_types = key_types_from_mirror(duck, &mirror, &key_cols)?;

    duck.execute_batch(&ids_ddl(&table.name, &key_cols, &key_types))?;
    duck.execute_batch(&ids_insert(&table.name, &key_cols, ids))?;
    duck.execute_batch(&present_sql(alias, &table.name, &columns, &key_cols, ids, scn))?;
    duck.execute_batch(&staging_sql(&table.name, &key_cols, &value_cols))?;
    duck.execute_batch(&crate::mirror::dedup_sql(
        &staging_name(&table.name),
        &key_cols,
        &dedup_name(&table.name),
    ))?;
    let (up, del): (i64, i64) = duck.query_row(
        &format!(
            "SELECT COUNT(*) FILTER (WHERE op='I'), COUNT(*) FILTER (WHERE op='D') FROM {}",
            quote_ident(&dedup_name(&table.name))
        ),
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    duck.execute_batch(&crate::mirror::merge_sql(
        &mirror,
        &dedup_name(&table.name),
        &key_cols,
        &value_cols,
    ))?;
    Ok((up as usize, del as usize, rebuilt))
}

fn key_types_from_mirror(duck: &duckdb::Connection, mirror: &str, key_cols: &[String]) -> Result<Vec<String>> {
    let mut stmt = duck.prepare(&format!("DESCRIBE {}", quote_ident(mirror)))?;
    let rows = stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let map: BTreeMap<String, String> = rows.into_iter().collect();
    key_cols
        .iter()
        .map(|c| {
            map.get(c)
                .cloned()
                .with_context(|| format!("mirror {mirror} has no column {c}"))
        })
        .collect()
}

/// Per-pass sync parameters, grouped so the plumbing functions stay under the argument-count lint.
pub struct SyncOpts<'a> {
    pub alias: &'a str,
    pub db: &'a str,
    pub batch: u64,
    pub timeout_us: u64,
    pub skip: &'a std::collections::BTreeSet<String>,
}

/// Schema fingerprint: column names in order plus the key. A change means the capture triggers and the
/// mirror table are both stale and must be rebuilt for that table only.
pub fn fingerprint(t: &TableSchema) -> String {
    let mut s = t.columns.join("\u{1}");
    s.push('\u{2}');
    s.push_str(&t.key.join("\u{1}"));
    s
}

#[derive(Debug, Default, PartialEq)]
pub struct ReconcileStats {
    pub added: usize,
    pub rebuilt: usize,
    pub dropped: usize,
    pub count_mismatch: usize,
    pub snapshot_rows: u64,
    pub snapshot_us: u64,
    /// Tables actually snapshotted this pass, for per-table rebuild counters.
    pub snapshotted: Vec<String>,
}

/// Bring the mirror in line with the source before applying incremental changes.
///
/// Row-count reconciliation is the safety net for changes capture cannot see. `TRUNCATE` was measured to
/// fire no DELETE trigger on SeekDB (the outbox stayed empty while the source went to zero rows), and it
/// leaves the schema fingerprint unchanged, so neither the outbox nor schema comparison would notice it.
/// `COUNT(*)` on SeekDB measured 0.32 ms over 10M rows, so checking every table each cycle is cheap.
/// `DROP TABLE` also fires no trigger, but discovery stops listing the table and the mirror is removed.
pub async fn reconcile(
    c: &mut Conn,
    duck: &duckdb::Connection,
    known: &mut BTreeMap<String, String>,
    opts: &SyncOpts<'_>,
) -> Result<(Vec<TableSchema>, ReconcileStats)> {
    let (alias, db, skip, timeout_us) = (opts.alias, opts.db, opts.skip, opts.timeout_us);
    let tables = discover_tables(c, db).await?;
    // The outbox must exist before any trigger body can insert into it.
    c.query_drop(capture::outbox_ddl()).await?;
    c.query_drop(capture::meta_ddl()).await?;
    c.query_drop(capture::meta_seed_ddl()).await?;
    let mut stats = ReconcileStats::default();
    let mut live: BTreeMap<String, String> = BTreeMap::new();
    // Row-count reconciliation is only meaningful once every committed change has been applied. While the
    // outbox still holds pending rows the counts legitimately differ, and re-snapshotting then would
    // replace the incremental path with a full rebuild on every cycle (observed: 76 rebuilds in 45s).
    let pending: i64 = c
        .query_first(format!("SELECT COUNT(*) FROM {}", crate::capture::OUTBOX_TABLE))
        .await?
        .unwrap_or(0);

    for t in &tables {
        let fp = fingerprint(t);
        live.insert(t.name.clone(), fp.clone());
        // A table inside a maintenance window must not be reconciled: its row count legitimately diverges
        // while capture is dropped, and re-snapshotting mid-window would defeat the window's purpose.
        if skip.contains(&t.name.to_ascii_lowercase()) {
            continue;
        }
        let mirror = mirror_name(&t.name);
        let exists = mirror_exists(duck, &mirror)?;
        let changed = known.get(&t.name).map(|prev| prev != &fp).unwrap_or(false);
        if !exists || changed {
            for sql in capture::drop_triggers(t) {
                c.query_drop(sql).await?;
            }
            for sql in capture::capture_triggers(t) {
                c.query_drop(sql).await?;
            }
            let cols = discover_columns(c, db, &t.name).await?;
            let t0 = std::time::Instant::now();
            snapshot_table(duck, alias, t, &cols, timeout_us)?;
            stats.snapshot_us += t0.elapsed().as_micros() as u64;
            stats.snapshot_rows += mirror_row_count(duck, &mirror)?;
            stats.snapshotted.push(t.name.clone());
            if exists {
                stats.rebuilt += 1;
            } else {
                stats.added += 1;
            }
            continue;
        }
        if pending != 0 {
            continue;
        }
        let src: i64 = c
            .query_first(format!("SELECT COUNT(*) FROM {}", quote_ident(&t.name)))
            .await?
            .unwrap_or(0);
        let dst: i64 = duck.query_row(&format!("SELECT COUNT(*) FROM {}", quote_ident(&mirror)), [], |r| {
            r.get(0)
        })?;
        if src != dst {
            stats.count_mismatch += 1;
            let cols = discover_columns(c, db, &t.name).await?;
            let t0 = std::time::Instant::now();
            snapshot_table(duck, alias, t, &cols, timeout_us)?;
            stats.snapshot_us += t0.elapsed().as_micros() as u64;
            stats.snapshot_rows += src as u64;
            stats.snapshotted.push(t.name.clone());
        }
    }

    for name in known.keys() {
        if !live.contains_key(name) {
            duck.execute_batch(&format!("DROP TABLE IF EXISTS {}", quote_ident(&mirror_name(name))))?;
            stats.dropped += 1;
        }
    }
    *known = live;
    Ok((tables, stats))
}

fn mirror_columns(duck: &duckdb::Connection, mirror: &str) -> Result<Vec<String>> {
    let mut stmt = duck.prepare(&format!("DESCRIBE {}", quote_ident(mirror)))?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn mirror_row_count(duck: &duckdb::Connection, mirror: &str) -> Result<u64> {
    let n: i64 = duck.query_row(&format!("SELECT COUNT(*) FROM {}", quote_ident(mirror)), [], |r| {
        r.get(0)
    })?;
    Ok(n as u64)
}

fn mirror_exists(duck: &duckdb::Connection, mirror: &str) -> Result<bool> {
    let n: i64 = duck.query_row(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [mirror],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

/// One full pass over a namespace: reconcile, then apply the outbox prefix.
pub async fn sync_once(
    c: &mut Conn,
    duck: &duckdb::Connection,
    known: &mut BTreeMap<String, String>,
    opts: &SyncOpts<'_>,
) -> Result<(ReconcileStats, CycleStats)> {
    let (tables, rec) = reconcile(c, duck, known, opts).await?;
    let cyc = run_cycle(c, duck, opts.alias, opts.db, &tables, opts.batch, opts.timeout_us).await?;
    Ok((rec, cyc))
}

/// Build the routing view: source column metadata (including collation) plus live row counts.
pub async fn build_view(
    c: &mut Conn,
    db: &str,
    tables: &[TableSchema],
    cache: std::sync::Arc<crate::route::RouteCache>,
    conn: std::sync::Arc<parking_lot::Mutex<duckdb::Connection>>,
) -> Result<crate::route::MirrorView> {
    let rows: Vec<(String, String, String, Option<u32>, String)> = c
        .query(format!(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, COLUMN_TYPE \
             FROM information_schema.columns WHERE TABLE_SCHEMA={} ORDER BY TABLE_NAME, ORDINAL_POSITION",
            quote_literal(db)
        ))
        .await?;
    let mut schema = crate::route::Schema::default();
    for (t, col, dt, prec, ct) in rows {
        if crate::capture::is_internal_table(&t) {
            continue;
        }
        schema
            .tables
            .entry(t.to_ascii_lowercase())
            .or_default()
            .push(crate::route::ColumnInfo {
                name: col,
                data_type: dt.to_ascii_lowercase(),
                numeric_precision: prec,
                column_type: ct,
            });
    }
    let mut counts = BTreeMap::new();
    for t in tables {
        let n: i64 = c
            .query_first(format!("SELECT COUNT(*) FROM {}", quote_ident(&t.name)))
            .await?
            .unwrap_or(0);
        counts.insert(t.name.to_ascii_lowercase(), n as u64);
    }
    let fingerprint = crate::route::schema_fingerprint(&schema);
    Ok(crate::route::MirrorView {
        duck: conn,
        schema,
        rows: counts,
        fingerprint,
        cache,
    })
}

pub async fn publish(
    c: &mut Conn,
    db: &str,
    cache: std::sync::Arc<crate::route::RouteCache>,
    conn: std::sync::Arc<parking_lot::Mutex<duckdb::Connection>>,
) -> Result<crate::route::MirrorView> {
    let tables = discover_tables(c, db).await?;
    build_view(c, db, &tables, cache, conn).await
}

/// A namespace's mirror: one DuckDB file plus the schema fingerprints last seen.
pub struct NsMirror {
    pub duck: duckdb::Connection,
    /// Kept so the worker can notice the file being unlinked underneath the open handle. On Unix the
    /// inode survives unlink, so DuckDB keeps serving correct results from a file that no longer exists
    /// and the mirror would silently vanish on restart.
    pub path: std::path::PathBuf,
    known: BTreeMap<String, String>,
}

impl NsMirror {
    pub fn open(cfg: &crate::config::Config, ns: &str, source: &crate::mirror::Source) -> Result<Self> {
        std::fs::create_dir_all(&cfg.mirror_dir).with_context(|| format!("create mirror dir {}", cfg.mirror_dir))?;
        let path = std::path::Path::new(&cfg.mirror_dir).join(format!("{ns}.duckdb"));
        let threads = std::thread::available_parallelism().map_or(1, |n| n.get()).min(4) as i64;
        let dcfg = duckdb::Config::default()
            // The SQLite-era mirror set enable_external_access(false); that blocks the mysql extension.
            .enable_external_access(true)?
            .enable_autoload_extension(false)?
            .with("memory_limit", format!("{}MB", cfg.mirror_memory_limit_mb))?
            .with("preserve_insertion_order", "false")?
            .threads(threads)?;
        let duck = match duckdb::Connection::open_with_flags(&path, dcfg) {
            Ok(d) => d,
            Err(e) => {
                // The mirror is a derived cache, fully rebuildable from SeekDB. Move an unopenable file
                // aside rather than deleting it, so the next pass rebuilds and the bad file stays
                // available for inspection.
                if path.exists() {
                    let aside = path.with_extension(format!(
                        "corrupt-{}",
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_or(0, |d| d.as_secs())
                    ));
                    match std::fs::rename(&path, &aside) {
                        Ok(()) => tracing::warn!(
                            namespace = %ns, error = %format!("{e:#}"), moved_to = %aside.display(),
                            "mirror unopenable, moved aside for rebuild"
                        ),
                        Err(re) => tracing::error!(
                            namespace = %ns, error = %format!("{e:#}"), rename_error = %format!("{re:#}"),
                            "mirror unopenable and could not be moved aside"
                        ),
                    }
                }
                return Err(e).with_context(|| format!("open mirror {}", path.display()));
            }
        };
        if duck.execute_batch("LOAD mysql").is_err() {
            duck.execute_batch("INSTALL mysql").context("INSTALL mysql extension")?;
            duck.execute_batch("LOAD mysql").context("LOAD mysql extension")?;
        }
        duck.execute_batch(&source.attach_sql("src")).context("ATTACH SeekDB")?;
        Ok(Self {
            duck,
            path,
            known: BTreeMap::new(),
        })
    }
}

fn source_for(cfg: &crate::config::Config, ns: &str) -> Result<crate::mirror::Source> {
    let (host, port, user, password) = if let Some(url) = &cfg.mysql_url {
        let opts = mysql_async::Opts::from_url(url).context("parse --mysql-url")?;
        (
            opts.ip_or_hostname().to_string(),
            opts.tcp_port(),
            opts.user().unwrap_or("root").to_string(),
            opts.pass().unwrap_or("").to_string(),
        )
    } else {
        (
            cfg.mysql_host.clone(),
            cfg.mysql_port,
            cfg.mysql_user.clone(),
            cfg.mysql_password.clone(),
        )
    };
    Ok(crate::mirror::Source {
        host,
        port,
        user,
        password,
        database: ns.to_string(),
    })
}

/// Run the sync loop on a dedicated thread.
///
/// DuckDB calls are blocking and a full snapshot of a large table takes seconds, so they must not run on a
/// tokio worker thread. The thread drives the async MySQL work through the runtime handle, which keeps the
/// blocking DuckDB sections off the reactor entirely.
pub fn spawn_worker(
    backend: std::sync::Arc<crate::backend::Backend>,
    cfg: std::sync::Arc<crate::config::Config>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> std::thread::JoinHandle<()> {
    let rt = tokio::runtime::Handle::current();
    std::thread::Builder::new()
        .name("mirror-sync".into())
        .spawn(move || {
            let mut mirrors: std::collections::HashMap<String, NsMirror> = std::collections::HashMap::new();
            let mut caches: std::collections::HashMap<String, std::sync::Arc<crate::route::RouteCache>> =
                std::collections::HashMap::new();
            let mut since_checkpoint: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
            let mut since_reopen: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
            let mut snap_us: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
            let mut churn: std::collections::HashMap<String, std::collections::HashMap<String, u64>> =
                std::collections::HashMap::new();
            let mut view_conns: std::collections::HashMap<
                String,
                std::sync::Arc<parking_lot::Mutex<duckdb::Connection>>,
            > = std::collections::HashMap::new();
            let interval = Duration::from_millis(cfg.mirror_interval_ms);
            loop {
                if *stop.borrow_and_update() {
                    break;
                }
                for ns in backend.open_namespaces() {
                    let name = ns.name.to_string();
                    // An unlinked mirror file keeps working through the open handle, so check the path
                    // explicitly and force a rebuild instead of serving from an inode that will vanish.
                    if let Some(m) = mirrors.get(&name)
                        && !m.path.exists()
                    {
                        tracing::warn!(namespace = %name, path = %m.path.display(),
                            "mirror file disappeared, rebuilding");
                        mirrors.remove(&name);
                        view_conns.remove(&name);
                        *ns.mirror.write() = None;
                    }
                    if !mirrors.contains_key(&name) {
                        let source = match source_for(&cfg, &name) {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::error!(namespace = %name, error = %format!("{e:#}"), "mirror source config failed");
                                continue;
                            }
                        };
                        match NsMirror::open(&cfg, &name, &source) {
                            Ok(m) => {
                                tracing::info!(namespace = %name, "mirror opened");
                                mirrors.insert(name.clone(), m);
                            }
                            Err(e) => {
                                tracing::error!(namespace = %name, error = %format!("{e:#}"), "mirror open failed");
                                continue;
                            }
                        }
                    }
                    let Some(m) = mirrors.get_mut(&name) else { continue };
                    let route_cache = caches.entry(name.clone()).or_default().clone();
                    let pool = ns.pool.clone();
                    let db = name.clone();
                    let batch = cfg.mirror_batch;
                    // The worker is an active pool user; without this an all-direct-MySQL
                    // namespace would be closed as idle and the mirror would silently stop syncing.
                    ns.note_mirror_activity();
                    let skip = ns.maintenance.read().clone();
                    let res = rt.block_on(async {
                        let mut c = pool.get_conn().await?;
                        sync_once(
                            &mut c,
                            &m.duck,
                            &mut m.known,
                            &SyncOpts {
                                alias: "src",
                                db: &db,
                                batch,
                                timeout_us: cfg.mirror_query_timeout_ms * 1000,
                                skip: &skip,
                            },
                        )
                        .await
                    });
                    match res {
                        Ok((rec, cyc)) => {
                            let published = rt.block_on(async {
                                let mut c2 = pool.get_conn().await?;
                                let vc = view_conns
                                    .entry(name.clone())
                                    .or_insert_with(|| {
                                        std::sync::Arc::new(parking_lot::Mutex::new(
                                            m.duck.try_clone().expect("clone mirror connection"),
                                        ))
                                    })
                                    .clone();
                                let v = publish(&mut c2, &db, route_cache.clone(), vc).await?;
                                let outbox: i64 = c2
                                    .query_first(format!("SELECT COUNT(*) FROM {}", crate::capture::OUTBOX_TABLE))
                                    .await?
                                    .unwrap_or(0);
                                Ok::<_, anyhow::Error>((v, outbox))
                            });
                            let wm: i64 = m.duck.query_row(&watermark_read_sql(), [], |r| r.get(0)).unwrap_or(0);
                            let mx = &ns.metrics;
                            mx.mirror_cycles.fetch_add(1, Ordering::Relaxed);
                            mx.mirror_applied.fetch_add(cyc.changes as u64, Ordering::Relaxed);
                            mx.mirror_watermark_scn.store(wm.max(0) as u64, Ordering::Relaxed);
                            mx.mirror_lag_scn
                                .store(cyc.scn.saturating_sub(wm.max(0) as u64), Ordering::Relaxed);
                            mx.mirror_tables.store(m.known.len() as u64, Ordering::Relaxed);
                            mx.mirror_route_cache_entries
                                .store(route_cache.len() as u64, Ordering::Relaxed);
                            // DuckDB keeps appended rows in the WAL until a checkpoint; without one the
                            // WAL grows without bound under sustained writes (observed 1 MB WAL against a
                            // 12 KB main file during the soak).
                            // Reclaim DuckDB row versions: MERGE leaves old versions behind and neither
                            // CHECKPOINT nor VACUUM reclaims them, so a churn-heavy table must be rebuilt.
                            let ch = churn.entry(name.clone()).or_default();
                            for (tbl, n) in &cyc.per_table {
                                *ch.entry(tbl.clone()).or_insert(0) += *n as u64;
                            }
                            let hot: Vec<String> = ch
                                .iter()
                                .filter(|(_, n)| **n >= cfg.mirror_rebuild_churn)
                                .map(|(t, _)| t.clone())
                                .collect();
                            if !hot.is_empty() {
                                let rebuilt = rt.block_on(async {
                                    let mut c3 = pool.get_conn().await?;
                                    let tables = discover_tables(&mut c3, &db).await?;
                                    let mut done = 0usize;
                                    let mut rows = 0u64;
                                    for t in tables.iter().filter(|t| hot.contains(&t.name)) {
                                        let cols = discover_columns(&mut c3, &db, &t.name).await?;
                                        snapshot_table(&m.duck, "src", t, &cols, cfg.mirror_query_timeout_ms * 1000)?;
                                        rows += mirror_row_count(&m.duck, &mirror_name(&t.name))?;
                                        done += 1;
                                    }
                                    Ok::<_, anyhow::Error>((done, rows))
                                });
                                match rebuilt {
                                    Ok((n, rows)) => {
                                        for t in &hot {
                                            ch.insert(t.clone(), 0);
                                            *mx.mirror_rebuilds.lock().entry(t.clone()).or_insert(0) += 1;
                                        }
                                        mx.mirror_snapshot_rows.fetch_add(rows, Ordering::Relaxed);
                                        mx.mirror_churn_rebuilds.fetch_add(n as u64, Ordering::Relaxed);
                                        tracing::info!(namespace = %name, tables = n, rows,
                                            "mirror rebuilt to reclaim row versions");
                                    }
                                    Err(e) => tracing::warn!(namespace = %name, error = %format!("{e:#}"),
                                        "churn rebuild failed"),
                                }
                            }
                            // Bound native memory: see --mirror-reopen-cycles. Dropping the instance
                            // releases untagged C++ allocations that neither CHECKPOINT nor a table
                            // rebuild reclaims. The next pass reopens and re-snapshots from SeekDB.
                            if rec.snapshot_us > 0 {
                                snap_us.insert(name.clone(), rec.snapshot_us);
                            }
                            // Adaptive period: a reopen re-snapshots every table, so its cost scales with
                            // mirror size. Keep re-snapshotting under the duty budget instead of paying a
                            // 19s snapshot every 48s on a 10M-row table. Small mirrors keep the base period.
                            let duty = cfg.mirror_reopen_duty_pct as f64 / 100.0;
                            let interval_us = cfg.mirror_interval_ms as f64 * 1000.0;
                            let cost_floor =
                                (*snap_us.get(&name).unwrap_or(&0) as f64 / (duty * interval_us)).ceil() as u64;
                            let threshold = cfg.mirror_reopen_cycles.max(cost_floor).max(1);
                            let ro = since_reopen.entry(name.clone()).or_insert(0);
                            *ro += 1;
                            if *ro >= threshold {
                                *ro = 0;
                                mirrors.remove(&name);
                                view_conns.remove(&name);
                                churn.remove(&name);
                                *ns.mirror.write() = None;
                                tracing::info!(namespace = %name, threshold, cost_floor,
                                    snapshot_ms = *snap_us.get(&name).unwrap_or(&0) / 1000,
                                    "mirror instance reopened to bound native memory");
                                continue;
                            }
                            let n = since_checkpoint.entry(name.clone()).or_insert(0);
                            *n += 1;
                            if *n >= cfg.mirror_checkpoint_cycles {
                                *n = 0;
                                if let Err(e) = m.duck.execute_batch("CHECKPOINT") {
                                    tracing::warn!(namespace = %name, error = %format!("{e:#}"), "mirror checkpoint failed");
                                }
                            }
                            if rec.snapshot_rows > 0 {
                                mx.mirror_snapshot_rows.fetch_add(rec.snapshot_rows, Ordering::Relaxed);
                                mx.mirror_snapshot.record(Duration::from_micros(rec.snapshot_us));
                                let mut rb = mx.mirror_rebuilds.lock();
                                for t in &rec.snapshotted {
                                    *rb.entry(t.clone()).or_insert(0) += 1;
                                }
                            }
                            match published {
                                Ok((v, outbox)) => {
                                    mx.mirror_outbox_rows.store(outbox.max(0) as u64, Ordering::Relaxed);
                                    *ns.mirror.write() = Some(std::sync::Arc::new(v));
                                }
                                Err(e) => {
                                    mx.mirror_errors.fetch_add(1, Ordering::Relaxed);
                                    tracing::warn!(namespace = %name, error = %format!("{e:#}"), "mirror view publish failed");
                                    *ns.mirror.write() = None;
                                }
                            }
                            if rec.added + rec.rebuilt + rec.dropped + rec.count_mismatch > 0 {
                                tracing::info!(
                                    namespace = %name,
                                    added = rec.added,
                                    rebuilt = rec.rebuilt,
                                    dropped = rec.dropped,
                                    count_mismatch = rec.count_mismatch,
                                    "mirror reconciled"
                                );
                            }
                            if cyc.changes > 0 {
                                let wm: i64 = m.duck.query_row(&watermark_read_sql(), [], |r| r.get(0)).unwrap_or(0);
                                tracing::debug!(
                                    namespace = %name,
                                    changes = cyc.changes,
                                    upserts = cyc.upserts,
                                    deletes = cyc.deletes,
                                    scn = cyc.scn,
                                    watermark_scn = wm,
                                    "mirror applied"
                                );
                            }
                        }
                        Err(e) => {
                            ns.metrics.mirror_errors.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(namespace = %name, error = %format!("{e:#}"), "mirror sync failed");
                            // Drop the handle so the next pass reopens and re-attaches cleanly. The view
                            // connection belongs to the old database instance and must go with it.
                            mirrors.remove(&name);
                            view_conns.remove(&name);
                            *ns.mirror.write() = None;
                        }
                    }
                }
                std::thread::sleep(interval);
            }
            tracing::info!("mirror sync worker stopped");
        })
        .expect("spawn mirror-sync thread")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rows() -> Vec<(i64, String, String, String)> {
        vec![
            (1, "orders".into(), "I".into(), "[1]".into()),
            (5, "orders".into(), "I".into(), "[1]".into()),
            (3, "orders".into(), "D".into(), "[2]".into()),
            (2, "line".into(), "I".into(), "[\"a\",\"b\"]".into()),
        ]
    }

    #[test]
    fn identities_collapse_to_newest_seq_per_key() {
        let g = group_identities(&rows());
        let orders = &g["orders"];
        assert_eq!(orders.len(), 2);
        let one = orders.iter().find(|i| i.pk == vec![json!(1)]).unwrap();
        assert_eq!(one.seq, 5, "the newest mention of a key must win");
        assert_eq!(g["line"].len(), 1);
        assert_eq!(g["line"][0].pk, vec![json!("a"), json!("b")]);
    }

    #[test]
    fn identity_grouping_is_order_independent() {
        let mut rev = rows();
        rev.reverse();
        let a = group_identities(&rows());
        let b = group_identities(&rev);
        assert_eq!(a["orders"].len(), b["orders"].len());
        for (x, y) in a["orders"].iter().zip(b["orders"].iter()) {
            assert_eq!(x.seq, y.seq);
        }
    }

    #[test]
    fn pk_must_be_a_json_array() {
        assert!(parse_pk("[1,2]").is_ok());
        assert!(parse_pk("{\"a\":1}").is_err());
        assert!(parse_pk("not json").is_err());
    }

    #[test]
    fn literals_keep_numbers_numeric_and_escape_strings() {
        assert_eq!(sql_literal(&json!(1)), "1");
        assert_eq!(sql_literal(&json!("a'b")), "'a''b'");
        assert_eq!(sql_literal(&Value::Null), "NULL");
        assert_eq!(sql_literal(&json!("a\\b")), "'a\\\\b'");
    }

    #[test]
    fn predicate_is_null_safe_so_null_key_components_are_not_dropped() {
        let ids = vec![Identity {
            seq: 1,
            pk: vec![json!(1), Value::Null],
        }];
        let p = identity_predicate(&["a".into(), "b".into()], &ids);
        assert_eq!(p, "(`a` <=> 1 AND `b` <=> NULL)");
        assert!(!p.contains("`b` = NULL"), "= NULL would never match");
    }

    #[test]
    fn empty_identity_set_matches_nothing() {
        assert_eq!(identity_predicate(&["a".into()], &[]), "1=0");
    }

    #[test]
    fn predicate_ors_every_identity() {
        let ids = vec![
            Identity {
                seq: 1,
                pk: vec![json!(1)],
            },
            Identity {
                seq: 2,
                pk: vec![json!(2)],
            },
        ];
        assert_eq!(identity_predicate(&["id".into()], &ids), "(`id` <=> 1) OR (`id` <=> 2)");
    }

    #[test]
    fn present_rereads_at_the_batch_scn_through_mysql_query() {
        let cols = vec![SourceColumn {
            name: "d".into(),
            data_type: "decimal".into(),
            numeric_precision: Some(65),
        }];
        let ids = vec![Identity {
            seq: 1,
            pk: vec![json!(7)],
        }];
        let sql = present_sql("src", "t", &cols, &["id".into()], &ids, 12345);
        assert!(sql.contains("mysql_query('src',"), "{sql}");
        assert!(sql.contains("AS OF SNAPSHOT 12345"), "{sql}");
        assert!(sql.contains("WHERE (`id` <=> 7)"), "{sql}");
        assert!(
            sql.contains("CAST(`d` AS CHAR)"),
            "snapshot guards must also apply to the re-read: {sql}"
        );
    }

    #[test]
    fn staging_marks_absent_rows_as_deletes() {
        let sql = staging_sql("t", &["id".into()], &["v".into()]);
        assert!(sql.contains("LEFT JOIN"), "{sql}");
        assert!(
            sql.contains("CASE WHEN p.__xsqld_present IS NULL THEN 'D' ELSE 'I' END AS op"),
            "{sql}"
        );
        assert!(
            !sql.contains("WHEN p.`id` IS NULL"),
            "a NULL key value must not be mistaken for an absent row: {sql}"
        );
        assert!(
            sql.contains("IS NOT DISTINCT FROM"),
            "null-safe join for keyless tables: {sql}"
        );
        assert!(sql.contains("i.seq,i.`id`,p.`v`"), "{sql}");
    }

    #[test]
    fn staging_for_key_only_table_has_no_value_columns() {
        let sql = staging_sql("t", &["x".into(), "y".into()], &[]);
        assert!(sql.contains("i.seq,i.`x`,i.`y`,CASE"), "{sql}");
    }

    #[test]
    fn schema_race_classifier_matches_the_real_soak_error_and_not_unrelated_failures() {
        // 60 分钟 steady-ddl soak 里实际出现的错误原文（源侧回读引用了已被 DROP 的列）
        let real = anyhow::anyhow!(
            "snapshot s: Binder Error: PREPARE error, query: \"SELECT `id`,`v`,`n`,`extra` FROM `s`\" \
             message: Failed to prepare MySQL query: Unknown column 'extra' in 'field list'"
        );
        assert!(is_schema_race(&real), "must retry the real mid-apply DDL race");
        assert!(is_schema_race(&anyhow::anyhow!(
            "Binder Error: Table \"m_s\" does not have a column with name \"extra\""
        )));
        assert!(is_schema_race(&anyhow::anyhow!("mirror m_s has no column id")));
        // 负样本： unrelated failures must NOT be retried (would mask real problems)
        assert!(!is_schema_race(&anyhow::anyhow!(
            "IO Error: Could not write file \"x.duckdb\": No space left on device"
        )));
        assert!(!is_schema_race(&anyhow::anyhow!("Connection refused (os error 61)")));
    }

    #[test]
    fn prune_targets_only_applied_seqs_never_a_range_or_the_whole_prefix() {
        let sqls = capture::outbox_prune_sql(&[3, 1, 2]);
        assert_eq!(sqls.len(), 1);
        assert_eq!(sqls[0], "DELETE FROM xsqld_changelog WHERE seq IN (3,1,2)");
        assert!(
            !sqls[0].contains("seq <="),
            "a range delete drops unapplied low-seq rows"
        );
        assert!(
            !sqls[0].contains("AS OF SNAPSHOT"),
            "the whole prefix may exceed the applied batch"
        );
        assert!(
            capture::outbox_prune_sql(&[]).is_empty(),
            "nothing applied means nothing pruned"
        );
        let many: Vec<i64> = (1..=1200).collect();
        assert_eq!(
            capture::outbox_prune_sql(&many).len(),
            3,
            "large batches must be chunked"
        );
    }

    #[test]
    fn watermark_is_stored_as_scn_in_duckdb_so_it_commits_with_the_apply() {
        assert_eq!(
            watermark_read_sql(),
            "SELECT watermark_scn FROM xsqld_mirror_meta WHERE id=1"
        );
        assert_eq!(
            watermark_write_sql(42),
            "UPDATE xsqld_mirror_meta SET watermark_scn=42 WHERE id=1"
        );
        assert!(duck_meta_ddl().contains("watermark_scn BIGINT"));
        assert!(
            !watermark_read_sql().contains("xsqld_htap_meta"),
            "a source-side watermark cannot be atomic with the mirror"
        );
    }

    #[test]
    fn ids_table_carries_seq_and_typed_keys() {
        let sql = ids_ddl("t", &["id".into()], &["INTEGER".into()]);
        assert_eq!(sql, "CREATE OR REPLACE TEMP TABLE `ids_t`(`id` INTEGER, seq BIGINT)");
        let ins = ids_insert(
            "t",
            &["id".into()],
            &[Identity {
                seq: 9,
                pk: vec![json!(3)],
            }],
        );
        assert_eq!(ins, "INSERT INTO `ids_t`(`id`,seq) VALUES (3,9)");
    }
}

/// End-to-end cycles against a live SeekDB plus an in-process DuckDB. Ignored by default; run with
/// `cargo test -- --ignored --test-threads=1` and no credential-bearing `http_proxy` in the environment.
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod live {
    use super::*;
    use crate::capture::OUTBOX_TABLE;
    use crate::mirror::Source;
    use duckdb::Connection;
    use mysql_async::OptsBuilder;

    const DB: &str = "xsqld_sync_it";

    fn source() -> Source {
        Source {
            host: std::env::var("XSQLD_TEST_MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
            port: std::env::var("XSQLD_TEST_MYSQL_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(12981),
            user: std::env::var("XSQLD_TEST_MYSQL_USER").unwrap_or_else(|_| "root".into()),
            password: std::env::var("XSQLD_TEST_MYSQL_PASSWORD").unwrap_or_default(),
            database: DB.into(),
        }
    }

    async fn connect(db: Option<&str>) -> Conn {
        let s = source();
        let opts = OptsBuilder::default()
            .ip_or_hostname(s.host)
            .tcp_port(s.port)
            .user(Some(s.user))
            .pass(if s.password.is_empty() { None } else { Some(s.password) })
            .db_name(db.map(|d| d.to_string()));
        Conn::new(opts).await.unwrap()
    }

    async fn fresh() -> Conn {
        let mut c = connect(None).await;
        c.query_drop(format!("DROP DATABASE IF EXISTS {DB}")).await.unwrap();
        c.query_drop(format!("CREATE DATABASE {DB}")).await.unwrap();
        c.query_drop(format!("USE {DB}")).await.unwrap();
        c
    }

    fn duck() -> Connection {
        let cfg = duckdb::Config::default()
            .enable_external_access(true)
            .unwrap()
            .enable_autoload_extension(false)
            .unwrap()
            .with("memory_limit", "512MB")
            .unwrap()
            .threads(2)
            .unwrap();
        let c = Connection::open_in_memory_with_flags(cfg).unwrap();
        if c.execute_batch("LOAD mysql").is_err() {
            c.execute_batch("INSTALL mysql").unwrap();
            c.execute_batch("LOAD mysql").unwrap();
        }
        c.execute_batch(&source().attach_sql("src")).unwrap();
        c
    }

    async fn setup(c: &mut Conn, d: &Connection, ddl: &str) -> Vec<TableSchema> {
        c.query_drop(ddl).await.unwrap();
        let tables = discover_tables(c, DB).await.unwrap();
        assert!(!tables.is_empty(), "discovery must find the user table");
        c.query_drop(capture::outbox_ddl()).await.unwrap();
        c.query_drop(capture::meta_ddl()).await.unwrap();
        c.query_drop(capture::meta_seed_ddl()).await.unwrap();
        for t in &tables {
            for sql in capture::capture_triggers(t) {
                c.query_drop(sql).await.unwrap();
            }
        }
        for t in &tables {
            let cols = discover_columns(c, DB, &t.name).await.unwrap();
            snapshot_table(d, "src", t, &cols, 600_000_000).unwrap();
        }
        tables
    }

    fn mirror_rows(d: &Connection, table: &str) -> Vec<(i64, String)> {
        let mut stmt = d
            .prepare(&format!(
                "SELECT id, v FROM {} ORDER BY id",
                quote_ident(&mirror_name(table))
            ))
            .unwrap();
        let rows = stmt
            .query_map([], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?.unwrap_or_default()))
            })
            .unwrap();
        rows.map(|r| r.unwrap()).collect()
    }

    async fn source_rows(c: &mut Conn, table: &str) -> Vec<(i64, String)> {
        c.query(format!("SELECT id, v FROM {table} ORDER BY id")).await.unwrap()
    }

    async fn outbox_count(c: &mut Conn) -> i64 {
        c.query_first(format!("SELECT COUNT(*) FROM {OUTBOX_TABLE}"))
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn cycle_propagates_insert_update_delete_and_is_idempotent() {
        let mut c = fresh().await;
        let d = duck();
        let tables = setup(&mut c, &d, "CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(32))").await;
        c.query_drop("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')")
            .await
            .unwrap();

        let s = run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();
        assert_eq!(s.changes, 3, "three inserts must be captured");
        assert_eq!(s.upserts, 3);
        assert_eq!(mirror_rows(&d, "t"), source_rows(&mut c, "t").await);
        assert_eq!(outbox_count(&mut c).await, 0, "applied changes must be pruned");

        c.query_drop("UPDATE t SET v='B' WHERE id=2").await.unwrap();
        c.query_drop("DELETE FROM t WHERE id=3").await.unwrap();
        c.query_drop("INSERT INTO t VALUES(4,'d')").await.unwrap();
        let s = run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();
        assert_eq!(s.upserts, 2, "update & insert");
        assert_eq!(s.deletes, 1);
        assert_eq!(mirror_rows(&d, "t"), source_rows(&mut c, "t").await);

        let before = mirror_rows(&d, "t");
        let s = run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();
        assert_eq!(s.changes, 0, "an idle cycle must see no changes");
        assert_eq!(mirror_rows(&d, "t"), before, "idle cycle must not disturb the mirror");

        let wm: i64 = d.query_row(&watermark_read_sql(), [], |r| r.get(0)).unwrap();
        assert!(wm > 0, "watermark must advance");
        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    /// R1: SeekDB allocates AUTO_INCREMENT in a different order than transactions commit. A snapshot taken
    /// after the later-seq transaction commits must not cause the earlier-seq, still-uncommitted row to be
    /// pruned. A `seq <= max` prune would lose it permanently.
    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn out_of_order_commit_does_not_lose_the_earlier_seq_row() {
        let mut c = fresh().await;
        let d = duck();
        let tables = setup(&mut c, &d, "CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(32))").await;

        let mut a = connect(Some(DB)).await;
        let mut b = connect(Some(DB)).await;
        a.query_drop("BEGIN").await.unwrap();
        a.query_drop("INSERT INTO t VALUES(1,'early-seq-late-commit')")
            .await
            .unwrap();
        b.query_drop("BEGIN").await.unwrap();
        b.query_drop("INSERT INTO t VALUES(2,'late-seq-early-commit')")
            .await
            .unwrap();
        b.query_drop("COMMIT").await.unwrap();

        // A's outbox row exists but is invisible to other sessions until A commits, so only B's is readable.
        let visible: Vec<(i64, String)> = c
            .query(format!("SELECT seq, pk FROM {OUTBOX_TABLE} ORDER BY seq"))
            .await
            .unwrap();
        assert_eq!(
            visible.len(),
            1,
            "only the committed transaction's outbox row is visible"
        );
        assert_eq!(visible[0].1, "[2]");
        let b_seq = visible[0].0;

        let s = run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();
        assert_eq!(s.upserts, 1, "only the committed row may be applied");
        assert_eq!(mirror_rows(&d, "t"), vec![(2, "late-seq-early-commit".into())]);

        a.query_drop("COMMIT").await.unwrap();
        let now: Vec<(i64, String)> = c
            .query(format!("SELECT seq, pk FROM {OUTBOX_TABLE} ORDER BY seq"))
            .await
            .unwrap();
        assert_eq!(now.len(), 1, "A's outbox entry must have survived the earlier prune");
        assert_eq!(now[0].1, "[1]");
        assert!(
            now[0].0 < b_seq,
            "A must hold a LOWER seq than the already-pruned B ({} vs {b_seq}); \
             a `seq <= max` prune would have deleted it and lost the row permanently",
            now[0].0
        );

        let s = run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();
        assert_eq!(s.upserts, 1);
        assert_eq!(mirror_rows(&d, "t"), source_rows(&mut c, "t").await);
        assert_eq!(
            mirror_rows(&d, "t").len(),
            2,
            "the earlier-seq row must not have been lost"
        );
        assert_eq!(outbox_count(&mut c).await, 0);

        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn replaying_an_unpruned_batch_converges() {
        let mut c = fresh().await;
        let d = duck();
        let tables = setup(&mut c, &d, "CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(32))").await;
        c.query_drop("INSERT INTO t VALUES(1,'a'),(2,'b')").await.unwrap();
        c.query_drop("UPDATE t SET v='A2' WHERE id=1").await.unwrap();
        c.query_drop("DELETE FROM t WHERE id=2").await.unwrap();

        // Simulate a crash after the DuckDB commit but before the prune: restore the outbox entries.
        let saved: Vec<(i64, String, String, String, i64)> = c
            .query(format!("SELECT seq,tbl,op,pk,ts FROM {OUTBOX_TABLE} ORDER BY seq"))
            .await
            .unwrap();
        run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();
        let after_first = mirror_rows(&d, "t");
        assert_eq!(outbox_count(&mut c).await, 0);
        for (seq, tbl, op, pk, ts) in &saved {
            c.query_drop(format!(
                "INSERT INTO {OUTBOX_TABLE}(seq,tbl,op,pk,ts) VALUES({seq},{},{},{},{ts})",
                quote_literal(tbl),
                quote_literal(op),
                quote_literal(pk)
            ))
            .await
            .unwrap();
        }
        let s = run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();
        assert!(s.changes > 0, "the replayed batch must actually be read");
        assert_eq!(
            mirror_rows(&d, "t"),
            after_first,
            "replay must converge to the same state"
        );
        assert_eq!(mirror_rows(&d, "t"), source_rows(&mut c, "t").await);
        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn keyless_table_with_nulls_syncs_by_whole_row_identity() {
        let mut c = fresh().await;
        let d = duck();
        let tables = setup(&mut c, &d, "CREATE TABLE nk(x INT, y VARCHAR(32))").await;
        assert!(tables[0].key.is_empty(), "fixture must have no primary key");
        c.query_drop("INSERT INTO nk VALUES(1,'a'),(NULL,'n'),(2,NULL)")
            .await
            .unwrap();
        run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();

        let cnt: i64 = d.query_row("SELECT COUNT(*) FROM m_nk", [], |r| r.get(0)).unwrap();
        assert_eq!(
            cnt, 3,
            "NULL key components must not be dropped by the identity predicate"
        );

        c.query_drop("UPDATE nk SET y='b' WHERE x=1").await.unwrap();
        run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();
        let cnt: i64 = d.query_row("SELECT COUNT(*) FROM m_nk", [], |r| r.get(0)).unwrap();
        assert_eq!(cnt, 3, "a keyless update retires the old row and inserts the new one");
        let v: String = d.query_row("SELECT y FROM m_nk WHERE x=1", [], |r| r.get(0)).unwrap();
        assert_eq!(v, "b");

        c.query_drop("DELETE FROM nk WHERE x IS NULL").await.unwrap();
        let s = run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();
        assert_eq!(s.deletes, 1, "the delete must be classified as a delete, not an upsert");
        let cnt: i64 = d.query_row("SELECT COUNT(*) FROM m_nk", [], |r| r.get(0)).unwrap();
        assert_eq!(cnt, 2, "deleting a row with a NULL component must work");
        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn maintenance_window_requires_a_resnapshot_to_catch_up() {
        let mut c = fresh().await;
        let d = duck();
        let tables = setup(&mut c, &d, "CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(32))").await;
        c.query_drop("INSERT INTO t VALUES(1,'a')").await.unwrap();
        run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();

        for sql in capture::drop_triggers(&tables[0]) {
            c.query_drop(sql).await.unwrap();
        }
        c.query_drop("INSERT INTO t VALUES(2,'bulk'),(3,'bulk')").await.unwrap();
        assert_eq!(outbox_count(&mut c).await, 0, "window must genuinely bypass capture");
        run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();
        let cnt: i64 = d.query_row("SELECT COUNT(*) FROM m_t", [], |r| r.get(0)).unwrap();
        assert_eq!(
            cnt, 1,
            "rows written inside the window are invisible to incremental sync"
        );

        for sql in capture::capture_triggers(&tables[0]) {
            c.query_drop(sql).await.unwrap();
        }
        let cols = discover_columns(&mut c, DB, "t").await.unwrap();
        snapshot_table(&d, "src", &tables[0], &cols, 600_000_000).unwrap();
        run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();
        assert_eq!(
            mirror_rows(&d, "t"),
            source_rows(&mut c, "t").await,
            "re-snapshot after the window must recover the missed rows"
        );
        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn watermark_never_regresses_and_uncommitted_rows_are_invisible() {
        let mut c = fresh().await;
        let d = duck();
        let tables = setup(&mut c, &d, "CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(32))").await;
        let wm = |d: &duckdb::Connection| -> i64 { d.query_row(&watermark_read_sql(), [], |r| r.get(0)).unwrap_or(0) };

        c.query_drop("INSERT INTO t VALUES(1,'a')").await.unwrap();
        run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();
        let w1 = wm(&d);
        assert!(w1 > 0);

        // An open transaction's outbox rows must not be applied, and must not advance the watermark
        // past what was actually committed.
        let mut other = connect(Some(DB)).await;
        other.query_drop("BEGIN").await.unwrap();
        other.query_drop("INSERT INTO t VALUES(2,'uncommitted')").await.unwrap();
        run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();
        let w2 = wm(&d);
        assert!(w2 >= w1, "watermark must never regress: {w1} -> {w2}");
        let n: i64 = d.query_row("SELECT COUNT(*) FROM m_t", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 1, "an uncommitted row must not reach the mirror");

        other.query_drop("COMMIT").await.unwrap();
        run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();
        let w3 = wm(&d);
        assert!(w3 >= w2, "watermark must never regress: {w2} -> {w3}");
        let n: i64 = d.query_row("SELECT COUNT(*) FROM m_t", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 2, "the row must appear once its transaction commits");

        for _ in 0..3 {
            let before = wm(&d);
            run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
                .await
                .unwrap();
            assert!(wm(&d) >= before, "idle cycles must not move the watermark backwards");
        }
        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn mirror_reads_local_columnar_storage_not_a_federated_mysql_scan() {
        let mut c = fresh().await;
        let d = duck();
        let tables = setup(&mut c, &d, "CREATE TABLE t(id INT PRIMARY KEY, v INT)").await;
        c.query_drop("INSERT INTO t VALUES(1,10),(2,20),(3,30)").await.unwrap();
        run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();

        let plan = |sql: &str| -> String {
            let mut stmt = d.prepare(&format!("EXPLAIN {sql}")).unwrap();
            let rows = stmt.query_map([], |r| r.get::<_, String>(1)).unwrap();
            rows.map(|r| r.unwrap()).collect::<Vec<_>>().join("\n")
        };
        let local = plan("SELECT COUNT(*) FROM m_t");
        assert!(
            !local.to_ascii_uppercase().contains("MYSQL"),
            "the mirror must be scanned locally, not federated back to SeekDB:\n{local}"
        );
        assert!(
            local.to_ascii_uppercase().contains("SCAN"),
            "expected a scan operator:\n{local}"
        );

        // Control: the same shape against the attached source must show the MySQL operator, otherwise
        // the assertion above proves nothing about which storage was read.
        let federated = plan("SELECT COUNT(*) FROM src.t");
        assert!(
            federated.to_ascii_uppercase().contains("MYSQL"),
            "control query must show the MySQL operator:\n{federated}"
        );

        let types: Vec<String> = {
            let mut stmt = d.prepare("DESCRIBE m_t").unwrap();
            let rows = stmt.query_map([], |r| r.get::<_, String>(1)).unwrap();
            rows.map(|r| r.unwrap()).collect()
        };
        assert!(
            !types.iter().any(|t| t.to_ascii_uppercase().contains("VARIANT")),
            "mirror must be strongly typed, not VARIANT: {types:?}"
        );
        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn batch_limit_must_not_prune_unapplied_rows() {
        let mut c = fresh().await;
        let d = duck();
        let tables = setup(&mut c, &d, "CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(32))").await;
        c.query_drop("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
            .await
            .unwrap();
        assert_eq!(outbox_count(&mut c).await, 5);

        // A batch smaller than the backlog must apply only its own rows and prune only those.
        let s = run_cycle(&mut c, &d, "src", DB, &tables, 2, 600_000_000).await.unwrap();
        assert_eq!(s.changes, 2, "the cycle must read at most `batch` rows");
        assert_eq!(
            outbox_count(&mut c).await,
            3,
            "the three unapplied rows must survive pruning"
        );

        for _ in 0..5 {
            if outbox_count(&mut c).await == 0 {
                break;
            }
            run_cycle(&mut c, &d, "src", DB, &tables, 2, 600_000_000).await.unwrap();
        }
        assert_eq!(outbox_count(&mut c).await, 0);
        assert_eq!(
            mirror_rows(&d, "t"),
            source_rows(&mut c, "t").await,
            "no row may be lost when the backlog exceeds the batch size"
        );
        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn schema_change_between_reconcile_and_apply_must_not_fail_the_cycle() {
        let mut c = fresh().await;
        let d = duck();
        let mut known = BTreeMap::new();
        c.query_drop("CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(32))")
            .await
            .unwrap();
        c.query_drop("INSERT INTO t VALUES(1,'a')").await.unwrap();
        let opts = SyncOpts {
            alias: "src",
            db: DB,
            batch: 10000,
            timeout_us: 600_000_000,
            skip: &std::collections::BTreeSet::new(),
        };
        sync_once(&mut c, &d, &mut known, &opts).await.unwrap();

        // Write a change, then land an ALTER after reconcile has already decided no rebuild is needed.
        c.query_drop("INSERT INTO t VALUES(2,'b')").await.unwrap();
        let (tables, _) = reconcile(&mut c, &d, &mut known, &opts).await.unwrap();
        c.query_drop("ALTER TABLE t ADD COLUMN extra INT").await.unwrap();
        c.query_drop("UPDATE t SET extra=7 WHERE id=2").await.unwrap();

        // run_cycle reads the new schema; the mirror still has the old one. It must rebuild, not fail.
        let cyc = run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();
        assert!(
            cyc.schema_rebuilds >= 1,
            "the mid-cycle schema change must force a rebuild"
        );
        let extra: i64 = d
            .query_row("SELECT extra FROM m_t WHERE id=2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(extra, 7);
        assert_eq!(mirror_rows(&d, "t"), source_rows(&mut c, "t").await);
        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn pending_changes_must_not_trigger_full_rebuilds() {
        let mut c = fresh().await;
        let d = duck();
        let mut known = BTreeMap::new();
        c.query_drop("CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(32))")
            .await
            .unwrap();
        c.query_drop("INSERT INTO t VALUES(1,'a')").await.unwrap();
        let opts = SyncOpts {
            alias: "src",
            db: DB,
            batch: 10000,
            timeout_us: 600_000_000,
            skip: &std::collections::BTreeSet::new(),
        };
        let (rec, _) = sync_once(&mut c, &d, &mut known, &opts).await.unwrap();
        assert_eq!(rec.added, 1);

        // Write a backlog, then reconcile WITHOUT applying it. The counts differ, but that difference is
        // expected: the incremental path owns it. Re-snapshotting here would replace incremental sync with
        // a full rebuild on every cycle.
        c.query_drop("INSERT INTO t VALUES(2,'b'),(3,'c'),(4,'d')")
            .await
            .unwrap();
        assert!(outbox_count(&mut c).await > 0, "fixture must leave pending changes");
        let (_, rec) = reconcile(&mut c, &d, &mut known, &opts).await.unwrap();
        assert_eq!(rec.count_mismatch, 0, "pending changes are not a sync failure");
        assert!(
            rec.snapshotted.is_empty(),
            "no table may be re-snapshotted while changes are pending"
        );

        // Once drained, counts must agree without any rebuild.
        let tables = known_tables(&mut c).await;
        run_cycle(&mut c, &d, "src", DB, &tables, 10000, 600_000_000)
            .await
            .unwrap();
        let (_, rec) = reconcile(&mut c, &d, &mut known, &opts).await.unwrap();
        assert_eq!(rec.count_mismatch, 0);
        assert!(rec.snapshotted.is_empty());
        assert_eq!(mirror_rows(&d, "t"), source_rows(&mut c, "t").await);
        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    async fn known_tables(c: &mut Conn) -> Vec<TableSchema> {
        discover_tables(c, DB).await.unwrap()
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn truncate_fires_no_trigger_and_is_recovered_by_count_reconciliation() {
        let mut c = fresh().await;
        let d = duck();
        let mut known = BTreeMap::new();
        c.query_drop("CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(32))")
            .await
            .unwrap();
        c.query_drop("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')")
            .await
            .unwrap();
        let (rec, cyc) = sync_once(
            &mut c,
            &d,
            &mut known,
            &SyncOpts {
                alias: "src",
                db: DB,
                batch: 10000,
                timeout_us: 600_000_000,
                skip: &std::collections::BTreeSet::new(),
            },
        )
        .await
        .unwrap();
        assert_eq!(rec.added, 1);
        assert_eq!(
            cyc.upserts, 0,
            "these rows predate capture, so the full snapshot covers them"
        );
        let cnt: i64 = d.query_row("SELECT COUNT(*) FROM m_t", [], |r| r.get(0)).unwrap();
        assert_eq!(cnt, 3);

        c.query_drop("TRUNCATE TABLE t").await.unwrap();
        assert_eq!(
            outbox_count(&mut c).await,
            0,
            "TRUNCATE must fire no DELETE trigger, otherwise this test proves nothing"
        );

        let (rec, _) = sync_once(
            &mut c,
            &d,
            &mut known,
            &SyncOpts {
                alias: "src",
                db: DB,
                batch: 10000,
                timeout_us: 600_000_000,
                skip: &std::collections::BTreeSet::new(),
            },
        )
        .await
        .unwrap();
        assert_eq!(rec.count_mismatch, 1, "count reconciliation must notice the truncate");
        let cnt: i64 = d.query_row("SELECT COUNT(*) FROM m_t", [], |r| r.get(0)).unwrap();
        assert_eq!(cnt, 0, "the mirror must not keep rows the source no longer has");
        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn schema_change_rebuilds_only_the_changed_table() {
        let mut c = fresh().await;
        let d = duck();
        let mut known = BTreeMap::new();
        c.query_drop("CREATE TABLE t1(id INT PRIMARY KEY, v VARCHAR(32))")
            .await
            .unwrap();
        c.query_drop("CREATE TABLE t2(id INT PRIMARY KEY, v VARCHAR(32))")
            .await
            .unwrap();
        c.query_drop("INSERT INTO t1 VALUES(1,'a')").await.unwrap();
        c.query_drop("INSERT INTO t2 VALUES(1,'b')").await.unwrap();
        let (rec, _) = sync_once(
            &mut c,
            &d,
            &mut known,
            &SyncOpts {
                alias: "src",
                db: DB,
                batch: 10000,
                timeout_us: 600_000_000,
                skip: &std::collections::BTreeSet::new(),
            },
        )
        .await
        .unwrap();
        assert_eq!(rec.added, 2);

        c.query_drop("ALTER TABLE t1 ADD COLUMN extra INT").await.unwrap();
        c.query_drop("UPDATE t1 SET extra=7 WHERE id=1").await.unwrap();
        let (rec, _) = sync_once(
            &mut c,
            &d,
            &mut known,
            &SyncOpts {
                alias: "src",
                db: DB,
                batch: 10000,
                timeout_us: 600_000_000,
                skip: &std::collections::BTreeSet::new(),
            },
        )
        .await
        .unwrap();
        assert_eq!(rec.rebuilt, 1, "only t1 changed schema, t2 must not be rebuilt");
        assert_eq!(rec.added, 0);
        let extra: i64 = d
            .query_row("SELECT extra FROM m_t1 WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(extra, 7, "the rebuilt mirror must carry the new column and its value");
        let t2: i64 = d.query_row("SELECT COUNT(*) FROM m_t2", [], |r| r.get(0)).unwrap();
        assert_eq!(t2, 1);
        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn direct_client_ddl_is_discovered_and_dropped_tables_lose_their_mirror() {
        let mut c = fresh().await;
        let d = duck();
        let mut known = BTreeMap::new();
        c.query_drop("CREATE TABLE t1(id INT PRIMARY KEY, v VARCHAR(32))")
            .await
            .unwrap();
        sync_once(
            &mut c,
            &d,
            &mut known,
            &SyncOpts {
                alias: "src",
                db: DB,
                batch: 10000,
                timeout_us: 600_000_000,
                skip: &std::collections::BTreeSet::new(),
            },
        )
        .await
        .unwrap();

        // A client writing straight to SeekDB, bypassing xsqld entirely.
        let mut other = connect(Some(DB)).await;
        other
            .query_drop("CREATE TABLE t2(id INT PRIMARY KEY, v VARCHAR(32))")
            .await
            .unwrap();
        other.query_drop("INSERT INTO t2 VALUES(5,'direct')").await.unwrap();
        let (rec, _) = sync_once(
            &mut c,
            &d,
            &mut known,
            &SyncOpts {
                alias: "src",
                db: DB,
                batch: 10000,
                timeout_us: 600_000_000,
                skip: &std::collections::BTreeSet::new(),
            },
        )
        .await
        .unwrap();
        assert_eq!(rec.added, 1, "a table created behind xsqld's back must be discovered");
        let v: String = d.query_row("SELECT v FROM m_t2 WHERE id=5", [], |r| r.get(0)).unwrap();
        assert_eq!(
            v, "direct",
            "rows written before discovery are covered by the full snapshot"
        );

        other.query_drop("DROP TABLE t1").await.unwrap();
        let (rec, _) = sync_once(
            &mut c,
            &d,
            &mut known,
            &SyncOpts {
                alias: "src",
                db: DB,
                batch: 10000,
                timeout_us: 600_000_000,
                skip: &std::collections::BTreeSet::new(),
            },
        )
        .await
        .unwrap();
        assert_eq!(rec.dropped, 1);
        assert!(
            !mirror_exists(&d, "m_t1").unwrap(),
            "a dropped table must not leave a stale mirror"
        );
        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }
}
