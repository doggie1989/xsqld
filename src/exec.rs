//! Executes the requests of one `/v2/pipeline` call against a `mysql_async::Conn`.

use std::time::{Duration, Instant};

use mysql_async::consts::{ColumnFlags, ColumnType, StatusFlags};
use mysql_async::prelude::Queryable;
use mysql_async::{Column, Conn, Params, QueryResult, Value as MyVal};

use crate::metrics::NsMetrics;
use crate::proto::{self, BatchCond, BatchResult, Col, StmtResult, StreamRequest, StreamResponse, StreamResult, Value};
use crate::sql::{self, Kind};

/// Fatal execution abort that invalidates or times out the HTTP request itself.
#[derive(Debug)]
pub enum ExecAbort {
    Deadline,
}

pub struct ExecResult {
    pub results: Vec<StreamResult>,
    pub closed: bool,
    pub in_trans: bool,
    pub transport_dirty: bool,
}

/// Protocol problems found before anything runs: answered with HTTP 400 and a text body, like sqld.
pub fn validate(requests: &[StreamRequest], max_statements: usize) -> Result<(), String> {
    let mut statements = 0usize;
    let mut closed = false;
    for r in requests {
        if closed && !matches!(r, StreamRequest::Close) {
            return Err("Stream for this baton was closed".into());
        }
        match r {
            StreamRequest::Close => closed = true,
            StreamRequest::Execute { stmt } => {
                statements += 1;
                check_sql(stmt)?;
            }
            StreamRequest::Batch { batch } => {
                statements += batch.steps.len();
                for (i, step) in batch.steps.iter().enumerate() {
                    check_sql(&step.stmt)?;
                    if let Some(c) = &step.condition {
                        check_cond(c, i)?;
                    }
                }
            }
        }
    }
    if statements > max_statements {
        return Err(format!(
            "Too many statements in one request: {statements} (limit {max_statements})"
        ));
    }
    Ok(())
}

fn check_sql(stmt: &proto::Stmt) -> Result<(), String> {
    match (&stmt.sql, stmt.sql_id) {
        (Some(_), None) => Ok(()),
        (None, Some(id)) => Err(format!("SQL text {id} not found")),
        (Some(_), Some(_)) => Err("Either `sql` or `sql_id` are required, but not both".into()),
        (None, None) => Err("Either `sql` or `sql_id` are required".into()),
    }
}

fn check_cond(c: &BatchCond, step: usize) -> Result<(), String> {
    match c {
        BatchCond::Ok { step: s } | BatchCond::Error { step: s } if (*s as usize) < step => Ok(()),
        BatchCond::Ok { .. } | BatchCond::Error { .. } => Err("Invalid reference to step in a batch condition".into()),
        BatchCond::Not { cond } => check_cond(cond, step),
        BatchCond::And { conds } | BatchCond::Or { conds } => conds.iter().try_for_each(|c| check_cond(c, step)),
        BatchCond::IsAutocommit => {
            Err("BatchCond of type `is_autocommit` is only supported in protocol version hrana3 and higher".into())
        }
    }
}

/// All SQL texts of the requests (for slow-query logs, never values)
pub fn statements(requests: &[StreamRequest]) -> impl Iterator<Item = &str> {
    requests.iter().flat_map(|r| -> Box<dyn Iterator<Item = &str>> {
        match r {
            StreamRequest::Close => Box::new(std::iter::empty()),
            StreamRequest::Execute { stmt } => Box::new(stmt.sql.as_deref().into_iter()),
            StreamRequest::Batch { batch } => Box::new(batch.steps.iter().filter_map(|s| s.stmt.sql.as_deref())),
        }
    })
}

pub async fn run(
    conn: &mut Conn,
    requests: &[StreamRequest],
    initial_in_trans: bool,
    mut budget: u64,
    deadline: Instant,
    metrics: &NsMetrics,
    route: &crate::route::RouteCtx,
) -> Result<ExecResult, ExecAbort> {
    let mut results = Vec::with_capacity(requests.len());
    let mut closed = false;
    let mut in_trans = initial_in_trans;
    let mut transport_dirty = false;

    for req in requests {
        if Instant::now() >= deadline {
            return Err(ExecAbort::Deadline);
        }
        match req {
            StreamRequest::Close => {
                closed = true;
                if in_trans && !transport_dirty {
                    match with_deadline(deadline, conn.query_drop("ROLLBACK")).await {
                        Ok(Ok(())) => {
                            in_trans = false;
                        }
                        Ok(Err(_)) => {
                            transport_dirty = true;
                            in_trans = false;
                        }
                        Err(()) => return Err(ExecAbort::Deadline),
                    }
                }
                results.push(StreamResult::Ok {
                    response: StreamResponse::Close,
                });
            }
            StreamRequest::Execute { stmt } => {
                if transport_dirty {
                    results.push(StreamResult::Error {
                        error: proto::Error::new(
                            "Stream connection was invalidated by an earlier error",
                            "MYSQL_IO_ERROR",
                        ),
                    });
                    continue;
                }
                match run_stmt(
                    conn,
                    stmt,
                    &mut budget,
                    deadline,
                    metrics,
                    &mut in_trans,
                    &mut transport_dirty,
                    route,
                )
                .await?
                {
                    Ok(result) => results.push(StreamResult::Ok {
                        response: StreamResponse::Execute { result },
                    }),
                    Err(error) => {
                        metrics.errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        results.push(StreamResult::Error { error });
                    }
                }
            }
            StreamRequest::Batch { batch } => {
                if transport_dirty {
                    results.push(StreamResult::Error {
                        error: proto::Error::new(
                            "Stream connection was invalidated by an earlier error",
                            "MYSQL_IO_ERROR",
                        ),
                    });
                    continue;
                }
                match run_batch(
                    conn,
                    &batch.steps,
                    &mut budget,
                    deadline,
                    metrics,
                    &mut in_trans,
                    &mut transport_dirty,
                )
                .await?
                {
                    Ok(result) => results.push(StreamResult::Ok {
                        response: StreamResponse::Batch { result },
                    }),
                    Err(error) => {
                        metrics.errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        results.push(StreamResult::Error { error });
                    }
                }
            }
        }
    }

    Ok(ExecResult {
        results,
        closed,
        in_trans,
        transport_dirty,
    })
}

struct PreparedInput {
    rewritten_sql: String,
    params: Vec<MyVal>,
    kind: Kind,
}

fn prepare_input(stmt: &proto::Stmt) -> Result<PreparedInput, proto::Error> {
    let text = stmt.sql.as_deref().unwrap_or_default();
    let kind = sql::classify(text);
    if kind == Kind::Empty {
        return Err(proto::Error::new(
            "SQL string does not contain any statement",
            "SQL_NO_STATEMENT",
        ));
    }
    if sql::many_statements(text) {
        return Err(proto::Error::new(
            "SQL string contains more than one statement",
            "SQL_MANY_STATEMENTS",
        ));
    }
    let (rewritten_sql, args) = sql::rewrite_params(text, stmt)?;
    let params = args.into_iter().map(hrana_to_mysql).collect();
    Ok(PreparedInput {
        rewritten_sql,
        params,
        kind,
    })
}

async fn run_batch(
    conn: &mut Conn,
    steps: &[proto::BatchStep],
    budget: &mut u64,
    deadline: Instant,
    metrics: &NsMetrics,
    in_trans: &mut bool,
    transport_dirty: &mut bool,
) -> Result<Result<BatchResult, proto::Error>, ExecAbort> {
    let mut prepared = Vec::with_capacity(steps.len());
    for s in steps {
        match prepare_input(&s.stmt) {
            Ok(p) => prepared.push(p),
            Err(e) => return Ok(Err(e)),
        }
    }
    let mut ok = Vec::with_capacity(steps.len());
    let mut res = BatchResult::default();
    for (step, input) in steps.iter().zip(prepared) {
        let enabled = step.condition.as_ref().is_none_or(|c| eval(c, &ok));
        if !enabled {
            ok.push(false);
            res.step_results.push(None);
            res.step_errors.push(None);
            continue;
        }
        if *transport_dirty {
            ok.push(false);
            res.step_results.push(None);
            res.step_errors.push(Some(proto::Error::new(
                "Stream connection was invalidated by an earlier error",
                "MYSQL_IO_ERROR",
            )));
            continue;
        }
        match execute_prepared(conn, input, budget, deadline, metrics, in_trans, transport_dirty).await? {
            Ok(r) => {
                ok.push(true);
                res.step_results.push(Some(r));
                res.step_errors.push(None);
            }
            Err(e) if e.code == "RESPONSE_TOO_LARGE" => return Ok(Err(e)),
            Err(e) => {
                metrics.errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                ok.push(false);
                res.step_results.push(None);
                res.step_errors.push(Some(e));
            }
        }
    }
    Ok(Ok(res))
}

// Private execution plumbing: the argument list mirrors the request lifecycle (connection, statement,
// budget, deadline, metrics, transaction state, routing context). Bundling the mutable flags would spread
// a state struct across four functions for no behavioural gain.
#[allow(clippy::too_many_arguments)]
async fn run_stmt(
    conn: &mut Conn,
    stmt: &proto::Stmt,
    budget: &mut u64,
    deadline: Instant,
    metrics: &NsMetrics,
    in_trans: &mut bool,
    transport_dirty: &mut bool,
    route: &crate::route::RouteCtx,
) -> Result<Result<StmtResult, proto::Error>, ExecAbort> {
    let input = match prepare_input(stmt) {
        Ok(i) => i,
        Err(e) => return Ok(Err(e)),
    };
    // Routing is limited to a single autocommit SELECT with no bound parameters. Transactions stay pinned
    // to SeekDB, and batches are never routed.
    if let Some(view) = route.view.as_ref()
        && !*in_trans
        && input.params.is_empty()
        && input.kind == Kind::Select
    {
        // Gate order is cheapest-first: the structural gate is cached and does no I/O, so a query it
        // refuses must not cost either the freshness probe or the EXPLAIN round-trip.
        // Started before the gates so a routed query's reported duration includes the routing decision.
        // Counters are recorded only on the routed success path: a refused query falls through to
        // execute_prepared, which does its own counting, and must not be counted twice.
        let started = Instant::now();
        metrics
            .route_considered
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let admitted = tokio::task::block_in_place(|| crate::route::admit(view, &input.rewritten_sql, route.min_rows));
        if let Err(reason) = admitted {
            *metrics.route_refused.lock().entry(reason.as_str()).or_insert(0) += 1;
        } else if let Ok(routed) = admitted {
            // Freshness gate: the outbox is pruned only after the mirror commits, so an empty outbox means
            // every committed change is already in the mirror. Anything pending means a stale read.
            let pending: Option<i64> = conn
                .query_first(format!("SELECT COUNT(*) FROM {}", crate::capture::OUTBOX_TABLE))
                .await
                .unwrap_or(None);
            if pending == Some(0) {
                // Benefit gate on the real access path: a selective index lookup is far faster on SeekDB
                // than a DuckDB scan, so routing it would be a net loss even though provably equivalent.
                let explain: Vec<String> = conn
                    .query(format!("EXPLAIN {}", input.rewritten_sql))
                    .await
                    .unwrap_or_default();
                if crate::route::plan_profitable(&explain, route.min_rows) {
                    let executed = tokio::task::block_in_place(|| crate::route::execute_routed(view, &routed, started));
                    match executed {
                        Ok(r) => {
                            metrics.queries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            metrics.query.record(started.elapsed());
                            metrics.routed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            return Ok(Ok(r));
                        }
                        Err(reason) => {
                            *metrics.route_refused.lock().entry(reason.as_str()).or_insert(0) += 1;
                        }
                    }
                } else {
                    *metrics
                        .route_refused
                        .lock()
                        .entry(crate::route::Refused::NotProfitable.as_str())
                        .or_insert(0) += 1;
                }
            }
        }
    }
    execute_prepared(conn, input, budget, deadline, metrics, in_trans, transport_dirty).await
}

async fn execute_prepared(
    conn: &mut Conn,
    input: PreparedInput,
    budget: &mut u64,
    deadline: Instant,
    metrics: &NsMetrics,
    in_trans: &mut bool,
    transport_dirty: &mut bool,
) -> Result<Result<StmtResult, proto::Error>, ExecAbort> {
    metrics.queries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let started = Instant::now();

    let res = if input.params.is_empty() {
        execute_text(conn, &input.rewritten_sql, input.kind, budget, deadline, started).await
    } else {
        execute_binary(
            conn,
            &input.rewritten_sql,
            input.params,
            input.kind,
            budget,
            deadline,
            started,
        )
        .await
    };

    metrics.query.record(started.elapsed());

    match res {
        Err(ExecAbort::Deadline) => Err(ExecAbort::Deadline),
        Ok(Ok(stmt_res)) => {
            *in_trans = refresh_in_trans(conn, *in_trans, deadline, transport_dirty).await?;
            Ok(Ok(stmt_res))
        }
        Ok(Err((err, dirty))) => {
            if dirty {
                *transport_dirty = true;
                *in_trans = false;
            } else {
                *in_trans = refresh_in_trans(conn, *in_trans, deadline, transport_dirty).await?;
            }
            Ok(Err(err))
        }
    }
}

/// Checks `Conn::last_ok_packet()` for `SERVER_STATUS_IN_TRANS`. When an earlier statement returned a
/// non-fatal `ERR` packet, `mysql_async` clears `last_ok_packet`; if we were inside a transaction (or unsure),
/// a lightweight `COM_PING` refreshes the true live `StatusFlags` from the server.
async fn refresh_in_trans(
    conn: &mut Conn,
    was_in_trans: bool,
    deadline: Instant,
    transport_dirty: &mut bool,
) -> Result<bool, ExecAbort> {
    if let Some(ok) = conn.last_ok_packet() {
        return Ok(ok.status_flags().contains(StatusFlags::SERVER_STATUS_IN_TRANS));
    }
    if !was_in_trans {
        return Ok(false);
    }
    match with_deadline(deadline, conn.ping()).await {
        Ok(Ok(())) => Ok(conn
            .last_ok_packet()
            .is_some_and(|ok| ok.status_flags().contains(StatusFlags::SERVER_STATUS_IN_TRANS))),
        Ok(Err(e)) => {
            if e.is_fatal() {
                *transport_dirty = true;
                Ok(false)
            } else {
                Ok(was_in_trans)
            }
        }
        Err(()) => Err(ExecAbort::Deadline),
    }
}

async fn execute_text(
    conn: &mut Conn,
    sql: &str,
    kind: Kind,
    budget: &mut u64,
    deadline: Instant,
    started: Instant,
) -> Result<Result<StmtResult, (proto::Error, bool)>, ExecAbort> {
    let qr = match with_deadline(deadline, conn.query_iter(sql)).await {
        Ok(Ok(qr)) => qr,
        Ok(Err(e)) => return Ok(Err(map_mysql_err(e))),
        Err(()) => return Err(ExecAbort::Deadline),
    };
    collect_query_result(qr, kind, budget, deadline, started).await
}

async fn execute_binary(
    conn: &mut Conn,
    sql: &str,
    params: Vec<MyVal>,
    kind: Kind,
    budget: &mut u64,
    deadline: Instant,
    started: Instant,
) -> Result<Result<StmtResult, (proto::Error, bool)>, ExecAbort> {
    let stmt = match with_deadline(deadline, conn.prep(sql)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Ok(Err(map_mysql_err(e))),
        Err(()) => return Err(ExecAbort::Deadline),
    };
    let qr = match with_deadline(deadline, conn.exec_iter(&stmt, Params::Positional(params))).await {
        Ok(Ok(qr)) => qr,
        Ok(Err(e)) => return Ok(Err(map_mysql_err(e))),
        Err(()) => return Err(ExecAbort::Deadline),
    };
    collect_query_result(qr, kind, budget, deadline, started).await
}

async fn collect_query_result<'a, 't: 'a, P: mysql_async::prelude::Protocol>(
    mut qr: QueryResult<'a, 't, P>,
    kind: Kind,
    budget: &mut u64,
    deadline: Instant,
    started: Instant,
) -> Result<Result<StmtResult, (proto::Error, bool)>, ExecAbort> {
    let raw_cols: Vec<Column> = qr.columns_ref().to_vec();
    let cols: Vec<Col> = raw_cols
        .iter()
        .map(|c| Col {
            name: Some(c.name_str().into_owned()),
            decltype: Some(decltype_name(c).to_owned()),
        })
        .collect();

    let mut rows = Vec::new();
    loop {
        let next = match with_deadline(deadline, qr.next()).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => return Ok(Err(map_mysql_err(e))),
            Err(()) => return Err(ExecAbort::Deadline),
        };
        let Some(row) = next else {
            break;
        };
        let mut vals = row.unwrap_raw();
        let mut out_row = Vec::with_capacity(vals.len());
        for (idx, cell) in vals.drain(..).enumerate() {
            let v = match cell {
                None | Some(MyVal::NULL) => Value::Null,
                Some(mv) => match raw_cols.get(idx) {
                    Some(col) => mysql_val_to_hrana(mv, col),
                    None => Value::Null,
                },
            };
            let sz = hrana_value_size(&v);
            if sz > *budget {
                // Drop unread stream cleanly by marking connection dirty so caller disconnects it
                // rather than blocking to drain an unbounded oversized result set.
                return Ok(Err((
                    proto::Error::new("Response is too large", "RESPONSE_TOO_LARGE"),
                    true,
                )));
            }
            *budget -= sz;
            out_row.push(v);
        }
        rows.push(out_row);
    }

    let raw_affected = if kind.is_dml() { qr.affected_rows() } else { 0 };
    let last_insert_rowid = if kind.is_insert() {
        qr.last_insert_id()
            .filter(|&id| id > 0)
            .and_then(|id| i64::try_from(id).ok())
    } else {
        None
    };

    // Drain any trailing extra result sets if present (e.g. CALL)
    if !qr.is_empty() {
        match with_deadline(deadline, qr.drop_result()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Ok(Err(map_mysql_err(e))),
            Err(()) => return Err(ExecAbort::Deadline),
        }
    }

    let rows_read = rows.len() as u64;

    Ok(Ok(StmtResult {
        cols,
        rows,
        affected_row_count: raw_affected,
        last_insert_rowid,
        replication_index: None,
        rows_read,
        rows_written: raw_affected,
        query_duration_ms: started.elapsed().as_micros() as f64 / 1000.0,
    }))
}

async fn with_deadline<F, T>(deadline: Instant, fut: F) -> Result<T, ()>
where
    F: std::future::Future<Output = T>,
{
    let Some(rem) = deadline.checked_duration_since(Instant::now()) else {
        return Err(());
    };
    if rem == Duration::ZERO {
        return Err(());
    }
    tokio::time::timeout(rem, fut).await.map_err(|_| ())
}

fn hrana_to_mysql(v: Value) -> MyVal {
    match v {
        Value::Null => MyVal::NULL,
        Value::Integer(i) => MyVal::Int(i),
        Value::Float(f) => MyVal::Double(f),
        Value::Text(s) => MyVal::Bytes(s.into_bytes()),
        Value::Blob(b) => MyVal::Bytes(b),
    }
}

/// Converts a MySQL value + column metadata (working for BOTH TextProtocol byte payloads and
/// BinaryProtocol typed payloads) into a Hrana JSON `Value`.
pub fn mysql_val_to_hrana(val: MyVal, col: &Column) -> Value {
    let ty = col.column_type();
    match val {
        MyVal::NULL => Value::Null,
        MyVal::Int(i) => Value::Integer(i),
        MyVal::UInt(u) => match i64::try_from(u) {
            Ok(i) => Value::Integer(i),
            Err(_) => Value::Text(u.to_string()),
        },
        MyVal::Float(f) => {
            let d = f as f64;
            if d.is_finite() { Value::Float(d) } else { Value::Null }
        }
        MyVal::Double(d) => {
            if d.is_finite() {
                Value::Float(d)
            } else {
                Value::Null
            }
        }
        MyVal::Date(y, m, d, hh, mm, ss, us) => {
            if matches!(ty, ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE) {
                Value::Text(format!("{y:04}-{m:02}-{d:02}"))
            } else if us > 0 {
                Value::Text(format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}.{us:06}"))
            } else {
                Value::Text(format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}"))
            }
        }
        MyVal::Time(neg, days, h, m, s, us) => {
            let total_h = days as u64 * 24 + h as u64;
            let sign = if neg { "-" } else { "" };
            if us > 0 {
                Value::Text(format!("{sign}{total_h:02}:{m:02}:{s:02}.{us:06}"))
            } else {
                Value::Text(format!("{sign}{total_h:02}:{m:02}:{s:02}"))
            }
        }
        MyVal::Bytes(b) => match ty {
            ColumnType::MYSQL_TYPE_TINY
            | ColumnType::MYSQL_TYPE_SHORT
            | ColumnType::MYSQL_TYPE_INT24
            | ColumnType::MYSQL_TYPE_LONG
            | ColumnType::MYSQL_TYPE_LONGLONG
            | ColumnType::MYSQL_TYPE_YEAR => match std::str::from_utf8(&b) {
                Ok(s) => {
                    if let Ok(i) = s.parse::<i64>() {
                        Value::Integer(i)
                    } else {
                        Value::Text(s.to_owned())
                    }
                }
                Err(_) => Value::Blob(b),
            },
            ColumnType::MYSQL_TYPE_FLOAT | ColumnType::MYSQL_TYPE_DOUBLE => match std::str::from_utf8(&b) {
                Ok(s) => match s.parse::<f64>() {
                    Ok(f) if f.is_finite() => Value::Float(f),
                    Ok(_) => Value::Null,
                    Err(_) => Value::Text(s.to_owned()),
                },
                Err(_) => Value::Blob(b),
            },
            ColumnType::MYSQL_TYPE_DECIMAL
            | ColumnType::MYSQL_TYPE_NEWDECIMAL
            | ColumnType::MYSQL_TYPE_DATE
            | ColumnType::MYSQL_TYPE_NEWDATE
            | ColumnType::MYSQL_TYPE_DATETIME
            | ColumnType::MYSQL_TYPE_DATETIME2
            | ColumnType::MYSQL_TYPE_TIMESTAMP
            | ColumnType::MYSQL_TYPE_TIMESTAMP2
            | ColumnType::MYSQL_TYPE_TIME
            | ColumnType::MYSQL_TYPE_TIME2
            | ColumnType::MYSQL_TYPE_JSON
            | ColumnType::MYSQL_TYPE_ENUM
            | ColumnType::MYSQL_TYPE_SET => Value::Text(String::from_utf8_lossy(&b).into_owned()),
            ColumnType::MYSQL_TYPE_BIT => Value::Blob(b),
            _ => {
                // charset 63 is MySQL's `binary` collation (BLOB, VARBINARY, BINARY, UNHEX, etc.)
                if col.character_set() == 63
                    || (col.flags().contains(ColumnFlags::BINARY_FLAG)
                        && matches!(
                            ty,
                            ColumnType::MYSQL_TYPE_TINY_BLOB
                                | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
                                | ColumnType::MYSQL_TYPE_LONG_BLOB
                                | ColumnType::MYSQL_TYPE_BLOB
                        ))
                {
                    Value::Blob(b)
                } else {
                    Value::Text(String::from_utf8_lossy(&b).into_owned())
                }
            }
        },
    }
}

fn decltype_name(col: &Column) -> &'static str {
    match col.column_type() {
        ColumnType::MYSQL_TYPE_TINY
        | ColumnType::MYSQL_TYPE_SHORT
        | ColumnType::MYSQL_TYPE_INT24
        | ColumnType::MYSQL_TYPE_LONG
        | ColumnType::MYSQL_TYPE_LONGLONG
        | ColumnType::MYSQL_TYPE_YEAR => "INTEGER",
        ColumnType::MYSQL_TYPE_FLOAT | ColumnType::MYSQL_TYPE_DOUBLE => "REAL",
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => "DECIMAL",
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => "DATE",
        ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_DATETIME2 => "DATETIME",
        ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_TIMESTAMP2 => "TIMESTAMP",
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => "TIME",
        ColumnType::MYSQL_TYPE_JSON => "JSON",
        ColumnType::MYSQL_TYPE_BIT | ColumnType::MYSQL_TYPE_GEOMETRY => "BLOB",
        _ => {
            if col.character_set() == 63 {
                "BLOB"
            } else {
                "TEXT"
            }
        }
    }
}

/// Rough JSON size of a Hrana value for enforcing `--max-response-bytes`.
fn hrana_value_size(v: &Value) -> u64 {
    match v {
        Value::Null => 15,
        Value::Integer(_) => 32,
        Value::Float(_) => 32,
        Value::Text(s) => 28 + s.len() as u64,
        Value::Blob(b) => 30 + (b.len() as u64).div_ceil(3) * 4,
    }
}

fn eval(c: &BatchCond, ok: &[bool]) -> bool {
    match c {
        BatchCond::Ok { step } => ok.get(*step as usize).copied().unwrap_or(false),
        BatchCond::Error { step } => !ok.get(*step as usize).copied().unwrap_or(false),
        BatchCond::Not { cond } => !eval(cond, ok),
        BatchCond::And { conds } => conds.iter().all(|c| eval(c, ok)),
        BatchCond::Or { conds } => conds.iter().any(|c| eval(c, ok)),
        BatchCond::IsAutocommit => false,
    }
}

fn map_mysql_err(err: mysql_async::Error) -> (proto::Error, bool) {
    let dirty = err.is_fatal();
    let proto_err = match &err {
        mysql_async::Error::Server(se) => {
            let code = if se.code == 1064 {
                "SQL_PARSE_ERROR".to_owned()
            } else {
                format!("MYSQL_{}", se.code)
            };
            proto::Error::new(format!("MySQL error {} ({}): {}", se.code, se.state, se.message), &code)
        }
        other => proto::Error::new(format!("MySQL client error: {other}"), "MYSQL_IO_ERROR"),
    };
    (proto_err, dirty)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_col(ty: ColumnType, charset: u16) -> Column {
        Column::new(ty).with_character_set(charset)
    }

    #[test]
    fn text_and_binary_type_mapping() {
        // TextProtocol integer bytes
        let c_int = mk_col(ColumnType::MYSQL_TYPE_LONGLONG, 63);
        assert_eq!(
            mysql_val_to_hrana(MyVal::Bytes(b"-42".to_vec()), &c_int),
            Value::Integer(-42)
        );
        // Unsigned integer > i64::MAX falls back to text
        let big = (u64::MAX).to_string();
        assert_eq!(
            mysql_val_to_hrana(MyVal::Bytes(big.as_bytes().to_vec()), &c_int),
            Value::Text(big.clone())
        );
        assert_eq!(mysql_val_to_hrana(MyVal::UInt(u64::MAX), &c_int), Value::Text(big));
        assert_eq!(mysql_val_to_hrana(MyVal::UInt(99), &c_int), Value::Integer(99));

        // Double / Decimal
        let c_dbl = mk_col(ColumnType::MYSQL_TYPE_DOUBLE, 63);
        assert_eq!(
            mysql_val_to_hrana(MyVal::Bytes(b"3.25".to_vec()), &c_dbl),
            Value::Float(3.25)
        );
        assert_eq!(mysql_val_to_hrana(MyVal::Double(f64::NAN), &c_dbl), Value::Null);

        let c_dec = mk_col(ColumnType::MYSQL_TYPE_NEWDECIMAL, 63);
        assert_eq!(
            mysql_val_to_hrana(MyVal::Bytes(b"12345.6789".to_vec()), &c_dec),
            Value::Text("12345.6789".into())
        );

        // Date / Datetime / Time
        let c_date = mk_col(ColumnType::MYSQL_TYPE_DATE, 63);
        assert_eq!(
            mysql_val_to_hrana(MyVal::Date(2026, 11, 22, 0, 0, 0, 0), &c_date),
            Value::Text("2026-11-22".into())
        );
        let c_dt = mk_col(ColumnType::MYSQL_TYPE_DATETIME, 63);
        assert_eq!(
            mysql_val_to_hrana(MyVal::Date(2026, 11, 22, 14, 5, 9, 1234), &c_dt),
            Value::Text("2026-11-22 14:05:09.001234".into())
        );
        let c_time = mk_col(ColumnType::MYSQL_TYPE_TIME, 63);
        assert_eq!(
            mysql_val_to_hrana(MyVal::Time(true, 1, 2, 3, 4, 0), &c_time),
            Value::Text("-26:03:04".into())
        );

        // Text vs Blob via charset 63 vs 45 (utf8mb4)
        let c_txt = mk_col(ColumnType::MYSQL_TYPE_VAR_STRING, 45);
        assert_eq!(
            mysql_val_to_hrana(MyVal::Bytes("你好".as_bytes().to_vec()), &c_txt),
            Value::Text("你好".into())
        );
        let c_blob = mk_col(ColumnType::MYSQL_TYPE_BLOB, 63);
        assert_eq!(
            mysql_val_to_hrana(MyVal::Bytes(vec![0, 1, 2]), &c_blob),
            Value::Blob(vec![0, 1, 2])
        );
        let c_json = mk_col(ColumnType::MYSQL_TYPE_JSON, 63);
        assert_eq!(
            mysql_val_to_hrana(MyVal::Bytes(b"{\"k\":1}".to_vec()), &c_json),
            Value::Text("{\"k\":1}".into())
        );
    }

    #[test]
    fn validates_pipeline_requests_and_conditions() {
        let ok_cond = BatchCond::And {
            conds: vec![
                BatchCond::Ok { step: 0 },
                BatchCond::Not {
                    cond: Box::new(BatchCond::Error { step: 1 }),
                },
            ],
        };
        assert!(check_cond(&ok_cond, 2).is_ok());
        assert!(check_cond(&BatchCond::Ok { step: 2 }, 2).is_err());
        assert!(check_cond(&BatchCond::IsAutocommit, 2).is_err());

        let bad_seq = vec![
            StreamRequest::Close,
            StreamRequest::Execute {
                stmt: proto::Stmt {
                    sql: Some("SELECT 1".into()),
                    sql_id: None,
                    args: vec![],
                    named_args: vec![],
                    want_rows: None,
                },
            },
        ];
        assert!(validate(&bad_seq, 100).is_err());
    }
}
