//! MySQL protocol server: accept MySQL clients and run their statements through the same
//! execution path the HTTP gateway uses, so HTAP routing, fallback, budgets and metrics are shared.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::backend::Backend;
use crate::config::Config;
use crate::proto::{Col, Stmt, StmtResult, StreamRequest, StreamResponse, Value};

const PROTOCOL_VERSION: u8 = 10;
const COM_QUIT: u8 = 0x01;
const COM_INIT_DB: u8 = 0x02;
const COM_QUERY: u8 = 0x03;
const COM_PING: u8 = 0x0e;
const SERVER_STATUS_NO_TRANSACTIONS: u16 = 0x0002;
/// Capability bits as `mysql_common` and every MySQL client number them.
const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
const CLIENT_SSL: u32 = 0x0000_0800;
const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x0020_0000;
const CLIENT_DEPRECATE_EOF: u32 = 0x0100_0000;
/// What xsqld actually implements: the v4.1 framing, an optional database name, and a plugin-named
/// native-password handshake whose response is never checked. xsqld reaches the backend with its own
/// `--mysql-user`, so a client credential opens the gateway without proving anything: the listener is
/// only as trusted as the network it is bound to (`--allow-non-loopback` is what makes it reachable).
const SERVER_CAPABILITIES: u32 = CLIENT_LONG_PASSWORD
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_PROTOCOL_41
    | CLIENT_SECURE_CONNECTION
    | CLIENT_PLUGIN_AUTH
    | CLIENT_DEPRECATE_EOF;
/// A payload chunk of this exact length means "more chunks follow", per the MySQL wire format.
const MAX_CHUNK: usize = 0xFF_FFFF;
const MAX_MESSAGE: usize = 256 * 1024 * 1024;

static STREAM_IDS: AtomicU64 = AtomicU64::new(1);

struct MysqlConnection {
    stream: TcpStream,
    seq_id: u8,
    /// One id per MySQL connection so a session cannot collide with another connection's stream.
    stream_id: u64,
    deprecate_eof: bool,
    /// True while the gateway holds a backend connection for this client. A MySQL session is one
    /// server thread, so `LAST_INSERT_ID()`, `@vars` and SQL-level `PREPARE` only work if every
    /// statement of a connection runs on the same backend connection.
    held: bool,
    /// True while a transaction is open on it; decides whether a SELECT may be routed to the mirror.
    in_trans: bool,
}

impl MysqlConnection {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            seq_id: 0,
            stream_id: STREAM_IDS.fetch_add(1, Ordering::Relaxed),
            deprecate_eof: false,
            held: false,
            in_trans: false,
        }
    }

    /// Hands back the backend connection when the session moves elsewhere or ends.
    async fn drop_session(&mut self, backend: &Backend) {
        if self.held {
            backend.release_stream(self.stream_id).await;
            self.held = false;
            self.in_trans = false;
        }
    }

    async fn write_packet(&mut self, data: &[u8]) -> io::Result<()> {
        let mut pkt = Vec::with_capacity(4 + data.len());
        put_u24(&mut pkt, data.len());
        pkt.put_u8(self.seq_id);
        pkt.put_slice(data);
        self.seq_id = self.seq_id.wrapping_add(1);
        self.stream.write_all(&pkt).await
    }

    /// Reads one framed packet, keeping the payload of a longer message intact for `read_message`.
    async fn read_packet(&mut self, buf: &mut BytesMut) -> Result<Option<Vec<u8>>> {
        loop {
            if buf.len() >= 4 {
                let len = u32::from_le_bytes([buf[0], buf[1], buf[2], 0]) as usize;
                if buf.len() >= 4 + len {
                    buf.advance(3);
                    let seq = buf.get_u8();
                    self.seq_id = seq.wrapping_add(1);
                    let pkt = buf.split_to(len).to_vec();
                    return Ok(Some(pkt));
                }
            }
            let n = self.stream.read_buf(buf).await?;
            if n == 0 {
                return if buf.is_empty() { Ok(None) } else { bail!("incomplete packet") };
            }
        }
    }

    /// Reads a whole client message, concatenating continuation chunks of a long payload.
    async fn read_message(&mut self, buf: &mut BytesMut) -> Result<Option<Vec<u8>>> {
        let Some(first) = self.read_packet(buf).await? else {
            return Ok(None);
        };
        if first.len() != MAX_CHUNK {
            return Ok(Some(first));
        }
        let mut message = first;
        loop {
            let Some(chunk) = self.read_packet(buf).await? else {
                bail!("connection closed mid long message");
            };
            message.extend_from_slice(&chunk);
            if message.len() > MAX_MESSAGE {
                bail!("message larger than {MAX_MESSAGE} bytes");
            }
            if chunk.len() != MAX_CHUNK {
                return Ok(Some(message));
            }
        }
    }

    async fn ok(&mut self, affected_rows: u64, last_insert_id: u64) -> io::Result<()> {
        let mut pkt = Vec::new();
        pkt.put_u8(0x00);
        put_lenenc_int(&mut pkt, affected_rows);
        put_lenenc_int(&mut pkt, last_insert_id);
        pkt.put_u16_le(SERVER_STATUS_NO_TRANSACTIONS);
        pkt.put_u16_le(0);
        self.write_packet(&pkt).await
    }

    async fn err(&mut self, code: u16, message: &str) -> io::Result<()> {
        self.err_state(code, "HY000", message).await
    }

    async fn err_state(&mut self, code: u16, sql_state: &str, message: &str) -> io::Result<()> {
        let mut pkt = Vec::new();
        pkt.put_u8(0xff);
        pkt.put_u16_le(code);
        pkt.put_u8(b'#');
        pkt.put_slice(sql_state.as_bytes());
        pkt.put_slice(message.as_bytes());
        self.write_packet(&pkt).await
    }

    /// End of the column definitions. A client that negotiated `CLIENT_DEPRECATE_EOF` counts
    /// `column_count` packets itself and expects the next packet to be the first row, so nothing is
    /// sent; classic clients need the terminator.
    async fn metadata_terminator(&mut self) -> io::Result<()> {
        if self.deprecate_eof {
            return Ok(());
        }
        self.eof().await
    }

    /// End of the rows. Always an EOF packet, matching what mysqld puts on the wire.
    async fn eof(&mut self) -> io::Result<()> {
        self.write_packet(&[0xfe, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]).await
    }

    async fn columns(&mut self, cols: &[Col]) -> io::Result<()> {
        let mut count = Vec::new();
        put_lenenc_int(&mut count, cols.len() as u64);
        self.write_packet(&count).await?;
        for col in cols {
            let (field_type, flags, decimals) = mysql_type(col.decltype.as_deref());
            let mut pkt = Vec::new();
            put_lenenc_str(&mut pkt, b"def");
            put_lenenc_str(&mut pkt, b"");
            put_lenenc_str(&mut pkt, b"");
            put_lenenc_str(&mut pkt, b"");
            put_lenenc_str(&mut pkt, col.name.as_deref().unwrap_or("").as_bytes());
            put_lenenc_str(&mut pkt, col.name.as_deref().unwrap_or("").as_bytes());
            pkt.put_u8(0x0c); // the fixed block that follows is exactly 2+4+1+2+1+2 = 12 bytes
            // 63 is the *binary* collation, not utf8mb4: advertising it for a text column makes
            // clients hand back raw bytes instead of decoded strings.
            pkt.put_u16_le(if field_type == 0xfc { 63 } else { 45 });
            pkt.put_u32_le(1024);
            pkt.put_u8(field_type);
            pkt.put_u16_le(flags);
            pkt.put_u8(decimals);
            pkt.put_u16_le(0);
            self.write_packet(&pkt).await?;
        }
        self.metadata_terminator().await
    }

    async fn rows(&mut self, rows: &[Vec<Value>]) -> io::Result<()> {
        for row in rows {
            let mut pkt = Vec::new();
            for value in row {
                put_text_value(&mut pkt, value);
            }
            self.write_packet(&pkt).await?;
        }
        self.eof().await
    }
}

/// Accept MySQL connections and dispatch queries.
pub async fn serve(
    listener: tokio::net::TcpListener,
    backend: Arc<Backend>,
    cfg: Arc<Config>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let slots = Arc::new(tokio::sync::Semaphore::new(cfg.max_connections));
    loop {
        let slot = tokio::select! {
            s = slots.clone().acquire_owned() => s.expect("semaphore is never closed"),
            _ = stop.changed() => break,
        };
        let stream = tokio::select! {
            r = listener.accept() => match r {
                Ok((stream, _)) => stream,
                Err(e) => {
                    tracing::warn!(error = %e, "mysql accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            _ = stop.changed() => break,
        };
        let _ = stream.set_nodelay(true);
        let backend = backend.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move {
            let _slot = slot;
            if let Err(e) = handle_connection(stream, backend, cfg).await {
                tracing::debug!(error = %e, "mysql connection error");
            }
        });
    }
}

async fn handle_connection(stream: TcpStream, backend: Arc<Backend>, cfg: Arc<Config>) -> Result<()> {
    let mut conn = MysqlConnection::new(stream);

    let mut salt = [0u8; 20];
    getrandom::fill(&mut salt).context("failed to generate auth salt")?;
    conn.write_packet(&build_handshake(&salt, backend.backend_version.as_deref())).await?;

    let mut buf = BytesMut::with_capacity(1024);
    let Some(auth) = conn.read_message(&mut buf).await? else {
        return Ok(());
    };
    // A client that wants TLS sends this same 32-byte prefix with no username and waits for a
    // continuation octet xsqld never sends, so name the real reason rather than a malformed packet.
    if auth.len() == 32 && client_caps(&auth).is_some_and(|c| c & CLIENT_SSL != 0) {
        return conn.err(1251, "xsqld does not support SSL connections").await.map_err(Into::into);
    }
    let Some((caps, namespace)) = parse_auth(&auth) else {
        return conn.err(1043, "Unsupported handshake").await.map_err(Into::into);
    };
    conn.deprecate_eof = caps & CLIENT_DEPRECATE_EOF != 0;
    let mut namespace = namespace.unwrap_or_else(|| "default".to_owned());
    // Refuse an unknown namespace during the handshake, as a MySQL server refuses an unknown database
    // given with -D: accepting the connection and then failing every statement would leave the client
    // believing its schema was selected.
    if backend.get_namespace(&namespace).await?.is_none() {
        return conn.err(1049, &format!("Unknown database '{namespace}'")).await.map_err(Into::into);
    }
    tracing::debug!(ns = %namespace, deprecate_eof = conn.deprecate_eof, "mysql client connected");
    conn.ok(0, 0).await?;

    buf.clear();
    // The loop runs in a block so the session is handed back on a read error too: a client that
    // unplugs rather than sending COM_QUIT would otherwise keep its backend connection forever,
    // since an idle session is deliberately exempt from the transaction reaper.
    let ran: Result<()> = async {
        loop {
            let Some(pkt) = conn.read_message(&mut buf).await? else {
                break;
            };
            let Some(&cmd) = pkt.first() else {
                continue;
            };
            match cmd {
                COM_QUIT => break,
                COM_INIT_DB => {
                    let ns = String::from_utf8_lossy(&pkt[1..]).to_string();
                    let switched = switch_namespace(&mut namespace, &ns, &backend).await;
                    if switched.is_ok() {
                        conn.drop_session(&backend).await;
                    }
                    reply_switch(&mut conn, switched).await?;
                }
                COM_QUERY => {
                    let query = String::from_utf8_lossy(&pkt[1..]).to_string();
                    // A schema is the namespace here, so `USE` has to move this connection. Forwarding it
                    // would report success while the gateway kept qualifying names with the old namespace,
                    // which reads the wrong schema silently.
                    if let Some(target) = use_target(&query) {
                        let switched = switch_namespace(&mut namespace, target, &backend).await;
                        if switched.is_ok() {
                            // The held connection belongs to the old schema's pool, and a transaction
                            // cannot move between backend connections; it is rolled back, not carried.
                            conn.drop_session(&backend).await;
                        }
                        reply_switch(&mut conn, switched).await?;
                        continue;
                    }
                    if let Err(e) = run_statement(&mut conn, &backend, &cfg, &namespace, &query).await {
                        tracing::debug!(ns = %namespace, query = %query, error = %e, "mysql reply failed");
                        break;
                    }
                }
                COM_PING => conn.ok(0, 0).await?,
                _ => conn.err(1047, "Unsupported command").await?,
            }
        }
        Ok(())
    }
    .await;

    conn.drop_session(&backend).await;
    ran
}

/// Points the connection at another namespace. The schema doubles as the namespace, so it has to
/// already exist: accepting an unknown name would let a client believe it were somewhere else.
async fn switch_namespace(current: &mut String, target: &str, backend: &Backend) -> Result<(), String> {
    match backend.get_namespace(target).await {
        Ok(Some(_)) => {
            *current = target.to_owned();
            tracing::debug!(ns = %target, "mysql connection switched namespace");
            Ok(())
        }
        Ok(None) => Err(format!("Unknown database '{target}'")),
        Err(e) => Err(e.to_string()),
    }
}

async fn reply_switch(conn: &mut MysqlConnection, result: Result<(), String>) -> io::Result<()> {
    match result {
        Ok(()) => conn.ok(0, 0).await,
        Err(message) => conn.err(1049, &message).await,
    }
}

/// The schema named by a `USE` statement, when that is all the statement is.
fn use_target(query: &str) -> Option<&str> {
    let (keyword, target) = query
        .trim()
        .trim_end_matches(';')
        .trim_end()
        .split_once(char::is_whitespace)?;
    if !keyword.eq_ignore_ascii_case("use") {
        return None;
    }
    let target = target.trim().trim_end_matches(';').trim();
    let quoted = |marker: char| target.strip_prefix(marker).and_then(|t| t.strip_suffix(marker));
    if let Some(name) = quoted('`').or_else(|| quoted('"')).or_else(|| quoted('\'')) {
        return (!name.is_empty() && !name.contains(';')).then_some(name);
    }
    // an unquoted schema name is an identifier; anything else is not worth guessing at
    (!target.is_empty() && target.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$'))
        .then_some(target)
}

/// Runs one statement through the gateway pipeline and writes the reply. Errors are reported to the
/// client, so a failure here only means the packet could not be written: the connection stays usable.
async fn run_statement(
    conn: &mut MysqlConnection,
    backend: &Arc<Backend>,
    cfg: &Arc<Config>,
    namespace: &str,
    query: &str,
) -> Result<()> {
    let ns_pool = match backend.get_namespace(namespace).await {
        Ok(Some(pool)) => pool,
        Ok(None) => {
            return conn.err(1049, &format!("Unknown database '{namespace}'")).await.map_err(Into::into);
        }
        Err(e) => return conn.err(1049, &format!("{e}")).await.map_err(Into::into),
    };

    let stmt = Stmt {
        sql: Some(query.to_owned()),
        sql_id: None,
        args: Vec::new(),
        named_args: Vec::new(),
        want_rows: None,
    };
    let requests = vec![StreamRequest::Execute { stmt }];
    let deadline = std::time::Instant::now() + cfg.request_timeout();
    let outcome = backend
        .run_pipeline(
            &ns_pool,
            conn.stream_id,
            crate::backend::StreamState {
                held: conn.held,
                in_trans: conn.in_trans,
                session: true,
            },
            requests,
            deadline,
        )
        .await;

    let crate::backend::Outcome::Done {
        results,
        pinned,
        in_trans,
        ..
    } = outcome
    else {
        // Whatever the gateway did with the stream, this connection no longer holds one.
        conn.held = false;
        conn.in_trans = false;
        let (code, message) = match outcome {
            crate::backend::Outcome::Deadline => (1205, "Request timeout: the statement was not run".to_owned()),
            crate::backend::Outcome::StreamExpired => (1047, "Stream expired".to_owned()),
            _ => (1049, format!("Namespace `{}` is not available", ns_pool.name)),
        };
        return conn.err(code, &message).await.map_err(Into::into);
    };
    conn.held = pinned;
    conn.in_trans = in_trans;

    let Some(result) = results.first() else {
        return conn.err(1105, "The statement produced no result").await.map_err(Into::into);
    };
    match result {
        crate::proto::StreamResult::Ok { response } => match response {
            StreamResponse::Execute { result } => write_stmt_result(conn, result).await.map_err(Into::into),
            StreamResponse::Batch { .. } | StreamResponse::Close => {
                conn.err(1105, "Unexpected response for a single statement").await.map_err(Into::into)
            }
        },
        crate::proto::StreamResult::Error { error } => {
            let (errno, sql_state) = server_error(&error.code, &error.message);
            conn.err_state(errno, sql_state, &error.message).await.map_err(Into::into)
        }
    }
}

async fn write_stmt_result(conn: &mut MysqlConnection, result: &StmtResult) -> io::Result<()> {
    if result.cols.is_empty() {
        let last_insert_id = u64::try_from(result.last_insert_rowid.unwrap_or(0)).unwrap_or(0);
        return conn.ok(result.affected_row_count, last_insert_id).await;
    }
    conn.columns(&result.cols).await?;
    conn.rows(&result.rows).await
}

/// The capability bits a client asked for, or nothing when the packet is too short to hold them.
fn client_caps(pkt: &[u8]) -> Option<u32> {
    Some(u32::from_le_bytes(<[u8; 4]>::try_from(pkt.get(..4)?).ok()?))
}

/// Splits HandshakeResponse41 into the negotiated capabilities and the namespace to run against,
/// which is the database the client named with `-D` when it sent one and otherwise its username.
/// A field is present only when both sides advertise it, so the negotiation decides how to walk the
/// packet; the fixed 32-byte prefix and the NUL terminated username are always there under 4.1.
fn parse_auth(pkt: &[u8]) -> Option<(u32, Option<String>)> {
    if pkt.len() < 32 {
        return None;
    }
    let negotiated = client_caps(pkt)? & SERVER_CAPABILITIES;
    if negotiated & CLIENT_PROTOCOL_41 == 0 {
        return None;
    }
    let mut rest = &pkt[32..];
    let user = take_nul(&mut rest)?;
    if negotiated & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
        let (len, used) = read_lenenc(rest)?;
        let skip = used + usize::try_from(len).ok()?;
        rest = rest.get(skip..)?;
    } else if negotiated & CLIENT_SECURE_CONNECTION != 0 {
        let len = usize::from(*rest.first()?);
        rest = rest.get(1 + len..)?;
    } else {
        take_nul(&mut rest)?;
    }
    let db = if negotiated & CLIENT_CONNECT_WITH_DB != 0 { take_nul(&mut rest)? } else { "" };
    let ns = if db.is_empty() { user.trim() } else { db.trim() };
    Some((negotiated, if ns.is_empty() { None } else { Some(ns.to_owned()) }))
}

/// Reads a NUL terminated string and advances past its terminator.
fn take_nul<'a>(rest: &mut &'a [u8]) -> Option<&'a str> {
    let end = rest.iter().position(|&b| b == 0)?;
    let s = std::str::from_utf8(&rest[..end]).ok()?;
    *rest = rest.get(end + 1..)?;
    Some(s)
}

/// Returns the value and the number of bytes it occupied.
fn read_lenenc(buf: &[u8]) -> Option<(u64, usize)> {
    match *buf.first()? {
        b if b < 251 => Some((u64::from(b), 1)),
        0xfc => Some((u64::from(u16::from_le_bytes(buf.get(1..3)?.try_into().ok()?)), 3)),
        0xfd => {
            let b = buf.get(1..4)?;
            Some((u64::from(u32::from_le_bytes([b[0], b[1], b[2], 0])), 4))
        }
        0xfe => Some((u64::from_le_bytes(buf.get(1..9)?.try_into().ok()?), 9)),
        _ => None,
    }
}

/// Clients gate their own behaviour on the reported version (a major below 5 switches off
/// multi-result handling, for instance), so the backend's version is reported when it is known:
/// the statements are forwarded there, and xsqld does not change what they can return.
fn build_handshake(salt: &[u8; 20], backend_version: Option<&str>) -> Vec<u8> {
    let mut buf = Vec::new();
    let version = match backend_version {
        Some(v) => format!("{v}-xsqld-{}\0", crate::config::VERSION),
        None => format!("{}-xsqld\0", crate::config::VERSION),
    };
    buf.put_u8(PROTOCOL_VERSION);
    buf.put_slice(version.as_bytes());
    buf.put_u32_le(0x1234); // connection id
    buf.put_slice(&salt[..8]); // auth-plugin-data part 1
    buf.put_u8(0); // filler
    buf.put_u16_le(SERVER_CAPABILITIES as u16);
    buf.put_u8(45); // utf8mb4_general_ci
    buf.put_u16_le(SERVER_STATUS_NO_TRANSACTIONS);
    buf.put_u16_le((SERVER_CAPABILITIES >> 16) as u16);
    buf.put_u8(21); // mysql_native_password scramble length, as MySQL itself reports
    buf.put_slice(&[0; 10]); // reserved
    buf.put_slice(&salt[8..]); // auth-plugin-data part 2, 12 bytes
    buf.put_u8(0);
    buf.put_slice(b"mysql_native_password\0");
    buf
}

fn server_error_code(code: &str) -> u16 {
    match code {
        "SQL_PARSE_ERROR" | "SQL_NO_STATEMENT" | "SQL_MANY_STATEMENTS" => 1064,
        "SQL_WRONG_ARGUMENTS" | "SQL_ARGUMENT_TYPE_MISMATCH" => 1210,
        "STREAM_EXPIRED" | "STREAM_CLOSED" => 1047,
        "TRANSACTION_STATUS" | "SQL_TRANSACTIONS_NOT_SUPPORTED" => 1287,
        "RESPONSE_TOO_LARGE" => 1105,
        _ => 1105,
    }
}

/// MySQL errno and SQLSTATE to report for a gateway error.
///
/// A statement that reached the backend and was refused there comes back coded `MYSQL_<errno>`, with
/// the backend's SQLSTATE rendered into the message. Both are relayed: drivers and schema tools branch
/// on errno -- `1146` no such table, `1062` duplicate key -- and flattening them to `1105` turns that
/// logic into dead code. Anything unrecognised degrades to the generic `1105`/`HY000`.
fn server_error<'a>(code: &str, message: &'a str) -> (u16, &'a str) {
    match code.strip_prefix("MYSQL_").and_then(|errno| errno.parse::<u16>().ok()) {
        Some(errno) => (errno, sqlstate_in(message).unwrap_or("HY000")),
        None => (server_error_code(code), "HY000"),
    }
}

/// The SQLSTATE as `exec` renders it: `MySQL error <errno> (<state>): <text>`.
fn sqlstate_in(message: &str) -> Option<&str> {
    let rest = message.strip_prefix("MySQL error ")?.split_once(" (")?.1;
    let state = rest.get(..5)?;
    (rest.as_bytes().get(5) == Some(&b')')).then_some(state)
}

/// Maps the gateway's decltype vocabulary onto MySQL column metadata.
///
/// Values always travel as text, so a type is only a parsing hint -- but it must not under-size the
/// value it describes. `proto::Value::Integer` is an `i64` and `Value::Float` an `f64` whatever the
/// backend declared, and the gateway flattens every width into `INTEGER`/`REAL`, so those two names
/// map to LONGLONG and DOUBLE rather than INT and FLOAT.
fn mysql_type(decltype: Option<&str>) -> (u8, u16, u8) {
    const UNSIGNED: u16 = 0x0020;
    const BLOB: u16 = 0x0040;
    match decltype.unwrap_or_default().to_ascii_lowercase().as_str() {
        "tinyint" | "bool" | "boolean" | "smallint" | "int" | "integer" | "mediumint" | "bigint"
        | "year" => (0x08, 0, 0),
        "utinyint" | "usmallint" | "uint" | "uinteger" | "ubigint" => (0x08, UNSIGNED, 0),
        "float" | "real" | "double" => (0x05, 0, 0),
        "decimal" | "numeric" => (0xf6, 0, 0),
        "date" => (0x0a, 0, 0),
        "time" => (0x0b, 0, 0),
        "datetime" => (0x0c, 0, 0),
        "timestamp" => (0x07, 0, 0),
        "blob" | "binary" | "varbinary" => (0xfc, BLOB, 0),
        _ => (0xfd, 0, 0),
    }
}

fn put_text_value(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => buf.put_u8(0xfb),
        Value::Integer(i) => put_lenenc_str(buf, i.to_string().as_bytes()),
        Value::Float(f) => put_lenenc_str(buf, format_f64(*f).as_bytes()),
        // Length-prefixed, so every byte value is legal here: blob contents go out exactly as the
        // backend returned them. The backslash escaping some tools display in `mysql` output is a
        // rendering convention of that client, not the protocol.
        Value::Text(s) => put_lenenc_str(buf, s.as_bytes()),
        Value::Blob(b) => put_lenenc_str(buf, b),
    }
}

fn format_f64(f: f64) -> String {
    if f.fract() == 0.0 && f.is_finite() { format!("{f:.1}") } else { f.to_string() }
}

fn put_u24(buf: &mut Vec<u8>, val: usize) {
    let val = (val as u32) & 0x00ff_ffff;
    buf.push(val as u8);
    buf.push((val >> 8) as u8);
    buf.push((val >> 16) as u8);
}

fn put_lenenc_int(buf: &mut Vec<u8>, val: u64) {
    if val < 251 {
        buf.put_u8(val as u8);
    } else if val < 1 << 16 {
        buf.put_u8(0xfc);
        buf.put_u16_le(val as u16);
    } else if val < 1 << 24 {
        buf.put_u8(0xfd);
        put_u24(buf, val as usize);
    } else {
        buf.put_u8(0xfe);
        buf.put_u64_le(val);
    }
}

fn put_lenenc_str(buf: &mut Vec<u8>, s: &[u8]) {
    put_lenenc_int(buf, s.len() as u64);
    buf.put_slice(s);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decltype_vocabulary_maps_to_types_that_hold_the_values() {
        // the gateway reports every integer width as INTEGER and every float as REAL, while the
        // values themselves are i64 / f64
        assert_eq!(mysql_type(Some("INTEGER")).0, 0x08);
        assert_eq!(mysql_type(Some("REAL")).0, 0x05);
        assert_eq!(mysql_type(Some("DECIMAL")).0, 0xf6);
        assert_eq!(mysql_type(Some("TEXT")).0, 0xfd, "text must not claim a binary collation");
        assert_eq!(mysql_type(Some("BLOB")).0, 0xfc);
        assert_eq!(mysql_type(None).0, 0xfd);
        assert_eq!(mysql_type(Some("TEXT")).1 & 0x0040, 0, "only blobs carry the BLOB flag");
    }

    #[test]
    fn backend_errno_and_sqlstate_reach_the_client() {
        assert_eq!(
            server_error("MYSQL_1146", "MySQL error 1146 (42S02): Table 'default.t' doesn't exist"),
            (1146, "42S02")
        );
        assert_eq!(server_error("SQL_MANY_STATEMENTS", "too many"), (1064, "HY000"));
        // a code that only looks like a backend error must not invent an errno
        assert_eq!(server_error("MYSQL_IO_ERROR", "MySQL client error: broken pipe"), (1105, "HY000"));
        assert_eq!(server_error("MYSQL_999999999999", "x"), (1105, "HY000"));
        // a message we do not generate ourselves must not be read as an SQLSTATE
        assert_eq!(sqlstate_in("MySQL error 1146 (42S02"), None);
        assert_eq!(sqlstate_in("MySQL error (42S02): nope"), None);
    }

    #[test]
    fn only_a_bare_use_statement_selects_a_namespace() {
        for query in ["USE xs", "use xs;", "  USE `xs` ", "USE \"xs\";", "USE 'xs'"] {
            assert_eq!(use_target(query), Some("xs"), "{query}");
        }
        assert_eq!(use_target("USE `my db`"), Some("my db"), "quoted names may hold spaces");
        // anything else must reach the pipeline untouched rather than be half-interpreted
        assert_eq!(use_target("USE xs; SELECT 1"), None);
        assert_eq!(use_target("USE"), None);
        assert_eq!(use_target("USE xs ys"), None);
        assert_eq!(use_target("SELECT 'USE xs'"), None);
        assert_eq!(use_target("USE db/other"), None, "a path is not a schema name");
    }
}
