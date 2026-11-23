//! MySQL protocol server: accept MySQL clients and route queries to SeekDB or DuckDB.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bytes::{BytesMut, BufMut, Buf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::backend::Backend;
use crate::config::Config;

const MYSQL_PROTOCOL_VERSION: u8 = 10;
const SERVER_CAPABILITIES: u32 = 0x0000_80c3; // CLIENT_LONG_FLAG | CLIENT_FOUND_ROWS | CLIENT_LONG_PASSWORD | CLIENT_PROTOCOL_41

struct MysqlConnection {
    stream: TcpStream,
    seq_id: u8,
}

impl MysqlConnection {
    async fn write_packet(&mut self, data: &[u8]) -> io::Result<()> {
        let mut pkt = Vec::new();
        pkt.put_u24_le(data.len() as u32);
        pkt.put_u8(self.seq_id);
        pkt.put_slice(data);
        self.seq_id = self.seq_id.wrapping_add(1);
        self.stream.write_all(&pkt).await
    }

    async fn read_packet(&mut self, buf: &mut BytesMut) -> Result<Option<Vec<u8>>> {
        loop {
            if buf.len() >= 4 {
                let len = u32::from_le_bytes([buf[0], buf[1], buf[2], 0]) as usize;
                if buf.len() >= 4 + len {
                    buf.advance(3);
                    let seq = buf[0];
                    buf.advance(1);
                    self.seq_id = seq.wrapping_add(1);
                    let pkt = buf[..len].to_vec();
                    buf.advance(len);
                    return Ok(Some(pkt));
                }
            }

            let n = self.stream.read_buf(buf).await?;
            if n == 0 {
                return if buf.is_empty() { Ok(None) } else { bail!("incomplete packet") };
            }
        }
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

async fn handle_connection(
    stream: TcpStream,
    backend: Arc<Backend>,
    cfg: Arc<Config>,
) -> Result<()> {
    let mut conn = MysqlConnection { stream, seq_id: 0 };

    // Send handshake
    let handshake = build_handshake();
    conn.write_packet(&handshake).await?;

    // Read auth response
    let mut buf = BytesMut::with_capacity(1024);
    let _auth_pkt = conn.read_packet(&mut buf).await?;

    // Send OK to complete auth
    let ok_pkt = build_ok(0, 0);
    conn.write_packet(&ok_pkt).await?;

    // Command loop
    buf.clear();
    loop {
        match conn.read_packet(&mut buf).await? {
            Some(pkt) => {
                if pkt.is_empty() {
                    continue;
                }
                let cmd = pkt[0];
                match cmd {
                    1 => {
                        // COM_QUIT
                        break;
                    }
                    3 => {
                        // COM_QUERY
                        let query = String::from_utf8_lossy(&pkt[1..]).to_string();
                        if let Err(e) = handle_query(&mut conn, &backend, &cfg, &query).await {
                            tracing::debug!(query = %query, error = %e, "query error");
                            let err_pkt = build_err(1064, &format!("Query error: {}", e));
                            let _ = conn.write_packet(&err_pkt).await;
                        }
                    }
                    _ => {
                        tracing::debug!(command = cmd, "unsupported mysql command");
                        let err_pkt = build_err(1064, "Unsupported command");
                        let _ = conn.write_packet(&err_pkt).await;
                    }
                }
            }
            None => break,
        }
    }

    Ok(())
}

async fn handle_query(
    conn: &mut MysqlConnection,
    backend: &Arc<Backend>,
    cfg: &Arc<Config>,
    query: &str,
) -> Result<()> {
    let query_lower = query.trim().to_lowercase();

    // Simple routing: SELECT -> try DuckDB if mirror enabled, else SeekDB
    // Other -> SeekDB only
    if query_lower.starts_with("select") {
        if cfg.mirror_enabled {
            // Try to execute on DuckDB via mirror
            // For now, fall back to SeekDB
            execute_on_seekdb(conn, backend, cfg, query).await?;
        } else {
            execute_on_seekdb(conn, backend, cfg, query).await?;
        }
    } else {
        // INSERT, UPDATE, DELETE, DDL -> always go to SeekDB
        execute_on_seekdb(conn, backend, cfg, query).await?;
    }

    Ok(())
}

async fn execute_on_seekdb(
    conn: &mut MysqlConnection,
    backend: &Arc<Backend>,
    _cfg: &Arc<Config>,
    _query: &str,
) -> Result<()> {
    // Get a connection from the default namespace pool
    let ns = "default";
    let _ns_pool = backend
        .get_namespace(ns)
        .await
        .context("failed to get namespace")?
        .context("namespace not found")?;

    // For now, just send OK
    // TODO: Properly execute query and return result set
    let ok_pkt = build_ok(1, 0);
    conn.write_packet(&ok_pkt).await?;

    Ok(())
}

fn build_handshake() -> Vec<u8> {
    let mut buf = Vec::new();

    // Handshake body (without packet header)
    buf.put_u8(MYSQL_PROTOCOL_VERSION);
    buf.put_slice(b"xsqld-1.0.0\0"); // server version
    buf.put_u32_le(0x1234); // connection id (arbitrary)
    buf.put_slice(&[0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48]); // auth plugin data part 1
    buf.put_u8(0); // filler
    buf.put_u16_le(SERVER_CAPABILITIES as u16); // capability flags (lower 2 bytes)
    buf.put_u8(33); // charset utf8mb4_unicode_ci
    buf.put_u16_le(0x0002); // status flags
    buf.put_u16_le((SERVER_CAPABILITIES >> 16) as u16); // capability flags (upper 2 bytes)
    buf.put_u8(0); // auth plugin data length
    buf.put_slice(&[0; 10]); // reserved
    buf.put_slice(&[0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 0x50, 0x51, 0x52, 0x53, 0x54, 0x00]); // auth plugin data part 2
    buf.put_slice(b"mysql_native_password\0"); // auth plugin name

    buf
}

fn build_ok(affected_rows: u64, last_insert_id: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u8(0x00); // OK header
    encode_lenenc_int(&mut buf, affected_rows);
    encode_lenenc_int(&mut buf, last_insert_id);
    buf.put_u16_le(0x0002); // status flags
    buf.put_u16_le(0); // warnings
    buf
}

fn build_err(code: u16, message: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u8(0xff); // ERR header
    buf.put_u16_le(code);
    buf.put_slice(b"#HY000"); // SQL state
    buf.put_slice(message.as_bytes());
    buf
}

fn encode_lenenc_int(buf: &mut Vec<u8>, val: u64) {
    if val < 251 {
        buf.put_u8(val as u8);
    } else if val < 2u64.pow(16) {
        buf.put_u8(0xfc);
        buf.put_u16_le(val as u16);
    } else if val < 2u64.pow(24) {
        buf.put_u8(0xfd);
        buf.put_u24_le(val as u32);
    } else {
        buf.put_u8(0xfe);
        buf.put_u64_le(val);
    }
}

