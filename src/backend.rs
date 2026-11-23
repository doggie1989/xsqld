//! SeekDB / MySQL connection pools, namespace isolation, pinned transaction map, and timeout reaper.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use parking_lot::RwLock;

use crate::config::Config;
use crate::exec::{self, ExecAbort};
use crate::metrics::NsMetrics;
use crate::proto::{StreamRequest, StreamResponse, StreamResult};
use crate::stream::Streams;

#[derive(Debug)]
pub enum AdminError {
    Exists(String),
    Missing(String),
    BadName(String),
    BadDump(String),
    Internal(anyhow::Error),
}

impl fmt::Display for AdminError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AdminError::Exists(n) => write!(f, "Namespace `{n}` already exists"),
            AdminError::Missing(n) => write!(f, "Namespace `{n}` doesn't exist"),
            AdminError::BadName(n) => write!(f, "Invalid namespace name `{n}`"),
            AdminError::BadDump(m) => write!(f, "{m}"),
            AdminError::Internal(e) => write!(f, "{e}"),
        }
    }
}

pub enum Outcome {
    Done {
        results: Vec<StreamResult>,
        closed: bool,
        pinned: bool,
    },
    StreamExpired,
    Deadline,
    Unavailable,
}

pub struct NsPool {
    pub name: Arc<str>,
    pub pool: Pool,
    pub metrics: Arc<NsMetrics>,
    /// Published by the sync worker after each committed cycle; read by the routing path.
    pub mirror: RwLock<Option<Arc<crate::route::MirrorView>>>,
    /// Tables inside a maintenance window: capture is dropped and reconciliation is suppressed.
    pub maintenance: Arc<RwLock<std::collections::BTreeSet<String>>>,
    origin: Instant,
    last_used_epoch_ms: AtomicU64,
}

impl NsPool {
    fn touch(&self, origin: Instant) {
        self.last_used_epoch_ms
            .store(origin.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    /// The mirror worker is an active user of this pool even when no HTTP request arrives. Without this,
    /// a namespace written only by direct MySQL clients would be closed as idle and the mirror would
    /// silently stop syncing — exactly the deployment this gateway is built for.
    pub fn note_mirror_activity(&self) {
        self.touch(self.origin);
    }

    fn idle_ms(&self, origin: Instant) -> u64 {
        let now = origin.elapsed().as_millis() as u64;
        now.saturating_sub(self.last_used_epoch_ms.load(Ordering::Relaxed))
    }
}

struct PinnedTx {
    ns: Arc<str>,
    conn: Conn,
    last_used: Instant,
}

pub struct Backend {
    pub cfg: Arc<Config>,
    pub streams: Arc<Streams>,
    pub idle_closed: AtomicU64,
    /// `SELECT VERSION()` of the backend, reported verbatim by the MySQL listener so that a client
    /// gating features on the server version sees the server it is actually querying.
    pub backend_version: Option<String>,
    origin: Instant,
    base_opts: Opts,
    admin_pool: Pool,
    namespaces: RwLock<HashMap<Arc<str>, Arc<NsPool>>>,
    pinned: tokio::sync::Mutex<HashMap<u64, PinnedTx>>,
    query_sem: Arc<tokio::sync::Semaphore>,
}

impl Backend {
    pub async fn connect(cfg: Arc<Config>) -> anyhow::Result<Arc<Self>> {
        let mut builder = if let Some(url) = &cfg.mysql_url {
            OptsBuilder::from_opts(Opts::from_url(url)?)
        } else {
            let pass = if cfg.mysql_password.is_empty() {
                None
            } else {
                Some(cfg.mysql_password.clone())
            };
            OptsBuilder::default()
                .ip_or_hostname(cfg.mysql_host.clone())
                .tcp_port(cfg.mysql_port)
                .user(Some(cfg.mysql_user.clone()))
                .pass(pass)
        };
        builder = builder.db_name(None::<String>).tcp_nodelay(true);
        let base_opts: Opts = builder.into();

        let admin_constraints = PoolConstraints::new(1, 8).expect("1 <= 8");
        let admin_opts: Opts = OptsBuilder::from_opts(base_opts.clone())
            .pool_opts(PoolOpts::default().with_constraints(admin_constraints))
            .stmt_cache_size(0)
            .into();
        let admin_pool = Pool::new(admin_opts);

        let mut c = admin_pool.get_conn().await?;
        let ver: Option<String> = c.query_first("SELECT VERSION()").await.unwrap_or(None);
        drop(c);

        tracing::info!(
            mysql_host = %base_opts.ip_or_hostname(),
            mysql_port = base_opts.tcp_port(),
            server_version = ver.as_deref().unwrap_or("unknown"),
            "connected to SeekDB / MySQL backend"
        );

        Ok(Arc::new(Self {
            streams: Streams::new(cfg.stream_idle()),
            idle_closed: AtomicU64::new(0),
            backend_version: ver,
            origin: Instant::now(),
            base_opts,
            admin_pool,
            namespaces: RwLock::new(HashMap::new()),
            pinned: tokio::sync::Mutex::new(HashMap::new()),
            query_sem: Arc::new(tokio::sync::Semaphore::new(cfg.max_concurrent_queries)),
            cfg,
        }))
    }

    /// Snapshot of currently cached namespace pools for `/metrics`
    pub fn open_namespaces(&self) -> Vec<Arc<NsPool>> {
        self.namespaces.read().values().cloned().collect()
    }

    pub async fn create(&self, ns: &str) -> Result<(), AdminError> {
        if !self.cfg.usable_namespace(ns) {
            return Err(AdminError::BadName(ns.to_owned()));
        }
        let mut conn = self
            .admin_pool
            .get_conn()
            .await
            .map_err(|e| AdminError::Internal(e.into()))?;
        let sql = format!("CREATE DATABASE `{ns}`");
        match conn.query_drop(sql).await {
            Ok(()) => {
                drop(conn);
                let _ = self.ensure_pool(ns);
                Ok(())
            }
            Err(mysql_async::Error::Server(se)) if se.code == 1007 => Err(AdminError::Exists(ns.to_owned())),
            Err(e) => Err(AdminError::Internal(e.into())),
        }
    }

    pub async fn delete(&self, ns: &str) -> Result<(), AdminError> {
        if !self.cfg.usable_namespace(ns) {
            return Err(AdminError::BadName(ns.to_owned()));
        }
        self.streams.kill_namespace(ns);

        // Drain any pinned transactions belonging to this namespace before dropping the DB
        let victims = {
            let mut g = self.pinned.lock().await;
            let ids: Vec<u64> = g.iter().filter_map(|(&id, p)| (&*p.ns == ns).then_some(id)).collect();
            ids.into_iter().filter_map(|id| g.remove(&id)).collect::<Vec<_>>()
        };
        for p in victims {
            cleanup_and_disconnect(p.conn).await;
        }

        let removed = self.namespaces.write().remove(ns);
        if let Some(np) = removed {
            let _ = np.pool.clone().disconnect().await;
        }

        let mut conn = self
            .admin_pool
            .get_conn()
            .await
            .map_err(|e| AdminError::Internal(e.into()))?;
        let sql = format!("DROP DATABASE `{ns}`");
        match conn.query_drop(sql).await {
            Ok(()) => Ok(()),
            Err(mysql_async::Error::Server(se)) if se.code == 1008 => Err(AdminError::Missing(ns.to_owned())),
            Err(e) => Err(AdminError::Internal(e.into())),
        }
    }

    pub async fn get_namespace(&self, ns: &str) -> anyhow::Result<Option<Arc<NsPool>>> {
        if !self.cfg.usable_namespace(ns) {
            return Ok(None);
        }
        if let Some(p) = self.namespaces.read().get(ns).cloned() {
            p.touch(self.origin);
            return Ok(Some(p));
        }

        let mut conn = self.admin_pool.get_conn().await?;
        let found: Option<String> = conn
            .exec_first(
                "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
                (ns,),
            )
            .await?;
        drop(conn);

        if found.is_none() {
            return Ok(None);
        }
        Ok(Some(self.ensure_pool(ns)))
    }

    fn ensure_pool(&self, ns: &str) -> Arc<NsPool> {
        if let Some(existing) = self.namespaces.read().get(ns).cloned() {
            existing.touch(self.origin);
            return existing;
        }
        let mut g = self.namespaces.write();
        if let Some(existing) = g.get(ns).cloned() {
            existing.touch(self.origin);
            return existing;
        }
        let constraints = PoolConstraints::new(self.cfg.pool_min, self.cfg.pool_max).expect("validated pool bounds");
        let opts: Opts = OptsBuilder::from_opts(self.base_opts.clone())
            .db_name(Some(ns.to_owned()))
            .pool_opts(PoolOpts::default().with_constraints(constraints))
            .stmt_cache_size(self.cfg.stmt_cache_size)
            .into();
        let key: Arc<str> = ns.into();
        let np = Arc::new(NsPool {
            name: key.clone(),
            pool: Pool::new(opts),
            metrics: Arc::new(NsMetrics::default()),
            mirror: RwLock::new(None),
            maintenance: Arc::new(RwLock::new(std::collections::BTreeSet::new())),
            origin: self.origin,
            last_used_epoch_ms: AtomicU64::new(self.origin.elapsed().as_millis() as u64),
        });
        g.insert(key, np.clone());
        np
    }

    pub async fn run_pipeline(
        &self,
        ns_pool: &Arc<NsPool>,
        stream_id: u64,
        was_pinned: bool,
        requests: Vec<StreamRequest>,
        deadline: Instant,
    ) -> Outcome {
        ns_pool.touch(self.origin);
        ns_pool.metrics.requests.fetch_add(1, Ordering::Relaxed);

        if !was_pinned && requests.iter().all(|r| matches!(r, StreamRequest::Close)) {
            return Outcome::Done {
                results: requests
                    .iter()
                    .map(|_| StreamResult::Ok {
                        response: StreamResponse::Close,
                    })
                    .collect(),
                closed: !requests.is_empty(),
                pinned: false,
            };
        }

        let Some(wait_rem) = deadline.checked_duration_since(Instant::now()) else {
            ns_pool.metrics.deadline_exceeded.fetch_add(1, Ordering::Relaxed);
            return Outcome::Deadline;
        };
        let Ok(Ok(_q_permit)) = tokio::time::timeout(wait_rem, self.query_sem.clone().acquire_owned()).await else {
            ns_pool.metrics.deadline_exceeded.fetch_add(1, Ordering::Relaxed);
            return Outcome::Deadline;
        };

        let mut conn = if was_pinned {
            let maybe = self.pinned.lock().await.remove(&stream_id);
            match maybe {
                Some(p) => {
                    ns_pool.metrics.pinned_streams.fetch_sub(1, Ordering::Relaxed);
                    p.conn
                }
                None => return Outcome::StreamExpired,
            }
        } else {
            let Some(rem) = deadline.checked_duration_since(Instant::now()) else {
                ns_pool.metrics.deadline_exceeded.fetch_add(1, Ordering::Relaxed);
                return Outcome::Deadline;
            };
            let t0 = Instant::now();
            match tokio::time::timeout(rem, ns_pool.pool.get_conn()).await {
                Ok(Ok(c)) => {
                    ns_pool.metrics.pool_wait.record(t0.elapsed());
                    c
                }
                Ok(Err(e)) => {
                    tracing::error!(ns = &*ns_pool.name, error = %e, "pool get_conn failed");
                    return Outcome::Unavailable;
                }
                Err(_) => {
                    ns_pool.metrics.deadline_exceeded.fetch_add(1, Ordering::Relaxed);
                    return Outcome::Deadline;
                }
            }
        };

        let conn_id = conn.id();
        // Bind before the match: a guard created inline would live to the end of the whole `match ... await`
        // statement and hold a non-Send lock across the await point.
        let mirror = ns_pool.mirror.read().clone();
        match exec::run(
            &mut conn,
            &requests,
            was_pinned,
            self.cfg.max_response_bytes,
            deadline,
            &ns_pool.metrics,
            &crate::route::RouteCtx {
                view: mirror,
                min_rows: self.cfg.route_min_rows,
            },
        )
        .await
        {
            Err(ExecAbort::Deadline) => {
                ns_pool.metrics.deadline_exceeded.fetch_add(1, Ordering::Relaxed);
                ns_pool.metrics.kill_queries.fetch_add(1, Ordering::Relaxed);
                self.spawn_kill_query(conn_id);
                // Dropping or disconnecting immediately prevents the pool from reusing a dirty stream
                tokio::spawn(async move {
                    let _ = tokio::time::timeout(Duration::from_secs(1), conn.disconnect()).await;
                });
                Outcome::Deadline
            }
            Ok(er) => {
                if er.transport_dirty {
                    tokio::spawn(async move {
                        let _ = tokio::time::timeout(Duration::from_secs(1), conn.disconnect()).await;
                    });
                    return Outcome::Done {
                        results: er.results,
                        // If a pinned transaction lost its underlying TCP stream, terminate the baton
                        // so the client knows the transaction died and must open a fresh stream.
                        closed: er.closed || was_pinned,
                        pinned: false,
                    };
                }

                if er.closed {
                    drop(conn);
                    return Outcome::Done {
                        results: er.results,
                        closed: true,
                        pinned: false,
                    };
                }

                if er.in_trans {
                    ns_pool.metrics.pinned_streams.fetch_add(1, Ordering::Relaxed);
                    self.pinned.lock().await.insert(
                        stream_id,
                        PinnedTx {
                            ns: ns_pool.name.clone(),
                            conn,
                            last_used: Instant::now(),
                        },
                    );
                    return Outcome::Done {
                        results: er.results,
                        closed: false,
                        pinned: true,
                    };
                }

                drop(conn);
                Outcome::Done {
                    results: er.results,
                    closed: false,
                    pinned: false,
                }
            }
        }
    }

    fn spawn_kill_query(&self, conn_id: u32) {
        if conn_id == 0 {
            return;
        }
        let admin = self.admin_pool.clone();
        tokio::spawn(async move {
            let fut = async {
                if let Ok(mut c) = admin.get_conn().await {
                    let _ = c.query_drop(format!("KILL QUERY {conn_id}")).await;
                }
            };
            let _ = tokio::time::timeout(Duration::from_secs(2), fut).await;
        });
    }

    /// Expires pinned transactions that stayed open across HTTP requests past `--pinned-tx-timeout-ms`
    /// Open a maintenance window: drop capture triggers and suppress reconciliation for one table so a
    /// bulk load does not pay the per-row trigger tax. The caller must close the window afterwards.
    pub async fn maintenance_begin(&self, ns: &str, table: &str) -> Result<(), AdminError> {
        let np = self
            .get_namespace(ns)
            .await
            .map_err(AdminError::Internal)?
            .ok_or_else(|| AdminError::Missing(ns.to_owned()))?;
        let mut c = np.pool.get_conn().await.map_err(|e| AdminError::Internal(e.into()))?;
        let t = self
            .find_table(&mut c, ns, table)
            .await
            .map_err(AdminError::Internal)?
            .ok_or_else(|| AdminError::BadName(table.to_owned()))?;
        // Suppress reconciliation first, otherwise the worker could re-snapshot mid-window.
        np.maintenance.write().insert(table.to_ascii_lowercase());
        for sql in crate::capture::drop_triggers(&t) {
            c.query_drop(sql).await.map_err(|e| AdminError::Internal(e.into()))?;
        }
        Ok(())
    }

    /// Close a maintenance window: reinstall capture and re-snapshot the table, which recovers every row
    /// written while capture was dropped.
    pub async fn maintenance_end(&self, ns: &str, table: &str) -> Result<(), AdminError> {
        let np = self
            .get_namespace(ns)
            .await
            .map_err(AdminError::Internal)?
            .ok_or_else(|| AdminError::Missing(ns.to_owned()))?;
        let mut c = np.pool.get_conn().await.map_err(|e| AdminError::Internal(e.into()))?;
        let t = self
            .find_table(&mut c, ns, table)
            .await
            .map_err(AdminError::Internal)?
            .ok_or_else(|| AdminError::BadName(table.to_owned()))?;
        for sql in crate::capture::capture_triggers(&t) {
            c.query_drop(sql).await.map_err(|e| AdminError::Internal(e.into()))?;
        }
        let view = np.mirror.read().clone();
        if let Some(v) = view {
            let cols = crate::sync::discover_columns(&mut c, ns, &t.name)
                .await
                .map_err(AdminError::Internal)?;
            let duck = v.duck.lock();
            crate::sync::snapshot_table(&duck, "src", &t, &cols, self.cfg.mirror_query_timeout_ms * 1000)
                .map_err(AdminError::Internal)?;
        }
        np.maintenance.write().remove(&table.to_ascii_lowercase());
        Ok(())
    }

    async fn find_table(
        &self,
        c: &mut Conn,
        ns: &str,
        table: &str,
    ) -> anyhow::Result<Option<crate::capture::TableSchema>> {
        let tables = crate::sync::discover_tables(c, ns).await?;
        Ok(tables.into_iter().find(|t| t.name.eq_ignore_ascii_case(table)))
    }

    pub async fn sweep_pinned(&self) {
        let limit = self.cfg.pinned_tx_timeout();
        let now = Instant::now();
        let expired: Vec<(u64, PinnedTx)> = {
            let mut g = self.pinned.lock().await;
            let ids: Vec<u64> = g
                .iter()
                .filter_map(|(&id, p)| (now.duration_since(p.last_used) >= limit).then_some(id))
                .collect();
            ids.into_iter()
                .filter_map(|id| g.remove(&id).map(|p| (id, p)))
                .collect()
        };

        for (id, p) in expired {
            self.streams.kill(id);
            if let Some(np) = self.namespaces.read().get(&*p.ns).cloned() {
                dec_non_neg(&np.metrics.pinned_streams);
                np.metrics.pinned_timeouts.fetch_add(1, Ordering::Relaxed);
            }
            tracing::warn!(ns = &*p.ns, stream_id = id, "rolled back expired pinned transaction");
            cleanup_and_disconnect(p.conn).await;
        }
    }

    /// Disconnects namespace pools idle for `--idle-close-ms` (when no pinned transactions are active)
    pub async fn close_idle_pools(&self) {
        if self.cfg.idle_close_ms == 0 {
            return;
        }
        let pinned_ns: std::collections::HashSet<Arc<str>> = {
            let g = self.pinned.lock().await;
            g.values().map(|p| p.ns.clone()).collect()
        };
        let to_close: Vec<Arc<NsPool>> = {
            let mut g = self.namespaces.write();
            let keys: Vec<Arc<str>> = g
                .iter()
                .filter_map(|(k, v)| {
                    (!pinned_ns.contains(k) && v.idle_ms(self.origin) >= self.cfg.idle_close_ms).then_some(k.clone())
                })
                .collect();
            keys.into_iter().filter_map(|k| g.remove(&k)).collect()
        };
        for np in to_close {
            self.idle_closed.fetch_add(1, Ordering::Relaxed);
            let _ = np.pool.clone().disconnect().await;
        }
    }

    pub async fn shutdown(&self) {
        let remaining: Vec<PinnedTx> = {
            let mut g = self.pinned.lock().await;
            g.drain().map(|(_, v)| v).collect()
        };
        for p in remaining {
            cleanup_and_disconnect(p.conn).await;
        }
        let pools: Vec<Arc<NsPool>> = {
            let mut g = self.namespaces.write();
            g.drain().map(|(_, v)| v).collect()
        };
        for np in pools {
            let _ = np.pool.clone().disconnect().await;
        }
        let _ = self.admin_pool.clone().disconnect().await;
    }
}

fn dec_non_neg(a: &AtomicI64) {
    let _ = a.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some((v - 1).max(0)));
}

async fn cleanup_and_disconnect(mut conn: Conn) {
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        let _ = conn.query_drop("ROLLBACK").await;
        let _ = conn.disconnect().await;
    })
    .await;
}
