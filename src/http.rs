//! HTTP: the Hrana port (`POST /v2/pipeline`, `GET /health`, `GET /metrics`) and the admin port
//! (`POST /v1/namespaces/{ns}/create`, `DELETE /v1/namespaces/{ns}`, `GET /health`, `GET /metrics`).

use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use serde::Deserialize;

use crate::backend::{AdminError, Backend, Outcome};
use crate::exec;
use crate::metrics;
use crate::proto::{PipelineReq, PipelineResp, StreamRequest};
use crate::sql;

pub type App = Arc<Backend>;

pub fn user_router(app: App) -> Router {
    let limits = Limits::new(&app.cfg, app.cfg.max_request_bytes);
    Router::new()
        .route("/v2/pipeline", post(pipeline))
        .route("/health", get(health))
        .route("/metrics", get(metrics_handler))
        .layer(DefaultBodyLimit::max(limits.max_body))
        .layer(axum::middleware::from_fn_with_state(limits, guard))
        .with_state(app)
}

pub fn admin_router(app: App) -> Router {
    let limits = Limits::new(&app.cfg, 64 * 1024);
    Router::new()
        .route("/v1/namespaces/{ns}/create", post(create))
        .route("/v1/namespaces/{ns}", delete(remove))
        .route("/v1/namespaces/{ns}/maintenance/{table}/begin", post(maintenance_begin))
        .route("/v1/namespaces/{ns}/maintenance/{table}/end", post(maintenance_end))
        .route("/health", get(health))
        .route("/metrics", get(metrics_handler))
        .layer(DefaultBodyLimit::max(limits.max_body))
        .layer(axum::middleware::from_fn_with_state(limits, guard))
        .with_state(app)
}

pub async fn serve(
    listener: tokio::net::TcpListener,
    router: Router,
    cfg: Arc<crate::config::Config>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let stopped = async move {
        let _ = stop.wait_for(|s| *s).await;
    };
    tokio::pin!(stopped);
    let slots = Arc::new(tokio::sync::Semaphore::new(cfg.max_connections));
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut http = hyper::server::conn::http1::Builder::new();
    http.timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(cfg.header_read_timeout());
    loop {
        let slot = tokio::select! {
            s = slots.clone().acquire_owned() => s.expect("semaphore is never closed"),
            _ = &mut stopped => break,
        };
        let stream = tokio::select! {
            r = listener.accept() => match r {
                Ok((stream, _)) => stream,
                Err(e) => {
                    tracing::warn!(error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            _ = &mut stopped => break,
        };
        let _ = stream.set_nodelay(true);
        let service = hyper_util::service::TowerToHyperService::new(router.clone());
        let io = WriteTimeout {
            io: stream,
            limit: cfg.response_write_timeout(),
            stalled: None,
        };
        let conn = graceful.watch(http.serve_connection(hyper_util::rt::TokioIo::new(io), service));
        tokio::spawn(async move {
            let _slot = slot;
            let _ = conn.await;
        });
    }
    graceful.shutdown().await;
}

struct WriteTimeout {
    io: tokio::net::TcpStream,
    limit: Duration,
    stalled: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

impl WriteTimeout {
    fn limit<T>(
        &mut self,
        cx: &mut std::task::Context<'_>,
        r: std::task::Poll<io::Result<T>>,
    ) -> std::task::Poll<io::Result<T>> {
        use std::future::Future;
        use std::task::Poll;
        if r.is_ready() {
            self.stalled = None;
            return r;
        }
        let limit = self.limit;
        let stalled = self.stalled.get_or_insert_with(|| Box::pin(tokio::time::sleep(limit)));
        match stalled.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "the client took no response data within --response-write-timeout-ms",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl tokio::io::AsyncRead for WriteTimeout {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().io).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for WriteTimeout {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        let this = self.get_mut();
        let r = std::pin::Pin::new(&mut this.io).poll_write(cx, buf);
        this.limit(cx, r)
    }

    fn poll_write_vectored(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> std::task::Poll<io::Result<usize>> {
        let this = self.get_mut();
        let r = std::pin::Pin::new(&mut this.io).poll_write_vectored(cx, bufs);
        this.limit(cx, r)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>> {
        let this = self.get_mut();
        let r = std::pin::Pin::new(&mut this.io).poll_flush(cx);
        this.limit(cx, r)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let this = self.get_mut();
        let r = std::pin::Pin::new(&mut this.io).poll_shutdown(cx);
        this.limit(cx, r)
    }
}

#[derive(Clone)]
pub(crate) struct Limits {
    cfg: Arc<crate::config::Config>,
    requests: Arc<tokio::sync::Semaphore>,
    body_timeout: Duration,
    max_body: usize,
}

impl Limits {
    pub(crate) fn new(cfg: &Arc<crate::config::Config>, max_body: usize) -> Self {
        Self {
            cfg: cfg.clone(),
            requests: Arc::new(tokio::sync::Semaphore::new(cfg.max_concurrent_requests)),
            body_timeout: cfg.body_read_timeout(),
            max_body,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Arrived(pub(crate) Instant);

pub(crate) async fn guard(
    State(limits): State<Limits>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let arrived = Instant::now();
    if let Some(why) = host_refused(&limits.cfg, req.headers()) {
        return error(StatusCode::FORBIDDEN, why);
    }
    let path = req.uri().path();
    if path != "/health" {
        if let Some(expected) = limits.cfg.auth_token.as_deref() {
            if !bearer_authorized(req.headers(), expected) {
                return error(StatusCode::UNAUTHORIZED, "Unauthorized");
            }
        } else if let Some(why) = csrf_refused(req.headers()) {
            return error(StatusCode::FORBIDDEN, why);
        }
    }
    let _permit = if path == "/health" {
        None
    } else {
        match limits.requests.clone().try_acquire_owned() {
            Ok(p) => Some(p),
            Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "Too many requests in flight"),
        }
    };
    let (mut parts, body) = req.into_parts();
    let body = match tokio::time::timeout(limits.body_timeout, axum::body::to_bytes(body, limits.max_body)).await {
        Ok(Ok(b)) => b,
        Ok(Err(e)) => {
            let too_large = std::error::Error::source(&e).is_some_and(|s| s.is::<http_body_util::LengthLimitError>());
            return if too_large {
                text(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "Failed to buffer the request body: length limit exceeded",
                )
            } else {
                text(StatusCode::BAD_REQUEST, format!("Failed to read the request body: {e}"))
            };
        }
        Err(_) => return text(StatusCode::REQUEST_TIMEOUT, "Request body not received in time"),
    };
    parts.extensions.insert(Arrived(arrived));
    next.run(axum::extract::Request::from_parts(parts, Body::from(body)))
        .await
}

fn host_refused(cfg: &crate::config::Config, headers: &HeaderMap) -> Option<String> {
    if let Some(host) = headers.get(header::HOST) {
        match host.to_str() {
            Ok(h) if cfg.host_allowed(h) => {}
            _ => {
                return Some(format!(
                    "Host `{}` is not accepted (see --allowed-host-suffixes)",
                    String::from_utf8_lossy(host.as_bytes())
                ));
            }
        }
    }
    None
}

fn csrf_refused(headers: &HeaderMap) -> Option<String> {
    (headers.contains_key(header::ORIGIN) || headers.contains_key("sec-fetch-site"))
        .then(|| "Requests from web browsers are not accepted".to_owned())
}

fn bearer_authorized(headers: &HeaderMap, expected: &str) -> bool {
    let Some(val) = headers.get(header::AUTHORIZATION) else {
        return false;
    };
    let Ok(s) = val.to_str() else {
        return false;
    };
    let Some((scheme, token)) = s.split_once(' ') else {
        return false;
    };
    scheme.eq_ignore_ascii_case("bearer") && ct_eq(token.as_bytes(), expected.as_bytes())
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let max_len = a.len().max(b.len());
    let mut diff: u8 = if a.len() == b.len() { 0 } else { 1 };
    for i in 0..max_len {
        let ba = a.get(i).copied().unwrap_or(0);
        let bb = b.get(i).copied().unwrap_or(0);
        diff |= ba ^ bb;
    }
    diff == 0
}

async fn health() -> Response {
    axum::Json(serde_json::json!({
        "version": crate::config::VERSION,
        "commit": crate::config::COMMIT,
    }))
    .into_response()
}

async fn metrics_handler(State(app): State<App>) -> Response {
    let out = metrics_text(&app);
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out).into_response()
}

pub fn metrics_text(app: &App) -> String {
    let mut out = String::new();
    metrics::build_info(&mut out);
    #[cfg(feature = "alloc-stats")]
    metrics::put(
        &mut out,
        "rust_live_bytes",
        "",
        crate::alloc_stats::LIVE.load(std::sync::atomic::Ordering::Relaxed).max(0) as u64,
    );
    let (streams, pinned) = app.streams.counts();
    metrics::put(&mut out, "streams", "", streams);
    metrics::put(&mut out, "streams_pinned", "", pinned);
    let nss = app.open_namespaces();
    let relaxed = std::sync::atomic::Ordering::Relaxed;
    metrics::put(&mut out, "namespaces_cached", "", nss.len());
    metrics::put(&mut out, "idle_closed_total", "", app.idle_closed.load(relaxed));
    for ns in nss {
        ns.metrics.render(&ns.name, &mut out);
    }
    out
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    #[derive(serde::Serialize)]
    struct E {
        error: String,
    }
    (status, axum::Json(E { error: message.into() })).into_response()
}

fn text(status: StatusCode, message: impl Into<String>) -> Response {
    (status, message.into()).into_response()
}

fn missing(ns: &str) -> Response {
    error(StatusCode::NOT_FOUND, format!("Namespace `{ns}` doesn't exist"))
}

/// The namespace is the first label of the Host header (`tenant.db`, `platform.local`), like sqld
pub fn namespace(headers: &HeaderMap) -> Option<String> {
    let host = headers.get(header::HOST)?.to_str().ok()?;
    let label = host.split(['.', ':']).next()?;
    (!label.is_empty()).then(|| label.to_owned())
}

async fn pipeline(
    State(app): State<App>,
    axum::Extension(Arrived(started)): axum::Extension<Arrived>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let ns = namespace(&headers).unwrap_or_default();
    let ns_pool = match app.get_namespace(&ns).await {
        Ok(Some(p)) => p,
        Ok(None) => return missing(&ns),
        Err(e) => {
            tracing::error!(ns, error = %e, "could not look up namespace");
            return error(StatusCode::INTERNAL_SERVER_ERROR, format!("Internal error: {e}"));
        }
    };

    let req: PipelineReq = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return text(
                StatusCode::BAD_REQUEST,
                format!("Cannot deserialize client message from JSON: {e}"),
            );
        }
    };
    drop(body);

    if let Err(m) = exec::validate(&req.requests, app.cfg.max_statements) {
        return text(StatusCode::BAD_REQUEST, m);
    }

    let lease = match app.streams.acquire(&ns_pool.name, req.baton.as_deref()) {
        Ok(l) => l,
        Err(e) => return text(StatusCode::BAD_REQUEST, e.message()),
    };

    let head = slow_log_head(&req.requests);
    let routed_before = ns_pool.metrics.routed.load(std::sync::atomic::Ordering::Relaxed);
    let deadline = started + app.cfg.request_timeout();
    let outcome = app
        .run_pipeline(&ns_pool, lease.id, lease.pinned, req.requests, deadline)
        .await;
    let elapsed = started.elapsed();
    if elapsed.as_millis() >= 1000 {
        let routed = ns_pool.metrics.routed.load(std::sync::atomic::Ordering::Relaxed) - routed_before;
        tracing::warn!(
            ns = &*ns_pool.name,
            ms = elapsed.as_millis() as u64,
            sql = head,
            engine = if routed > 0 { "duckdb" } else { "seekdb" },
            routed_statements = routed,
            "slow request"
        );
    }

    match outcome {
        Outcome::Done {
            results,
            closed,
            pinned,
        } => {
            let baton = app.streams.finish(lease, closed, pinned);
            axum::Json(PipelineResp {
                baton,
                base_url: None,
                results,
            })
            .into_response()
        }
        Outcome::StreamExpired => {
            app.streams.kill(lease.id);
            text(StatusCode::BAD_REQUEST, crate::stream::StreamError::Expired.message())
        }
        Outcome::Deadline => error(StatusCode::GATEWAY_TIMEOUT, "Request timeout: the request was not run"),
        Outcome::Unavailable => error(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("Namespace `{}` is not available", ns_pool.name),
        ),
    }
}

fn slow_log_head(requests: &[StreamRequest]) -> String {
    exec::statements(requests).next().map(sql::head).unwrap_or_default()
}

#[derive(Deserialize)]
struct CreateReq {
    dump_url: Option<String>,
}

async fn create(State(app): State<App>, Path(ns): Path<String>, headers: HeaderMap, body: Bytes) -> Response {
    let json = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';')
                .next()
                .is_some_and(|m| m.trim().eq_ignore_ascii_case("application/json"))
        });
    if !json {
        return text(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Expected request with `Content-Type: application/json`",
        );
    }
    let req: CreateReq = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return text(
                StatusCode::BAD_REQUEST,
                format!("Failed to parse the request body as JSON: {e}"),
            );
        }
    };
    if req.dump_url.is_some() {
        return admin_error(AdminError::BadDump(
            "dump_url is not supported with the SeekDB / MySQL backend".into(),
        ));
    }
    match app.create(&ns).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => admin_error(e),
    }
}

async fn remove(State(app): State<App>, Path(ns): Path<String>) -> Response {
    match app.delete(&ns).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => admin_error(e),
    }
}

async fn maintenance_begin(State(app): State<App>, Path((ns, table)): Path<(String, String)>) -> Response {
    match app.maintenance_begin(&ns, &table).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => admin_error(e),
    }
}

async fn maintenance_end(State(app): State<App>, Path((ns, table)): Path<(String, String)>) -> Response {
    match app.maintenance_end(&ns, &table).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => admin_error(e),
    }
}

fn admin_error(e: AdminError) -> Response {
    match e {
        AdminError::Exists(n) => error(StatusCode::BAD_REQUEST, format!("Namespace `{n}` already exists")),
        AdminError::Missing(n) => missing(&n),
        AdminError::BadName(n) => error(StatusCode::BAD_REQUEST, format!("Invalid namespace name `{n}`")),
        AdminError::BadDump(m) => error(StatusCode::BAD_REQUEST, m),
        AdminError::Internal(e) => error(StatusCode::INTERNAL_SERVER_ERROR, format!("Internal error: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    #[test]
    fn host_header_extracts_namespace() {
        let mut h = HeaderMap::new();
        for (host, ns) in [
            ("tenant.db", "tenant"),
            ("platform.local:8080", "platform"),
            ("restore-check.localhost", "restore-check"),
        ] {
            h.insert(header::HOST, host.parse().unwrap());
            assert_eq!(namespace(&h).as_deref(), Some(ns));
        }
    }

    #[test]
    fn auth_and_csrf_checks() {
        assert!(ct_eq(b"secret", b"secret"));
        assert!(!ct_eq(b"secret", b"secrex"));
        assert!(!ct_eq(b"secret", b"secret2"));

        let mut h = HeaderMap::new();
        assert!(!bearer_authorized(&h, "tok123"));
        h.insert(header::AUTHORIZATION, "Basic dXNlcjpwYXNz".parse().unwrap());
        assert!(!bearer_authorized(&h, "tok123"));
        h.insert(header::AUTHORIZATION, "Bearer wrong".parse().unwrap());
        assert!(!bearer_authorized(&h, "tok123"));
        h.insert(header::AUTHORIZATION, "bearer tok123".parse().unwrap());
        assert!(bearer_authorized(&h, "tok123"));

        let cfg = crate::config::Config::for_tests();
        let mut h2 = HeaderMap::new();
        h2.insert(header::HOST, "tenant.evil.example".parse().unwrap());
        assert!(host_refused(&cfg, &h2).is_some());
        h2.insert(header::HOST, "tenant.db".parse().unwrap());
        assert!(host_refused(&cfg, &h2).is_none());

        h2.insert(header::ORIGIN, "https://evil.example".parse().unwrap());
        assert!(csrf_refused(&h2).is_some());
    }

    #[tokio::test]
    async fn guard_enforces_bearer_token_except_health() {
        async fn ok_handler() -> &'static str {
            "ok"
        }
        let mut cfg = crate::config::Config::for_tests();
        cfg.auth_token = Some("top-secret".into());
        let limits = Limits::new(&Arc::new(cfg), 1024);
        let router = Router::new()
            .route("/health", get(ok_handler))
            .route("/metrics", get(ok_handler))
            .layer(axum::middleware::from_fn_with_state(limits, guard));

        let r = router
            .clone()
            .oneshot(
                axum::http::Request::get("/health")
                    .header(header::HOST, "tenant.db")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);

        let r = router
            .clone()
            .oneshot(
                axum::http::Request::get("/metrics")
                    .header(header::HOST, "tenant.db")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);

        let r = router
            .oneshot(
                axum::http::Request::get("/metrics")
                    .header(header::HOST, "tenant.db")
                    .header(header::AUTHORIZATION, "Bearer top-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
    }
}
