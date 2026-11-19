//! The web console: parameters, schema, metrics and a SQL workbench on a port of its own.
//!
//! The Hrana and admin ports refuse every request that carries `Origin` or `Sec-Fetch-Site` (see [`crate::http`]),
//! which is what keeps a web page open on this machine from reaching the data API. This port is the one place a
//! browser is answered, so it is off unless `--ui-listen-addr` names it, it only binds loopback, and every request
//! must carry the token printed at startup. A page on another origin cannot read that token, and the custom header
//! it would have to set forces a preflight this router does not answer.
//!
//! SQL runs through the same [`crate::http::pipeline`] and [`crate::http::analytics`] handlers as a client, with the
//! namespace put in a Host header the console builds itself (a browser cannot set one). Deadlines, response limits,
//! the authorizer and the slow-request log therefore apply unchanged.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use clap::Parser;

use crate::config::Config;
use crate::http::App;

const PAGE: &str = include_str!("../ui/index.html");
const TOKEN_HEADER: &str = "x-xsqld-ui-token";
/// Samples kept for the trend charts: 20 minutes at one sample every two seconds
const TREND_SAMPLES: usize = 600;
const TREND_EVERY: Duration = Duration::from_secs(2);

/// A token for this process. `RandomState` is seeded from the operating system, so two of them give 128 bits a
/// page on another origin cannot guess; no random-number dependency is needed for it.
pub fn new_token() -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut out = String::with_capacity(32);
    for salt in [0u64, 1u64] {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u64(salt);
        out.push_str(&format!("{:016x}", hasher.finish()));
    }
    out
}

#[derive(Clone)]
struct Ui {
    app: App,
    token: Arc<str>,
    trends: Arc<Mutex<VecDeque<Sample>>>,
}

#[derive(Clone, Serialize)]
struct Sample {
    t: u64,
    series: BTreeMap<String, f64>,
}

pub fn router(app: App, token: String) -> Router {
    let ui = Ui {
        app: app.clone(),
        token: Arc::from(token),
        trends: Arc::new(Mutex::new(VecDeque::with_capacity(TREND_SAMPLES))),
    };
    spawn_sampler(ui.clone());
    Router::new()
        .route("/ui", get(page))
        .route("/ui/api/overview", get(overview))
        .route("/ui/api/trends", get(trends))
        .route("/ui/api/ns/{ns}/state", get(state))
        .route("/ui/api/ns/{ns}/counts", get(counts))
        .route("/ui/api/ns/{ns}/sql", post(sql))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(axum::middleware::from_fn_with_state(ui.clone(), guard))
        .with_state(ui)
}

/// Keeps the last [`TREND_SAMPLES`] readings of every series, so a page opened later still shows the history
fn spawn_sampler(ui: Ui) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(TREND_EVERY);
        loop {
            tick.tick().await;
            let sample = Sample {
                t: now_ms(),
                series: parse_metrics(&crate::http::metrics_text(&ui.app)),
            };
            let mut q = ui.trends.lock();
            if q.len() == TREND_SAMPLES {
                q.pop_front();
            }
            q.push_back(sample);
        }
    });
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// `name{ns="x"} 12` becomes `name|ns=x` → 12; a series without labels keeps its bare name
fn parse_metrics(text: &str) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let Ok(value) = value.parse::<f64>() else {
            continue;
        };
        let key = match key.split_once('{') {
            Some((name, labels)) => {
                let labels = labels.trim_end_matches('}');
                let mut parts: Vec<&str> = labels.split(',').collect();
                parts.sort_unstable();
                format!("{name}|{}", parts.join(","))
            }
            None => key.to_owned(),
        };
        out.insert(key, value);
    }
    out
}

async fn guard(State(ui): State<Ui>, req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    // A page on another origin must not reach the console even if it somehow learned the token
    if let Some(site) = req.headers().get("sec-fetch-site")
        && site != "same-origin"
        && site != "none"
    {
        return error(StatusCode::FORBIDDEN, "cross-site requests are not accepted");
    }
    let query_token = req
        .uri()
        .query()
        .and_then(|q| q.split('&').find_map(|kv| kv.strip_prefix("t=")));
    let sent = req
        .headers()
        .get(TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .or(query_token);
    match sent {
        Some(sent) if same_token(sent, &ui.token) => next.run(req).await,
        _ => error(
            StatusCode::FORBIDDEN,
            "missing or wrong console token; open the URL printed at startup",
        ),
    }
}

/// Compares every byte regardless of where the first difference is
fn same_token(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

async fn page() -> Response {
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], PAGE).into_response()
}

#[derive(Serialize)]
struct Param {
    name: String,
    value: serde_json::Value,
    default: serde_json::Value,
    changed: bool,
}

#[derive(Serialize)]
struct NsInfo {
    name: String,
    open: bool,
    files: Vec<(String, u64)>,
}

async fn overview(State(ui): State<Ui>) -> Response {
    let cfg = &ui.app.cfg;
    let defaults = Config::parse_from(["xsqld"]);
    let current = serde_json::to_value(cfg.as_ref()).unwrap_or_default();
    let default = serde_json::to_value(&defaults).unwrap_or_default();
    let mut params = Vec::new();
    if let (Some(cur), Some(def)) = (current.as_object(), default.as_object()) {
        for (name, value) in cur {
            let d = def.get(name).cloned().unwrap_or(serde_json::Value::Null);
            params.push(Param {
                name: name.clone(),
                changed: *value != d,
                value: value.clone(),
                default: d,
            });
        }
    }
    let open: std::collections::HashSet<String> = ui.app.open_dbs().iter().map(|d| d.name.to_string()).collect();
    let namespaces = ui
        .app
        .namespaces()
        .into_iter()
        .map(|name| NsInfo {
            open: open.contains(&name),
            files: files_of(cfg, &name),
            name,
        })
        .collect::<Vec<_>>();
    axum::Json(serde_json::json!({
        "version": crate::config::VERSION,
        "commit": crate::config::COMMIT,
        "sqlite": rusqlite::version(),
        "uptime_seconds": ui.app.uptime().as_secs(),
        "params": params,
        "namespaces": namespaces,
        "metrics": parse_metrics(&crate::http::metrics_text(&ui.app)),
    }))
    .into_response()
}

/// The database file and its side files, with the HTAP mirror beside them
fn files_of(cfg: &Config, ns: &str) -> Vec<(String, u64)> {
    let base = cfg.db_file(ns);
    let mut out = Vec::new();
    for suffix in ["", "-wal", "-shm", "-journal", ".duckdb", ".duckdb.wal"] {
        let mut path = base.clone().into_os_string();
        path.push(suffix);
        let path = std::path::PathBuf::from(path);
        if let Ok(meta) = std::fs::metadata(&path) {
            let label = if suffix.is_empty() {
                "data"
            } else {
                suffix.trim_start_matches(['-', '.'])
            };
            out.push((label.to_owned(), meta.len()));
        }
    }
    out
}

async fn trends(State(ui): State<Ui>) -> Response {
    let q = ui.trends.lock();
    axum::Json(q.iter().collect::<Vec<_>>()).into_response()
}

/// Runs statements through the client path and returns the Hrana results as JSON
async fn hrana(app: &App, ns: &str, requests: serde_json::Value) -> Result<serde_json::Value, Box<Response>> {
    let mut headers = HeaderMap::new();
    let host = format!("{ns}.localhost");
    let value = host
        .parse()
        .map_err(|_| Box::new(error(StatusCode::BAD_REQUEST, format!("bad namespace `{ns}`"))))?;
    headers.insert(header::HOST, value);
    let body = serde_json::to_vec(&serde_json::json!({ "requests": requests }))
        .map_err(|e| Box::new(error(StatusCode::BAD_REQUEST, e.to_string())))?;
    let response = crate::http::pipeline(
        State(app.clone()),
        axum::Extension(crate::http::Arrived(Instant::now())),
        headers,
        Bytes::from(body),
    )
    .await;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024 * 1024)
        .await
        .map_err(|e| Box::new(error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())))?;
    let json: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| Box::new(error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())))?;
    if !status.is_success() {
        let message = json
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("request failed")
            .to_owned();
        return Err(Box::new(error(status, message)));
    }
    Ok(json)
}

fn execute(sql: &str) -> serde_json::Value {
    serde_json::json!({"type":"execute","stmt":{"sql":sql}})
}

/// The rows of the first result, or an empty list when the statement returned none
fn rows(json: &serde_json::Value, index: usize) -> Vec<Vec<serde_json::Value>> {
    json.get("results")
        .and_then(|r| r.get(index))
        .and_then(|r| r.get("response"))
        .and_then(|r| r.get("result"))
        .and_then(|r| r.get("rows"))
        .and_then(|r| r.as_array())
        .map(|rows| rows.iter().filter_map(|r| r.as_array().cloned()).collect())
        .unwrap_or_default()
}

fn cell_text(value: &serde_json::Value) -> String {
    match value.get("type").and_then(|t| t.as_str()) {
        Some("text") => value.get("value").and_then(|v| v.as_str()).unwrap_or("").to_owned(),
        Some("integer") => value.get("value").and_then(|v| v.as_str()).unwrap_or("").to_owned(),
        Some("float") => value.get("value").map(|v| v.to_string()).unwrap_or_default(),
        Some("null") | None => String::new(),
        _ => value.to_string(),
    }
}

const PRAGMAS: [&str; 12] = [
    "journal_mode",
    "synchronous",
    "page_size",
    "page_count",
    "freelist_count",
    "cache_size",
    "wal_autocheckpoint",
    "foreign_keys",
    "encoding",
    "auto_vacuum",
    "user_version",
    "application_id",
];

async fn state(State(ui): State<Ui>, Path(ns): Path<String>) -> Response {
    if !ui.app.cfg.usable_namespace(&ns) {
        return error(StatusCode::BAD_REQUEST, format!("bad namespace `{ns}`"));
    }
    let mut requests: Vec<serde_json::Value> = PRAGMAS.iter().map(|p| execute(&format!("PRAGMA {p}"))).collect();
    requests.push(execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY type,name",
    ));
    requests.push(execute(
        "SELECT name,type,wr FROM pragma_table_list WHERE schema='main' ORDER BY name",
    ));
    requests.push(serde_json::json!({"type":"close"}));
    let json = match hrana(&ui.app, &ns, serde_json::Value::Array(requests)).await {
        Ok(json) => json,
        Err(response) => return *response,
    };
    let pragmas = PRAGMAS
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let value = rows(&json, i)
                .first()
                .and_then(|r| r.first())
                .map(cell_text)
                .unwrap_or_default();
            (*name, value)
        })
        .collect::<BTreeMap<_, _>>();
    let schema = rows(&json, PRAGMAS.len())
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "type": cell_text(r.first().unwrap_or(&serde_json::Value::Null)),
                "name": cell_text(r.get(1).unwrap_or(&serde_json::Value::Null)),
                "table": cell_text(r.get(2).unwrap_or(&serde_json::Value::Null)),
                "sql": cell_text(r.get(3).unwrap_or(&serde_json::Value::Null)),
            })
        })
        .collect::<Vec<_>>();
    let kinds = rows(&json, PRAGMAS.len() + 1)
        .into_iter()
        .map(|r| {
            (
                cell_text(r.first().unwrap_or(&serde_json::Value::Null)),
                cell_text(r.get(1).unwrap_or(&serde_json::Value::Null)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let metrics = parse_metrics(&crate::http::metrics_text(&ui.app));
    let htap: BTreeMap<String, f64> = metrics
        .iter()
        .filter(|(k, _)| k.contains(&format!("ns=\"{ns}\"")) && k.contains("htap_"))
        .map(|(k, v)| (k.split('|').next().unwrap_or(k).to_owned(), *v))
        .collect();
    axum::Json(serde_json::json!({
        "namespace": ns,
        "pragmas": pragmas,
        "schema": schema,
        "table_kinds": kinds,
        "htap": htap,
        "files": files_of(&ui.app.cfg, &ns),
    }))
    .into_response()
}

async fn counts(State(ui): State<Ui>, Path(ns): Path<String>) -> Response {
    if !ui.app.cfg.usable_namespace(&ns) {
        return error(StatusCode::BAD_REQUEST, format!("bad namespace `{ns}`"));
    }
    let listing = match hrana(
        &ui.app,
        &ns,
        serde_json::json!([
            execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"),
            serde_json::json!({"type":"close"})
        ]),
    )
    .await
    {
        Ok(json) => json,
        Err(response) => return *response,
    };
    let names: Vec<String> = rows(&listing, 0)
        .iter()
        .filter_map(|r| r.first().map(cell_text))
        .collect();
    let mut requests: Vec<serde_json::Value> = names
        .iter()
        .map(|n| execute(&format!("SELECT count(*) FROM {}", quote(n))))
        .collect();
    requests.push(serde_json::json!({"type":"close"}));
    let json = match hrana(&ui.app, &ns, serde_json::Value::Array(requests)).await {
        Ok(json) => json,
        Err(response) => return *response,
    };
    let counts = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let n = rows(&json, i)
                .first()
                .and_then(|r| r.first())
                .map(cell_text)
                .unwrap_or_default();
            (name.clone(), n)
        })
        .collect::<BTreeMap<_, _>>();
    axum::Json(counts).into_response()
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[derive(Deserialize)]
struct SqlRequest {
    engine: String,
    sql: String,
    #[serde(default)]
    args: Vec<crate::proto::Value>,
}

async fn sql(State(ui): State<Ui>, Path(ns): Path<String>, body: Bytes) -> Response {
    if !ui.app.cfg.usable_namespace(&ns) {
        return error(StatusCode::BAD_REQUEST, format!("bad namespace `{ns}`"));
    }
    let request: SqlRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(e) => return error(StatusCode::BAD_REQUEST, format!("invalid request: {e}")),
    };
    let mut headers = HeaderMap::new();
    let Ok(host) = format!("{ns}.localhost").parse() else {
        return error(StatusCode::BAD_REQUEST, format!("bad namespace `{ns}`"));
    };
    headers.insert(header::HOST, host);
    let started = Instant::now();
    match request.engine.as_str() {
        "duckdb" => {
            let Ok(body) = serde_json::to_vec(&serde_json::json!({"sql": request.sql, "args": request.args})) else {
                return error(StatusCode::BAD_REQUEST, "cannot encode request");
            };
            crate::http::analytics(
                State(ui.app.clone()),
                axum::Extension(crate::http::Arrived(started)),
                headers,
                Bytes::from(body),
            )
            .await
        }
        "sqlite" => {
            let Ok(body) = serde_json::to_vec(&serde_json::json!({
                "requests": [
                    {"type":"execute","stmt":{"sql": request.sql, "args": request.args}},
                    {"type":"close"}
                ]
            })) else {
                return error(StatusCode::BAD_REQUEST, "cannot encode request");
            };
            crate::http::pipeline(
                State(ui.app.clone()),
                axum::Extension(crate::http::Arrived(started)),
                headers,
                Bytes::from(body),
            )
            .await
        }
        other => error(StatusCode::BAD_REQUEST, format!("unknown engine `{other}`")),
    }
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    #[derive(Serialize)]
    struct E {
        error: String,
    }
    (status, axum::Json(E { error: message.into() })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Registry;
    use tower::ServiceExt;

    fn fixture() -> (tempfile::TempDir, Router, String) {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path());
        let app = Registry::new(Arc::new(cfg));
        let token = new_token();
        let router = router(app, token.clone());
        (dir, router, token)
    }

    async fn send(router: &Router, request: axum::http::Request<axum::body::Body>) -> (StatusCode, serde_json::Value) {
        let response = router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 8 * 1024 * 1024)
            .await
            .unwrap();
        let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, json)
    }

    fn get(uri: &str, token: Option<&str>) -> axum::http::Request<axum::body::Body> {
        let mut builder = axum::http::Request::get(uri);
        if let Some(token) = token {
            builder = builder.header(TOKEN_HEADER, token);
        }
        builder.body(axum::body::Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn the_console_answers_only_with_its_token() {
        let (_dir, router, token) = fixture();
        let (status, _) = send(&router, get("/ui/api/overview", None)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, _) = send(
            &router,
            get("/ui/api/overview", Some("00000000000000000000000000000000")),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, _) = send(&router, get("/ui/api/overview", Some(&token))).await;
        assert_eq!(status, StatusCode::OK);
        // the page itself takes the token from the URL the server printed
        let (status, _) = send(&router, get(&format!("/ui?t={token}"), None)).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn a_page_on_another_origin_cannot_use_the_console() {
        let (_dir, router, token) = fixture();
        for site in ["cross-site", "same-site"] {
            let request = axum::http::Request::get("/ui/api/overview")
                .header(TOKEN_HEADER, &token)
                .header("sec-fetch-site", site)
                .body(axum::body::Body::empty())
                .unwrap();
            let (status, _) = send(&router, request).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{site}");
        }
        // a cross-origin fetch with the custom header must not get a preflight it can use
        let request = axum::http::Request::options("/ui/api/overview")
            .header(TOKEN_HEADER, &token)
            .header("origin", "http://elsewhere.example")
            .body(axum::body::Body::empty())
            .unwrap();
        let (status, _) = send(&router, request).await;
        assert!(!status.is_success(), "{status}");
    }

    #[tokio::test]
    async fn the_console_reports_parameters_against_their_defaults() {
        let (_dir, router, token) = fixture();
        let (status, json) = send(&router, get("/ui/api/overview", Some(&token))).await;
        assert_eq!(status, StatusCode::OK);
        let params = json["params"].as_array().unwrap();
        assert!(params.len() > 30, "{} params", params.len());
        let pool = params.iter().find(|p| p["name"] == "read_pool_size").unwrap();
        assert_eq!(pool["value"], pool["default"], "a default is reported as changed");
        assert_eq!(pool["changed"], false);
        let idle = params.iter().find(|p| p["name"] == "htap_worker_idle_ms").unwrap();
        assert_eq!(idle["changed"], true, "for_tests lowers this one");
    }

    #[tokio::test]
    async fn the_console_runs_sql_and_the_capture_stays_protected() {
        let (_dir, router, token) = fixture();
        let app_request = |sql: &str, engine: &str| {
            axum::http::Request::post("/ui/api/ns/shop/sql")
                .header(TOKEN_HEADER, &token)
                .header(header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({"engine": engine, "sql": sql, "args": []}).to_string(),
                ))
                .unwrap()
        };
        // the namespace does not exist yet
        let (status, _) = send(&router, app_request("SELECT 1", "sqlite")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path());
        let app = Registry::new(Arc::new(cfg));
        app.create("shop", None).await.unwrap();
        let router = super::router(app.clone(), token.clone());
        for sql in [
            "CREATE TABLE orders(id INTEGER PRIMARY KEY, total_cents INTEGER)",
            "INSERT INTO orders VALUES(1,100),(2,250)",
        ] {
            let (status, json) = send(&router, app_request(sql, "sqlite")).await;
            assert_eq!(status, StatusCode::OK, "{sql}: {json}");
        }
        let (status, json) = send(
            &router,
            app_request("SELECT SUM(total_cents) AS s FROM orders", "sqlite"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["results"][0]["response"]["result"]["rows"][0][0]["value"], "350");

        // the mirror is built on the first analytical query, and the same protections apply
        let (status, json) = send(
            &router,
            app_request(
                "SELECT SUM(CAST(total_cents AS BIGINT)) AS s FROM orders_native",
                "duckdb",
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{json}");
        assert_eq!(json["result"]["rows"][0][0]["value"], "350");
        let (_, json) = send(&router, app_request("DELETE FROM xsqld_changelog", "sqlite")).await;
        assert_eq!(json["results"][0]["type"], "error", "{json}");
        let (status, _) = send(&router, app_request("DELETE FROM orders_native", "duckdb")).await;
        assert!(!status.is_success(), "the mirror must stay read-only");
        let (status, _) = send(&router, app_request("SELECT 1", "oracle")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        drop(router);
        let closing = app.clone();
        tokio::task::spawn_blocking(move || closing.shutdown()).await.unwrap();
    }

    /// A registry dropped without `shutdown` must not take the process with it: the sync worker holds only a Weak
    /// to the mirror, so it can still be running when the database's writer goes away
    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_a_registry_with_a_live_mirror_is_safe() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path());
        let app = Registry::new(Arc::new(cfg));
        app.create("shop", None).await.unwrap();
        let token = new_token();
        let router = super::router(app.clone(), token.clone());
        let request = |sql: &str, engine: &str| {
            axum::http::Request::post("/ui/api/ns/shop/sql")
                .header(TOKEN_HEADER, &token)
                .header(header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({"engine": engine, "sql": sql, "args": []}).to_string(),
                ))
                .unwrap()
        };
        send(
            &router,
            request("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)", "sqlite"),
        )
        .await;
        send(&router, request("INSERT INTO t VALUES(1,10),(2,20)", "sqlite")).await;
        let (status, _) = send(
            &router,
            request("SELECT SUM(CAST(v AS BIGINT)) FROM t_native", "duckdb"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        send(&router, request("INSERT INTO t VALUES(3,30)", "sqlite")).await;
        drop(router);
        drop(app);
        // the worker is woken by the write above and may be mid-batch here
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[test]
    fn metrics_text_becomes_series_keyed_by_namespace() {
        let text = "# HELP x y\nxsqld_worker_threads 3\nxsqld_htap_lag{ns=\"shop\"} 12\nxsqld_read_requests_total{ns=\"a\"} 5\nnot_a_number abc\n";
        let series = parse_metrics(text);
        assert_eq!(series.get("xsqld_worker_threads"), Some(&3.0));
        assert_eq!(series.get("xsqld_htap_lag|ns=\"shop\""), Some(&12.0));
        assert_eq!(series.get("xsqld_read_requests_total|ns=\"a\""), Some(&5.0));
        assert!(!series.contains_key("not_a_number"));
    }

    #[test]
    fn a_token_matches_only_itself() {
        let token = new_token();
        assert_eq!(token.len(), 32);
        assert!(same_token(&token, &token));
        assert!(!same_token(&token, &new_token()));
        assert!(!same_token(&token, &token[..31]));
        assert!(!same_token("", &token));
    }

    #[test]
    fn the_console_refuses_a_non_loopback_address() {
        let mut cfg = Config::for_tests(std::path::Path::new("/tmp"));
        cfg.ui_listen_addr = Some("0.0.0.0:8090".parse().unwrap());
        let e = cfg.validate().unwrap_err().to_string();
        assert!(e.contains("--ui-listen-addr"), "{e}");
        cfg.ui_listen_addr = Some("127.0.0.1:8090".parse().unwrap());
        cfg.validate().unwrap();
    }
}
