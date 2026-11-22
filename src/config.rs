use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;

/// Release version; Cargo.toml is its only source
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// Git commit the binary was built from (`build.rs`), `unknown` without git
pub const COMMIT: &str = env!("XSQLD_GIT_COMMIT");

/// Backend system databases that must never be exposed or dropped as Hrana namespaces
pub const RESERVED_NAMESPACES: [&str; 5] = [
    "oceanbase",
    "mysql",
    "information_schema",
    "performance_schema",
    "sys",
];

#[derive(Parser, Debug, Clone, serde::Serialize)]
#[command(
    name = "xsqld",
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("XSQLD_GIT_COMMIT"), ")"),
    about = "Hrana-over-HTTP gateway backed by SeekDB / OceanBase / MySQL"
)]
pub struct Config {
    /// MySQL / SeekDB host to connect to
    #[arg(long, env = "XSQLD_MYSQL_HOST", default_value = "127.0.0.1")]
    pub mysql_host: String,
    /// MySQL / SeekDB port to connect to
    #[arg(long, env = "XSQLD_MYSQL_PORT", default_value_t = 2881)]
    pub mysql_port: u16,
    /// MySQL / SeekDB username
    #[arg(long, env = "XSQLD_MYSQL_USER", default_value = "root")]
    pub mysql_user: String,
    /// MySQL / SeekDB password (can also be supplied via XSQLD_MYSQL_PASSWORD)
    #[arg(long, env = "XSQLD_MYSQL_PASSWORD", default_value = "")]
    #[serde(skip_serializing)]
    pub mysql_password: String,
    /// Full MySQL connection URL (e.g. mysql://user:pass@host:2881). Overrides --mysql-host/port/user/password
    #[arg(long, env = "XSQLD_MYSQL_URL")]
    #[serde(skip_serializing)]
    pub mysql_url: Option<String>,

    /// Optional static Bearer token checked via `Authorization: Bearer <token>` on all endpoints except `/health`
    #[arg(long, env = "XSQLD_AUTH_TOKEN")]
    #[serde(skip_serializing)]
    pub auth_token: Option<String>,

    /// Hrana over HTTP (`/v2/pipeline`, `/health`, `/metrics`)
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub http_listen_addr: SocketAddr,
    /// Admin API (`/v1/namespaces/<ns>/create`, `DELETE /v1/namespaces/<ns>`, `/health`, `/metrics`)
    #[arg(long, default_value = "127.0.0.1:8081")]
    pub admin_listen_addr: SocketAddr,

    /// Accepted for sqld command line compatibility; namespaces are always on
    #[arg(long, hide = true)]
    pub enable_namespaces: bool,
    /// Accepted for sqld command line compatibility; ignored
    #[arg(long, hide = true)]
    pub max_log_size: Option<u64>,

    /// Allow listening on non-loopback addresses (refused by default)
    #[arg(long)]
    pub allow_non_loopback: bool,
    /// Requests are answered only when their Host header is an IP address, a name without a dot (`localhost`,
    /// `sqld-admin`) or a name under one of these suffixes (comma separated); others get 403. `*` accepts every Host
    #[arg(long, value_delimiter = ',', default_value = "db,local,localhost")]
    pub allowed_host_suffixes: Vec<String>,

    /// Minimum idle connections retained in each namespace's MySQL connection pool
    #[arg(long, default_value_t = 2)]
    pub pool_min: usize,
    /// Maximum connections opened in each namespace's MySQL connection pool
    #[arg(long, default_value_t = 64)]
    pub pool_max: usize,
    /// Prepared statement cache capacity per pooled MySQL connection
    #[arg(long, default_value_t = 64)]
    pub stmt_cache_size: usize,
    /// Maximum queries executing concurrently against the MySQL backend across all namespaces
    #[arg(long, default_value_t = 128)]
    pub max_concurrent_queries: usize,
    /// Close and release a namespace's connection pool after it has been idle this long (0: never close)
    #[arg(long, default_value_t = 300_000)]
    pub idle_close_ms: u64,

    /// Time limit of one HTTP request (acquiring connection plus SQL execution); SQL still running at deadline is killed
    #[arg(long, default_value_t = 40_000)]
    pub request_timeout_ms: u64,
    /// A stream (baton) unused for this long expires
    #[arg(long, default_value_t = 10_000)]
    pub stream_idle_ms: u64,
    /// A transaction left open across HTTP requests is rolled back and unpinned after this long
    #[arg(long, default_value_t = 5_000)]
    pub pinned_tx_timeout_ms: u64,

    /// Largest HTTP request body in bytes
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    pub max_request_bytes: usize,
    /// Open HTTP client connections per port; further connections wait in the listen backlog
    #[arg(long, default_value_t = 1024)]
    pub max_connections: usize,
    /// Requests being handled at once per port; more are answered with 503
    #[arg(long, default_value_t = 512)]
    pub max_concurrent_requests: usize,
    /// A connection must send a complete request head within this long (also ends idle keep-alive connections)
    #[arg(long, default_value_t = 10_000)]
    pub header_read_timeout_ms: u64,
    /// A request body must arrive within this long after its head
    #[arg(long, default_value_t = 10_000)]
    pub body_read_timeout_ms: u64,
    /// A response the client takes no data of for this long is abandoned and its connection closed
    #[arg(long, default_value_t = 60_000)]
    pub response_write_timeout_ms: u64,
    /// Most statements (execute requests plus batch steps) in one HTTP request
    #[arg(long, default_value_t = 4096)]
    pub max_statements: usize,
    /// Largest result of one HTTP request (estimated JSON size of all rows)
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    pub max_response_bytes: u64,
}

impl Config {
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }
    pub fn stream_idle(&self) -> Duration {
        Duration::from_millis(self.stream_idle_ms)
    }
    pub fn pinned_tx_timeout(&self) -> Duration {
        Duration::from_millis(self.pinned_tx_timeout_ms)
    }
    pub fn header_read_timeout(&self) -> Duration {
        Duration::from_millis(self.header_read_timeout_ms)
    }
    pub fn body_read_timeout(&self) -> Duration {
        Duration::from_millis(self.body_read_timeout_ms)
    }
    pub fn response_write_timeout(&self) -> Duration {
        Duration::from_millis(self.response_write_timeout_ms)
    }

    /// Is a request with this Host header answered? (`--allowed-host-suffixes`)
    pub fn host_allowed(&self, host: &str) -> bool {
        if self.allowed_host_suffixes.iter().any(|s| s == "*") {
            return true;
        }
        if host.starts_with('[') {
            return true;
        }
        let name = host.split(':').next().unwrap_or_default();
        let name = name.strip_suffix('.').unwrap_or(name);
        if !name.contains('.') || name.parse::<std::net::Ipv4Addr>().is_ok() {
            return true;
        }
        let name = name.as_bytes();
        self.allowed_host_suffixes.iter().any(|s| {
            let s = s.trim_start_matches('.').as_bytes();
            !s.is_empty()
                && name.len() > s.len()
                && name[name.len() - s.len() - 1] == b'.'
                && name[name.len() - s.len()..].eq_ignore_ascii_case(s)
        })
    }

    /// Whether a namespace name is structurally valid and does not target a system database
    pub fn usable_namespace(&self, ns: &str) -> bool {
        valid_namespace(ns) && !RESERVED_NAMESPACES.iter().any(|r| r.eq_ignore_ascii_case(ns))
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        for addr in [self.http_listen_addr, self.admin_listen_addr] {
            if !addr.ip().is_loopback() && !self.allow_non_loopback {
                anyhow::bail!("refusing to listen on non-loopback address {addr} (pass --allow-non-loopback)");
            }
        }
        if self.pool_min > self.pool_max {
            anyhow::bail!(
                "--pool-min ({}) must not exceed --pool-max ({})",
                self.pool_min,
                self.pool_max
            );
        }
        if let Some(tok) = &self.auth_token
            && tok.is_empty()
        {
            anyhow::bail!("--auth-token must not be empty");
        }
        if self.mysql_host.trim().is_empty() && self.mysql_url.is_none() {
            anyhow::bail!("--mysql-host must not be empty");
        }
        if self.mysql_user.trim().is_empty() && self.mysql_url.is_none() {
            anyhow::bail!("--mysql-user must not be empty");
        }
        for (flag, value, min, max) in self.numeric_bounds() {
            if !(min..=max).contains(&value) {
                anyhow::bail!("--{flag} is {value}; it must be between {min} and {max}");
            }
        }
        Ok(())
    }

    fn numeric_bounds(&self) -> [(&'static str, i128, i128, i128); 17] {
        const HOUR_MS: i128 = 3_600_000;
        const GIB: i128 = 1 << 30;
        let n = |v: usize| v as i128;
        let u = |v: u64| v as i128;
        [
            ("mysql-port", self.mysql_port as i128, 1, 65_535),
            ("pool-min", n(self.pool_min), 0, 1024),
            ("pool-max", n(self.pool_max), 1, 1024),
            ("stmt-cache-size", n(self.stmt_cache_size), 0, 4096),
            ("max-concurrent-queries", n(self.max_concurrent_queries), 1, 65_536),
            ("idle-close-ms", u(self.idle_close_ms), 0, 24 * HOUR_MS),
            ("request-timeout-ms", u(self.request_timeout_ms), 1, HOUR_MS),
            ("stream-idle-ms", u(self.stream_idle_ms), 1, 24 * HOUR_MS),
            ("pinned-tx-timeout-ms", u(self.pinned_tx_timeout_ms), 1, HOUR_MS),
            ("max-request-bytes", n(self.max_request_bytes), 1024, GIB),
            ("max-connections", n(self.max_connections), 1, 1_000_000),
            ("max-concurrent-requests", n(self.max_concurrent_requests), 1, 1_000_000),
            ("header-read-timeout-ms", u(self.header_read_timeout_ms), 1, HOUR_MS),
            ("body-read-timeout-ms", u(self.body_read_timeout_ms), 1, HOUR_MS),
            (
                "response-write-timeout-ms",
                u(self.response_write_timeout_ms),
                1,
                HOUR_MS,
            ),
            ("max-statements", n(self.max_statements), 1, 1_000_000),
            ("max-response-bytes", u(self.max_response_bytes), 1024, 16 * GIB),
        ]
    }

    #[cfg(test)]
    pub fn for_tests() -> Self {
        Config::parse_from(["xsqld"])
    }
}

/// Namespace names: letters, digits, `-` and `_`, starting with a letter or digit (<= 64 chars)
pub fn valid_namespace(ns: &str) -> bool {
    let b = ns.as_bytes();
    !b.is_empty()
        && b.len() <= 64
        && b[0].is_ascii_alphanumeric()
        && b.iter().all(|c| c.is_ascii_alphanumeric() || *c == b'-' || *c == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn namespace_names() {
        let c = Config::for_tests();
        assert!(valid_namespace("tenant"));
        assert!(valid_namespace("restore-check"));
        assert!(valid_namespace("TDB_000"));
        assert!(!valid_namespace(""));
        assert!(!valid_namespace("../x"));
        assert!(!valid_namespace(".tmp"));
        assert!(!valid_namespace("a/b"));
        assert!(!valid_namespace(&"a".repeat(65)));

        assert!(c.usable_namespace("tenant"));
        for sys in ["oceanbase", "OceanBase", "mysql", "MySQL", "information_schema", "performance_schema", "sys"] {
            assert!(!c.usable_namespace(sys), "{sys} must be rejected");
        }
    }

    #[test]
    fn host_allowlist() {
        let c = Config::for_tests();
        for host in [
            "tenant.db",
            "tenant.local",
            "TDB_000.Local:8080",
            "platform.localhost:8080",
            "tenant.db.",
            "sqld-admin",
            "localhost",
            "localhost:8081",
            "127.0.0.1",
            "127.0.0.1:8080",
            "10.0.0.5:8080",
            "[::1]:8080",
            "",
        ] {
            assert!(c.host_allowed(host), "{host}");
        }
        for host in [
            "tenant.evil.example",
            "tenant.evil.example:8080",
            "tenant.db.evil.example",
            "evil.example",
            "tenantdb.xlocal",
            "127.0.0.1.evil.example",
            "a.b",
        ] {
            assert!(!c.host_allowed(host), "{host}");
        }
        let c = Config::parse_from(["xsqld", "--allowed-host-suffixes", "db,.Sqld.Example"]);
        assert!(c.host_allowed("tenant.sqld.example:8080"));
        assert!(c.host_allowed("tenant.db"));
        assert!(!c.host_allowed("tenant.local"));
        assert!(!c.host_allowed("sqld.example"));
        assert!(Config::parse_from(["xsqld", "--allowed-host-suffixes", "*"]).host_allowed("tenant.evil.example"));
        assert!(!Config::parse_from(["xsqld", "--allowed-host-suffixes", ""]).host_allowed("tenant.db"));
    }

    #[test]
    fn version_names_the_commit() {
        let e = Config::try_parse_from(["xsqld", "--version"]).unwrap_err();
        assert_eq!(e.kind(), clap::error::ErrorKind::DisplayVersion);
        assert_eq!(e.to_string(), format!("xsqld {VERSION} ({COMMIT})\n"));
        assert!(!COMMIT.trim().is_empty());
    }

    #[test]
    fn every_numeric_flag_is_bounded_on_both_sides() {
        let ok = |args: &[&str]| {
            let mut v = vec!["xsqld"];
            v.extend_from_slice(args);
            Config::try_parse_from(v)
                .map_err(|e| e.to_string())?
                .validate()
                .map_err(|e| e.to_string())
        };
        ok(&[
            "--http-listen-addr",
            "127.0.0.1:8080",
            "--admin-listen-addr",
            "127.0.0.1:8081",
            "--enable-namespaces",
            "--max-log-size",
            "32",
        ])
        .unwrap();
        let c = Config::parse_from(["xsqld"]);
        let bounds = c.numeric_bounds();
        let mut flags: Vec<&str> = bounds.iter().map(|b| b.0).collect();
        flags.sort_unstable();
        flags.dedup();
        assert_eq!(flags.len(), bounds.len(), "a flag is listed twice");
        for (flag, value, min, max) in bounds {
            assert!((min..=max).contains(&value), "default of --{flag}");
            // keep pool_min <= pool_max valid when testing boundaries of either
            let extra: &[&str] = match flag {
                "pool-min" => &["--pool-max=1024"],
                "pool-max" => &["--pool-min=0"],
                _ => &[],
            };
            for bad in [min - 1, max + 1] {
                let mut args = vec![format!("--{flag}={bad}")];
                args.extend(extra.iter().map(|s| (*s).to_owned()));
                let refs: Vec<&str> = args.iter().map(String::as_str).collect();
                let e = ok(&refs).unwrap_err();
                assert!(
                    e.contains(&format!("--{flag}")) || e.contains("invalid value"),
                    "--{flag}={bad}: {e}"
                );
            }
            for good in [min, max] {
                let mut args = vec![format!("--{flag}={good}")];
                args.extend(extra.iter().map(|s| (*s).to_owned()));
                let refs: Vec<&str> = args.iter().map(String::as_str).collect();
                ok(&refs).unwrap_or_else(|e| panic!("--{flag}={good}: {e}"));
            }
        }
        let numeric = Config::command()
            .get_arguments()
            .filter(|a| a.get_action().takes_values() && !a.is_hide_set())
            .filter(|a| {
                let id = a.get_id().as_str();
                ![
                    "mysql_host",
                    "mysql_user",
                    "mysql_password",
                    "mysql_url",
                    "auth_token",
                    "http_listen_addr",
                    "admin_listen_addr",
                    "allowed_host_suffixes",
                ]
                .contains(&id)
            })
            .count();
        assert_eq!(numeric, bounds.len(), "a numeric flag has no bounds");
    }

    #[test]
    fn validates_pool_auth_and_loopback() {
        let mut c = Config::for_tests();
        c.http_listen_addr = "0.0.0.0:8080".parse().unwrap();
        assert!(c.validate().is_err());
        c.allow_non_loopback = true;
        assert!(c.validate().is_ok());

        c.pool_min = 10;
        c.pool_max = 5;
        assert!(c.validate().is_err());
        c.pool_min = 2;
        assert!(c.validate().is_ok());

        c.auth_token = Some(String::new());
        assert!(c.validate().is_err());
        c.auth_token = Some("secret".into());
        assert!(c.validate().is_ok());
    }
}
