//! Counters for `GET /metrics` (Prometheus text format). Plain atomics on the hot path.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

/// Count, sum, and reset-on-read maximum duration in microseconds
#[derive(Default)]
pub struct Timer {
    pub count: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
}

impl Timer {
    pub fn record(&self, d: Duration) {
        let us = d.as_micros() as u64;
        self.count.fetch_add(1, Relaxed);
        self.sum_us.fetch_add(us, Relaxed);
        self.max_us.fetch_max(us, Relaxed);
    }
}

#[derive(Default)]
pub struct NsMetrics {
    pub requests: AtomicU64,
    pub queries: AtomicU64,
    pub errors: AtomicU64,
    pub pinned_streams: AtomicI64,
    pub pinned_timeouts: AtomicU64,
    pub deadline_exceeded: AtomicU64,
    pub kill_queries: AtomicU64,
    pub query: Timer,
    pub pool_wait: Timer,
}

impl NsMetrics {
    pub fn render(&self, ns: &str, out: &mut String) {
        let u = |a: &AtomicU64| a.load(Relaxed);
        let i = |a: &AtomicI64| a.load(Relaxed);
        put(out, "requests_total", ns, u(&self.requests));
        put(out, "queries_total", ns, u(&self.queries));
        put(out, "errors_total", ns, u(&self.errors));
        put(out, "pinned_streams", ns, i(&self.pinned_streams));
        put(out, "pinned_timeout_rollbacks_total", ns, u(&self.pinned_timeouts));
        put(out, "deadline_exceeded_total", ns, u(&self.deadline_exceeded));
        put(out, "kill_queries_total", ns, u(&self.kill_queries));
        for (name, t) in [("query", &self.query), ("pool_wait", &self.pool_wait)] {
            put(out, &format!("{name}_count"), ns, u(&t.count));
            put(out, &format!("{name}_seconds_sum"), ns, Secs(u(&t.sum_us)));
            put(out, &format!("{name}_seconds_max"), ns, Secs(t.max_us.swap(0, Relaxed)));
        }
    }
}

struct Secs(u64);

impl std::fmt::Display for Secs {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:.6}", self.0 as f64 / 1e6)
    }
}

/// `xsqld_build_info{version="…",commit="…"} 1`
pub fn build_info(out: &mut String) {
    let _ = writeln!(
        out,
        "xsqld_build_info{{version=\"{}\",commit=\"{}\"}} 1",
        crate::config::VERSION,
        crate::config::COMMIT
    );
}

pub fn put(out: &mut String, name: &str, ns: &str, v: impl std::fmt::Display) {
    let _ = if ns.is_empty() {
        writeln!(out, "xsqld_{name} {v}")
    } else {
        writeln!(out, "xsqld_{name}{{ns=\"{ns}\"}} {v}")
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_prometheus_metrics() {
        let m = NsMetrics::default();
        m.requests.fetch_add(3, Relaxed);
        m.queries.fetch_add(5, Relaxed);
        m.errors.fetch_add(1, Relaxed);
        m.query.record(Duration::from_millis(2));
        m.query.record(Duration::from_millis(6));

        let mut out = String::new();
        m.render("tenant", &mut out);
        assert!(out.contains("xsqld_requests_total{ns=\"tenant\"} 3\n"));
        assert!(out.contains("xsqld_queries_total{ns=\"tenant\"} 5\n"));
        assert!(out.contains("xsqld_errors_total{ns=\"tenant\"} 1\n"));
        assert!(out.contains("xsqld_query_count{ns=\"tenant\"} 2\n"));
        assert!(out.contains("xsqld_query_seconds_sum{ns=\"tenant\"} 0.008000\n"));
        assert!(out.contains("xsqld_query_seconds_max{ns=\"tenant\"} 0.006000\n"));

        let mut again = String::new();
        m.render("tenant", &mut again);
        assert!(again.contains("xsqld_query_seconds_max{ns=\"tenant\"} 0.000000\n"));
    }
}
