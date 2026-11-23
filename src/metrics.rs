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
    pub route_considered: AtomicU64,
    pub routed: AtomicU64,
    pub route_refused: parking_lot::Mutex<std::collections::BTreeMap<&'static str, u64>>,
    /// Mirror gauges and counters, published by the sync worker after each cycle.
    pub mirror_watermark_scn: AtomicU64,
    pub mirror_lag_scn: AtomicU64,
    pub mirror_outbox_rows: AtomicU64,
    pub mirror_tables: AtomicU64,
    pub mirror_cycles: AtomicU64,
    pub mirror_applied: AtomicU64,
    pub mirror_errors: AtomicU64,
    pub mirror_churn_rebuilds: AtomicU64,
    pub mirror_rebuilds: parking_lot::Mutex<std::collections::BTreeMap<String, u64>>,
    pub mirror_snapshot_rows: AtomicU64,
    pub mirror_route_cache_entries: AtomicU64,
    pub mirror_snapshot: Timer,
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
        put(out, "route_considered_total", ns, u(&self.route_considered));
        put(out, "routed_total", ns, u(&self.routed));
        for (reason, n) in self.route_refused.lock().iter() {
            out.push_str(&format!(
                "xsqld_route_refused_total{{ns=\"{ns}\",reason=\"{reason}\"}} {n}\n"
            ));
        }
        put(out, "mirror_watermark_scn", ns, u(&self.mirror_watermark_scn));
        put(out, "mirror_lag_scn", ns, u(&self.mirror_lag_scn));
        put(out, "mirror_outbox_rows", ns, u(&self.mirror_outbox_rows));
        put(out, "mirror_tables", ns, u(&self.mirror_tables));
        put(out, "mirror_cycles_total", ns, u(&self.mirror_cycles));
        put(out, "mirror_applied_total", ns, u(&self.mirror_applied));
        put(out, "mirror_errors_total", ns, u(&self.mirror_errors));
        put(out, "mirror_churn_rebuilds_total", ns, u(&self.mirror_churn_rebuilds));
        put(out, "mirror_snapshot_rows_total", ns, u(&self.mirror_snapshot_rows));
        put(
            out,
            "mirror_route_cache_entries",
            ns,
            u(&self.mirror_route_cache_entries),
        );
        for (table, n) in self.mirror_rebuilds.lock().iter() {
            out.push_str(&format!(
                "xsqld_mirror_rebuild_total{{ns=\"{ns}\",table=\"{table}\"}} {n}\n"
            ));
        }
        for (name, t) in [
            ("query", &self.query),
            ("pool_wait", &self.pool_wait),
            ("mirror_snapshot", &self.mirror_snapshot),
        ] {
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
