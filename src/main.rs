//! xsqld: Hrana-over-HTTP gateway backed by SeekDB / OceanBase / MySQL.

mod backend;
mod capture;
mod config;
mod exec;
mod http;
mod metrics;
mod mirror;
mod proto;
mod route;
mod sql;
mod stream;
mod sync;

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;

#[cfg(not(feature = "alloc-stats"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Counting wrapper used only under `--features alloc-stats`. Live Rust bytes growing in step with RSS
/// means a Rust-side leak; RSS growing while live bytes stay flat means native (C++) allocations, which
/// neither the Rust allocator nor DuckDB's tagged `duckdb_memory()` accounting can see.
#[cfg(feature = "alloc-stats")]
pub mod alloc_stats {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicI64, Ordering::Relaxed};

    pub static LIVE: AtomicI64 = AtomicI64::new(0);
    pub struct Counting;
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            let p = unsafe { System.alloc(l) };
            if !p.is_null() {
                LIVE.fetch_add(l.size() as i64, Relaxed);
            }
            p
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            LIVE.fetch_sub(l.size() as i64, Relaxed);
            unsafe { System.dealloc(p, l) }
        }
        unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
            let np = unsafe { System.realloc(p, l, new) };
            if !np.is_null() {
                LIVE.fetch_add(new as i64 - l.size() as i64, Relaxed);
            }
            np
        }
    }
}

#[cfg(feature = "alloc-stats")]
#[global_allocator]
static GLOBAL: alloc_stats::Counting = alloc_stats::Counting;

/// A panic anywhere ends the process so a service supervisor can restart cleanly.
fn abort_on_panic() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let thread = std::thread::current();
        tracing::error!(thread = thread.name().unwrap_or("unnamed"), %info, "panic, aborting");
        default(info);
        std::process::abort();
    }));
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    abort_on_panic();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let cfg = config::Config::parse();
    cfg.validate()?;
    let cfg = Arc::new(cfg);

    let backend = backend::Backend::connect(cfg.clone()).await?;
    let user = tokio::net::TcpListener::bind(cfg.http_listen_addr).await?;
    let admin = tokio::net::TcpListener::bind(cfg.admin_listen_addr).await?;

    tracing::info!(
        version = config::VERSION,
        commit = config::COMMIT,
        http = %cfg.http_listen_addr,
        admin = %cfg.admin_listen_addr,
        pool_min = cfg.pool_min,
        pool_max = cfg.pool_max,
        auth_enabled = cfg.auth_token.is_some(),
        "xsqld started"
    );

    let sweeper = backend.clone();
    let pinned_limit = cfg.pinned_tx_timeout();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        loop {
            tick.tick().await;
            sweeper.sweep_pinned().await;
            sweeper.streams.sweep(pinned_limit);
        }
    });

    if cfg.idle_close_ms > 0 {
        let idler = backend.clone();
        let every = Duration::from_millis((cfg.idle_close_ms / 4).clamp(1_000, 30_000));
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            loop {
                tick.tick().await;
                idler.close_idle_pools().await;
            }
        });
    }

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let mirror_worker = if cfg.mirror_enabled {
        Some(sync::spawn_worker(backend.clone(), cfg.clone(), stop_rx.clone()))
    } else {
        None
    };
    let user_srv = http::serve(user, http::user_router(backend.clone()), cfg.clone(), stop_rx.clone());
    let admin_srv = http::serve(admin, http::admin_router(backend.clone()), cfg.clone(), stop_rx);
    let servers = tokio::spawn(async move {
        tokio::join!(user_srv, admin_srv);
    });

    shutdown_signal().await;
    tracing::info!("stopping: finishing requests in flight");
    let _ = stop_tx.send(true);
    if tokio::time::timeout(cfg.request_timeout() + Duration::from_secs(5), servers)
        .await
        .is_err()
    {
        tracing::warn!("requests still running at shutdown");
    }
    backend.shutdown().await;
    if let Some(w) = mirror_worker {
        let _ = w.join();
    }
    tracing::info!("stopped");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_panic_on_any_thread_aborts_the_process() {
        if std::env::var_os("XSQLD_PANIC_CHILD").is_some() {
            super::abort_on_panic();
            let _ = std::thread::Builder::new()
                .name("xsqld-worker".into())
                .spawn(|| panic!("boom"))
                .unwrap()
                .join();
            std::thread::sleep(std::time::Duration::from_secs(5));
            println!("survived the panic");
            return;
        }
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::a_panic_on_any_thread_aborts_the_process",
                "--nocapture",
            ])
            .env("XSQLD_PANIC_CHILD", "thread")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(!out.status.success(), "{stdout}");
        assert!(!stdout.contains("survived the panic"), "{stdout}");
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(out.status.signal(), Some(6), "SIGABRT expected: {:?}", out.status);
        }
    }
}
