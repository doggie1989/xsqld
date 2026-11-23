# xsqld (SeekDB / MySQL Backend Gateway)

`xsqld` is a Rust gateway sidecar that exposes [`sqld`](https://github.com/tursodatabase/libsql)'s **Hrana-over-HTTP** (`POST /v2/pipeline`) and **Admin** (`/v1/namespaces/...`) APIs backed by **SeekDB / OceanBase CE / MySQL** over native MySQL protocol (`mysql_async`).

Clients get simultaneous dual-protocol access to the exact same data:
- **MySQL / SeekDB (`2881`)** for ORMs, BI tools, MySQL CLI, vectorized scans, and hybrid/columnar tables.
- **Hrana HTTP (`8080`)** for lightweight HTTP/Edge clients (`@libsql/client`, curl, workers) speaking MySQL SQL dialect.

## Endpoints

### User port (`--http-listen-addr`, default `127.0.0.1:8080`)
- `POST /v2/pipeline` — Hrana v2 JSON (`execute`, `batch` with `BatchCond`, `close`, stateful `baton` transactions). Namespace is selected from the first label of the `Host` header (`app1.db` -> `app1`).
- `GET /health` — JSON version & commit info.
- `GET /metrics` — Prometheus text metrics.

### Admin port (`--admin-listen-addr`, default `127.0.0.1:8081`)
- `POST /v1/namespaces/{ns}/create` — Creates MySQL database `{ns}` (body: `{}` JSON). System databases (`oceanbase`, `mysql`, `information_schema`, `performance_schema`, `sys`) are rejected.
- `DELETE /v1/namespaces/{ns}` — Closes pools/transactions for `{ns}` and drops database `{ns}`.
- `GET /health`, `GET /metrics`

Databases created directly via the MySQL port (`2881`) are discovered lazily via `information_schema.SCHEMATA`.

## Authentication & Security
- **Without `--auth-token`**: Protected by Host allowlist (`--allowed-host-suffixes`, default `db,local,localhost`) and browser CSRF rejection (`Origin` / `Sec-Fetch-Site` -> `403`).
- **With `--auth-token` / `XSQLD_AUTH_TOKEN`**: Requires `Authorization: Bearer <token>` on all non-`/health` endpoints (`401` on missing/invalid token) in addition to Host checking.

## Key Flags
| Flag | Env | Default | Description |
| --- | --- | --- | --- |
| `--mysql-host` | `XSQLD_MYSQL_HOST` | `127.0.0.1` | SeekDB / MySQL host |
| `--mysql-port` | `XSQLD_MYSQL_PORT` | `2881` | SeekDB / MySQL port |
| `--mysql-user` | `XSQLD_MYSQL_USER` | `root` | Database user |
| `--mysql-password` | `XSQLD_MYSQL_PASSWORD` | `""` | Database password |
| `--mysql-url` | `XSQLD_MYSQL_URL` | — | Optional full `mysql://...` URL |
| `--auth-token` | `XSQLD_AUTH_TOKEN` | — | Optional Bearer token |
| `--pool-min` / `--pool-max` | — | `2` / `64` | Per-namespace pool bounds |
| `--pinned-tx-timeout-ms` | — | `5000` | Auto-rollback idle baton transaction |
| `--request-timeout-ms` | — | `40000` | Pipeline deadline (+ `KILL QUERY` on backend) |

## Columnar mirror (opt-in HTAP)

`--mirror-enabled` keeps an in-process **DuckDB** mirror of every user table in each namespace and
serves eligible `SELECT`s from it. SeekDB stays the row-store authority: clients writing over the
MySQL port (`2881`) are captured by per-database triggers into an outbox, and a worker applies changes
at a commit-SCN watermark. Writes never go through DuckDB.

Routing is conservative — a query is served from the mirror **only when it is provably equivalent and
actually faster**; everything else falls back to SeekDB. Refusals are counted per reason in
`xsqld_route_refused_total{reason}`, and the slow log tags `engine="duckdb"|"seekdb"`.

| Flag | Default | Description |
| --- | --- | --- |
| `--mirror-enabled` | off | Turn the mirror and router on |
| `--mirror-dir` | `xsqld-mirrors` | One DuckDB file per namespace |
| `--mirror-interval-ms` | `1000` | Sync cycle period |
| `--mirror-batch` | `10000` | Outbox rows applied per cycle |
| `--mirror-memory-limit-mb` | `512` | DuckDB `memory_limit` (spills beyond it) |
| `--route-min-rows` | `200000` | Minimum table size before routing is considered |
| `--mirror-query-timeout-ms` | `600000` | Per-query hint for snapshots (SeekDB's own default is 10s) |
| `--mirror-checkpoint-cycles` | `150` | `CHECKPOINT` cadence, keeps the WAL bounded |
| `--mirror-reopen-cycles` | `120` | Base period for dropping and rebuilding the DuckDB instance |
| `--mirror-reopen-duty-pct` | `5` | Cap on time spent re-snapshotting; raises the period for large mirrors |

### Maintenance window — required for bulk operations

```
POST /v1/namespaces/{ns}/maintenance/{table}/begin   # admin port: drop capture triggers
POST /v1/namespaces/{ns}/maintenance/{table}/end     # rebuild triggers + full snapshot
```

**`LOAD DATA` does not work on a mirrored table.** SeekDB rejects it with
`1235 if table has insert or update trigger, load data not supported`, and the mirror installs
INSERT/UPDATE triggers on every table by default. Open a maintenance window first, run `LOAD DATA`,
then close it — the closing snapshot picks up everything loaded. The same applies to other bulk
loads: per-row triggers cost ~11% on single-row OLTP but ~72% on 1000-row batches.

### Memory: `--mirror-reopen-cycles`

DuckDB's hash-join build side retains `BlockHandle` metadata that `CHECKPOINT` does not reclaim, so a
long-running mirror grows RSS slowly (measured ~2.7 MB/min under continuous apply). Periodically
dropping and rebuilding the instance releases it; the mirror is a derived cache, so this is safe.

The period **adapts to mirror size**. A reopen re-snapshots every table, so its cost scales with row
count: re-snapshotting a 10M-row table (~16s measured) every 48s would spend a third of the time
rebuilding and fall back to row-store reads meanwhile. The effective period is therefore
`max(--mirror-reopen-cycles, last_snapshot_time / (duty% × cycle_interval))`, keeping re-snapshotting
under `--mirror-reopen-duty-pct`. Small mirrors keep the short base period and stay tightly bounded
(120 cycles held RSS growth to +3.9% over a 60-minute soak); large ones stretch automatically.
Measured: a 3-row table kept the base period, a 10M-row table with a 15.7s snapshot had its period
raised accordingly. `xsqld_mirror_*` metrics and the reopen log line report the effective threshold.

## Build & Run (Docker)
```bash
docker run --rm \
  -v "$PWD":/work -w /work \
  -e CARGO_TARGET_DIR=/tmp/cargo-target \
  rust:latest sh -c 'cargo test && cargo clippy --all-targets -- -D warnings && cargo build --release'
```
