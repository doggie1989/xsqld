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

## Build & Run (Docker)
```bash
docker run --rm \
  -v "$PWD":/work -w /work \
  -e CARGO_TARGET_DIR=/tmp/cargo-target \
  rust:latest sh -c 'cargo test && cargo clippy --all-targets -- -D warnings && cargo build --release'
```
