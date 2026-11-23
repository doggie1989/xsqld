# MySQL protocol listener

`xsqld --mysql-listen-addr 127.0.0.1:3306` speaks the MySQL wire protocol, so `mysql`, ORMs,
BI tools and `pymysql`/`mysql2` can connect to a SeekDB-backed namespace without going through
Hrana. Every statement arrives at the same entry point the HTTP gateway uses
(`Backend::run_pipeline`), which means the HTAP routing, budgets, timeouts, metrics and the
outbox freshness rules apply to MySQL clients exactly as they do to HTTP clients. There is no
second query engine here, and no second set of semantics.

## Namespace selection

A schema *is* a namespace. The listener resolves it in this order:

1. the database the client named with `-D` / `database=` / `initialDatabase` (CONNECT_WITH_DB);
2. otherwise the username, so `mysql -u myns` reaches namespace `myns`;
3. otherwise `default`.

The name must already exist. An unknown name is refused during the handshake with
`ER_BAD_DB_ERROR` (1049) rather than being accepted and then failing every statement, which would
leave the client believing it were somewhere else.

`USE <db>` (as the whole statement) and `COM_INIT_DB` move the connection to another existing
namespace; an unknown target returns 1049 and leaves the session usable. A statement that merely
starts with `USE`, such as `USE db; SELECT 1` or `SELECT 'USE db'`, is forwarded untouched instead
of being treated as a switch.

`SELECT DATABASE()` reports the current namespace.

## What is implemented

| | |
|---|---|
| Commands | `COM_QUERY`, `COM_INIT_DB`, `COM_PING`, `COM_QUIT`. Anything else → 1047 `ER_UNKNOWN_COM_ERROR`. |
| Protocol | HandshakeV10, 4.1 capability negotiation, `mysql_native_password` (the token is read and ignored — there is no authentication yet). |
| Capabilities | `0x01088209`: LONG_PASSWORD, CONNECT_WITH_DB, PROTOCOL_41, SECURE_CONNECTION, PLUGIN_AUTH, DEPRECATE_EOF. |
| Result sets | Text protocol. Column definitions carry the full `0x0c` fixed block (charset, column length, type, flags, decimals). EOF packets are used unless the client negotiated DEPRECATE_EOF, in which case OK packets replace them. |
| Packets | Chunked at 2^24-1 bytes and reassembled; a message is capped at 256 MB. |
| Errors | The backend's own errno and SQLSTATE reach the client (e.g. `1146 / #42S02` for a missing table). Gateway-level failures get 1205 for a deadline, 1049 for an unavailable namespace, 1047 for an expired stream, 1105 otherwise. |

Type metadata comes from the gateway's coarse decltype vocabulary, widened so that a declared type
can always hold the value it describes:

| gateway decltype | MySQL type sent | charset |
|---|---|---|
| `INTEGER` (any width) | `LONGLONG` (8) | 45 |
| `REAL` | `DOUBLE` (5) | 45 |
| `DECIMAL` | `NEWDECIMAL` (246) | 45 |
| `BLOB` | `BLOB` (252) | 63 (binary) |
| `TEXT` and anything unknown | `VAR_STRING` (253) | 45 |

Values are length-encoded raw bytes. Blob bytes are sent as they are, with no escaping, so a value
containing `0x00`, `0x1a`, a backslash or a quote arrives intact. Text columns advertise a text
collation, not the binary one, so clients decode them as strings instead of handing back bytes.

## Transactions

`BEGIN` / `START TRANSACTION`, `COMMIT` and `ROLLBACK` work. One TCP connection is one gateway
stream, so a transaction pins its backend connection for the whole session: reads inside it see
their own writes, another session does not see them, and `ROLLBACK` really undoes them. Inside a
transaction nothing is routed to the DuckDB mirror — the mirror is not part of the snapshot the
transaction is reading.

A client that dies without committing does not hang the pool: the reaper rolls the transaction
back and disconnects it after `--pinned-tx-timeout-ms` (default 5000), counted by
`xsqld_pinned_timeout_rollbacks_total`.

## Statements the listener does not run as written

* **`SET ...`** is answered with OK and otherwise ignored. Session variables would have to apply to
  a backend connection that is shared with other clients and may be reused at any moment;
  applying them would leak one client's settings into another's.
* **Multiple statements in one `COM_QUERY`** are refused with 1064 before reaching the backend.
  The gateway executes one statement per request, and silently running only the first would be
  worse than saying no.
* **Prepared statements** (`COM_STMT_PREPARE`, `COM_STMT_EXECUTE`) are not implemented. Most
  clients default to `COM_QUERY` (pymysql always, mysql-connector/JDBC unless
  `useServerPrepStmts`/`prepared_statement` is turned on); those that insist must be configured
  not to prepare.
* **TLS**: refused with 1251 `ER_NOT_SUPPORTED_YET` against the real reason, not a garbled
  handshake. Connect with `--ssl-mode=DISABLED` (mysql CLI) or `ssl_mode=DISABLED`.
* `SHOW TABLES`, `SHOW DATABASES`, `SELECT @@sql_mode`, `EXPLAIN` and `DO` are forwarded to the
  backend, so they report the backend's state. That includes `xsqld_changelog`, the outbox the
  mirror is fed from, and `CONNECTION_ID()`, which is the *backend* connection id rather than a
  session id.

## Routing through the listener

The gates are the gateway's: a single `SELECT` in autocommit, passing the structural and collation
equivalence checks and the `EXPLAIN` benefit gate, with the mirror fresh enough (`--route-min-rows`
bounds the tiny-table case). `xsqld_routed_total{ns=...}` moving when a statement arrives over the
MySQL port is the proof, and `tests/test_mysql_route.py` asserts it rather than inferring it from
latency.

## Running it

```bash
xsqld \
  --mysql-host 127.0.0.1 --mysql-port 2881 --mysql-user root \
  --http-listen-addr 127.0.0.1:8080 --admin-listen-addr 127.0.0.1:8081 \
  --mysql-listen-addr 127.0.0.1:3306 --mirror-enabled
```

The listener binds whenever xsqld runs and there is no switch to turn it off; if the port is
already taken (a local MySQL on 3306 is the usual case) xsqld refuses to start, so point it
somewhere else with `--mysql-listen-addr 127.0.0.1:3307`. It binds loopback only unless
`--allow-non-loopback` is passed, and it is subject to `--max-connections`,
`--max-concurrent-queries` and `--request-timeout-ms` like any other front end.

```bash
mysql -h 127.0.0.1 -P 3306 -u default -D myns --ssl-mode=DISABLED -e "SELECT COUNT(*) FROM t"
```

## Tests

Three suites, all against a real backend; they do not mock the protocol.

```bash
cargo test                                              # unit tests, incl. packet framing and USE parsing

python3 tests/test_mysql_protocol.py                    # raw socket client: byte-exact handshake,
                                                        # EOF vs DEPRECATE_EOF, errno+SQLSTATE, chunking
python3 tests/test_mysql_e2e.py                         # pymysql: types, blobs, HTTP/MySQL parity,
                                                        # USE and COM_INIT_DB, transactions, errors
python3 tests/test_mysql_route.py                       # spawns xsqld with the mirror on and proves
                                                        # the listener's SELECTs are routed

# env: XS_BACKEND_PORT (default 12981), XS_BACKEND_USER, XS_BACKEND_PASSWORD,
#      XS_MYSQL_PORT (default 3308), XS_HTTP_PORT, XS_ADMIN_PORT, XS_NAMESPACE, XS_BIN
```

`test_mysql_route.py` starts its own `xsqld` because routing needs flags the default server does
not have (`--mirror-enabled`, and `--route-min-rows` far below the 200000 default).

## Limitations

No authentication, no TLS, no prepared statements, no session variables. The listener shares the
gateway's stream semantics, so a MySQL session is a stream: it is subject to
`--stream-idle-ms`/`--pinned-tx-timeout-ms` sweeping, and a session that is idle long enough loses
an open transaction (the next statement gets `Stream expired`, 1047).
