#!/usr/bin/env python3
"""Live end-to-end integration tests & latency comparison for SeekDB-backed xsqld."""

import argparse
import base64
import http.client
import json
import statistics
import time
import urllib.parse

import pymysql


class HttpClient:
    def __init__(self, base_url: str):
        u = urllib.parse.urlparse(base_url)
        self.host = u.hostname
        self.port = u.port or 80

    def request(self, method: str, path: str, headers=None, body: bytes | None = None):
        conn = http.client.HTTPConnection(self.host, self.port, timeout=15)
        try:
            conn.request(method, path, body=body, headers=headers or {})
            resp = conn.getresponse()
            raw = resp.read()
            return resp.status, dict(resp.getheaders()), raw
        finally:
            conn.close()


class KeepAliveHttp:
    def __init__(self, base_url: str, headers: dict):
        u = urllib.parse.urlparse(base_url)
        self.host = u.hostname
        self.port = u.port or 80
        self.headers = headers
        self.conn = http.client.HTTPConnection(self.host, self.port, timeout=15)

    def post_json(self, path: str, payload: dict):
        body = json.dumps(payload).encode()
        for attempt in range(2):
            try:
                self.conn.request("POST", path, body=body, headers=self.headers)
                resp = self.conn.getresponse()
                raw = resp.read()
                if resp.status != 200:
                    raise AssertionError(f"HTTP {resp.status}: {raw.decode(errors='replace')}")
                return json.loads(raw)
            except (http.client.RemoteDisconnected, BrokenPipeError, ConnectionResetError):
                self.conn.close()
                self.conn = http.client.HTTPConnection(self.host, self.port, timeout=15)
                if attempt == 1:
                    raise

    def close(self):
        self.conn.close()


def exec_stmt(http_cli: HttpClient, ns_host: str, sql: str, args=None, named_args=None, baton=None, extra_headers=None):
    stmt = {"sql": sql}
    if args is not None:
        stmt["args"] = args
    if named_args is not None:
        stmt["named_args"] = named_args
    payload = {"baton": baton, "requests": [{"type": "execute", "stmt": stmt}]}
    hdrs = {"Host": ns_host, "Content-Type": "application/json"}
    if extra_headers:
        hdrs.update(extra_headers)
    status, _, raw = http_cli.request("POST", "/v2/pipeline", hdrs, json.dumps(payload).encode())
    return status, json.loads(raw) if raw.startswith(b"{") else raw.decode(errors="replace")


def must_ok(http_cli: HttpClient, ns_host: str, sql: str, args=None, named_args=None, baton=None):
    st, data = exec_stmt(http_cli, ns_host, sql, args=args, named_args=named_args, baton=baton)
    assert st == 200, f"expected 200 got {st}: {data}"
    res0 = data["results"][0]
    assert res0["type"] == "ok", f"expected ok got {res0} for sql={sql!r}"
    return data["baton"], res0["response"]["result"]


def must_err(http_cli: HttpClient, ns_host: str, sql: str, args=None, named_args=None, baton=None):
    st, data = exec_stmt(http_cli, ns_host, sql, args=args, named_args=named_args, baton=baton)
    assert st == 200, f"expected 200 got {st}: {data}"
    res0 = data["results"][0]
    assert res0["type"] == "error", f"expected error got {res0} for sql={sql!r}"
    return data["baton"], res0["error"]


def mysql_conn(host: str, port: int, user: str, password: str, db: str | None = None):
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db,
        autocommit=True,
        charset="utf8mb4",
    )


def run_core_tests(user_http: HttpClient, admin_http: HttpClient, mhost: str, mport: int, muser: str, mpass: str):
    for stale in ("app1", "native_db", "bench_ovh"):
        admin_http.request("DELETE", f"/v1/namespaces/{stale}", {"Host": "localhost"})
    print("[1/10] Health & Metrics...")
    st, _, raw = user_http.request("GET", "/health", {"Host": "localhost"})
    assert st == 200, raw
    h = json.loads(raw)
    assert "version" in h and "commit" in h, h
    st, _, raw = admin_http.request("GET", "/metrics", {"Host": "localhost"})
    assert st == 200
    assert b"xsqld_build_info" in raw and b"xsqld_streams" in raw

    print("[2/10] Admin CRUD, Reserved DBs & Bad Names...")
    for bad in ["oceanbase", "OceanBase", "mysql", "MySQL", "information_schema", "performance_schema", "sys", "../x", ".bad", "a/b"]:
        st, _, raw = admin_http.request(
            "POST",
            f"/v1/namespaces/{urllib.parse.quote(bad, safe='')}/create",
            {"Host": "localhost", "Content-Type": "application/json"},
            b"{}",
        )
        assert st == 400, f"expected 400 for {bad}, got {st} {raw}"

    st, _, _ = admin_http.request("POST", "/v1/namespaces/app1/create", {"Host": "localhost"}, b"{}")
    assert st == 415, f"expected 415 without json content-type, got {st}"

    st, _, _ = admin_http.request(
        "POST",
        "/v1/namespaces/app1/create",
        {"Host": "localhost", "Content-Type": "application/json"},
        b'{"dump_url":"http://127.0.0.1:9999/d"}',
    )
    assert st == 400, f"expected 400 for dump_url, got {st}"

    st, _, raw = admin_http.request(
        "POST",
        "/v1/namespaces/app1/create",
        {"Host": "localhost", "Content-Type": "application/json"},
        b"{}",
    )
    assert st == 200, raw

    # Duplicate create -> 400
    st, _, raw = admin_http.request(
        "POST",
        "/v1/namespaces/app1/create",
        {"Host": "localhost", "Content-Type": "application/json"},
        b"{}",
    )
    assert st == 400, raw

    # Missing namespace -> 404
    st, data = exec_stmt(user_http, "missing_ns.db", "SELECT 1")
    assert st == 404, (st, data)
    st, _, _ = admin_http.request("DELETE", "/v1/namespaces/missing_ns", {"Host": "localhost"})
    assert st == 404

    print("[3/10] Host Allowlist & Browser CSRF Guard...")
    st, _ = exec_stmt(user_http, "app1.evil.example", "SELECT 1")
    assert st == 403
    st, _ = exec_stmt(user_http, "app1.db", "SELECT 1", extra_headers={"Origin": "https://evil.example"})
    assert st == 403
    st, _ = exec_stmt(user_http, "app1.db", "SELECT 1", extra_headers={"Sec-Fetch-Site": "cross-site"})
    assert st == 403

    print("[4/10] Data Types (Text & Binary Protocols)...")
    must_ok(
        user_http,
        "app1.db",
        """CREATE TABLE types_t (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ti TINYINT,
            bi BIGINT,
            bu BIGINT UNSIGNED,
            fl FLOAT,
            db DOUBLE,
            dc DECIMAL(18,4),
            vc VARCHAR(64),
            tx TEXT,
            vb VARBINARY(32),
            bl BLOB,
            dt DATE,
            dtt DATETIME(6),
            ts TIMESTAMP NULL,
            tm TIME,
            js JSON,
            nul INT
        )""",
    )
    raw_bytes = b"\x00\x01\x7f\x80\xff"
    b64_unpadded = base64.b64encode(raw_bytes).decode().rstrip("=")

    _, ins1 = must_ok(
        user_http,
        "app1.db",
        """INSERT INTO types_t (ti, bi, bu, fl, db, dc, vc, tx, vb, bl, dt, dtt, ts, tm, js, nul)
           VALUES (1, -9223372036854775800, 18446744073709551615, 1.5, 3.25, 12345.6789,
                   'hello', '你好🦀', UNHEX('00017F80FF'), UNHEX('00017F80FF'),
                   '2026-11-22', '2026-11-22 14:05:09.123456', '2026-11-22 14:05:09', '12:34:56',
                   '{"k":1}', NULL)""",
    )
    assert ins1["affected_row_count"] == 1 and ins1["last_insert_rowid"] == "1", ins1

    _, ins2 = must_ok(
        user_http,
        "app1.db",
        """INSERT INTO types_t (ti, bi, bu, fl, db, dc, vc, tx, vb, bl, dt, dtt, ts, tm, js, nul)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        args=[
            {"type": "integer", "value": "0"},
            {"type": "integer", "value": "9223372036854775807"},
            {"type": "text", "value": "18446744073709551614"},
            {"type": "float", "value": 2.5},
            {"type": "float", "value": 7.75},
            {"type": "text", "value": "99.0001"},
            {"type": "text", "value": "param"},
            {"type": "text", "value": "文本"},
            {"type": "blob", "base64": b64_unpadded},
            {"type": "blob", "base64": b64_unpadded},
            {"type": "text", "value": "2026-01-02"},
            {"type": "text", "value": "2026-01-02 03:04:05.000678"},
            {"type": "text", "value": "2026-01-02 03:04:05"},
            {"type": "text", "value": "01:02:03"},
            {"type": "text", "value": '{"p":true}'},
            {"type": "null"},
        ],
    )
    assert ins2["affected_row_count"] == 1 and ins2["last_insert_rowid"] == "2", ins2

    # Check both Text Protocol (no args) and Binary Protocol (with args) read identical semantics
    _, sel_text = must_ok(user_http, "app1.db", "SELECT * FROM types_t WHERE id = 1")
    _, sel_bin = must_ok(
        user_http,
        "app1.db",
        "SELECT * FROM types_t WHERE id = ?",
        args=[{"type": "integer", "value": "1"}],
    )
    for tag, res in [("text", sel_text), ("binary", sel_bin)]:
        row = res["rows"][0]
        assert row[0] == {"type": "integer", "value": "1"}, (tag, row[0])
        assert row[1] == {"type": "integer", "value": "1"}, (tag, row[1])
        assert row[2] == {"type": "integer", "value": "-9223372036854775800"}, (tag, row[2])
        assert row[3] == {"type": "text", "value": "18446744073709551615"}, (tag, row[3])
        assert row[4]["type"] == "float" and abs(row[4]["value"] - 1.5) < 1e-5, (tag, row[4])
        assert row[5] == {"type": "float", "value": 3.25}, (tag, row[5])
        assert row[6] == {"type": "text", "value": "12345.6789"}, (tag, row[6])
        assert row[7] == {"type": "text", "value": "hello"}, (tag, row[7])
        assert row[8] == {"type": "text", "value": "你好🦀"}, (tag, row[8])
        assert row[9] == {"type": "blob", "base64": b64_unpadded}, (tag, row[9])
        assert row[10] == {"type": "blob", "base64": b64_unpadded}, (tag, row[10])
        assert row[11] == {"type": "text", "value": "2026-11-22"}, (tag, row[11])
        assert row[12]["type"] == "text" and row[12]["value"].startswith("2026-11-22 14:05:09.123456"), (tag, row[12])
        assert row[13]["type"] == "text" and "2026-11-22 14:05:09" in row[13]["value"], (tag, row[13])
        assert row[14] == {"type": "text", "value": "12:34:56"}, (tag, row[14])
        assert row[15]["type"] == "text" and json.loads(row[15]["value"]) == {"k": 1}, (tag, row[15])
        assert row[16] == {"type": "null"}, (tag, row[16])

    _, upd = must_ok(user_http, "app1.db", "UPDATE types_t SET vc = 'updated' WHERE id = 2")
    assert upd["affected_row_count"] == 1 and upd["last_insert_rowid"] is None, upd
    _, dlt = must_ok(user_http, "app1.db", "DELETE FROM types_t WHERE id = 2")
    assert dlt["affected_row_count"] == 1 and dlt["last_insert_rowid"] is None, dlt

    print("[5/10] Parameters, @user_var & SQL Errors...")
    _, r = must_ok(
        user_http,
        "app1.db",
        "SELECT ?2, ?1, 'keep?literal:1'",
        args=[{"type": "text", "value": "first"}, {"type": "integer", "value": "99"}],
    )
    assert r["rows"][0] == [
        {"type": "integer", "value": "99"},
        {"type": "text", "value": "first"},
        {"type": "text", "value": "keep?literal:1"},
    ], r

    _, r = must_ok(
        user_http,
        "app1.db",
        "SELECT :a, $b, @c, :a # hash :comment ?",
        named_args=[
            {"name": "a", "value": {"type": "integer", "value": "1"}},
            {"name": ":a", "value": {"type": "integer", "value": "7"}},
            {"name": "b", "value": {"type": "text", "value": "beta"}},
            {"name": "@c", "value": {"type": "float", "value": 4.5}},
        ],
    )
    assert r["rows"][0] == [
        {"type": "integer", "value": "7"},
        {"type": "text", "value": "beta"},
        {"type": "float", "value": 4.5},
        {"type": "integer", "value": "7"},
    ], r

    # Same pipeline keeps MySQL session connection so SET @u := 40; SELECT @u + 2 works!
    st, _, resp = user_http.request(
        "POST",
        "/v2/pipeline",
        {"Host": "app1.db", "Content-Type": "application/json"},
        json.dumps(
            {
                "requests": [
                    {"type": "execute", "stmt": {"sql": "SET @myu := 40"}},
                    {"type": "execute", "stmt": {"sql": "SELECT @myu + 2, @@version"}},
                    {"type": "close"},
                ]
            }
        ).encode(),
    )
    assert st == 200
    d = json.loads(resp)
    assert d["results"][1]["response"]["result"]["rows"][0][0] == {"type": "integer", "value": "42"}, d
    assert d["results"][1]["response"]["result"]["rows"][0][1]["type"] == "text", d
    assert d["baton"] is None

    assert must_err(user_http, "app1.db", "SELECT 1; SELECT 2")[1]["code"] == "SQL_MANY_STATEMENTS"
    assert must_err(user_http, "app1.db", "   -- only comment\n # also comment")[1]["code"] == "SQL_NO_STATEMENT"
    assert (
        must_err(
            user_http,
            "app1.db",
            "SELECT ?",
            args=[{"type": "integer", "value": "1"}],
            named_args=[{"name": "a", "value": {"type": "integer", "value": "1"}}],
        )[1]["code"]
        == "ARGS_BOTH_POSITIONAL_AND_NAMED"
    )
    assert must_err(user_http, "app1.db", "SELECT 1", args=[{"type": "integer", "value": "1"}])[1]["code"] == "ARGS_INVALID"

    print("[6/10] Batch Conditions (ok/error/not/and/or)...")
    must_ok(user_http, "app1.db", "CREATE TABLE b_t (id INT PRIMARY KEY, v VARCHAR(32))")
    batch_req = {
        "requests": [
            {
                "type": "batch",
                "batch": {
                    "steps": [
                        {"stmt": {"sql": "INSERT INTO b_t VALUES (1, 'v1')"}},
                        {
                            "condition": {"type": "ok", "step": 0},
                            "stmt": {"sql": "INSERT INTO b_t VALUES (1, 'dup')"},
                        },
                        {
                            "condition": {"type": "error", "step": 1},
                            "stmt": {"sql": "UPDATE b_t SET v = 'recovered' WHERE id = 1"},
                        },
                        {
                            "condition": {"type": "and", "conds": [{"type": "ok", "step": 0}, {"type": "ok", "step": 1}]},
                            "stmt": {"sql": "DELETE FROM b_t WHERE id = 1"},
                        },
                        {
                            "condition": {
                                "type": "or",
                                "conds": [{"type": "ok", "step": 1}, {"type": "not", "cond": {"type": "error", "step": 2}}],
                            },
                            "stmt": {"sql": "SELECT v FROM b_t WHERE id = 1"},
                        },
                    ]
                },
            },
            {"type": "close"},
        ]
    }
    st, _, raw = user_http.request(
        "POST",
        "/v2/pipeline",
        {"Host": "app1.db", "Content-Type": "application/json"},
        json.dumps(batch_req).encode(),
    )
    assert st == 200
    bres = json.loads(raw)["results"][0]["response"]["result"]
    assert bres["step_results"][0] is not None and bres["step_errors"][0] is None
    assert bres["step_results"][1] is None and bres["step_errors"][1] is not None
    assert bres["step_results"][2]["affected_row_count"] == 1
    assert bres["step_results"][3] is None and bres["step_errors"][3] is None
    assert bres["step_results"][4]["rows"] == [[{"type": "text", "value": "recovered"}]]

    print("[7/10] Stateful Baton Transactions, Error Persistence, DDL Unpin, Timeout Rollback...")
    must_ok(user_http, "app1.db", "CREATE TABLE tx_t (id INT PRIMARY KEY, v VARCHAR(32))")
    mc = mysql_conn(mhost, mport, muser, mpass, "app1")
    cur = mc.cursor()

    b1, _ = must_ok(user_http, "app1.db", "BEGIN")
    assert b1 is not None
    b2, _ = must_ok(user_http, "app1.db", "INSERT INTO tx_t VALUES (10, 'pending')", baton=b1)
    assert b2 is not None and b2 != b1
    # Reusing b1 -> 400
    st, _ = exec_stmt(user_http, "app1.db", "SELECT 1", baton=b1)
    assert st == 400, f"expected 400 on reused baton, got {st}"

    # External MySQL connection should NOT see row 10 yet
    cur.execute("SELECT COUNT(*) FROM tx_t WHERE id = 10")
    assert cur.fetchone()[0] == 0

    # Same baton sees uncommitted row
    b3, sel = must_ok(user_http, "app1.db", "SELECT v FROM tx_t WHERE id = 10", baton=b2)
    assert sel["rows"] == [[{"type": "text", "value": "pending"}]], sel

    # Non-fatal statement error inside open transaction must KEEP tx pinned!
    b4, _ = must_err(user_http, "app1.db", "INSERT INTO tx_t VALUES (10, 'dup')", baton=b3)
    assert b4 is not None
    b5, _ = must_ok(user_http, "app1.db", "INSERT INTO tx_t VALUES (11, 'ok_after_err')", baton=b4)
    b6, _ = must_ok(user_http, "app1.db", "COMMIT", baton=b5)
    assert b6 is not None

    cur.execute("SELECT COUNT(*) FROM tx_t WHERE id IN (10, 11)")
    assert cur.fetchone()[0] == 2

    # Rollback test
    b1, _ = must_ok(user_http, "app1.db", "BEGIN", baton=b6)
    b2, _ = must_ok(user_http, "app1.db", "INSERT INTO tx_t VALUES (12, 'rb')", baton=b1)
    b3, _ = must_ok(user_http, "app1.db", "ROLLBACK", baton=b2)
    cur.execute("SELECT COUNT(*) FROM tx_t WHERE id = 12")
    assert cur.fetchone()[0] == 0

    # DDL implicit commit inside transaction -> commits row 13 and clears IN_TRANS cleanly
    b1, _ = must_ok(user_http, "app1.db", "BEGIN", baton=b3)
    b2, _ = must_ok(user_http, "app1.db", "INSERT INTO tx_t VALUES (13, 'ddl_commit')", baton=b1)
    b3, _ = must_ok(user_http, "app1.db", "CREATE TABLE tx_ddl_tmp (id INT PRIMARY KEY)", baton=b2)
    cur.execute("SELECT COUNT(*) FROM tx_t WHERE id = 13")
    assert cur.fetchone()[0] == 1

    # Close stream rolls back open tx
    b4, _ = must_ok(user_http, "app1.db", "BEGIN", baton=b3)
    b5, _ = must_ok(user_http, "app1.db", "INSERT INTO tx_t VALUES (14, 'close_rb')", baton=b4)
    st, _, cresp = user_http.request(
        "POST",
        "/v2/pipeline",
        {"Host": "app1.db", "Content-Type": "application/json"},
        json.dumps({"baton": b5, "requests": [{"type": "close"}]}).encode(),
    )
    assert st == 200
    assert json.loads(cresp)["baton"] is None
    cur.execute("SELECT COUNT(*) FROM tx_t WHERE id = 14")
    assert cur.fetchone()[0] == 0

    # Pinned transaction timeout (server configured with --pinned-tx-timeout-ms 1500)
    bt1, _ = must_ok(user_http, "app1.db", "BEGIN")
    bt2, _ = must_ok(user_http, "app1.db", "INSERT INTO tx_t VALUES (999, 'timeout_rb')", baton=bt1)
    time.sleep(2.3)
    st, msg = exec_stmt(user_http, "app1.db", "COMMIT", baton=bt2)
    assert st == 400, f"expected 400 on expired pinned tx, got {st} {msg}"
    cur.execute("SELECT COUNT(*) FROM tx_t WHERE id = 999")
    assert cur.fetchone()[0] == 0

    print("[8/10] Request Timeout (KILL QUERY) & RESPONSE_TOO_LARGE...")
    t0 = time.perf_counter()
    st, tout = exec_stmt(user_http, "app1.db", "SELECT SLEEP(5)")
    dt = time.perf_counter() - t0
    assert st == 504 and dt < 3.0, (st, dt, tout)
    # Pool must remain healthy after deadline abort
    _, ok_after = must_ok(user_http, "app1.db", "SELECT 123")
    assert ok_after["rows"] == [[{"type": "integer", "value": "123"}]]

    # Exceed --max-response-bytes 8192
    must_ok(user_http, "app1.db", "CREATE TABLE big_t (id INT PRIMARY KEY, s VARCHAR(500))")
    for i in range(40):
        must_ok(
            user_http,
            "app1.db",
            "INSERT INTO big_t VALUES (?, ?)",
            args=[{"type": "integer", "value": str(i)}, {"type": "text", "value": "x" * 350}],
        )
    _, err_big = must_err(user_http, "app1.db", "SELECT * FROM big_t")
    assert err_big["code"] == "RESPONSE_TOO_LARGE", err_big
    _, ok_after2 = must_ok(user_http, "app1.db", "SELECT COUNT(*) FROM big_t")
    assert ok_after2["rows"] == [[{"type": "integer", "value": "40"}]]

    print("[9/10] Dual-Protocol Coexistence (8080 <-> 2881) & Lazy DB Discovery...")
    # Write via PyMySQL -> create brand new DB directly on SeekDB 2881 -> query immediately via Hrana 8080
    cur.execute("CREATE DATABASE native_db")
    cur.execute("CREATE TABLE native_db.items (id INT PRIMARY KEY, label VARCHAR(64))")
    cur.execute("INSERT INTO native_db.items VALUES (1, 'from_mysql'), (2, 'dual_mode')")
    _, native_res = must_ok(user_http, "native_db.db", "SELECT label FROM items ORDER BY id")
    assert native_res["rows"] == [
        [{"type": "text", "value": "from_mysql"}],
        [{"type": "text", "value": "dual_mode"}],
    ], native_res

    # Write via Hrana 8080 -> read immediately via PyMySQL 2881
    must_ok(user_http, "native_db.db", "INSERT INTO items VALUES (3, 'from_hrana')")
    cur.execute("SELECT label FROM native_db.items WHERE id = 3")
    assert cur.fetchone()[0] == "from_hrana"

    # Drop both namespaces via Admin API
    for ns in ("app1", "native_db"):
        st, _, raw = admin_http.request("DELETE", f"/v1/namespaces/{ns}", {"Host": "localhost"})
        assert st == 200, (ns, st, raw)
        st, _ = exec_stmt(user_http, f"{ns}.db", "SELECT 1")
        assert st == 404
    cur.close()
    mc.close()

    print("[10/10] Proxy Overhead Sanity Benchmark (Keep-Alive Hrana 8080 vs Direct PyMySQL 2881)...")
    st, _, raw = admin_http.request(
        "POST",
        "/v1/namespaces/bench_ovh/create",
        {"Host": "localhost", "Content-Type": "application/json"},
        b"{}",
    )
    assert st == 200, raw
    must_ok(user_http, "bench_ovh.db", "CREATE TABLE kv (id INT PRIMARY KEY, v VARCHAR(64))")
    for i in range(1, 101):
        must_ok(
            user_http,
            "bench_ovh.db",
            "INSERT INTO kv VALUES (?, ?)",
            args=[{"type": "integer", "value": str(i)}, {"type": "text", "value": f"val_{i}"}],
        )

    ka = KeepAliveHttp(
        f"http://{user_http.host}:{user_http.port}",
        {"Host": "bench_ovh.db", "Content-Type": "application/json"},
    )
    mc = mysql_conn(mhost, mport, muser, mpass, "bench_ovh")
    cur = mc.cursor()

    # Warmup
    for i in range(1, 20):
        ka.post_json(
            "/v2/pipeline",
            {"requests": [{"type": "execute", "stmt": {"sql": "SELECT v FROM kv WHERE id = ?", "args": [{"type": "integer", "value": str(i)}]}}]},
        )
        cur.execute("SELECT v FROM kv WHERE id = %s", (i,))
        cur.fetchone()

    def bench_fn(n: int, fn):
        lats = []
        t_start = time.perf_counter()
        for k in range(n):
            t0 = time.perf_counter()
            fn(k)
            lats.append((time.perf_counter() - t0) * 1000.0)
        total = time.perf_counter() - t_start
        lats.sort()
        return {
            "qps": n / total,
            "avg_ms": statistics.mean(lats),
            "p50_ms": lats[int(0.50 * n)],
            "p95_ms": lats[int(0.95 * n)],
        }

    hrana_sel = bench_fn(
        300,
        lambda k: ka.post_json(
            "/v2/pipeline",
            {
                "requests": [
                    {
                        "type": "execute",
                        "stmt": {
                            "sql": "SELECT v FROM kv WHERE id = ?",
                            "args": [{"type": "integer", "value": str((k % 100) + 1)}],
                        },
                    }
                ]
            },
        ),
    )
    mysql_sel = bench_fn(
        300,
        lambda k: (cur.execute("SELECT v FROM kv WHERE id = %s", ((k % 100) + 1,)), cur.fetchone()),
    )

    hrana_upd = bench_fn(
        150,
        lambda k: ka.post_json(
            "/v2/pipeline",
            {
                "requests": [
                    {
                        "type": "execute",
                        "stmt": {
                            "sql": "UPDATE kv SET v = ? WHERE id = ?",
                            "args": [
                                {"type": "text", "value": f"u_{k}"},
                                {"type": "integer", "value": str((k % 100) + 1)},
                            ],
                        },
                    }
                ]
            },
        ),
    )
    mysql_upd = bench_fn(
        150,
        lambda k: cur.execute("UPDATE kv SET v = %s WHERE id = %s", (f"u_{k}", (k % 100) + 1)),
    )

    ka.close()
    cur.close()
    mc.close()
    admin_http.request("DELETE", "/v1/namespaces/bench_ovh", {"Host": "localhost"})

    print(
        json.dumps(
            {
                "select_hrana_8080": hrana_sel,
                "select_direct_2881": mysql_sel,
                "update_hrana_8080": hrana_upd,
                "update_direct_2881": mysql_upd,
            },
            indent=2,
        )
    )
    print("CORE INTEGRATION TESTS PASSED!")


def run_auth_tests(user_http: HttpClient, admin_http: HttpClient, token: str):
    admin_http.request(
        "DELETE",
        "/v1/namespaces/authdb",
        {"Host": "localhost", "Authorization": f"Bearer {token}"},
    )
    print("[AUTH] Testing optional --auth-token guard...")
    st, _, _ = user_http.request("GET", "/health", {"Host": "localhost"})
    assert st == 200, st
    st, _, _ = user_http.request("GET", "/metrics", {"Host": "localhost"})
    assert st == 401, st
    st, _ = exec_stmt(user_http, "authdb.db", "SELECT 1")
    assert st == 401, st
    st, _ = exec_stmt(
        user_http,
        "authdb.db",
        "SELECT 1",
        extra_headers={"Authorization": "Bearer wrong-token"},
    )
    assert st == 401, st

    st, _, raw = admin_http.request(
        "POST",
        "/v1/namespaces/authdb/create",
        {"Host": "localhost", "Content-Type": "application/json", "Authorization": "Bearer wrong"},
        b"{}",
    )
    assert st == 401, (st, raw)

    auth_hdr = {"Authorization": f"Bearer {token}"}
    st, _, raw = admin_http.request(
        "POST",
        "/v1/namespaces/authdb/create",
        {"Host": "localhost", "Content-Type": "application/json", **auth_hdr},
        b"{}",
    )
    assert st == 200, (st, raw)

    # With Bearer auth configured, browser Origin header does not 403 valid token callers
    _, r = must_ok_hdr(
        user_http,
        "authdb.db",
        "SELECT 42",
        {**auth_hdr, "Origin": "https://app.example"},
    )
    assert r["rows"] == [[{"type": "integer", "value": "42"}]]

    st, _, raw = admin_http.request(
        "DELETE",
        "/v1/namespaces/authdb",
        {"Host": "localhost", **auth_hdr},
    )
    assert st == 200, (st, raw)
    print("AUTH INTEGRATION TESTS PASSED!")


def must_ok_hdr(http_cli: HttpClient, ns_host: str, sql: str, extra_headers: dict):
    st, data = exec_stmt(http_cli, ns_host, sql, extra_headers=extra_headers)
    assert st == 200, (st, data)
    res0 = data["results"][0]
    assert res0["type"] == "ok", res0
    return data["baton"], res0["response"]["result"]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["core", "auth"], required=True)
    p.add_argument("--http-url", default="http://127.0.0.1:18080")
    p.add_argument("--admin-url", default="http://127.0.0.1:18081")
    p.add_argument("--mysql-host", default="127.0.0.1")
    p.add_argument("--mysql-port", type=int, default=12881)
    p.add_argument("--mysql-user", default="root")
    p.add_argument("--mysql-password", default="")
    p.add_argument("--token", default="")
    args = p.parse_args()

    user_http = HttpClient(args.http_url)
    admin_http = HttpClient(args.admin_url)

    if args.mode == "core":
        run_core_tests(user_http, admin_http, args.mysql_host, args.mysql_port, args.mysql_user, args.mysql_password)
    else:
        run_auth_tests(user_http, admin_http, args.token)


if __name__ == "__main__":
    main()
