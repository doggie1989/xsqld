#!/usr/bin/env python3
"""Proves a SELECT that arrives over the MySQL protocol is served by the DuckDB mirror.

The listener shares the gateway's pipeline, so routing is not a separate code path -- but it is a
separate entry point, and an assertion made with an HTTP client does not cover it. The evidence is
the counter rather than the latency: `xsqld_routed_total{ns=...}` has to move when the statement
comes in through the MySQL port, and the answer has to match the backend's.

`--route-min-rows` defaults to 200000, so the test starts its own xsqld with the threshold lowered
instead of pretending a 500-row table should be routed.

Backend credentials come from the environment; the repo's usual local setup is:
    XS_BACKEND_PORT=12981 python3 tests/test_mysql_route.py
"""

import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

try:
    import pymysql
except ImportError:
    print("needs pymysql: python3 -m venv .venv && .venv/bin/pip install pymysql", file=sys.stderr)
    sys.exit(2)

NS = os.environ.get("XS_NAMESPACE", "mysql_route_e2e")
BACKEND_HOST = os.environ.get("XS_BACKEND_HOST", "127.0.0.1")
BACKEND_PORT = int(os.environ.get("XS_BACKEND_PORT", "12981"))
BACKEND_USER = os.environ.get("XS_BACKEND_USER", "root")
BACKEND_PASSWORD = os.environ.get("XS_BACKEND_PASSWORD", "")

HTTP_PORT = int(os.environ.get("XS_HTTP_PORT", "18390"))
ADMIN_PORT = int(os.environ.get("XS_ADMIN_PORT", "18391"))
LISTEN_PORT = int(os.environ.get("XS_MYSQL_PORT", "18392"))
GATEWAY = f"http://127.0.0.1:{HTTP_PORT}"
ADMIN = f"http://127.0.0.1:{ADMIN_PORT}"

# -D selects the namespace, exactly as a client would do it
# autocommit on purpose: a driver defaults it to off, and then its writes legitimately stay
# invisible to the backend session until COMMIT -- which also keeps them out of the mirror.
LISTENER = dict(host=BACKEND_HOST, port=LISTEN_PORT, user=BACKEND_USER, password="",
                database=NS, connect_timeout=15, autocommit=True)
DIRECT = dict(host=BACKEND_HOST, port=BACKEND_PORT, user=BACKEND_USER,
              password=BACKEND_PASSWORD, autocommit=True)

MIRROR_DIR = "/tmp/xsqld-mysql-route"
LOG = "/tmp/xsqld-mysql-route.log"
BIN = os.path.abspath(os.environ.get("XS_BIN", "./target/debug/xsqld"))

results = []


def rec(name, ok, evidence=""):
    results.append((name, ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       {evidence}", flush=True)


def http(method, url, body=None, timeout=30):
    """Returns (status, decoded json); an admin 4xx is a result to check, not a crash."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, (json.loads(raw) if raw else None)


def metric(name):
    with urllib.request.urlopen(f"{GATEWAY}/metrics", timeout=15) as r:
        text = r.read().decode()
    m = re.search(r'xsqld_%s\{ns="%s"\}\s+(\d+)' % (name, re.escape(NS)), text)
    return int(m.group(1)) if m else 0


def start_xsqld():
    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], check=False)
    time.sleep(1)
    shutil.rmtree(MIRROR_DIR, ignore_errors=True)
    env = {k: v for k, v in os.environ.items()
           if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    env["RUST_LOG"] = "warn"
    proc = subprocess.Popen(
        [BIN, "--mysql-host", BACKEND_HOST, "--mysql-port", str(BACKEND_PORT),
         "--mysql-user", BACKEND_USER, "--mysql-password", BACKEND_PASSWORD,
         "--http-listen-addr", f"127.0.0.1:{HTTP_PORT}",
         "--admin-listen-addr", f"127.0.0.1:{ADMIN_PORT}",
         "--mysql-listen-addr", f"127.0.0.1:{LISTEN_PORT}",
         "--mirror-enabled", "--mirror-dir", MIRROR_DIR, "--mirror-interval-ms", "400",
         "--route-min-rows", "1"],
        stdout=open(LOG, "w"), stderr=subprocess.STDOUT, env=env)
    for _ in range(120):
        try:
            with urllib.request.urlopen(f"{GATEWAY}/health", timeout=2) as r:
                if r.status == 200:
                    return proc
        except Exception:
            pass
        time.sleep(0.5)
    raise SystemExit(f"xsqld did not come up; see {LOG}")


def main():
    if not os.path.exists(BIN):
        print(f"{BIN} not found -- run `cargo build` first", file=sys.stderr)
        return 2
    proc = start_xsqld()
    try:
        status, _ = http("POST", f"{ADMIN}/v1/namespaces/{NS}/create", {})
        # 400/409 only means the namespace was already there
        rec(f"namespace {NS} is available", status in (200, 201, 400, 409), str(status))

        listener = pymysql.connect(**LISTENER)
        cur = listener.cursor()
        rec("the listener serves that namespace", listener.open, NS)

        cur.execute("DROP TABLE IF EXISTS rt")
        cur.execute("CREATE TABLE rt(id INT PRIMARY KEY, uid INT, agg VARCHAR(64))")
        rows = ",".join(f"({i},{i % 50},'v{i}')" for i in range(1, 501))
        cur.execute(f"INSERT INTO rt VALUES {rows}")
        rec("500 rows written over the MySQL protocol", cur.rowcount == 500, str(cur.rowcount))

        # the mirror only answers once it has caught up, so wait for the outbox to empty
        pending = None
        for _ in range(120):
            try:
                check = pymysql.connect(**DIRECT, database=NS)
                ccur = check.cursor()
                ccur.execute("SELECT COUNT(*) FROM xsqld_changelog")
                pending = ccur.fetchone()[0]
                check.close()
                if pending == 0:
                    break
            except Exception:
                pending = None
            time.sleep(0.5)
        rec("the mirror caught up with the writes", pending == 0, f"pending={pending}")

        # and for the worker to publish a MirrorView: the counter moving is the readiness signal
        routed = 0
        for _ in range(60):
            before = metric("routed_total")
            cur.execute("SELECT COUNT(*) FROM rt")
            cur.fetchall()
            routed = metric("routed_total")
            if routed > before:
                break
            time.sleep(0.5)

        base = metric("routed_total")
        cur.execute("SELECT COUNT(*) FROM rt")
        count = cur.fetchall()[0][0]
        rec("a SELECT through the MySQL listener is routed to the mirror",
            metric("routed_total") > base, f"routed_total {base} -> {metric('routed_total')}")
        rec("the routed answer is correct", count == 500, str(count))
        rec("the routed answer is typed, not text", isinstance(count, int), type(count).__name__)

        direct = pymysql.connect(**DIRECT, database=NS)
        dcur = direct.cursor()
        dcur.execute("SELECT COUNT(*) FROM rt")
        backend_count = dcur.fetchall()[0][0]
        rec("the mirror agrees with the backend", int(backend_count) == int(count),
            f"routed={count} backend={backend_count}")

        sql = "SELECT uid, COUNT(*) FROM rt WHERE uid < 20 GROUP BY uid ORDER BY uid"
        cur.execute(sql)
        grouped = [tuple(r) for r in cur.fetchall()]
        rec("GROUP BY through the listener is ordered and complete",
            len(grouped) == 20 and grouped[0] == (0, 10), f"{grouped[:3]}")
        dcur.execute(sql)
        backend_grouped = [tuple(r) for r in dcur.fetchall()]
        rec("the grouped answer matches the backend", backend_grouped == grouped,
            f"listener={grouped[:3]} backend={backend_grouped[:3]}")

        # a write must not be served by the mirror, and reading afterwards must see it
        cur.execute("UPDATE rt SET agg='x' WHERE id=1")
        cur.execute("SELECT COUNT(*) FROM rt WHERE agg='x'")
        seen = cur.fetchall()[0][0]
        rec("a read right after a write sees the write", seen == 1, str(seen))

        # inside a transaction nothing may be routed: the mirror is not part of the snapshot the
        # transaction is reading, and this also proves BEGIN holds its own backend connection
        base = metric("routed_total")
        cur.execute("BEGIN")
        cur.execute("SELECT COUNT(*) FROM rt")
        txn_count = cur.fetchall()[0][0]
        rec("a SELECT inside a transaction is not routed", metric("routed_total") == base,
            f"routed_total stays {base}")
        rec("the transaction reads the backend", txn_count == 500, str(txn_count))
        cur.execute("ROLLBACK")

        cur.execute("DROP TABLE rt")
        listener.close()
        direct.close()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()

    failed = [n for n, ok in results if not ok]
    print()
    if failed:
        print(f"{len(failed)} check(s) failed: {', '.join(failed)}")
        print(f"server log: {LOG}")
        return 1
    print("all mirror-routing checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
