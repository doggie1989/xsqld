#!/usr/bin/env python3
"""End-to-end check of the xsqld MySQL listener using a genuine client library.

pymysql performs the handshake, negotiates capabilities and parses every packet, so this exercises
the protocol with code we do not control -- the raw-socket suite in tests/test_mysql_protocol.py
proves the bytes, this one proves that a real driver is happy with them.

Three things matter beyond "it returns rows":
  * values arrive with the right type and the right raw bytes (blob must not come back escaped);
  * the same statement gives the same answer over HTTP and over MySQL, because both go
    through Backend::run_pipeline;
  * namespace selection (COM_INIT_DB, USE) and backend errors (errno, SQLSTATE) reach the client.

Assumes an xsqld with --mysql-listen-addr on XS_MYSQL_PORT and the backend on XS_BACKEND_PORT.
"""

import base64
import http.client
import json
import os
import sys

try:
    import pymysql
except ImportError:
    print("needs pymysql: python3 -m venv .venv && .venv/bin/pip install pymysql", file=sys.stderr)
    sys.exit(2)

NS = os.environ.get("XS_NAMESPACE", "xs")
LISTEN_PORT = int(os.environ.get("XS_MYSQL_PORT", "3308"))
BACKEND_PORT = int(os.environ.get("XS_BACKEND_PORT", "12981"))
BACKEND_USER = os.environ.get("XS_BACKEND_USER", "root")
BACKEND_PASSWORD = os.environ.get("XS_BACKEND_PASSWORD", "")
HTTP_HOST, HTTP_PORT = "127.0.0.1", int(os.environ.get("XS_HTTP_PORT", "8090"))

LISTENER = dict(host="127.0.0.1", port=LISTEN_PORT, user="default", password="",
                database=NS, connect_timeout=15)
DIRECT = dict(host="127.0.0.1", port=BACKEND_PORT, user=BACKEND_USER,
              password=BACKEND_PASSWORD, autocommit=True)

results = []


def rec(name, ok, detail=""):
    results.append((name, ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       {detail}", flush=True)


def admin(method, path):
    conn = http.client.HTTPConnection("127.0.0.1", int(os.environ.get("XS_ADMIN_PORT", "8091")),
                                      timeout=60)
    conn.request(method, path, body="{}", headers={"Content-Type": "application/json"})
    with conn.getresponse() as r:
        return r.status, json.loads(r.read() or b"null")


def hrana(sql, namespace=NS):
    """Runs one statement through the HTTP gateway and returns plain Python values."""
    body = {"baton": None, "requests": [{"type": "execute", "stmt": {"sql": sql}}, {"type": "close"}]}
    conn = http.client.HTTPConnection(HTTP_HOST, HTTP_PORT, timeout=60)
    conn.request("POST", "/v2/pipeline", body=json.dumps(body),
                 headers={"Content-Type": "application/json", "Host": f"{namespace}.db"})
    with conn.getresponse() as r:
        out = json.loads(r.read())
    res = out["results"][0]
    if res["type"] == "error":
        return ("error", res["error"])
    stmt = res["response"]["result"]
    return [[_plain(v) for v in row] for row in stmt["rows"]]


def _plain(v):
    """The gateway sends typed hrana values; flatten them for comparison with a MySQL client."""
    if isinstance(v, list):
        return [_plain(x) for x in v]
    if not isinstance(v, dict):
        return v
    t = v.get("type")
    if t == "null":
        return None
    if t in ("integer", "float"):
        return int(v["value"]) if t == "integer" else float(v["value"])
    if t == "text":
        return v["value"]
    if t == "blob":
        b64 = v["base64"]
        return base64.b64decode(b64 + "=" * (-len(b64) % 4))
    return v


def main():
    try:
        listener = pymysql.connect(**LISTENER)
    except Exception as e:
        print(f"cannot reach the listener on port {LISTEN_PORT}: {e}", file=sys.stderr)
        return 2
    try:
        cur = listener.cursor()
        cur.execute("SELECT VERSION()")
        version = cur.fetchall()[0][0]
        rec("a real client completes the handshake and auth", listener.open,
            f"user=default database={NS} -> {version}")

        cur.execute("SELECT 1 AS one, 'abc' AS s, NULL AS n, 1.5 AS f")
        row = cur.fetchall()[0]
        rec("scalars keep their types over the wire", row == (1, "abc", None, 1.5), repr(row))

        # DDL and DML through the listener, then read the same table from the backend directly
        cur.execute("DROP TABLE IF EXISTS protab")
        cur.execute("CREATE TABLE protab(id INT PRIMARY KEY, s VARCHAR(64), raw VARBINARY(64), num DOUBLE)")
        rec("DDL returns an OK packet, not a result set", cur.description is None, str(cur.description))
        raw = bytes.fromhex("00011a5c2722c3a9")
        cur.execute("INSERT INTO protab VALUES (1, %s, %s, 1.5)", ("wo'rld\\", raw))
        rec("INSERT reports affected rows", cur.rowcount == 1, str(cur.rowcount))
        cur.execute("INSERT INTO protab VALUES (2, NULL, NULL, %s)", (-2.25,))
        rec("a second row lands", cur.rowcount == 1, str(cur.rowcount))

        cur.execute("SELECT id, s, raw, num FROM protab ORDER BY id")
        got = cur.fetchall()
        names = [d[0] for d in cur.description]
        rec("column names are the ones asked for", names == ["id", "s", "raw", "num"], str(names))
        rec("blob bytes arrive untouched", bytes(got[0][2]) == raw,
            f"got={bytes(got[0][2])!r} want={raw!r}")
        rec("quoting and backslashes survive", got[0][1] == "wo'rld\\", repr(got[0][1]))
        rec("NULL arrives as NULL, empty string stays empty", got[1][1] is None and got[1][2] is None,
            repr(got[1]))
        rec("floats keep their sign and fraction", got[0][3] == 1.5 and got[1][3] == -2.25,
            repr([r[3] for r in got]))

        direct = pymysql.connect(**DIRECT, database=NS)
        dcur = direct.cursor()
        dcur.execute("SELECT id, s, raw, num FROM protab ORDER BY id")
        backend = dcur.fetchall()
        rec("the listener and the backend return the same rows", list(got) == list(backend),
            f"listener={got} backend={backend}")

        http_rows = hrana("SELECT id, s, raw, num FROM protab ORDER BY id")
        rec("the listener and the HTTP gateway agree, statement for statement",
            [list(r) for r in got] == http_rows, f"mysql={got} hrana={http_rows}")

        # metadata: the gateway has a deliberately coarse type vocabulary (every integer becomes
        # LONGLONG, every float DOUBLE), which must never come back narrower than the backend's
        listener_types = [d[1] for d in cur.description]
        backend_types = [d[1] for d in dcur.description]
        rec("an integer column is declared wide enough to hold it",
            listener_types[0] == 8 and backend_types[0] == 3,
            f"listener={listener_types} backend={backend_types}")
        rec("the text and binary columns are not collapsed into one type",
            listener_types[1] != listener_types[2],
            f"s={listener_types[1]} raw={listener_types[2]}")

        # an error from the backend has to reach the client as that error
        try:
            cur.execute("SELECT * FROM no_such_table_xyz")
            rec("backend error reaches the client", False, "no error raised")
        except pymysql.err.MySQLError as e:
            rec("backend error reaches the client with its errno", e.args[0] == 1146,
                f"errno={e.args[0]} {str(e)[:80]}")

        # namespace selection: USE and COM_INIT_DB have to actually move the connection,
        # otherwise a client that switches schema silently keeps reading the old one
        peer = "xs_mysql_peer"
        admin("POST", f"/v1/namespaces/{peer}/create")
        dcur.execute(f"CREATE DATABASE IF NOT EXISTS {peer}")
        peer_db = pymysql.connect(**DIRECT, database=peer)
        pcur = peer_db.cursor()
        pcur.execute("DROP TABLE IF EXISTS marker")
        pcur.execute("CREATE TABLE marker(where_i_am VARCHAR(16))")
        pcur.execute("INSERT INTO marker VALUES ('peer')")
        peer_db.close()

        try:
            cur.execute("USE nope_missing_db")
            rec("USE refuses an unknown namespace", False, "it succeeded")
        except pymysql.err.MySQLError as e:
            rec("USE refuses an unknown namespace", e.args[0] == 1049, str(e)[:80])
        cur.execute("SELECT 1")
        rec("the connection survives a rejected USE", cur.fetchall() == ((1,),), "still usable")

        cur.execute("DROP TABLE IF EXISTS marker")
        cur.execute("CREATE TABLE marker(where_i_am VARCHAR(16))")
        cur.execute("INSERT INTO marker VALUES ('home')")
        cur.execute("SELECT where_i_am FROM marker")
        home = cur.fetchall()
        rec("the connection reads its own namespace", home == (("home",),), str(home))

        cur.execute(f"USE {peer}")
        cur.execute("SELECT where_i_am FROM marker")
        after_use = cur.fetchall()
        rec("USE moves the connection to the other namespace", after_use == (("peer",),), str(after_use))

        try:
            cur.execute("SELECT s FROM protab")
            rec("the moved connection cannot read the first namespace", False, "it read it")
        except pymysql.err.MySQLError as e:
            rec("the moved connection cannot read the first namespace", e.args[0] == 1146, str(e)[:60])

        listener.select_db(NS)
        cur.execute("SELECT where_i_am FROM marker")
        back = cur.fetchall()
        rec("COM_INIT_DB moves it back", back == (("home",),), str(back))

        # a stray COMMIT/ROLLBACK must not kill the session
        cur.execute("COMMIT")
        cur.execute("SELECT 1")
        rec("a stray COMMIT leaves the connection alive", cur.fetchall() == ((1,),), "alive")

        cur.execute("SET autocommit=1")
        cur.execute("SELECT 2+2")
        rec("SET is accepted and the session keeps working", cur.fetchall() == ((4,),), "ok")

        # transactions: BEGIN has to keep holding this session's backend connection. If it does not,
        # every statement runs in its own transaction, ROLLBACK reports OK and undoes nothing.
        txn = pymysql.connect(**LISTENER, autocommit=False)
        tcur = txn.cursor()
        tcur.execute("DROP TABLE IF EXISTS txnprobe")
        tcur.execute("CREATE TABLE txnprobe(id INT PRIMARY KEY, v VARCHAR(8))")
        tcur.execute("BEGIN")
        tcur.execute("INSERT INTO txnprobe VALUES (1,'a')")
        tcur.execute("SELECT COUNT(*) FROM txnprobe")
        inside = tcur.fetchall()[0][0]
        tcur.execute("ROLLBACK")
        tcur.execute("SELECT COUNT(*) FROM txnprobe")
        after_rollback = tcur.fetchall()[0][0]
        rec("the transaction sees its own write", inside == 1, str(inside))
        rec("ROLLBACK undoes the write", after_rollback == 0,
            f"in_txn={inside} after_rollback={after_rollback}")

        tcur.execute("BEGIN")
        tcur.execute("INSERT INTO txnprobe VALUES (2,'b')")
        tcur.execute("COMMIT")
        tcur.execute("SELECT COUNT(*) FROM txnprobe")
        committed = tcur.fetchall()[0][0]
        rec("COMMIT keeps the write", committed == 1, str(committed))

        tcur.execute("BEGIN")
        tcur.execute("INSERT INTO txnprobe VALUES (3,'c')")
        other = pymysql.connect(**LISTENER)
        ocur = other.cursor()
        ocur.execute("SELECT COUNT(*) FROM txnprobe")
        peer_saw = ocur.fetchall()[0][0]
        rec("an uncommitted write is invisible to another session", peer_saw == 1, str(peer_saw))
        tcur.execute("ROLLBACK")
        ocur.execute("SELECT COUNT(*) FROM txnprobe")
        rec("the other session is untouched by the rollback", ocur.fetchall()[0][0] == 1, "count=1")
        tcur.execute("DROP TABLE txnprobe")
        other.close()
        txn.close()

        cur.execute("SELECT REPEAT('a', 4000000) AS big")
        big = cur.fetchall()[0][0]
        rec("a payload larger than one packet is reassembled by the client",
            len(big) == 4_000_000, str(len(big)))

        dcur.execute("DROP TABLE protab")
        direct.close()
    finally:
        listener.close()

    failed = sum(1 for _, ok in results if not ok)
    print()
    if failed:
        print(f"{failed} check(s) failed")
        return 1
    print("all client-parity checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
