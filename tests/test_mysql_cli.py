#!/usr/bin/env python3
"""Runs the official `mysql` client against the listener.

Hand-written and driver-based tests both hid a bug: libmysqlclient stops reading column metadata at
`column_count` packets, so a terminator placed between the definitions and the rows is parsed as a
row and the query never finishes. Only the real client exercises that.

Every check here is an exit status and the exact text the client prints -- if `mysql` is happy, the
framing is right for everything built on libmysqlclient (mysqlbinlog, mysqldump, MySQL Shell's
SQL mode, most GUI tools).

    XS_BACKEND_PORT=12981 python3 tests/test_mysql_cli.py
"""

import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

NS = os.environ.get("XS_NAMESPACE", "xs_mysql_cli")
PEER = os.environ.get("XS_PEER_NAMESPACE", "xs_mysql_cli_peer")
BACKEND_HOST = os.environ.get("XS_BACKEND_HOST", "127.0.0.1")
BACKEND_PORT = int(os.environ.get("XS_BACKEND_PORT", "12981"))
BACKEND_USER = os.environ.get("XS_BACKEND_USER", "root")
BACKEND_PASSWORD = os.environ.get("XS_BACKEND_PASSWORD", "")
HTTP_PORT = int(os.environ.get("XS_HTTP_PORT", "18430"))
ADMIN_PORT = int(os.environ.get("XS_ADMIN_PORT", "18431"))
LISTEN_PORT = int(os.environ.get("XS_MYSQL_PORT", "18432"))
ADMIN = f"http://127.0.0.1:{ADMIN_PORT}"
GATEWAY = f"http://127.0.0.1:{HTTP_PORT}"
BIN = os.path.abspath(os.environ.get("XS_BIN", "./target/debug/xsqld"))
LOG = "/tmp/xsqld-mysql-cli.log"

CANDIDATES = [os.environ.get("MYSQL_CLI"), "mysql", "/opt/homebrew/opt/mysql-client/bin/mysql",
              "/usr/local/opt/mysql-client/bin/mysql", "/usr/bin/mysql"]

results = []


def rec(name, ok, evidence=""):
    results.append((name, ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       {evidence}", flush=True)


def find_client():
    for c in CANDIDATES:
        if not c:
            continue
        path = shutil.which(c) or (c if os.path.exists(c) else None)
        if path:
            try:
                out = subprocess.run([path, "--version"], capture_output=True, text=True,
                                     timeout=20).stdout.strip()
                if out:
                    return path, out
            except Exception:
                pass
    return None, None


def http(method, url, body=b"{}", timeout=30):
    req = urllib.request.Request(url, data=body, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


def start_xsqld():
    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], check=False)
    time.sleep(1)
    env = {k: v for k, v in os.environ.items()
           if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    env["RUST_LOG"] = "warn"
    proc = subprocess.Popen(
        [BIN, "--mysql-host", BACKEND_HOST, "--mysql-port", str(BACKEND_PORT),
         "--mysql-user", BACKEND_USER, "--mysql-password", BACKEND_PASSWORD,
         "--http-listen-addr", f"127.0.0.1:{HTTP_PORT}",
         "--admin-listen-addr", f"127.0.0.1:{ADMIN_PORT}",
         "--mysql-listen-addr", f"127.0.0.1:{LISTEN_PORT}"],
        env=env, stdout=open(LOG, "w"), stderr=subprocess.STDOUT, preexec_fn=os.setsid)
    for _ in range(120):
        try:
            with urllib.request.urlopen(f"{GATEWAY}/health", timeout=2) as r:
                if r.status == 200:
                    return proc
        except Exception:
            pass
        time.sleep(0.5)
    raise SystemExit(f"xsqld did not come up; see {LOG}")


def base(cli, port):
    # an offline client would try TLS first and get 1251 back, which is not what is under test here
    return [cli, "-h", "127.0.0.1", "-P", str(port), "-u", BACKEND_USER,
            "--ssl-mode=DISABLED", "--connect-timeout=10", "--batch", "--raw"]


def run_script(cli, script, port=LISTEN_PORT, database=NS, timeout=40):  # noqa: PLR0913
    """Feeds statements on stdin, the way `mysql < dump.sql` does: the client splits them itself,
    so each one arrives as its own COM_QUERY on one connection."""
    args = base(cli, port)
    if database is not None:
        args += ["--database", database]
    r = subprocess.run(args, input=script, capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout, r.stderr


def run_exec(cli, statement, timeout=40):
    r = subprocess.run(base(cli, LISTEN_PORT) + ["--database", NS, "--execute", statement],
                       capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout, r.stderr


def clean(text):
    return text.replace("\r", "").strip()


def main():
    if not os.path.exists(BIN):
        print(f"{BIN} not found -- run `cargo build` first", file=sys.stderr)
        return 2
    cli, version = find_client()
    if not cli:
        print("no `mysql` client found; set MYSQL_CLI=/path/to/mysql", file=sys.stderr)
        return 2
    print(f"client: {version}\n")

    proc = start_xsqld()
    try:
        for ns in (NS, PEER):
            code = http("POST", f"{ADMIN}/v1/namespaces/{ns}/create")
            rec(f"namespace {ns} is available", code in (200, 201, 400, 409), str(code))

        rc, out, err = run_exec(cli, "SELECT 1 AS one")
        rec("a single value comes back (this hung once, with DEPRECATE_EOF on)",
            rc == 0 and clean(out) == "one\n1", f"rc={rc} out={clean(out)!r} err={err.strip()[:80]!r}")

        rc, out, err = run_exec(cli, "SELECT 2 AS a, 'x' AS b UNION ALL SELECT 3, 'y'")
        rec("two columns and two rows print in order",
            rc == 0 and clean(out) == "a\tb\n2\tx\n3\ty", f"rc={rc} out={clean(out)!r}")

        rc, out, err = run_exec(cli, "SELECT 4 AS n WHERE 1 = 0")
        rec("an empty result set is empty, not a truncated one",
            rc == 0 and clean(out) in ("", "n"), f"rc={rc} out={clean(out)!r} err={err.strip()[:60]!r}")

        rc, out, err = run_exec(cli, "SELECT NULL AS nothing, 1.5 AS halve")
        rec("NULL and a float render the way the client expects",
            rc == 0 and clean(out) == "nothing\thalve\nNULL\t1.5", f"rc={rc} out={clean(out)!r}")

        rc, out, err = run_exec(cli, "SELECT REPEAT('d', 120000) AS big")
        rec("a value spanning packets arrives complete",
            rc == 0 and len(clean(out).split("\n", 1)[1]) == 120000, f"rc={rc} len={len(clean(out))}")

        setup = ("DROP TABLE IF EXISTS cli_t; "
                 "CREATE TABLE cli_t(id INT PRIMARY KEY, name VARCHAR(32), raw VARBINARY(32));")
        rc, out, err = run_script(cli, setup)
        rec("DDL over the listener reports no error", rc == 0, f"rc={rc} err={err.strip()[:80]!r}")

        rc, out, err = run_script(
            cli, "INSERT INTO cli_t VALUES (1,'quo\"te',0x00011a5c2722c3a9),(2,'back\\\\slash',NULL);")
        rec("quotes, backslashes and binary literals are accepted", rc == 0,
            f"rc={rc} err={err.strip()[:90]!r}")

        rc, out, err = run_script(
            cli, "SELECT id, name, HEX(raw) AS h FROM cli_t ORDER BY id;")
        rec("the row and its bytes come back unchanged",
            clean(out) == "id\tname\th\n1\tquo\"te\t00011A5C2722C3A9\n2\tback\\slash\tNULL",
            f"out={clean(out)!r}")

        rc, out, err = run_script(cli, "SELECT * FROM cli_missing_table_z;\n")
        rec("a backend error reaches the shell as an error",
            rc != 0 and "1146" in err, f"rc={rc} err={err.strip()[:100]!r}")

        # SQL-level PREPARE is the backend's own feature, so the listener has to answer exactly as
        # the backend does. (COM_STMT_PREPARE -- the binary protocol -- is refused; that is checked
        # on the wire in test_mysql_protocol.py.)
        script = "PREPARE p FROM 'SELECT 2 AS two';\nEXECUTE p;\nDEALLOCATE PREPARE p;\n"
        rc_l, out_l, err_l = run_script(cli, script)
        rc_b, out_b, err_b = run_script(cli, script, port=BACKEND_PORT)
        rec("SQL PREPARE/EXECUTE works inside the session, as it does on the backend",
            (rc_l, clean(out_l)) == (rc_b, clean(out_b)),
            f"listener rc={rc_l} out={clean(out_l)!r} | backend rc={rc_b} out={clean(out_b)!r}"
            f" | err={err_l.strip()[:60]!r}")

        rc, out, err = run_script(cli, "USE xs_mysql_cli_peer;\nSELECT 'now in ' AS where_, DATABASE();\n")
        rec("USE moves the session and reports it",
            rc == 0 and "xs_mysql_cli_peer" in clean(out), f"rc={rc} out={clean(out)!r}")

        rc, out, err = run_script(cli, "BEGIN;\nUPDATE cli_t SET name='dirty' WHERE id=1;\nROLLBACK;\n"
                                       "SELECT name FROM cli_t WHERE id=1;\n")
        rec("a rollback undoes the write in the same client session",
            rc == 0 and clean(out).endswith("quo\"te"), f"rc={rc} out={clean(out)!r}")

        rc, out, err = run_script(cli, "BEGIN;\nUPDATE cli_t SET name='kept' WHERE id=1;\nCOMMIT;\n")
        rec("a commit is accepted", rc == 0, f"rc={rc} err={err.strip()[:60]!r}")
        rc, out, err = run_exec(cli, "SELECT name FROM cli_t WHERE id=1")
        rec("and the write is really there", clean(out) == "name\nkept", f"out={clean(out)!r}")

        rc, out, err = run_exec(cli, "SHOW TABLES")
        rec("SHOW works like any other statement", rc == 0 and "cli_t" in out, f"out={clean(out)!r}")

        rc, out, err = run_script(cli, "DESCRIBE cli_t;\n")
        body = [ln for ln in clean(out).split("\n")[1:] if ln]
        rec("DESCRIBE renders a line per column of its metadata",
            rc == 0 and clean(out).startswith("Field\tType") and len(body) == 3,
            f"rc={rc} rows={len(body)} out={clean(out)[:60]!r}")

        # A MySQL session is one server thread: statement-to-statement state has to survive inside
        # one client connection and must not survive outside it.
        rc, out, err = run_script(cli, "SET @a = 5;\nSELECT @a AS myvar;\n")
        rec("a user variable set in the session is readable in the same session",
            rc == 0 and clean(out) == "myvar\n5", f"rc={rc} out={clean(out)!r}")

        auto = ("DROP TABLE IF EXISTS cli_ai;\n"
                "CREATE TABLE cli_ai(id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(10));\n"
                "INSERT INTO cli_ai VALUES (NULL, 'a');\nINSERT INTO cli_ai VALUES (NULL, 'b');\n"
                "SELECT LAST_INSERT_ID() AS lid;\n")
        rc_l, out_l, err_l = run_script(cli, auto)
        rc_b, out_b, err_b = run_script(cli, auto, port=BACKEND_PORT)
        rec("LAST_INSERT_ID() reports the id this session generated",
            rc_l == 0 and clean(out_l) == clean(out_b) == "lid\n2",
            f"listener rc={rc_l} out={clean(out_l)!r} err={err_l.strip()[:60]!r} | "
            f"backend out={clean(out_b)!r}")

        # Every client connection took a backend connection; if they leaked, the pool would run out.
        fails = 0
        for _ in range(40):
            if run_exec(cli, "SELECT 1")[0] != 0:
                fails += 1
        rec("40 short-lived client connections neither hang nor exhaust the pool", fails == 0,
            f"{fails} failed")
    finally:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=30)

    bad = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(bad)}/{len(results)} real-client checks passed")
    if bad:
        print("failed: " + ", ".join(bad) + f"\n(server log: {LOG})")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
