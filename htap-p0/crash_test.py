import json, os, signal, subprocess, sys, time
import common as C

HTAP = C.HTAP_DIR
BASE = C.BASE
XSQLD_PID = os.path.join(BASE, "xsqld.pid")
SYNCER_LOG = os.path.join(HTAP, "crash-syncer.log")


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def start_syncer():
    f = open(SYNCER_LOG, "w")
    p = subprocess.Popen([sys.executable, "syncer.py", "--verify-every", "1"],
                         cwd=HTAP, stdout=f, stderr=subprocess.STDOUT)
    time.sleep(1.5)
    return p


def stop_syncer(p):
    if p and p.poll() is None:
        p.terminate()
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
    time.sleep(0.5)


def kill_syncer(p):
    if p and p.poll() is None:
        p.kill()
        p.wait()
    time.sleep(0.5)


def last_metrics():
    try:
        with open(SYNCER_LOG) as f:
            lines = [l for l in f if l.startswith("METRICS")]
        return json.loads(lines[-1][8:]) if lines else {}
    except Exception:
        return {}


def wait_converge(timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        m = last_metrics()
        if m.get("pending") == 0 and m.get("drift") is False:
            return m
        time.sleep(1)
    return last_metrics()


def assert_consistent(label):
    """Syncer must be STOPPED so DuckDB lock is free."""
    sc = C.sqlite_ro()
    dc = C.duckdb_connect(read_only=True)
    s_cnt, s_sum = sc.execute("SELECT count(*), COALESCE(sum(amount_cents),0) FROM orders").fetchone()
    d_cnt, d_sum = dc.execute("SELECT count(*), COALESCE(sum(amount_cents),0) FROM orders_native").fetchone()
    wm = C.get_watermark(dc)
    max_seq = sc.execute("SELECT COALESCE(MAX(seq),0) FROM xsqld_changelog").fetchone()[0]
    sc.close(); dc.close()
    ok = (s_cnt == d_cnt) and (s_sum == d_sum) and (wm == max_seq)
    log(f"{label}: sqlite={s_cnt}/{s_sum} duckdb={d_cnt}/{d_sum} wm={wm}/{max_seq} -> {'OK' if ok else 'FAIL'}")
    if not ok:
        raise AssertionError(f"{label} inconsistent")
    return ok


def gen_writes(n):
    sc = C.sqlite_ro()
    max_id = sc.execute("SELECT MAX(id) FROM orders").fetchone()[0]
    sc.close()
    base = 1763000000
    vals = []
    for i in range(1, n + 1):
        rid = max_id + i
        vals.append(f"({rid},{rid % 5000000},{1000 + i % 9000},{i % 6},{i % 4},{base + i})")
    sql = "INSERT INTO orders VALUES " + ",".join(vals)
    C.hrana(sql, timeout=120)
    return max_id + n


def restart_xsqld():
    pid = int(open(XSQLD_PID).read())
    os.kill(pid, signal.SIGKILL)
    time.sleep(0.5)
    f = open(os.path.join(BASE, "xsqld.log"), "ab")
    p = subprocess.Popen(["./xsqld", "--db-path", os.path.join(BASE, "data")],
                         cwd=BASE, stdout=f, stderr=subprocess.STDOUT, start_new_session=True)
    open(XSQLD_PID, "w").write(str(p.pid))
    t0 = time.time()
    import urllib.request
    while time.time() - t0 < 30:
        try:
            with urllib.request.urlopen("http://127.0.0.1:8080/health", timeout=1) as r:
                if r.status == 200:
                    return time.time() - t0
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("xsqld did not become healthy")


def scenario_a():
    log("=== Scenario A: kill syncer mid-apply ===")
    p = start_syncer()
    gen_writes(8000)
    time.sleep(0.3)
    kill_syncer(p)
    log("syncer killed mid-apply")
    p = start_syncer()
    m = wait_converge()
    stop_syncer(p)
    assert_consistent("A")


def scenario_b():
    log("=== Scenario B: kill -9 xsqld with committed-unapplied changelog ===")
    gen_writes(5000)
    sc = C.sqlite_ro()
    cnt_before = sc.execute("SELECT count(*) FROM orders").fetchone()[0]
    sc.close()
    rt = restart_xsqld()
    log(f"xsqld killed+restarted in {rt:.3f}s")
    sc = C.sqlite_ro()
    cnt_after = sc.execute("SELECT count(*) FROM orders").fetchone()[0]
    sc.close()
    assert cnt_before == cnt_after, "xsqld lost committed rows!"
    log(f"committed rows intact: {cnt_after}")
    p = start_syncer()
    wait_converge()
    stop_syncer(p)
    assert_consistent("B")


def scenario_c():
    log("=== Scenario C: DuckDB corruption -> rebuild ===")
    dc = C.duckdb_connect()
    dc.execute("DELETE FROM orders_native WHERE id % 7 = 0")
    dc.close()
    log("corrupted mirror (deleted 1/7 of rows)")
    dc = C.duckdb_connect()
    wm = C.rebuild_mirror(dc)
    dc.close()
    log(f"rebuilt mirror from SQLite, watermark={wm}")
    p = start_syncer()
    wait_converge()
    stop_syncer(p)
    assert_consistent("C")


def idempotency():
    log("=== Idempotency: re-apply already-applied changelog ===")
    sc = C.sqlite_ro()
    max_seq = sc.execute("SELECT COALESCE(MAX(seq),0) FROM xsqld_changelog").fetchone()[0]
    sc.close()
    dc = C.duckdb_connect()
    C.init_duckdb_schema(dc)
    dc.execute("INSERT OR REPLACE INTO xsqld_meta VALUES('watermark','0')")
    dc.close()
    log(f"reset watermark to 0 (max_seq={max_seq}), full replay")
    p = start_syncer()
    wait_converge(timeout=120)
    stop_syncer(p)
    assert_consistent("IDEMPOTENCY")


def main():
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    scenarios = {"a": scenario_a, "b": scenario_b, "c": scenario_c, "idem": idempotency}
    # ensure no syncer running at start
    try:
        pid = int(open(os.path.join(HTAP, "syncer.pid")).read())
        os.kill(pid, signal.SIGTERM)
        time.sleep(1)
    except Exception:
        pass
    if which == "all":
        for name in ["a", "b", "c", "idem"]:
            scenarios[name]()
    else:
        scenarios[which]()
    log("ALL CRASH TESTS PASSED")


if __name__ == "__main__":
    main()