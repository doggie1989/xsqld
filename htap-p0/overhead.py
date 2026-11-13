import json, os, subprocess, sys, time
import common as C

HTAP = C.HTAP_DIR
BASE = C.BASE
GO = os.path.join(BASE, "xsqld-bench-linux")


def rss_kb(pid):
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS"):
                    return int(line.split()[1])
    except Exception:
        return 0
    return 0


def run_burst(dur=20):
    out = os.path.join(HTAP, "overhead-burst.json")
    subprocess.run([GO, "-mode", "ord-write", "-workers", "4", "-dur", f"{dur}s",
                    "-host", "htap.db", "-out", out],
                   cwd=BASE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return json.load(open(out))


def changelog_size():
    sc = C.sqlite_ro()
    n = sc.execute("SELECT count(*) FROM xsqld_changelog").fetchone()[0]
    sc.close()
    return n


def set_triggers(enabled):
    if enabled:
        sql = open(os.path.join(HTAP, "schema.sql")).read()
        import setup
        for s in setup.split_statements(sql):
            if s.upper().startswith("CREATE TRIGGER"):
                C.hrana(s)
    else:
        for t in ["orders_ai", "orders_au", "orders_ad"]:
            C.hrana(f"DROP TRIGGER IF EXISTS {t}")


def main():
    xsqld_pid = int(open(os.path.join(BASE, "xsqld.pid")).read())
    syncer = subprocess.Popen([sys.executable, "syncer.py", "--verify-every", "2"],
                              cwd=HTAP, stdout=open(os.path.join(HTAP, "overhead-syncer.log"), "w"),
                              stderr=subprocess.STDOUT)
    time.sleep(2)

    print("== WITH triggers ==", flush=True)
    cl0 = changelog_size()
    d1 = run_burst(20)
    cl1 = changelog_size()
    mem1 = (rss_kb(xsqld_pid), rss_kb(syncer.pid))
    print(f"  throughput {d1['ops_per_s']:.0f} ops/s, err {d1['errors']}, "
          f"changelog +{cl1-cl0}, RSS xsqld={mem1[0]//1024}MB syncer={mem1[1]//1024}MB", flush=True)

    print("== WITHOUT triggers ==", flush=True)
    set_triggers(False)
    cl2 = changelog_size()
    d2 = run_burst(20)
    cl3 = changelog_size()
    mem2 = (rss_kb(xsqld_pid), rss_kb(syncer.pid))
    print(f"  throughput {d2['ops_per_s']:.0f} ops/s, err {d2['errors']}, "
          f"changelog +{cl3-cl2}, RSS xsqld={mem2[0]//1024}MB syncer={mem2[1]//1024}MB", flush=True)

    set_triggers(True)
    print("== triggers re-installed ==", flush=True)

    overhead = (d2['ops_per_s'] - d1['ops_per_s']) / d2['ops_per_s'] * 100
    result = {
        "with_triggers_ops_s": round(d1['ops_per_s']),
        "without_triggers_ops_s": round(d2['ops_per_s']),
        "write_overhead_pct": round(overhead, 1),
        "changelog_rows_per_insert_with": (cl1 - cl0) / max(d1['ops'], 1),
        "rss_xsqld_mb": mem1[0] // 1024,
        "rss_syncer_mb": mem1[1] // 1024,
    }
    print("RESULT " + json.dumps(result), flush=True)
    json.dump(result, open(os.path.join(HTAP, "overhead.json"), "w"), indent=2)
    syncer.terminate()
    syncer.wait()


if __name__ == "__main__":
    main()