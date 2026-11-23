#!/usr/bin/env python3
"""维护窗口端到端：证明窗口内捕获真被摘掉、worker 不会中途重建快照、退出后镜像追平。

关键非空测：窗口期间镜像行数必须与源表**不一致**（否则说明抑制逻辑没生效或窗口没打开）。
"""
import json
import re
import subprocess
import sys
import time

import pymysql
import requests

USER = "http://127.0.0.1:18280"
ADMIN = "http://127.0.0.1:18281"
NS = "maint_e2e"
HOST = {"Host": f"{NS}.db"}
MY = dict(host="127.0.0.1", port=12981, user="root", password="", autocommit=True)
results = []


def rec(n, ok, ev):
    results.append((n, ok, ev))
    print(f"[{'PASS' if ok else 'FAIL'}] {n}\n       {ev}", flush=True)


def pipe(sql):
    r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
        {"type": "execute", "stmt": {"sql": sql}}, {"type": "close"}]}, headers=HOST, timeout=60)
    return r.status_code, r.json()


def routed_total():
    t = requests.get(f"{USER}/metrics", timeout=10).text
    m = re.search(r'xsqld_routed_total\{ns="%s"\}\s+(\d+)' % NS, t)
    return int(m.group(1)) if m else 0


def mirror_count():
    """只接受真正走了 DuckDB 的读数；回退到 SeekDB 的读数无效，返回 None。"""
    b0 = routed_total()
    st, b = pipe("SELECT COUNT(*) FROM t")
    if routed_total() <= b0:
        return None
    try:
        res = b["results"][0]["response"]["result"]
        return int(res["rows"][0][0]["value"])
    except Exception:
        return None


def source_count():
    c = pymysql.connect(**MY, database=NS); cu = c.cursor()
    cu.execute("SELECT COUNT(*) FROM t"); n = cu.fetchone()[0]
    try:
        cu.execute("SELECT COUNT(*) FROM xsqld_changelog"); ob = cu.fetchone()[0]
    except Exception:
        ob = -1
    c.close()
    return n, ob


def main():
    for _ in range(60):
        try:
            if requests.get(f"{USER}/health", timeout=2).status_code == 200:
                break
        except Exception:
            time.sleep(0.5)
    requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=30)
    pipe("DROP TABLE IF EXISTS t")
    pipe("CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(64))")
    pipe("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')")
    for _ in range(80):
        n, ob = source_count()
        if ob == 0 and mirror_count() == 3:
            break
        time.sleep(0.4)
    rec("初始同步完成", mirror_count() == 3, f"mirror={mirror_count()} source={source_count()}")

    r = requests.post(f"{ADMIN}/v1/namespaces/{NS}/maintenance/t/begin", timeout=30)
    rec("进入维护窗口", r.status_code == 200, f"{r.status_code} {r.text[:120]}")

    c = pymysql.connect(**MY, database=NS); cu = c.cursor()
    vals = ",".join(f"({i},'bulk{i}')" for i in range(100, 1100))
    t0 = time.perf_counter()
    cu.execute(f"INSERT INTO t VALUES{vals}")
    bulk_dt = time.perf_counter() - t0
    c.close()
    n, ob = source_count()
    rec("窗口内批量写入不产生 outbox（捕获真被摘掉）", ob == 0, f"写 1000 行耗时 {bulk_dt:.3f}s, outbox={ob}")

    # 等若干个 worker 周期，确认它没有中途重建快照
    time.sleep(4)
    mc, (sc, ob2) = mirror_count(), source_count()
    rec("窗口期间 worker 未自动重建快照（抑制生效）", mc == 3 and sc == 1003,
        f"mirror={mc}（应仍为 3，None 表示该次读数回退到了 SeekDB） source={sc} outbox={ob2}")

    r = requests.post(f"{ADMIN}/v1/namespaces/{NS}/maintenance/t/end", timeout=60)
    rec("退出维护窗口", r.status_code == 200, f"{r.status_code} {r.text[:120]}")

    for _ in range(60):
        if mirror_count() == source_count()[0]:
            break
        time.sleep(0.4)
    mc, (sc, _) = mirror_count(), source_count()
    rec("退出后镜像追平源表（快照兜住窗口内写入）", mc == sc == 1003, f"mirror={mc} source={sc}")

    # 退出后捕获必须恢复
    c = pymysql.connect(**MY, database=NS); cu = c.cursor()
    cu.execute("INSERT INTO t VALUES(9999,'after')")
    c.close()
    for _ in range(60):
        if mirror_count() == 1004:
            break
        time.sleep(0.4)
    rec("退出后增量捕获已恢复", mirror_count() == 1004, f"mirror={mirror_count()}")

    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], check=False)
    np = sum(1 for _, o, _ in results if o); nf = sum(1 for _, o, _ in results if not o)
    print(f"\nPASS={np} FAIL={nf}", flush=True)
    json.dump([{"check": n, "ok": o, "evidence": str(e)} for n, o, e in results],
              open("/tmp/maint-e2e-results.json", "w"), ensure_ascii=False, indent=2)
    return 0 if nf == 0 else 1


if __name__ == "__main__":
    sys.exit(main())