#!/usr/bin/env python3
"""真实二进制端到端：xsqld --mirror-enabled 对着活 SeekDB，验证自动同步。

运行中用 outbox 是否被清空证明周期真的 apply+prune 了；停机后直接读 DuckDB 文件核对值。
覆盖：HTTP 写入、直连 MySQL 写入（绕过 xsqld）、TRUNCATE 行数对账、UPDATE/DELETE。
"""
import json
import os
import re
import signal
import subprocess
import sys
import time

import duckdb
import pymysql
import requests

USER = "http://127.0.0.1:18280"
ADMIN = "http://127.0.0.1:18281"
NS = "e2e"
HOST = {"Host": f"{NS}.db"}
MIRROR = f"/tmp/xsqld-mirrors/{NS}.duckdb"
MY = dict(host="127.0.0.1", port=12981, user="root", password="", autocommit=True)

results = []


def rec(name, ok, ev):
    results.append((name, ok, ev))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       {ev}", flush=True)


def pipeline(sqls, baton=None):
    reqs = [{"type": "execute", "stmt": {"sql": s}} for s in sqls]
    reqs.append({"type": "close"})
    r = requests.post(f"{USER}/v2/pipeline", json={"baton": baton, "requests": reqs},
                      headers=HOST, timeout=60)
    return r.status_code, r.json()


def wait_health():
    for _ in range(60):
        try:
            if requests.get(f"{USER}/health", timeout=2).status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def outbox_rows():
    try:
        c = pymysql.connect(**MY, database=NS)
    except Exception:
        return -1
    cu = c.cursor()
    try:
        cu.execute("SELECT COUNT(*) FROM xsqld_changelog")
        n = cu.fetchone()[0]
    except Exception:
        n = -1
    c.close()
    return n


def wait_outbox_drained(timeout=25):
    t0 = time.time()
    while time.time() - t0 < timeout:
        n = outbox_rows()
        if n == 0:
            return True, time.time() - t0
        time.sleep(0.4)
    return False, outbox_rows()


def main():
    if not wait_health():
        print("xsqld 未就绪")
        return 2
    rec("xsqld 健康检查", True, "GET /health 200")

    r = requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=30)
    rec("创建命名空间", r.status_code in (200, 201, 400, 409), f"{r.status_code} {r.text[:120]}")

    pipeline(["DROP TABLE IF EXISTS orders"])
    st, body = pipeline(["CREATE TABLE orders(id INT PRIMARY KEY, uid INT, amt DECIMAL(12,2), s VARCHAR(64))"])
    rec("HTTP 建表", st == 200, f"{st} {json.dumps(body)[:200]}")

    st, body = pipeline(["INSERT INTO orders VALUES(1,10,99.50,'a'),(2,20,10.00,'b'),(3,10,5.25,'c')"])
    rec("HTTP 插入 3 行", st == 200, f"{st} {json.dumps(body)[:200]}")

    # 就绪判据必须包含"至少一个同步周期已完成"。只等镜像文件存在是不够的：文件在 NsMirror::open 时就创建，
    # 而 fixture 数据早于触发器安装，outbox 恒为 0，于是所有 wait_outbox_drained 都立刻返回——整个测试可能
    # 跑在首个周期之前，靠一次快照恰好捕获最终状态而"通过"，等于没验证增量同步。
    def cycles():
        try:
            mt = requests.get(f"{USER}/metrics", timeout=10).text
            m = re.search(r'xsqld_mirror_cycles_total\{ns="%s"\}\s+(\d+)' % NS, mt)
            return int(m.group(1)) if m else 0
        except Exception:
            return 0

    for _ in range(150):
        if os.path.exists(MIRROR) and cycles() > 0:
            break
        time.sleep(0.5)
    ok, info = wait_outbox_drained()
    rec("自动同步：outbox 被清空（证明 apply+prune 真发生）", ok, f"{info}")
    rec("镜像文件已生成", os.path.exists(MIRROR), f"{MIRROR} exists={os.path.exists(MIRROR)}")
    rec("至少一个同步周期已完成（增量路径真的跑过）", cycles() > 0, f"mirror_cycles_total={cycles()}")

    # 直连 MySQL 写入，完全绕过 xsqld
    c = pymysql.connect(**MY, database=NS)
    cu = c.cursor()
    cu.execute("INSERT INTO orders VALUES(4,30,7.00,'direct')")
    cu.execute("UPDATE orders SET s='B2' WHERE id=2")
    cu.execute("DELETE FROM orders WHERE id=3")
    c.close()
    ok, info = wait_outbox_drained()
    rec("直连 MySQL 写入也被自动同步", ok, f"{info}")

    # TRUNCATE：不触发触发器，只能靠行数对账
    c = pymysql.connect(**MY, database=NS)
    cu = c.cursor()
    cu.execute("SELECT COUNT(*) FROM xsqld_changelog")
    cu.execute("TRUNCATE TABLE orders")
    cu.execute("SELECT COUNT(*) FROM xsqld_changelog")
    ob_after_trunc = cu.fetchone()[0]
    cu.execute("INSERT INTO orders VALUES(9,90,1.00,'after-truncate')")
    c.close()
    rec("TRUNCATE 确实不产生 outbox（否则对账测试无意义）", ob_after_trunc == 0, f"outbox={ob_after_trunc}")
    # TRUNCATE 不产生 outbox，outbox 恒 0 ⇒ wait_outbox_drained 立即返回，证明不了对账已跑过。
    # 必须等**可观测的收敛**：镜像侧 COUNT 真的变成 1。且只认 routed_total 真增加的读数，
    # 否则回退到 SeekDB 的读数会让这条断言空过。
    def routed_total():
        mt = requests.get(f"{USER}/metrics", timeout=10).text
        m = re.search(r'xsqld_routed_total\{ns="%s"\}\s+(\d+)' % NS, mt)
        return int(m.group(1)) if m else 0

    def mirror_count():
        before = routed_total()
        st, body = pipeline(["SELECT COUNT(*) FROM orders"])
        if st != 200 or routed_total() <= before:
            return None
        return int(body["results"][0]["response"]["result"]["rows"][0][0]["value"])

    t0 = time.time()
    mc = None
    while time.time() - t0 < 30:
        mc = mirror_count()
        if mc == 1:
            break
        time.sleep(0.4)
    rec("TRUNCATE 后行数对账把镜像追平（等真收敛，非固定 sleep）", mc == 1,
        f"镜像 COUNT={mc}，等待 {time.time()-t0:.2f}s")

    # worker 运行证据用指标而非日志：日志级别可变，指标不受影响。必须在停机前抓取。
    cyc = cycles()

    # 停机后读镜像文件核对精确值
    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], check=False)
    time.sleep(3)
    try:
        d = duckdb.connect(MIRROR, read_only=True)
        rows = d.execute("SELECT id, uid, CAST(amt AS VARCHAR), s FROM m_orders ORDER BY id").fetchall()
        rec("停机后镜像内容正确（TRUNCATE 已生效，只剩 1 行）",
            rows == [(9, 90, "1.00", "after-truncate")], f"m_orders={rows}")
        tabs = [r[0] for r in d.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()]
        rec("镜像只含用户表，无内部表泄漏",
            not any(t.startswith("__all") or t.startswith("DBA_") for t in tabs), f"tables={tabs}")
        wm = d.execute("SELECT watermark_scn FROM xsqld_mirror_meta WHERE id=1").fetchone()
        rec("水位已推进且存于 DuckDB", wm is not None and wm[0] > 0, f"watermark_scn={wm}")
        d.close()
    except Exception as e:
        rec("读取镜像文件", False, str(e)[:300])

    print("\n=== xsqld 日志中的镜像活动 ===", flush=True)
    with open("/tmp/xsqld-run.log") as f:
        lines = [l.rstrip() for l in f if "mirror" in l.lower()]
    for l in lines[-12:]:
        print("  " + l[:200], flush=True)
    rec("指标证明 worker 真的在跑（与日志级别无关）", cyc > 0, f"mirror_cycles_total={cyc}, 镜像日志 {len(lines)} 条")

    np = sum(1 for _, ok, _ in results if ok)
    nf = sum(1 for _, ok, _ in results if not ok)
    print(f"\nPASS={np} FAIL={nf}", flush=True)
    json.dump([{"check": n, "ok": o, "evidence": str(e)} for n, o, e in results],
              open("/tmp/xsqld-e2e-results.json", "w"), ensure_ascii=False, indent=2)
    return 0 if nf == 0 else 1


if __name__ == "__main__":
    sys.exit(main())