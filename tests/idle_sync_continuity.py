#!/usr/bin/env python3
"""回归：命名空间只有直连 MySQL 写入、无 HTTP 流量时，镜像同步不得因池空闲关闭而停止。

bug 背景：worker 遍历 open_namespaces()，而 close_idle_pools 会按 HTTP 活跃度关闭空闲池。
soak 全程用 pymysql 直连写入，5 分钟后池被关掉，worker 不再看到该命名空间，镜像静默变陈旧
（表现为指标里 wm/cycles 归零）。修复：worker 每轮调用 note_mirror_activity() 声明自己是活跃使用者。
"""
import json
import os
import re
import signal
import subprocess
import sys
import time

import pymysql
import requests

USER = "http://127.0.0.1:18280"
ADMIN = "http://127.0.0.1:18281"
NS = "idle_sync"
HOST = {"Host": f"{NS}.db"}
MY = dict(host="127.0.0.1", port=12981, user="root", password="", autocommit=True)
MIRROR_DIR = "/tmp/xsqld-idle"
BIN = os.path.abspath("./target/debug/xsqld")
IDLE_MS = 2000
results = []


def rec(n, ok, ev):
    results.append((n, ok, ev))
    print(f"[{'PASS' if ok else 'FAIL'}] {n}\n       {ev}", flush=True)


def cycles():
    try:
        t = requests.get(f"{USER}/metrics", timeout=10).text
        m = re.search(r'xsqld_mirror_cycles_total\{ns="%s"\}\s+(\d+)' % NS, t)
        return int(m.group(1)) if m else 0
    except Exception:
        return -1


def main():
    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], check=False)
    time.sleep(2)
    subprocess.run(["rm", "-rf", MIRROR_DIR], check=False)
    env = {k: v for k, v in os.environ.items() if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    env["RUST_LOG"] = "warn"
    proc = subprocess.Popen(
        [BIN, "--mysql-host", "127.0.0.1", "--mysql-port", "12981", "--mysql-user", "root",
         "--http-listen-addr", "127.0.0.1:18280", "--admin-listen-addr", "127.0.0.1:18281",
         "--mirror-enabled", "--mirror-dir", MIRROR_DIR, "--mirror-interval-ms", "400",
         "--route-min-rows", "1", "--idle-close-ms", str(IDLE_MS)],
        stdout=open("/tmp/xsqld-idle.log", "w"), stderr=subprocess.STDOUT, env=env)
    try:
        for _ in range(120):
            try:
                if requests.get(f"{USER}/health", timeout=2).status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(0.5)
        requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=30)
        # 唯一一次 HTTP 请求：建表并打开命名空间。此后全程只用直连 MySQL。
        requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
            {"type": "execute", "stmt": {"sql": "DROP TABLE IF EXISTS t"}},
            {"type": "execute", "stmt": {"sql": "CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(32))"}},
            {"type": "close"}]}, headers=HOST, timeout=60)
        for _ in range(80):
            if cycles() > 0:
                break
            time.sleep(0.5)
        c0 = cycles()
        rec("初始同步已运行", c0 > 0, f"cycles={c0}")

        # 静默期：远超 idle 超时，只写直连 MySQL，不发任何 HTTP 数据请求
        quiet = IDLE_MS / 1000 * 4
        print(f"  静默 {quiet:.0f}s（idle 超时 {IDLE_MS}ms 的 4 倍），仅直连写入...", flush=True)
        c = pymysql.connect(**MY, database=NS); cu = c.cursor()
        t0 = time.time()
        n = 0
        while time.time() - t0 < quiet:
            cu.execute(f"INSERT INTO t VALUES({1000+n},'q{n}')")
            n += 1
            time.sleep(0.2)
        cu.execute("SELECT COUNT(*) FROM t"); src_n = cu.fetchone()[0]
        c.close()
        time.sleep(3)

        c1 = cycles()
        rec("静默期后同步周期仍在推进（池未被空闲关闭）", c1 > c0, f"cycles {c0} -> {c1}")

        # 现在才发 HTTP 查询核对镜像
        r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
            {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) FROM t"}}, {"type": "close"}]},
            headers=HOST, timeout=90)
        mir_n = int(r.json()["results"][0]["response"]["result"]["rows"][0][0]["value"])
        rec("静默期写入全部进入镜像", mir_n == src_n, f"mirror={mir_n} source={src_n} 静默期写入={n}")
        log = open("/tmp/xsqld-idle.log").read()
        rec("无 panic", "panic" not in log.lower(), f"panic={'panic' in log.lower()}")
        np = sum(1 for _, o, _ in results if o); nf = sum(1 for _, o, _ in results if not o)
        print(f"\nPASS={np} FAIL={nf}", flush=True)
        json.dump([{"check": a, "ok": b, "evidence": str(d)} for a, b, d in results],
                  open("/tmp/idle-sync-results.json", "w"), ensure_ascii=False, indent=2)
        return 0 if nf == 0 else 1
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except Exception:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())