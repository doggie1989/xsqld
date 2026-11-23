#!/usr/bin/env python3
"""T6 点名项：增量 apply 延迟——直连 MySQL 提交 → 镜像侧可读，直接测而非用 outbox 排空作代理。

只认 routed_total 真增加的读数，否则回退到 SeekDB 的读数会让延迟恒为 0（空测）。
"""
import os
import re
import signal
import statistics
import subprocess
import sys
import time

import pymysql
import requests

BIN = os.path.abspath("./target/debug/xsqld")
USER = "http://127.0.0.1:18280"
ADMIN = "http://127.0.0.1:18281"
MY = dict(host="127.0.0.1", port=12981, user="root", password="", autocommit=True)
NS = "lag"
ROUNDS = 20
results = []


def rec(n, ok, ev):
    results.append((n, ok, ev))
    print(f"[{'PASS' if ok else 'FAIL'}] {n}\n       {ev}", flush=True)


def metric(name):
    t = requests.get(f"{USER}/metrics", timeout=10).text
    m = re.search(r'xsqld_%s\{ns="%s"\}\s+(\d+)' % (name, NS), t)
    return int(m.group(1)) if m else 0


def mirror_read(sql):
    before = metric("routed_total")
    r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
        {"type": "execute", "stmt": {"sql": sql}}, {"type": "close"}]},
        headers={"Host": f"{NS}.db", "Content-Type": "application/json"}, timeout=60)
    res = r.json()["results"][0]
    if res["type"] != "ok" or metric("routed_total") <= before:
        return None
    return res["response"]["result"]["rows"]


def main():
    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], capture_output=True)
    time.sleep(3)
    env = {k: v for k, v in os.environ.items()
           if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    env["RUST_LOG"] = "warn"
    p = subprocess.Popen(
        [BIN, "--mysql-host", "127.0.0.1", "--mysql-port", "12981", "--mysql-user", "root",
         "--http-listen-addr", "127.0.0.1:18280", "--admin-listen-addr", "127.0.0.1:18281",
         "--mirror-enabled", "--mirror-dir", "/tmp/xsqld-lag-mirrors",
         "--mirror-interval-ms", "200", "--route-min-rows", "0"],
        stdout=open("/tmp/xsqld-lag.log", "w"), stderr=subprocess.STDOUT, env=env)
    mc = pymysql.connect(**MY)
    cu = mc.cursor()
    try:
        t0 = time.time()
        while time.time() - t0 < 90:
            try:
                if requests.get(f"{USER}/health", timeout=2).status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(0.5)
        cu.execute(f"CREATE DATABASE IF NOT EXISTS {NS}")
        cu.execute(f"USE {NS}")
        cu.execute("SHOW TABLES")
        for (t,) in cu.fetchall():
            cu.execute(f"DROP TABLE IF EXISTS `{t}`")
        cu.execute("CREATE TABLE m(id INT PRIMARY KEY, v INT)")
        cu.execute("INSERT INTO m VALUES(1,0)")
        requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=60)
        requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
            {"type": "execute", "stmt": {"sql": "SELECT 1"}}, {"type": "close"}]},
            headers={"Host": f"{NS}.db", "Content-Type": "application/json"}, timeout=60)
        t0 = time.time()
        while time.time() - t0 < 120:
            if metric("mirror_cycles_total") > 0 and metric("mirror_tables") >= 1:
                break
            time.sleep(0.5)
        rec("镜像就绪且路由可用（非空测）", mirror_read("SELECT COUNT(*) FROM m") is not None,
            f"cycles={metric('mirror_cycles_total')} tables={metric('mirror_tables')}")
        lat = []
        for i in range(1, ROUNDS + 1):
            cu.execute(f"UPDATE m SET v={i} WHERE id=1")
            t0 = time.perf_counter()
            seen = None
            while time.perf_counter() - t0 < 30:
                rows = mirror_read("SELECT v, COUNT(*) FROM m GROUP BY v")
                if rows and int(rows[0][0]["value"]) == i:
                    seen = (time.perf_counter() - t0) * 1000
                    break
                time.sleep(0.02)
            if seen is None:
                rec(f"第 {i} 轮写入在 30s 内可见", False, "超时")
                continue
            lat.append(seen)
        rec("全部写入都在镜像侧可见", len(lat) == ROUNDS, f"{len(lat)}/{ROUNDS}")
        if lat:
            lat.sort()
            rec("apply 延迟 p50 < 2s", statistics.median(lat) < 2000,
                f"p50={statistics.median(lat):.0f}ms p95={lat[int(len(lat)*0.95)-1]:.0f}ms "
                f"max={lat[-1]:.0f}ms min={lat[0]:.0f}ms（周期 200ms）")
        rec("零同步错误", metric("mirror_errors_total") == 0,
            f"errors={metric('mirror_errors_total')}")
    finally:
        cu.close()
        mc.close()
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
            try:
                p.wait(timeout=30)
            except subprocess.TimeoutExpired:
                p.kill()
    bad = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results)-len(bad)}/{len(results)} PASS" + (f"，失败：{bad}" if bad else ""))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())