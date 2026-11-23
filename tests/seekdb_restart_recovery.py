#!/usr/bin/env python3
"""T7 补充：SeekDB 重启后 xsqld 与镜像同步能否恢复。

会重启 seekdb 压测容器（数据保留在容器可写层）。默认不跑，需显式 --restart 才执行，
避免常规测试套件意外重启共享资源。
"""
import argparse
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
NS = "restart_t7"
HOST = {"Host": f"{NS}.db"}
MY = dict(host="127.0.0.1", port=12981, user="root", password="", autocommit=True)
MIRROR_DIR = "/tmp/xsqld-restart"
BIN = os.path.abspath("./target/debug/xsqld")
CONTAINER = os.environ.get("XSQLD_TEST_CONTAINER", "xsqld-htap-bench")
results = []


def rec(n, ok, ev):
    results.append((n, ok, ev))
    print(f"[{'PASS' if ok else 'FAIL'}] {n}\n       {ev}", flush=True)


def pipe(sql, timeout=90):
    r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
        {"type": "execute", "stmt": {"sql": sql}}, {"type": "close"}]}, headers=HOST, timeout=timeout)
    return r.status_code, r.json()


def http_count():
    try:
        st, b = pipe("SELECT COUNT(*) FROM t")
        return int(b["results"][0]["response"]["result"]["rows"][0][0]["value"])
    except Exception:
        return None


def src_count():
    c = pymysql.connect(**MY, database=NS); cu = c.cursor()
    cu.execute("SELECT COUNT(*) FROM t"); n = cu.fetchone()[0]
    c.close()
    return n


def seekdb_up(timeout=180):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            c = pymysql.connect(**MY, connect_timeout=5); c.close()
            return True
        except Exception:
            time.sleep(2)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--restart", action="store_true", help="真的重启 seekdb 容器")
    args = ap.parse_args()
    if not args.restart:
        print("未加 --restart，跳过（避免意外重启容器）")
        return 0

    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], check=False)
    time.sleep(2)
    subprocess.run(["rm", "-rf", MIRROR_DIR], check=False)
    env = {k: v for k, v in os.environ.items() if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    env["RUST_LOG"] = "warn"
    proc = subprocess.Popen(
        [BIN, "--mysql-host", "127.0.0.1", "--mysql-port", "12981", "--mysql-user", "root",
         "--http-listen-addr", "127.0.0.1:18280", "--admin-listen-addr", "127.0.0.1:18281",
         "--mirror-enabled", "--mirror-dir", MIRROR_DIR, "--mirror-interval-ms", "500",
         "--route-min-rows", "1"],
        stdout=open("/tmp/xsqld-restart.log", "w"), stderr=subprocess.STDOUT, env=env)
    try:
        for _ in range(120):
            try:
                if requests.get(f"{USER}/health", timeout=2).status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(0.5)
        requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=30)
        pipe("DROP TABLE IF EXISTS t")
        pipe("CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(64))")
        pipe("INSERT INTO t VALUES(1,'a'),(2,'b')")
        for _ in range(120):
            if http_count() == 2:
                break
            time.sleep(0.5)
        rec("重启前同步正常", http_count() == src_count() == 2, f"mirror={http_count()} source={src_count()}")

        print(f"\n重启容器 {CONTAINER} ...", flush=True)
        r = subprocess.run(["docker", "restart", CONTAINER], capture_output=True, text=True, timeout=300)
        rec("容器重启命令成功", r.returncode == 0, f"exit={r.returncode} {r.stdout.strip()[:80]}")
        back = seekdb_up()
        rec("SeekDB 恢复可用", back, f"耗时内恢复={back}")
        if not back:
            return 1

        # 重启期间/之后 xsqld 的 HTTP 层必须仍活着
        try:
            h = requests.get(f"{USER}/health", timeout=5).status_code
        except Exception:
            h = None
        rec("xsqld HTTP 层在后端重启后仍存活", h == 200, f"health={h}")

        # 后端恢复后写入必须重新传播
        for attempt in range(6):
            try:
                c = pymysql.connect(**MY, database=NS); cu = c.cursor()
                cu.execute(f"INSERT INTO t VALUES({100+attempt},'after-restart')")
                c.close()
                break
            except Exception:
                time.sleep(3)
        target = src_count()
        ok = False
        for _ in range(150):
            if http_count() == target:
                ok = True
                break
            time.sleep(1)
        rec("后端恢复后增量同步自动恢复", ok, f"mirror={http_count()} source={target}")

        log = open("/tmp/xsqld-restart.log").read()
        rec("日志无 panic/abort", "panic" not in log.lower(), f"panic={'panic' in log.lower()}")
        return 0 if all(o for _, o, _ in results) else 1
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except Exception:
            proc.kill()
        json.dump([{"check": n, "ok": o, "evidence": str(e)} for n, o, e in results],
                  open("/tmp/t7-restart-results.json", "w"), ensure_ascii=False, indent=2)


if __name__ == "__main__":
    sys.exit(main())