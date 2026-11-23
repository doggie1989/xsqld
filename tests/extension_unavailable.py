#!/usr/bin/env python3
"""T7 补充：DuckDB mysql 扩展不可用时的降级行为。

模拟方式：把 xsqld 的 HOME 指向一个只读目录，DuckDB 无法把扩展写进
~/.duckdb/extensions，INSTALL 因此失败。检验：
  - 镜像打开失败被明确上报（mirror_errors_total 或日志），不静默
  - **HTTP 与行存路径完全不受影响**（这是降级的基本要求）
  - 不 panic、不崩溃
"""
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time

import pymysql
import requests

USER = "http://127.0.0.1:18280"
ADMIN = "http://127.0.0.1:18281"
NS = "noext_t7"
HOST = {"Host": f"{NS}.db"}
MY = dict(host="127.0.0.1", port=12981, user="root", password="", autocommit=True)
BIN = os.path.abspath("./target/debug/xsqld")
results = []


def rec(n, ok, ev):
    results.append((n, ok, ev))
    print(f"[{'PASS' if ok else 'FAIL'}] {n}\n       {ev}", flush=True)


def main():
    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], check=False)
    time.sleep(2)
    home = tempfile.mkdtemp(prefix="xsqld-noext-home-")
    os.chmod(home, 0o555)  # 只读：DuckDB 无法写入扩展目录
    mirror_dir = "/tmp/xsqld-noext"
    subprocess.run(["rm", "-rf", mirror_dir], check=False)
    env = {k: v for k, v in os.environ.items() if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    env["RUST_LOG"] = "info"
    env["HOME"] = home
    log = "/tmp/xsqld-noext.log"
    proc = subprocess.Popen(
        [BIN, "--mysql-host", "127.0.0.1", "--mysql-port", "12981", "--mysql-user", "root",
         "--http-listen-addr", "127.0.0.1:18280", "--admin-listen-addr", "127.0.0.1:18281",
         "--mirror-enabled", "--mirror-dir", mirror_dir, "--mirror-interval-ms", "500",
         "--route-min-rows", "1"],
        stdout=open(log, "w"), stderr=subprocess.STDOUT, env=env)
    try:
        healthy = False
        for _ in range(120):
            try:
                if requests.get(f"{USER}/health", timeout=2).status_code == 200:
                    healthy = True
                    break
            except Exception:
                pass
            time.sleep(0.5)
        rec("扩展不可用时 xsqld 仍能启动并提供 HTTP", healthy, f"health={healthy}")
        if not healthy:
            return 1
        requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=30)
        r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
            {"type": "execute", "stmt": {"sql": "CREATE TABLE t(id INT PRIMARY KEY, v INT)"}},
            {"type": "close"}]}, headers=HOST, timeout=60)
        rec("行存 DDL 正常", r.status_code == 200, f"status={r.status_code}")
        r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
            {"type": "execute", "stmt": {"sql": "INSERT INTO t VALUES(1,10),(2,20)"}},
            {"type": "close"}]}, headers=HOST, timeout=60)
        rec("行存写入正常", r.status_code == 200, f"status={r.status_code}")
        time.sleep(8)
        r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
            {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) FROM t"}}, {"type": "close"}]},
            headers=HOST, timeout=60)
        j = r.json()
        res = j["results"][0]
        val = res["response"]["result"]["rows"][0][0]["value"] if res.get("type") == "ok" else None
        rec("行存查询返回正确值（降级不影响正确性）", val == "2", f"value={val}")
        c = pymysql.connect(**MY, database=NS); cu = c.cursor()
        cu.execute("SELECT COUNT(*) FROM t"); n = cu.fetchone()[0]; c.close()
        rec("与直连 SeekDB 结果一致", str(n) == val, f"direct={n} http={val}")

        txt = open(log).read()
        failed = ("mirror open failed" in txt) or ("INSTALL mysql" in txt) or ("extension" in txt.lower())
        rec("镜像不可用被明确上报（不静默）", failed,
            f"日志含镜像失败/扩展相关记录={failed}")
        rec("进程存活且无 panic", proc.poll() is None and "panic" not in txt.lower(),
            f"exit={proc.poll()} panic={'panic' in txt.lower()}")
        sample = [l for l in txt.splitlines() if "mirror" in l.lower() or "extension" in l.lower()]
        print("  日志样本:", (sample[-1][:200] if sample else "(无)"), flush=True)
        np = sum(1 for _, o, _ in results if o); nf = sum(1 for _, o, _ in results if not o)
        print(f"\nPASS={np} FAIL={nf}", flush=True)
        json.dump([{"check": a, "ok": b, "evidence": str(d)} for a, b, d in results],
                  open("/tmp/t7-noext-results.json", "w"), ensure_ascii=False, indent=2)
        return 0 if nf == 0 else 1
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except Exception:
            proc.kill()
        os.chmod(home, 0o755)
        subprocess.run(["rm", "-rf", home], check=False)


if __name__ == "__main__":
    sys.exit(main())