#!/usr/bin/env python3
"""强制触发 apply 路径的 schema 竞态，验证有界重试真的被走到。

自然 soak 里该竞态约 178 次 DDL 才 1 次，短跑观测不到 ⇒ "errors=0" 是弱证据。
本脚本把窗口放大：同步周期压到配置下限 50ms、持续写入使每周期都有变更、
高频 ADD/DROP COLUMN。判据：**重试警告必须出现 >0 次**（非空测），且最终零错误、逐值一致。
"""
import os
import re
import signal
import subprocess
import sys
import threading
import time

import pymysql
import requests

BIN = os.path.abspath("./target/debug/xsqld")
USER = "http://127.0.0.1:18280"
ADMIN = "http://127.0.0.1:18281"
MY = dict(host="127.0.0.1", port=12981, user="root", password="", autocommit=True)
NS = "race"
LOG = "/tmp/xsqld-race.log"
DUR = int(os.environ.get("RACE_SECONDS", "180"))
results = []


def rec(n, ok, ev):
    results.append((n, ok, ev))
    print(f"[{'PASS' if ok else 'FAIL'}] {n}\n       {ev}", flush=True)


def metric(name):
    try:
        t = requests.get(f"{USER}/metrics", timeout=10).text
    except Exception:
        return -1
    m = re.search(r'xsqld_%s\{ns="%s"\}\s+(\d+)' % (name, NS), t)
    return int(m.group(1)) if m else 0


def main():
    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], capture_output=True)
    time.sleep(3)
    env = {k: v for k, v in os.environ.items()
           if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    env["RUST_LOG"] = "warn"
    p = subprocess.Popen(
        [BIN, "--mysql-host", "127.0.0.1", "--mysql-port", "12981", "--mysql-user", "root",
         "--http-listen-addr", "127.0.0.1:18280", "--admin-listen-addr", "127.0.0.1:18281",
         "--mirror-enabled", "--mirror-dir", "/tmp/xsqld-race-mirrors",
         "--mirror-interval-ms", "50", "--route-min-rows", "0"],
        stdout=open(LOG, "w"), stderr=subprocess.STDOUT, env=env)
    stop = threading.Event()
    counts = {"writes": 0, "ddl": 0, "ddl_err": 0}
    try:
        t0 = time.time()
        while time.time() - t0 < 90:
            try:
                if requests.get(f"{USER}/health", timeout=2).status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(0.5)
        mc = pymysql.connect(**MY)
        cu = mc.cursor()
        cu.execute(f"CREATE DATABASE IF NOT EXISTS {NS}")
        cu.execute(f"USE {NS}")
        cu.execute("SHOW TABLES")
        for (t,) in cu.fetchall():
            cu.execute(f"DROP TABLE IF EXISTS `{t}`")
        cu.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        cu.execute("INSERT INTO t VALUES(1,0),(2,0),(3,0),(4,0),(5,0)")
        requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=60)
        requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
            {"type": "execute", "stmt": {"sql": "SELECT 1"}}, {"type": "close"}]},
            headers={"Host": f"{NS}.db", "Content-Type": "application/json"}, timeout=60)
        t0 = time.time()
        while time.time() - t0 < 120:
            if metric("mirror_cycles_total") > 0:
                break
            time.sleep(0.5)

        def writer():
            c = pymysql.connect(**MY, database=NS)
            k = c.cursor()
            i = 0
            while not stop.is_set():
                i += 1
                try:
                    k.execute(f"UPDATE t SET v={i} WHERE id BETWEEN 1 AND 5")
                    counts["writes"] += 1
                except Exception:
                    pass
                time.sleep(0.01)
            c.close()

        def ddler():
            c = pymysql.connect(**MY, database=NS)
            k = c.cursor()
            have = False
            while not stop.is_set():
                sql = "ALTER TABLE t DROP COLUMN extra" if have else "ALTER TABLE t ADD COLUMN extra INT"
                for _ in range(6):
                    try:
                        k.execute(sql)
                        have = not have
                        counts["ddl"] += 1
                        break
                    except pymysql.err.OperationalError as e:
                        if e.args[0] in (4179, 1064, 1054, 1060):
                            time.sleep(0.05)
                            continue
                        counts["ddl_err"] += 1
                        break
                    except Exception:
                        counts["ddl_err"] += 1
                        break
                time.sleep(0.12)
            c.close()

        ths = [threading.Thread(target=writer), threading.Thread(target=ddler)]
        for t in ths:
            t.start()
        time.sleep(DUR)
        stop.set()
        for t in ths:
            t.join(timeout=30)

        log = open(LOG, errors="replace").read()
        retries = log.count("schema changed mid-apply")
        rec("重试分支真的被触发（非空测，本脚本的核心目的）", retries > 0,
            f"重试警告 {retries} 次，DDL {counts['ddl']} 次，写入 {counts['writes']} 批")
        rec("放大竞态后仍零同步错误", metric("mirror_errors_total") == 0,
            f"errors={metric('mirror_errors_total')}")
        rec("同步周期持续推进", metric("mirror_cycles_total") > 100,
            f"cycles={metric('mirror_cycles_total')}")
        cu.execute("SELECT COUNT(*), SUM(v) FROM t")
        sv = cu.fetchone()
        got = None
        t0 = time.time()
        while time.time() - t0 < 120:
            r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
                {"type": "execute", "stmt": {"sql": "SELECT COUNT(*), MIN(id), MAX(id) FROM t"}},
                {"type": "close"}]}, headers={"Host": f"{NS}.db", "Content-Type": "application/json"}, timeout=60)
            res = r.json()["results"][0]
            if res["type"] == "ok":
                got = [v["value"] for v in res["response"]["result"]["rows"][0]]
                if str(got[0]) == str(sv[0]):
                    break
            time.sleep(2)
        cu.execute("SELECT COUNT(*), MIN(id), MAX(id) FROM t")
        sv2 = cu.fetchone()
        rec("最终逐值一致（重试未丢数据）",
            got is not None and str(got[0]) == str(sv2[0]) and str(got[1]) == str(sv2[1]) and str(got[2]) == str(sv2[2]),
            f"镜像={got} 源={list(sv2)}")
        rec("无 panic", "panic" not in log.lower(), "日志无 panic")
        cu.close()
        mc.close()
    finally:
        stop.set()
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
            try:
                p.wait(timeout=40)
            except subprocess.TimeoutExpired:
                p.kill()
    bad = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results)-len(bad)}/{len(results)} PASS" + (f"，失败：{bad}" if bad else ""))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())