#!/usr/bin/env python3
"""T7 点名项：outbox 暴涨（百万行未 prune）下的排空正确性。

做法：先让 xsqld 建好捕获，然后**停掉 xsqld**，直连 MySQL 写入大量行——
触发器在 SeekDB 侧仍然生效，outbox 会堆积到百万级。再以**小 batch** 启动 xsqld，
检验：分多轮排空、精确 prune 不丢未应用行、最终逐值一致、水位推进、零错误。

这是对"批量上限会删掉未应用 outbox 行"那个修复的大规模压测。
"""
import os
import re
import signal
import subprocess
import sys
import time

import pymysql
import requests

BIN = os.path.abspath("./target/debug/xsqld")
USER = "http://127.0.0.1:18280"
ADMIN = "http://127.0.0.1:18281"
MY = dict(host="127.0.0.1", port=12981, user="root", password="", autocommit=True)
NS = "obx"
ROWS = int(os.environ.get("OUTBOX_ROWS", "1000000"))
BATCH = 5000
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


def start(extra=()):
    env = {k: v for k, v in os.environ.items()
           if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    env["RUST_LOG"] = "warn"
    p = subprocess.Popen(
        [BIN, "--mysql-host", "127.0.0.1", "--mysql-port", "12981", "--mysql-user", "root",
         "--http-listen-addr", "127.0.0.1:18280", "--admin-listen-addr", "127.0.0.1:18281",
         "--mirror-enabled", "--mirror-dir", "/tmp/xsqld-obx-mirrors",
         "--mirror-interval-ms", "200", "--mirror-batch", str(BATCH), *extra],
        stdout=open("/tmp/xsqld-obx.log", "w"), stderr=subprocess.STDOUT, env=env)
    t0 = time.time()
    while time.time() - t0 < 90:
        if p.poll() is not None:
            return p
        try:
            if requests.get(f"{USER}/health", timeout=2).status_code == 200:
                break
        except Exception:
            pass
        time.sleep(0.5)
    return p


def stop(p):
    if p.poll() is None:
        p.send_signal(signal.SIGTERM)
        try:
            p.wait(timeout=40)
        except subprocess.TimeoutExpired:
            p.kill()


def main():
    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], capture_output=True)
    time.sleep(3)
    mc = pymysql.connect(**MY)
    cu = mc.cursor()
    cu.execute(f"CREATE DATABASE IF NOT EXISTS {NS}")
    cu.execute(f"USE {NS}")
    cu.execute("SHOW TABLES")
    for (t,) in cu.fetchall():
        cu.execute(f"DROP TABLE IF EXISTS `{t}`")
    cu.execute("CREATE TABLE big(id BIGINT PRIMARY KEY, v VARCHAR(64), n INT)")
    p = start()
    try:
        requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=60)
        requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
            {"type": "execute", "stmt": {"sql": "SELECT 1"}}, {"type": "close"}]},
            headers={"Host": f"{NS}.db", "Content-Type": "application/json"}, timeout=60)
        t0 = time.time()
        while time.time() - t0 < 120:
            if metric("mirror_cycles_total") > 0:
                break
            time.sleep(1)
        cu.execute("SELECT COUNT(*) FROM xsqld_changelog")
        rec("捕获已安装（停机前 outbox 可写）", metric("mirror_cycles_total") > 0,
            f"cycles={metric('mirror_cycles_total')}")
        stop(p)

        digits = ("(SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4"
                  " UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9)")
        t0 = time.time()
        base = 0
        while base < ROWS:
            n = min(10000, ROWS - base)
            cu.execute(
                "INSERT INTO big(id,v,n) SELECT %s+k, MD5(%s+k), %s+k FROM "
                f"(SELECT a.n+10*b.n+100*c.n+1000*d.n+10000*e.n AS k FROM {digits} a,{digits} b,"
                f"{digits} c,{digits} d,{digits} e) s WHERE k<%s", (base, base, base, n))
            base += n
        cu.execute("SELECT COUNT(*) FROM big")
        src = cu.fetchone()[0]
        cu.execute("SELECT COUNT(*) FROM xsqld_changelog")
        backlog = cu.fetchone()[0]
        rec("停机期间真的堆出百万级 outbox（非空测）", backlog >= ROWS * 0.99,
            f"源 {src} 行，outbox={backlog}，写入耗时 {time.time()-t0:.1f}s")

        p = start()
        requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=60)
        requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
            {"type": "execute", "stmt": {"sql": "SELECT 1"}}, {"type": "close"}]},
            headers={"Host": f"{NS}.db", "Content-Type": "application/json"}, timeout=60)
        t0 = time.time()
        drained = False
        while time.time() - t0 < 1800:
            cu.execute("SELECT COUNT(*) FROM xsqld_changelog")
            ob = cu.fetchone()[0]
            if ob == 0:
                drained = True
                break
            time.sleep(5)
        rec("小 batch 下百万积压被完全排空", drained,
            f"outbox={ob}，耗时 {time.time()-t0:.1f}s，batch={BATCH}")
        rec("排空过程零同步错误", metric("mirror_errors_total") == 0,
            f"errors={metric('mirror_errors_total')}")
        rec("水位已推进", metric("mirror_watermark_scn") > 0,
            f"watermark={metric('mirror_watermark_scn')}")
        got = None
        t0 = time.time()
        while time.time() - t0 < 300:
            cu.execute("SELECT COUNT(*) FROM big")
            s = cu.fetchone()[0]
            r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
                {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) FROM big"}}, {"type": "close"}]},
                headers={"Host": f"{NS}.db", "Content-Type": "application/json"}, timeout=180)
            res = r.json()["results"][0]
            if res["type"] == "ok":
                got = int(res["response"]["result"]["rows"][0][0]["value"])
                if got == s:
                    break
            time.sleep(3)
        rec("最终逐值一致（无丢行）", got == src, f"镜像={got} 源={src}")
        cu.execute("SELECT SUM(n), MIN(id), MAX(id) FROM big")
        sv = cu.fetchone()
        r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
            {"type": "execute", "stmt": {"sql": "SELECT MIN(id), MAX(id) FROM big"}}, {"type": "close"}]},
            headers={"Host": f"{NS}.db", "Content-Type": "application/json"}, timeout=180)
        res = r.json()["results"][0]
        mv = [v["value"] for v in res["response"]["result"]["rows"][0]] if res["type"] == "ok" else None
        rec("边界值一致", mv is not None and str(mv[0]) == str(sv[1]) and str(mv[1]) == str(sv[2]),
            f"镜像={mv} 源MIN/MAX={sv[1]}/{sv[2]}")
        rec("无 panic", "panic" not in open("/tmp/xsqld-obx.log", errors="replace").read().lower(),
            "日志无 panic")
    finally:
        stop(p)
        cu.close()
        mc.close()
    bad = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results)-len(bad)}/{len(results)} PASS" + (f"，失败：{bad}" if bad else ""))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())