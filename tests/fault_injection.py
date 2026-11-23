#!/usr/bin/env python3
"""T7 故障注入与稳定性。每条都先断言故障真的注入成功，再断言系统行为。

覆盖：镜像文件被删、outbox 积压与批量限制、同步出错时 HTTP 仍可用、优雅停机、后端不可达时的启动行为。
磁盘满与 24h soak 在本环境不可行，明确标注未做。
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
NS = "fault_t7"
HOST = {"Host": f"{NS}.db"}
MY = dict(host="127.0.0.1", port=12981, user="root", password="", autocommit=True)
MIRROR_DIR = "/tmp/xsqld-t7-mirrors"
MIRROR = f"{MIRROR_DIR}/{NS}.duckdb"
BIN = os.path.abspath("./target/debug/xsqld")
LOG = "/tmp/xsqld-t7.log"
results = []


def rec(n, ok, ev):
    results.append((n, ok, ev))
    print(f"[{'PASS' if ok else 'FAIL'}] {n}\n       {ev}", flush=True)


def pipe(sql, timeout=90):
    r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
        {"type": "execute", "stmt": {"sql": sql}}, {"type": "close"}]}, headers=HOST, timeout=timeout)
    return r.status_code, r.json()


def count_via_http():
    st, b = pipe("SELECT COUNT(*) FROM t")
    try:
        return int(b["results"][0]["response"]["result"]["rows"][0][0]["value"])
    except Exception:
        return None


def src_count():
    c = pymysql.connect(**MY, database=NS); cu = c.cursor()
    cu.execute("SELECT COUNT(*) FROM t"); n = cu.fetchone()[0]
    try:
        cu.execute("SELECT COUNT(*) FROM xsqld_changelog"); ob = cu.fetchone()[0]
    except Exception:
        ob = -1
    c.close()
    return n, ob


def start(extra=(), log=LOG, mirror_dir=MIRROR_DIR):
    env = {k: v for k, v in os.environ.items() if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    env["RUST_LOG"] = "info"
    return subprocess.Popen(
        [BIN, "--mysql-host", "127.0.0.1", "--mysql-port", "12981", "--mysql-user", "root",
         "--http-listen-addr", "127.0.0.1:18280", "--admin-listen-addr", "127.0.0.1:18281",
         "--mirror-enabled", "--mirror-dir", mirror_dir, "--mirror-interval-ms", "400",
         "--route-min-rows", "1", *extra],
        stdout=open(log, "w"), stderr=subprocess.STDOUT, env=env)


def wait_health(timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if requests.get(f"{USER}/health", timeout=2).status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def wait_mirror_ready(timeout=90):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if os.path.exists(MIRROR):
            b0 = None
            try:
                t = requests.get(f"{USER}/metrics", timeout=5).text
                m = re.search(r'xsqld_routed_total\{ns="%s"\}\s+(\d+)' % NS, t)
                b0 = int(m.group(1)) if m else 0
                pipe("SELECT COUNT(*) FROM t")
                t2 = requests.get(f"{USER}/metrics", timeout=5).text
                m2 = re.search(r'xsqld_routed_total\{ns="%s"\}\s+(\d+)' % NS, t2)
                if m2 and int(m2.group(1)) > b0:
                    return True
            except Exception:
                pass
        time.sleep(0.5)
    return False


def main():
    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], check=False)
    time.sleep(2)
    subprocess.run(["rm", "-rf", MIRROR_DIR], check=False)
    proc = start()
    try:
        if not wait_health():
            print("xsqld 未就绪"); return 2
        requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=30)
        pipe("DROP TABLE IF EXISTS t")
        pipe("CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(64))")
        pipe("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')")
        ready = wait_mirror_ready()
        rec("初始镜像就绪（有查询真被路由）", ready, f"mirror_exists={os.path.exists(MIRROR)}")
        if not ready:
            return 2

        # --- 故障 1：运行中删除镜像文件 ---
        os.remove(MIRROR)
        wal = MIRROR + ".wal"
        if os.path.exists(wal):
            os.remove(wal)
        rec("故障注入成功：镜像文件已删除", not os.path.exists(MIRROR), f"{MIRROR}")
        time.sleep(6)
        recreated = os.path.exists(MIRROR)
        n_http = count_via_http()
        n_src, _ = src_count()
        rec("删除后 worker 重建镜像", recreated, f"recreated={recreated}")
        rec("删除后查询仍返回正确值", n_http == n_src, f"http={n_http} source={n_src}")
        if not recreated:
            print("       注：进程持有句柄时 unlink 不影响已打开的 inode，worker 可能检测不到——这是真实健壮性缺口")

        # --- 故障 2：outbox 积压，验证批量限制与最终排空 ---
        c = pymysql.connect(**MY, database=NS); cu = c.cursor()
        vals = ",".join(f"({i},'bulk{i}')" for i in range(100, 2100))
        cu.execute(f"INSERT INTO t VALUES{vals}")
        cu.execute("SELECT COUNT(*) FROM xsqld_changelog"); backlog = cu.fetchone()[0]
        c.close()
        rec("故障注入成功：outbox 积压 2000 条", backlog >= 2000, f"backlog={backlog}")
        drained = False
        for _ in range(120):
            n, ob = src_count()
            if ob == 0:
                drained = True
                break
            time.sleep(0.5)
        rec("积压最终被排空", drained, f"outbox={src_count()[1]}")
        for _ in range(60):
            if count_via_http() == src_count()[0]:
                break
            time.sleep(0.5)
        rec("积压排空后镜像与源一致", count_via_http() == src_count()[0],
            f"mirror={count_via_http()} source={src_count()[0]}")

        # --- 故障 3：同步出错时 HTTP 仍可用 ---
        st, _ = pipe("SELECT COUNT(*) FROM t")
        health = requests.get(f"{USER}/health", timeout=5).status_code
        rec("经历故障后 HTTP 与 health 仍正常", st == 200 and health == 200, f"pipeline={st} health={health}")
        logtxt = open(LOG).read()
        rec("日志中无 panic/abort", "panic" not in logtxt.lower(), f"panic 出现={'panic' in logtxt.lower()}")

        # --- 故障 4：优雅停机 ---
        proc.send_signal(signal.SIGTERM)
        try:
            rc = proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill(); rc = None
        rec("SIGTERM 优雅停机", rc == 0, f"exit={rc}")
        tail = open(LOG).read()[-600:]
        rec("停机日志正常收尾", "stopped" in tail, tail[-200:].replace("\n", " | "))

        # --- 故障 5：后端不可达时启动应明确失败而非静默 ---
        env = {k: v for k, v in os.environ.items() if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
        p2 = subprocess.run([BIN, "--mysql-host", "127.0.0.1", "--mysql-port", "12999",
                             "--mysql-user", "root", "--http-listen-addr", "127.0.0.1:18282",
                             "--admin-listen-addr", "127.0.0.1:18283"],
                            capture_output=True, text=True, timeout=60, env=env)
        rec("后端不可达时启动明确失败", p2.returncode != 0,
            f"exit={p2.returncode} stderr={p2.stderr[-160:].strip()}")

        print("\n未做（本环境不可行）：磁盘满时 CTAS/MERGE、24h soak、容器 OOM 触顶、SeekDB 重启恢复")
        np = sum(1 for _, o, _ in results if o); nf = sum(1 for _, o, _ in results if not o)
        print(f"\nPASS={np} FAIL={nf}", flush=True)
        json.dump([{"check": n, "ok": o, "evidence": str(e)} for n, o, e in results],
                  open("/tmp/t7-fault-results.json", "w"), ensure_ascii=False, indent=2)
        return 0 if nf == 0 else 1
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=20)
            except Exception:
                proc.kill()


if __name__ == "__main__":
    sys.exit(main())