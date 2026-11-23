#!/usr/bin/env python3
"""T6 点名项：连接数 1/8/64/256 下的吞吐与延迟。

混合两种形状：point_pk（等价闸/收益闸拒绝，走行存）与 agg_count（被路由到列存）。
断言：零错误、routed_total 真增长（非空测）、p99 有界、并发提升时吞吐不塌陷。
"""
import os
import re
import signal
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests

BIN = os.path.abspath("./target/debug/xsqld")
USER = "http://127.0.0.1:18280"
ADMIN = "http://127.0.0.1:18281"
NS = "cg"
POINT = "SELECT id,uid,amt,status,s FROM row_t WHERE id=5000000"
AGG = "SELECT COUNT(*) FROM row_t WHERE status=2"
LEVELS = [1, 8, 64, 256]
PER_LEVEL = 240
results = []


def rec(n, ok, ev):
    results.append((n, ok, ev))
    print(f"[{'PASS' if ok else 'FAIL'}] {n}\n       {ev}", flush=True)


def metric(name):
    t = requests.get(f"{USER}/metrics", timeout=10).text
    m = re.search(r'xsqld_%s\{ns="%s"\}\s+(\d+)' % (name, NS), t)
    return int(m.group(1)) if m else 0


def one(sql):
    t0 = time.perf_counter()
    try:
        r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
            {"type": "execute", "stmt": {"sql": sql}}, {"type": "close"}]},
            headers={"Host": f"{NS}.db", "Content-Type": "application/json"}, timeout=120)
        el = (time.perf_counter() - t0) * 1000
        if r.status_code != 200:
            return el, False
        res = r.json()["results"][0]
        return el, res["type"] == "ok"
    except Exception:
        return (time.perf_counter() - t0) * 1000, False


def main():
    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], capture_output=True)
    time.sleep(3)
    env = {k: v for k, v in os.environ.items()
           if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    env["RUST_LOG"] = "warn"
    p = subprocess.Popen(
        [BIN, "--mysql-host", "127.0.0.1", "--mysql-port", "12981", "--mysql-user", "root",
         "--http-listen-addr", "127.0.0.1:18280", "--admin-listen-addr", "127.0.0.1:18281",
         "--mirror-enabled", "--mirror-dir", "/tmp/xsqld-conc-mirrors",
         "--mirror-interval-ms", "1000", "--route-min-rows", "0",
         "--pool-max", "256", "--max-concurrent-queries", "256"],
        stdout=open("/tmp/xsqld-conc.log", "w"), stderr=subprocess.STDOUT, env=env)
    try:
        t0 = time.time()
        while time.time() - t0 < 90:
            try:
                if requests.get(f"{USER}/health", timeout=2).status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(0.5)
        requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=60)
        one("SELECT 1")
        t0 = time.time()
        while time.time() - t0 < 300:
            if metric("mirror_tables") >= 1:
                break
            time.sleep(2)
        rec("镜像真的建成（非空测）", metric("mirror_tables") >= 1,
            f"mirror_tables={metric('mirror_tables')}")
        r0 = metric("routed_total")
        one(AGG)
        rec("分析形状确实被路由（非空测）", metric("routed_total") > r0,
            f"routed_total {r0} -> {metric('routed_total')}")

        summary = {}
        for c in LEVELS:
            sqls = [POINT if i % 2 == 0 else AGG for i in range(PER_LEVEL)]
            t0 = time.perf_counter()
            with ThreadPoolExecutor(max_workers=c) as ex:
                out = list(ex.map(one, sqls))
            wall = time.perf_counter() - t0
            lat = sorted(e for e, _ in out)
            errs = sum(1 for _, ok in out if not ok)
            p50 = lat[len(lat) // 2]
            p99 = lat[int(len(lat) * 0.99)]
            tps = PER_LEVEL / wall
            summary[c] = (tps, p50, p99, errs)
            rec(f"并发 {c}：零错误且 p99 有界", errs == 0 and p99 < 10_000,
                f"tps={tps:.1f} p50={p50:.2f}ms p99={p99:.2f}ms errors={errs}")
        rec("并发 256 吞吐不低于并发 1（不塌陷）",
            summary[256][0] >= summary[1][0],
            f"tps 1={summary[1][0]:.1f} 8={summary[8][0]:.1f} 64={summary[64][0]:.1f} 256={summary[256][0]:.1f}")
        rec("全程无 panic", "panic" not in open("/tmp/xsqld-conc.log", errors="replace").read().lower(),
            "日志无 panic")
    finally:
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