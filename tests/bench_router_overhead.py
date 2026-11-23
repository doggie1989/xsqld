#!/usr/bin/env python3
"""验证项 5 的端到端口径：镜像开/关两台 xsqld **同时在线、逐请求交替**测延迟退化。

为什么不用按轮交替：亚毫秒点查的轮间摆动达 ±30~60%，5% 判据低于噪声底。
细粒度交错（每对请求交换先后）才能抵消宿主机漂移。

同时断言这些形状确实未被路由（routed_total 不增），否则测的不是"拒绝后的开销"。
判据：点查与宽结果退化 ≤5%；并报告 IQR 以说明噪声量级。
"""
import os
import re
import signal
import statistics
import subprocess
import sys
import time

import requests

BIN = os.path.abspath("./target/debug/xsqld")
NS = "cg"
N = 120
QUERIES = {
    "point_pk": "SELECT id,uid,amt,status,s FROM row_t WHERE id=5000000",
    "point_range_100": "SELECT id,uid,amt,status,s FROM row_t WHERE id BETWEEN 500000 AND 500099",
    "wide_5000": "SELECT id,uid,amt,status,s FROM row_t WHERE id BETWEEN 2000000 AND 2004999",
}
results = []


def rec(n, ok, ev):
    results.append((n, ok, ev))
    print(f"[{'PASS' if ok else 'FAIL'}] {n}\n       {ev}", flush=True)


def metric(base, name):
    t = requests.get(f"{base}/metrics", timeout=10).text
    m = re.search(r'xsqld_%s\{ns="%s"\}\s+(\d+)' % (name, NS), t)
    return int(m.group(1)) if m else 0


def q(base, sql):
    t0 = time.perf_counter()
    r = requests.post(f"{base}/v2/pipeline", json={"baton": None, "requests": [
        {"type": "execute", "stmt": {"sql": sql}}, {"type": "close"}]},
        headers={"Host": f"{NS}.db", "Content-Type": "application/json"}, timeout=180)
    el = (time.perf_counter() - t0) * 1000
    r.raise_for_status()
    res = r.json()["results"][0]
    if res["type"] != "ok":
        raise RuntimeError(res)
    return el, len(res["response"]["result"]["rows"])


def start(mirror, port):
    env = {k: v for k, v in os.environ.items()
           if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    env["RUST_LOG"] = "warn"
    cmd = [BIN, "--mysql-host", "127.0.0.1", "--mysql-port", "12981", "--mysql-user", "root",
           "--http-listen-addr", f"127.0.0.1:{port}", "--admin-listen-addr", f"127.0.0.1:{port+1}"]
    if mirror:
        cmd += ["--mirror-enabled", "--mirror-dir", f"/tmp/xsqld-ab-mirrors-{port}",
                "--mirror-interval-ms", os.environ.get("MIRROR_INTERVAL", "400"),
                "--route-min-rows", "0"]
    p = subprocess.Popen(cmd, stdout=open(f"/tmp/xsqld-ab-{port}.log", "w"),
                         stderr=subprocess.STDOUT, env=env)
    base = f"http://127.0.0.1:{port}"
    t0 = time.time()
    while time.time() - t0 < 90:
        try:
            if requests.get(f"{base}/health", timeout=2).status_code == 200:
                break
        except Exception:
            pass
        time.sleep(0.5)
    requests.post(f"http://127.0.0.1:{port+1}/v1/namespaces/{NS}/create", json={}, timeout=60)
    q(base, "SELECT 1")
    if mirror:
        t0 = time.time()
        while time.time() - t0 < 300:
            if metric(base, "mirror_tables") >= 1:
                break
            time.sleep(2)
    return p, base


def iqr(v):
    s = sorted(v)
    return s[len(s) // 4], s[3 * len(s) // 4]


def main():
    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], capture_output=True)
    time.sleep(3)
    procs = []
    try:
        poff, off = start(False, 18280)
        procs.append(poff)
        pon, on = start(True, 18282)
        procs.append(pon)
        rec("镜像真的建成（非空测）", metric(on, "mirror_tables") >= 1,
            f"mirror_tables={metric(on, 'mirror_tables')}")
        for name, sql in QUERIES.items():
            for _ in range(10):
                q(off, sql)
                q(on, sql)
            a, b, rows = [], [], None
            for i in range(N):
                if i % 2 == 0:
                    e1, n1 = q(off, sql)
                    e2, n2 = q(on, sql)
                else:
                    e2, n2 = q(on, sql)
                    e1, n1 = q(off, sql)
                a.append(e1)
                b.append(e2)
                rows = (n1, n2)
            ma, mb = statistics.median(a), statistics.median(b)
            deg = (mb - ma) / ma * 100
            qa, qb = iqr(a), iqr(b)
            rec(f"{name} 端到端退化 ≤5%", deg <= 5.0 and rows[0] == rows[1],
                f"关={ma:.3f}ms 开={mb:.3f}ms 退化={deg:+.1f}% "
                f"IQR关=[{qa[0]:.3f},{qa[1]:.3f}] IQR开=[{qb[0]:.3f},{qb[1]:.3f}] 行数{rows[0]}={rows[1]}")
        before = metric(on, "routed_total")
        for sql in QUERIES.values():
            q(on, sql)
        rec("这些形状确实未被路由（否则测的不是拒绝开销）",
            metric(on, "routed_total") == before,
            f"routed_total {before} -> {metric(on, 'routed_total')}")
    finally:
        for p in procs:
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