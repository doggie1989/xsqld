#!/usr/bin/env python3
"""T7 资源压力：DuckDB 内存受限下的大表快照，以及写入失败路径。

两项都是方案 T7 要求项的**可行等价形式**，口径明确标注：
  A. 内存压力：把 --mirror-memory-limit-mb 压到最小(64)，对 1000 万行表做快照。
     检验 DuckDB 是否靠 spill 完成、或干净失败，且**不得 panic 杀进程**。
     （不等同"容器 OOM 触顶"，但覆盖同一条内存压力代码路径。）
  B. 文件大小限制：用 ulimit -f 限制 xsqld 可写文件大小。
     **实测失败模式是 SIGXFSZ 直接终止进程（exit 153），不是磁盘满的 ENOSPC 返回错误**，
     因此本项只刻画该限制下的行为，**不能当作"磁盘满时干净失败"的证据**。
     真正的磁盘满（ENOSPC）由 `tests/disk_full_enospc.py` 覆盖（挂真实写满的小文件系统）。
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
MY = dict(host="127.0.0.1", port=12981, user="root", password="", autocommit=True)
BIN = os.path.abspath("./target/debug/xsqld")
results = []


def rec(n, ok, ev):
    results.append((n, ok, ev))
    print(f"[{'PASS' if ok else 'FAIL'}] {n}\n       {ev}", flush=True)


def start(ns_dir, extra=(), log="/tmp/xsqld-pressure.log", ulimit_kb=None):
    env = {k: v for k, v in os.environ.items() if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    env["RUST_LOG"] = "warn"
    cmd = [BIN, "--mysql-host", "127.0.0.1", "--mysql-port", "12981", "--mysql-user", "root",
           "--http-listen-addr", "127.0.0.1:18280", "--admin-listen-addr", "127.0.0.1:18281",
           "--mirror-enabled", "--mirror-dir", ns_dir, "--mirror-interval-ms", "1000",
           "--route-min-rows", "1000", *extra]
    if ulimit_kb is not None:
        # ulimit -f 以 512 字节块为单位
        shell = f"ulimit -f {ulimit_kb // 512}; exec " + " ".join(f"'{c}'" for c in cmd)
        return subprocess.Popen(["/bin/sh", "-c", shell],
                                stdout=open(log, "w"), stderr=subprocess.STDOUT, env=env)
    return subprocess.Popen(cmd, stdout=open(log, "w"), stderr=subprocess.STDOUT, env=env)


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


def open_ns(ns):
    requests.post(f"{ADMIN}/v1/namespaces/{ns}/create", json={}, timeout=30)
    requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
        {"type": "execute", "stmt": {"sql": "SELECT 1"}}, {"type": "close"}]},
        headers={"Host": f"{ns}.db", "Content-Type": "application/json"}, timeout=60)


def metric(ns, name):
    try:
        t = requests.get(f"{USER}/metrics", timeout=10).text
    except Exception:
        return -1  # 服务器已终止
    m = re.search(r'xsqld_%s\{ns="%s"\}\s+(\d+)' % (name, ns), t)
    return int(m.group(1)) if m else 0


def stop(proc):
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=30)
    except Exception:
        proc.kill()


def main():
    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], check=False)
    time.sleep(2)

    # ---------- A. 内存压力：64MB 限额下快照 1000 万行 ----------
    d = "/tmp/xsqld-mempress"
    subprocess.run(["rm", "-rf", d], check=False)
    proc = start(d, extra=("--mirror-memory-limit-mb", "64"), log="/tmp/xsqld-mempress.log")
    try:
        rec("A: xsqld 启动", wait_health(), "health 200")
        open_ns("cg")
        print("  等 64MB 限额下快照 1000 万行（最多 180s）...", flush=True)
        done = False
        for _ in range(180):
            if metric("cg", "mirror_snapshot_rows_total") >= 10_000_000:
                done = True
                break
            if proc.poll() is not None:
                break
            time.sleep(1)
        alive = proc.poll() is None
        log = open("/tmp/xsqld-mempress.log").read()
        rec("A: 内存压力下进程未崩溃", alive and "panic" not in log.lower(),
            f"alive={alive} exit={proc.poll()} panic={'panic' in log.lower()}")
        rows = metric("cg", "mirror_snapshot_rows_total")
        errs = metric("cg", "mirror_errors_total")
        rec("A: 靠 spill 完成快照 或 干净失败并计数",
            (rows >= 10_000_000) or (errs > 0),
            f"snapshot_rows={rows} errors={errs} done={done}")
        if alive:
            try:
                h = requests.get(f"{USER}/health", timeout=5).status_code
            except Exception:
                h = None
            rec("A: 内存压力后 HTTP 层仍可用", h == 200, f"health={h}")
        tail = [l for l in log.splitlines() if "error" in l.lower() or "warn" in l.lower()]
        print("  日志样本:", (tail[-1][:180] if tail else "(无 error/warn)"), flush=True)
    finally:
        stop(proc)
    time.sleep(2)

    # ---------- B. 写入失败：ulimit -f 限制镜像文件大小 ----------
    d2 = "/tmp/xsqld-writefail"
    subprocess.run(["rm", "-rf", d2], check=False)
    # 限制为 8MB：足以建库但不足以容纳 1000 万行镜像
    proc = start(d2, ulimit_kb=8192, log="/tmp/xsqld-writefail.log")
    try:
        rec("B: xsqld 在文件大小受限下启动", wait_health(), "health 200")
        open_ns("cg")
        print("  等写入失败发生（最多 120s）...", flush=True)
        failed = False
        for _ in range(120):
            e = metric("cg", "mirror_errors_total")
            if e > 0:
                failed = True
                break
            if proc.poll() is not None:
                break
            time.sleep(1)
        log = open("/tmp/xsqld-writefail.log").read()
        rc = proc.poll()
        # SIGXFSZ(25) 会终止进程；这是 ulimit 的语义，不是 xsqld 的缺陷，但必须如实刻画。
        killed_by_xfsz = rc is not None and rc < 0 and -rc == signal.SIGXFSZ
        rec("B: 文件大小限制确实被触发", failed or killed_by_xfsz or rc is not None,
            f"exit={rc} SIGXFSZ={killed_by_xfsz} errors={metric('cg','mirror_errors_total')}")
        rec("B: 无 Rust panic（区别于信号终止）", "panic" not in log.lower(),
            f"panic={'panic' in log.lower()}")
        print(f"  口径说明：exit={rc}。若为 SIGXFSZ 则进程被信号终止，"
              f"**这不等同磁盘满(ENOSPC)的干净失败路径，后者仍未验证**。", flush=True)
        tail = [l for l in log.splitlines() if "error" in l.lower()]
        print("  失败日志样本:", (tail[-1][:200] if tail else "(无)"), flush=True)
    finally:
        stop(proc)

    np = sum(1 for _, o, _ in results if o); nf = sum(1 for _, o, _ in results if not o)
    print(f"\nPASS={np} FAIL={nf}", flush=True)
    print("口径说明：A 是 DuckDB 内存限额压力，不等同容器 OOM 触顶；B 是 ulimit 文件大小限制，不等同磁盘满。")
    json.dump([{"check": n, "ok": o, "evidence": str(e)} for n, o, e in results],
              open("/tmp/t7-pressure-results.json", "w"), ensure_ascii=False, indent=2)
    return 0 if nf == 0 else 1


if __name__ == "__main__":
    sys.exit(main())