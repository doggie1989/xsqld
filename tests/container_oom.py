#!/usr/bin/env python3
"""T7 容器 OOM 触顶：cgroup 内存上限下镜像快照被 OOM kill 后的行为与恢复。

与 `resource_pressure.py` A 段的区别（后者不能冒充本项）：
  A 段把 DuckDB 自己的 memory_limit 压到 64MB，DuckDB **知道自己内存少**于是 spill，进程存活；
  本项让 DuckDB 以为有 512MB 而 **cgroup 只给 200MB** —— 这是生产里最常见的误配，
  内核直接 OOM kill，进程没有任何机会清理。

判据：内存上限真的生效（非空测）、确实被 OOM kill（OOMKilled/137）、
      **SeekDB 行存真相源不受影响**、**重启后能从被写坏的镜像文件恢复并与源逐值一致**。

需要 Linux 版 xsqld。用 --build 让脚本在 Docker 里构建（产物在仓库外）。
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time

import pymysql
import requests

MY = dict(host="127.0.0.1", port=12981, user="root", password="", autocommit=True)
SEEKDB_CONTAINER = "xsqld-htap-bench"
NAME = "xsqld-oom-test"
USER = "http://127.0.0.1:18290"
ADMIN = "http://127.0.0.1:18291"
NS = "cg"
LINUX_TARGET = "/tmp/xsqld-linux-target"
MIRRORS = "/tmp/xsqld-oom-mirrors"
results = []


def rec(n, ok, ev):
    results.append((n, ok, ev))
    print(f"[{'PASS' if ok else 'FAIL'}] {n}\n       {ev}", flush=True)


def sh(*args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def build_linux():
    os.makedirs(LINUX_TARGET, exist_ok=True)
    os.makedirs("/tmp/xsqld-cargo-reg", exist_ok=True)
    sh("docker", "rm", "-f", "xsqld-linux-build")
    r = sh("docker", "run", "-d", "--name", "xsqld-linux-build",
           "-v", f"{os.getcwd()}:/w", "-v", f"{LINUX_TARGET}:/target",
           "-v", "/tmp/xsqld-cargo-reg:/usr/local/cargo/registry", "-w", "/w",
           "rust:1.99.1-bookworm", "bash", "-c",
           "apt-get update -qq >/dev/null 2>&1 && apt-get install -y -qq cmake git >/dev/null 2>&1 "
           "&& cargo build --target-dir /target 2>&1; echo BUILD_EXIT=$?")
    if r.returncode != 0:
        raise RuntimeError(r.stderr)
    t0 = time.time()
    while time.time() - t0 < 3600:
        logs = sh("docker", "logs", "xsqld-linux-build").stdout
        if "BUILD_EXIT=" in logs:
            ok = "BUILD_EXIT=0" in logs
            print(logs[-400:], flush=True)
            if not ok:
                raise RuntimeError("linux build failed")
            return
        time.sleep(10)
    raise RuntimeError("linux build timeout")


def seekdb_ip():
    r = sh("docker", "inspect", SEEKDB_CONTAINER, "--format",
           "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}")
    ip = r.stdout.strip()
    if not ip:
        raise RuntimeError(f"cannot resolve {SEEKDB_CONTAINER} IP: {r.stderr}")
    return ip


def run_container(ip, mem_mb, duck_mb, extra=()):
    sh("docker", "rm", "-f", NAME)
    os.makedirs(MIRRORS, exist_ok=True)
    cmd = ["docker", "run", "-d", "--name", NAME,
           "--memory", f"{mem_mb}m", "--memory-swap", f"{mem_mb}m",
           "-p", "18290:18290", "-p", "18291:18291",
           "-v", f"{LINUX_TARGET}/debug/xsqld:/xsqld:ro", "-v", f"{MIRRORS}:/mirrors",
           "--entrypoint", "/xsqld", "rust:1.99.1-bookworm",
           "--mysql-host", ip, "--mysql-port", "2881", "--mysql-user", "root",
           "--http-listen-addr", "0.0.0.0:18290", "--admin-listen-addr", "0.0.0.0:18291",
           "--allow-non-loopback", "--allowed-host-suffixes", "*",
           "--mirror-enabled", "--mirror-dir", "/mirrors", "--mirror-interval-ms", "1000",
           "--mirror-memory-limit-mb", str(duck_mb), *extra]
    r = sh(*cmd)
    if r.returncode != 0:
        raise RuntimeError(r.stderr)
    return r.stdout.strip()


def state():
    r = sh("docker", "inspect", NAME, "--format",
           "{{.State.Status}} {{.State.OOMKilled}} {{.State.ExitCode}} {{.HostConfig.Memory}}")
    p = r.stdout.split()
    if len(p) < 4:
        return None
    return dict(status=p[0], oom=p[1] == "true", exit=int(p[2]), mem=int(p[3]))


def wait_health(timeout=120):
    t0 = time.time()
    while time.time() - t0 < timeout:
        st = state()
        if st and st["status"] != "running":
            return False
        try:
            if requests.get(f"{USER}/health", timeout=2).status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def metric(name, ns=NS):
    try:
        t = requests.get(f"{USER}/metrics", timeout=10).text
    except Exception:
        return -1
    m = re.search(r'xsqld_%s\{ns="%s"\}\s+(\d+)' % (name, ns), t)
    return int(m.group(1)) if m else 0


def routed_total():
    try:
        t = requests.get(f"{USER}/metrics", timeout=10).text
    except Exception:
        return -1
    m = re.search(r'xsqld_routed_total\{ns="%s"\}\s+(\d+)' % NS, t)
    return int(m.group(1)) if m else 0


def mirror_q(sql, timeout=300):
    """只返回真被路由到镜像的读数；回退到 SeekDB 时返回 None（拒绝空测）。"""
    before = routed_total()
    rows = q(sql, timeout)
    if routed_total() <= before:
        return None
    return rows


def q(sql, timeout=300):
    r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
        {"type": "execute", "stmt": {"sql": sql}}, {"type": "close"}]},
        headers={"Host": f"{NS}.db", "Content-Type": "application/json"}, timeout=timeout)
    r.raise_for_status()
    res = r.json()["results"][0]
    if res["type"] != "ok":
        raise RuntimeError(res)
    return res["response"]["result"]["rows"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true", help="在 Docker 里构建 Linux 二进制")
    a = ap.parse_args()
    if a.build:
        build_linux()
    binary = f"{LINUX_TARGET}/debug/xsqld"
    if not os.path.exists(binary):
        print("SKIP: 缺 Linux 二进制，用 --build 构建")
        return 0

    mc = pymysql.connect(**MY)
    cur = mc.cursor()
    cur.execute("SELECT COUNT(*), SUM(amt) FROM cg.row_t")
    src_count, src_sum = cur.fetchone()
    src_sum = int(src_sum)
    ip = seekdb_ip()
    print(f"源 cg.row_t: {src_count} 行 SUM(amt)={src_sum}；seekdb ip={ip}", flush=True)

    try:
        # 阶段一：DuckDB 以为有 512MB，cgroup 只给 200MB
        run_container(ip, mem_mb=200, duck_mb=512)
        st = state()
        rec("cgroup 内存上限真的生效（非空测）", st and st["mem"] == 200 * 1024 * 1024,
            f"HostConfig.Memory={st['mem'] if st else None}")
        rec("容器内 xsqld 能启动并服务", wait_health(), f"state={state()}")
        requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=60)
        q("SELECT 1")  # 打开命名空间池，否则 mirror worker 无表可镜像

        killed = False
        t0 = time.time()
        while time.time() - t0 < 600:
            st = state()
            if st and (st["oom"] or st["exit"] == 137):
                killed = True
                break
            if st and st["status"] == "exited" and st["exit"] != 0:
                break
            time.sleep(2)
        lg = sh("docker", "logs", "--tail", "30", NAME)
        logs = lg.stdout + lg.stderr
        rec("大表快照触顶被 OOM kill", killed,
            f"state={state()}，等待 {time.time()-t0:.1f}s\n       日志尾: {logs[-300:]}")

        cur.execute("SELECT COUNT(*), SUM(amt) FROM cg.row_t")
        c2, s2 = cur.fetchone()
        rec("SeekDB 行存真相源不受影响", c2 == src_count and int(s2) == src_sum,
            f"{c2} 行 SUM={int(s2)}（原 {src_count}/{src_sum}）")
        torn = [f for f in os.listdir(MIRRORS) if f.endswith((".duckdb", ".wal", ".tmp"))]
        rec("OOM 时确实留下了镜像文件（证明写在途）", bool(torn), f"{torn}")

        # 阶段二：给足内存重启，必须能从被写坏的文件恢复
        run_container(ip, mem_mb=1536, duck_mb=256)
        rec("重启后容器存活", wait_health(timeout=180), f"state={state()}")
        requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=60)
        q("SELECT 1")
        built = False
        t0 = time.time()
        while time.time() - t0 < 900:
            if metric("mirror_tables") >= 1:
                built = True
                break
            time.sleep(3)
        rec("恢复阶段镜像真的建成（非空测）", built,
            f"mirror_tables={metric('mirror_tables')}，等待 {time.time()-t0:.1f}s")
        got = None
        t0 = time.time()
        while time.time() - t0 < 900:
            try:
                rows = mirror_q("SELECT COUNT(*) FROM row_t")
                got = int(rows[0][0]["value"]) if rows else None
                if got == src_count:
                    break
            except Exception:
                pass
            time.sleep(3)
        rec("恢复后镜像与源逐值一致（行数）", got == src_count,
            f"镜像={got} 源={src_count}，等待 {time.time()-t0:.1f}s")
        rows = mirror_q("SELECT COUNT(*), MIN(id), MAX(id) FROM row_t")
        vals = [v["value"] for v in rows[0]] if rows else None
        cur.execute("SELECT COUNT(*), MIN(id), MAX(id) FROM cg.row_t")
        sv = cur.fetchone()
        same = vals is not None and str(vals[0]) == str(sv[0]) and str(vals[1]) == str(sv[1]) and str(vals[2]) == str(sv[2])
        rec("恢复后聚合值与源一致", same, f"镜像={vals} 源={list(sv)}")
        st = state()
        rec("恢复阶段无再次 OOM", st and st["status"] == "running" and not st["oom"], f"state={st}")
        aside = [f for f in os.listdir(MIRRORS) if ".corrupt-" in f]
        print(f"       旁路的坏文件: {aside}", flush=True)
    finally:
        sh("docker", "rm", "-f", NAME)
        cur.close()
        mc.close()

    bad = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results)-len(bad)}/{len(results)} PASS" + (f"，失败：{bad}" if bad else ""))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())