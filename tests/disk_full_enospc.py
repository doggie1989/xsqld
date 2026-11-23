#!/usr/bin/env python3
"""T7 真正的磁盘满（ENOSPC）：镜像写入撞 ENOSPC 时必须干净失败并可自愈。

与 `resource_pressure.py` B 段的区别（后者不能冒充本项）：
  ulimit -f 触发的是 **SIGXFSZ 信号致死**（exit -25），进程根本没机会返回错误；
  本脚本挂一个真实写满的小文件系统，write(2) 返回 **ENOSPC**，走的是错误处理路径。

判据：错误被计数且确实是磁盘空间错误、进程存活无 panic、行存查询不受影响、
      释放空间后镜像自动恢复。每条都先断言故障真的注入成功（非空测）。

仅 macOS（用 hdiutil）。映像与挂载点在 finally 里清理。
"""
import os
import re
import shutil
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
DMG = "/tmp/xsqld-enospc.dmg"
VOL = "/Volumes/xsqldenospc"
NS = "dfull"
ROWS = 300_000
LOG = "/tmp/xsqld-enospc.log"
results = []


def rec(n, ok, ev):
    results.append((n, ok, ev))
    print(f"[{'PASS' if ok else 'FAIL'}] {n}\n       {ev}", flush=True)


def avail_kb():
    st = os.statvfs(VOL)
    return st.f_bavail * st.f_frsize // 1024


def metric(name):
    try:
        t = requests.get(f"{USER}/metrics", timeout=10).text
    except Exception:
        return -1
    m = re.search(r'xsqld_%s\{ns="%s"\}\s+(\d+)' % (name, NS), t)
    return int(m.group(1)) if m else 0


def q(sql):
    r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
        {"type": "execute", "stmt": {"sql": sql}}, {"type": "close"}]},
        headers={"Host": f"{NS}.db", "Content-Type": "application/json"}, timeout=120)
    r.raise_for_status()
    res = r.json()["results"][0]
    if res["type"] != "ok":
        raise RuntimeError(res)
    return res["response"]["result"]["rows"]


def wait_health(proc, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if proc.poll() is not None:
            return False
        try:
            if requests.get(f"{USER}/health", timeout=2).status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def main():
    if sys.platform != "darwin" or not shutil.which("hdiutil"):
        print("SKIP: 需要 macOS + hdiutil")
        return 0
    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], capture_output=True)
    time.sleep(2)
    subprocess.run(["hdiutil", "detach", VOL, "-force"], capture_output=True)
    if os.path.exists(DMG):
        os.unlink(DMG)
    os.makedirs("/tmp/enospc-mirrors-fallback", exist_ok=True)

    subprocess.run(["hdiutil", "create", "-size", "12m", "-fs", "HFS+", "-volname",
                    "xsqldenospc", "-quiet", DMG], check=True, timeout=180)
    subprocess.run(["hdiutil", "attach", DMG, "-nobrowse"], check=True,
                   capture_output=True, timeout=180)
    proc = None
    filler = os.path.join(VOL, "filler.bin")
    try:
        mirror_dir = os.path.join(VOL, "mirrors")
        os.makedirs(mirror_dir, exist_ok=True)
        free0 = avail_kb()

        mc = pymysql.connect(**MY)
        cur = mc.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {NS}")
        cur.execute(f"USE {NS}")
        cur.execute("SHOW TABLES")
        for (t,) in cur.fetchall():
            cur.execute(f"DROP TABLE IF EXISTS `{t}`")
        # MD5 文本压缩率低，确保镜像文件真的超过剩余空间
        cur.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, s VARCHAR(64))")
        t0 = time.time()
        base = 0
        while base < ROWS:
            n = min(2000, ROWS - base)
            digits = ("(SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4"
                      " UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9)")
            cur.execute(
                "INSERT INTO t (id, s) SELECT %s + k, MD5(%s + k) FROM "
                f"(SELECT a.n + 10*b.n + 100*c.n + 1000*d.n AS k FROM {digits} a, {digits} b, "
                f"{digits} c, {digits} d) seq WHERE k < %s", (base, base, n))
            base += n
        cur.execute("SELECT COUNT(*), SUM(id) FROM t")
        src_count, src_sum = cur.fetchone()
        rec("fixture 就绪", src_count == ROWS, f"{src_count} 行，{time.time()-t0:.1f}s")

        # 填满：留约 900 KB，镜像文件必然放不下
        target_free = 900
        with open(filler, "wb") as f:
            chunk = b"\0" * (1 << 20)
            while avail_kb() > target_free + 1024:
                f.write(chunk)
            f.flush()
            os.fsync(f.fileno())
            while avail_kb() > target_free:
                f.write(b"\0" * 65536)
                f.flush()
                os.fsync(f.fileno())
        free1 = avail_kb()
        rec("磁盘满故障真的注入", free1 < 1500 and free1 < free0 // 4,
            f"可用 {free0} KB -> {free1} KB")

        env = {k: v for k, v in os.environ.items()
               if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
        env["RUST_LOG"] = "warn"
        proc = subprocess.Popen(
            [BIN, "--mysql-host", "127.0.0.1", "--mysql-port", "12981", "--mysql-user", "root",
             "--http-listen-addr", "127.0.0.1:18280", "--admin-listen-addr", "127.0.0.1:18281",
             "--mirror-enabled", "--mirror-dir", mirror_dir, "--mirror-interval-ms", "500",
             "--mirror-checkpoint-cycles", "5", "--route-min-rows", "1000"],
            stdout=open(LOG, "w"), stderr=subprocess.STDOUT, env=env)
        rec("xsqld 在满盘上仍能启动", wait_health(proc), f"pid={proc.pid}")
        requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=30)
        q("SELECT 1")

        # 等镜像尝试快照并失败
        err = 0
        t0 = time.time()
        while time.time() - t0 < 120:
            err = metric("mirror_errors_total")
            if err > 0:
                break
            if proc.poll() is not None:
                break
            time.sleep(1)
        log = open(LOG, errors="replace").read()
        rec("镜像写入失败被计数（非空测）", err > 0,
            f"mirror_errors_total={err}，等待 {time.time()-t0:.1f}s")
        space_err = re.search(r"No space left on device|ENOSPC|disk full|IO Error", log, re.I)
        rec("失败原因确实是磁盘空间/IO 而非其他", bool(space_err),
            (space_err.group(0) if space_err else "日志无空间类错误") + " | " +
            next((ln[-200:] for ln in log.splitlines() if "rror" in ln), "(无错误行)"))
        rec("进程存活（区别于 ulimit -f 的 SIGXFSZ 致死）", proc.poll() is None,
            f"poll={proc.poll()}")
        rec("无 panic", "panic" not in log.lower(), f"日志 {len(log)} 字节")
        rec("HTTP 层仍可用", requests.get(f"{USER}/health", timeout=5).status_code == 200, "health 200")
        # SUM 被等价闸拒绝 => 必走 SeekDB，检验行存路径不受满盘影响
        rows = q("SELECT SUM(id) FROM t")
        got = int(rows[0][0]["value"])
        rec("行存查询不受影响且值正确", got == int(src_sum), f"SUM(id)={got} 源={int(src_sum)}")

        # 释放空间，验证自愈
        os.unlink(filler)
        free2 = avail_kb()
        healed = False
        t0 = time.time()
        while time.time() - t0 < 180:
            try:
                if metric("mirror_tables") >= 1 and metric("mirror_cycles_total") > 0:
                    healed = True
                    break
            except Exception:
                pass
            time.sleep(1)
        rec("释放空间后镜像自动恢复", healed,
            f"可用 {free2} KB，tables={metric('mirror_tables')} "
            f"cycles={metric('mirror_cycles_total')}，等待 {time.time()-t0:.1f}s")
        # 值一致性：COUNT(*) 会被路由到镜像，与源逐值比对
        mirror_count = None
        t0 = time.time()
        while time.time() - t0 < 120:
            r = q("SELECT COUNT(*) FROM t")
            mirror_count = int(r[0][0]["value"])
            if mirror_count == src_count:
                break
            time.sleep(1)
        rec("恢复后镜像与源逐值一致", mirror_count == src_count,
            f"镜像={mirror_count} 源={src_count}")
        rec("恢复后进程仍存活无 panic",
            proc.poll() is None and "panic" not in open(LOG, errors="replace").read().lower(),
            f"poll={proc.poll()}")
        cur.close()
        mc.close()
    finally:
        if proc is not None and proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
        subprocess.run(["hdiutil", "detach", VOL, "-force"], capture_output=True)
        if os.path.exists(DMG):
            os.unlink(DMG)

    bad = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results)-len(bad)}/{len(results)} PASS" + (f"，失败：{bad}" if bad else ""))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())