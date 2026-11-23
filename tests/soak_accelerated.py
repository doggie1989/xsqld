#!/usr/bin/env python3
"""T7 加速 soak：持续混合写入 + 周期性 DDL，采样 RSS/fd/水位/错误。

**这是缩短版 soak，不是方案要求的 24h**。默认 8 分钟，用 --minutes 调整。
判据按同样口径检查：RSS 增长、fd 泄漏、水位持续前进、零错误、最终一致。
"""
import argparse
import json
import os
import random
import re
import signal
import subprocess
import sys
import time

import pymysql
import requests

USER = "http://127.0.0.1:18280"
ADMIN = "http://127.0.0.1:18281"
NS = "soak_t7"
HOST = {"Host": f"{NS}.db"}
MY = dict(host="127.0.0.1", port=12981, user="root", password="", autocommit=True)
MIRROR_DIR = "/tmp/xsqld-soak"
BIN = os.path.abspath("./target/debug/xsqld")
LOG = "/tmp/xsqld-soak.log"


def metrics():
    t = requests.get(f"{USER}/metrics", timeout=10).text
    def g(name):
        m = re.search(r'xsqld_%s\{ns="%s"\}\s+(\d+)' % (name, NS), t)
        return int(m.group(1)) if m else 0
    return {
        "watermark": g("mirror_watermark_scn"),
        "outbox": g("mirror_outbox_rows"),
        "cycles": g("mirror_cycles_total"),
        "errors": g("mirror_errors_total"),
        "applied": g("mirror_applied_total"),
        "rebuilds": sum(int(x) for x in re.findall(
            r'xsqld_mirror_rebuild_total\{ns="%s",table="[^"]+"\}\s+(\d+)' % NS, t)),
        "rust_live_mb": (lambda m: int(m.group(1)) / 1048576 if m else None)(
            re.search(r'xsqld_rust_live_bytes\s+(\d+)', t)),
    }


def proc_stats(pid):
    rss = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    fds = subprocess.run(["lsof", "-p", str(pid)], capture_output=True, text=True).stdout
    return int(rss) if rss.isdigit() else 0, max(0, len(fds.splitlines()) - 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=8.0)
    ap.add_argument("--churn", type=int, default=None, help="覆盖 --mirror-rebuild-churn")
    ap.add_argument("--stmt-cache", type=int, default=None, help="覆盖 --stmt-cache-size")
    ap.add_argument("--no-mirror", action="store_true", help="对照：关掉镜像 worker")
    ap.add_argument("--reopen", type=int, default=None, help="覆盖 --mirror-reopen-cycles")
    ap.add_argument("--steady", action="store_true",
                    help="固定数据量、只 UPDATE、禁 DDL：用于把 RSS 增长归因到数据/版本累积还是真泄漏")
    ap.add_argument("--steady-ddl", action="store_true",
                    help="主表数据恒定但保留 aux 表的 DDL 循环：长跑既有 DDL 覆盖，RSS 判据又可解释"
                         "（混合模式下数据增长会让 RSS 判据失去意义）")
    args = ap.parse_args()
    dur = args.minutes * 60

    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], check=False)
    time.sleep(2)
    subprocess.run(["rm", "-rf", MIRROR_DIR], check=False)
    env = {k: v for k, v in os.environ.items() if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    env["RUST_LOG"] = "warn"
    proc = subprocess.Popen(
        [BIN, "--mysql-host", "127.0.0.1", "--mysql-port", "12981", "--mysql-user", "root",
         "--http-listen-addr", "127.0.0.1:18280", "--admin-listen-addr", "127.0.0.1:18281",
         *(["--mirror-enabled", "--mirror-dir", MIRROR_DIR, "--mirror-interval-ms", "400"] if not args.no_mirror else []),
         "--route-min-rows", "1",
         *(["--mirror-rebuild-churn", str(args.churn)] if args.churn else []),
         *(["--stmt-cache-size", str(args.stmt_cache)] if args.stmt_cache is not None else []),
         *(["--mirror-reopen-cycles", str(args.reopen)] if args.reopen else [])],
        stdout=open(LOG, "w"), stderr=subprocess.STDOUT, env=env)
    samples = []
    ddl_count = 0
    write_count = 0
    try:
        for _ in range(120):
            try:
                if requests.get(f"{USER}/health", timeout=2).status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(0.5)
        requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=30)
        # 必须发一次带 Host 的 HTTP 请求，否则 NsPool 不会被创建，worker 也就不会镜像该命名空间
        requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
            {"type": "execute", "stmt": {"sql": "SELECT 1"}}, {"type": "close"}]},
            headers=HOST, timeout=60)
        c = pymysql.connect(**MY, database=NS); cu = c.cursor()
        # 清掉历史运行残留的表，否则 discover_tables 会把它们一起镜像，污染 steady 对照
        cu.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=%s", (NS,))
        for (tn,) in cu.fetchall():
            cu.execute(f"DROP TABLE IF EXISTS `{tn}`")
        cu.execute("DROP TABLE IF EXISTS s")
        cu.execute("CREATE TABLE s(id INT PRIMARY KEY, v VARCHAR(64), n INT)")
        if args.steady or args.steady_ddl:
            cu.execute("INSERT INTO s(id,v,n) VALUES" + ",".join(f"({i},'s{i}',{i})" for i in range(1, 5001)))
        else:
            cu.execute("INSERT INTO s VALUES(1,'seed',1)")
        c.close()
        time.sleep(4)

        t0 = time.time()
        next_sample = 0
        next_ddl = 20
        rnd = random.Random(42)
        has_extra = False
        while time.time() - t0 < dur:
            el = time.time() - t0
            c = pymysql.connect(**MY, database=NS); cu = c.cursor()
            if args.steady or args.steady_ddl:
                # 数据量恒定：只改已有行，不增不删，不做 DDL
                lo = 1 + (write_count * 10) % 4990
                cu.execute(f"UPDATE s SET n=n+1, v='u{write_count}' WHERE id BETWEEN {lo} AND {lo+9}")
            else:
                base = 1000 + write_count * 10
                vals = ",".join(f"({base+i},'w{write_count}_{i}',{rnd.randint(0,999)})" for i in range(10))
                cu.execute(f"INSERT INTO s(id,v,n) VALUES{vals}")
                cu.execute(f"UPDATE s SET n=n+1 WHERE id BETWEEN {base} AND {base+4}")
                cu.execute(f"DELETE FROM s WHERE id={base+9}")
            write_count += 1
            if not args.steady and el >= next_ddl:
                kind = ddl_count % 3
                try:
                    if kind == 0:
                        cu.execute(f"CREATE TABLE aux{ddl_count}(id INT PRIMARY KEY, v INT)")
                        cu.execute(f"INSERT INTO aux{ddl_count} VALUES(1,{ddl_count})")
                    elif kind == 1:
                        # 加列/删列交替，避免重复加列报错，也真正触发按表重建
                        if has_extra:
                            cu.execute("ALTER TABLE s DROP COLUMN extra")
                        else:
                            cu.execute("ALTER TABLE s ADD COLUMN extra INT")
                        has_extra = not has_extra
                    else:
                        cu.execute(f"DROP TABLE IF EXISTS aux{ddl_count-2}")
                except Exception as e:
                    print("DDL 失败(可接受，如列已存在):", str(e)[:90], flush=True)
                ddl_count += 1
                next_ddl = el + 20
            c.close()

            if el >= next_sample:
                m = metrics()
                rss, fds = proc_stats(proc.pid)
                samples.append({"t": round(el, 1), "rss_kb": rss, "fds": fds, **m})
                rl = m.get("rust_live_mb")
                print(f"  t={el:6.1f}s rss={rss/1024:7.1f}MB rust_live={rl if rl is None else round(rl,1)}MB fds={fds:4d} wm={m['watermark']} "
                      f"outbox={m['outbox']:5d} cycles={m['cycles']:5d} err={m['errors']} "
                      f"rebuilds={m['rebuilds']}", flush=True)
                next_sample = el + 15
            time.sleep(0.2)

        # 收尾：等 outbox 排空并核对最终一致
        for _ in range(120):
            if metrics()["outbox"] == 0:
                break
            time.sleep(1)
        c = pymysql.connect(**MY, database=NS); cu = c.cursor()
        cu.execute("SELECT COUNT(*) FROM s"); src_n = cu.fetchone()[0]
        c.close()
        r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
            {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) FROM s"}}, {"type": "close"}]},
            headers=HOST, timeout=120)
        mir_n = int(r.json()["results"][0]["response"]["result"]["rows"][0][0]["value"])
        m = metrics()
        rss, fds = proc_stats(proc.pid)
        samples.append({"t": round(time.time() - t0, 1), "rss_kb": rss, "fds": fds, **m})

        print("\n=== 判据 ===", flush=True)
        ok = True
        first_rss = samples[0]["rss_kb"]; last_rss = samples[-1]["rss_kb"]
        growth = (last_rss - first_rss) / first_rss * 100 if first_rss else 0
        # 前几个采样含初始快照的一次性分配，用中段之后的样本判增长更公平
        mid = samples[len(samples)//3:]
        g2 = (mid[-1]["rss_kb"] - mid[0]["rss_kb"]) / mid[0]["rss_kb"] * 100 if mid[0]["rss_kb"] else 0
        print(f"RSS: 首 {first_rss/1024:.1f}MB → 末 {last_rss/1024:.1f}MB（全程 {growth:+.1f}%，中段后 {g2:+.1f}%）")
        c1 = g2 < 5.0
        print(f"  中段后 RSS 增长 <5%: {'PASS' if c1 else 'FAIL'}"); ok &= c1
        fd_first = samples[0]["fds"]; fd_last = samples[-1]["fds"]
        c2 = fd_last <= fd_first + 10
        print(f"fd: 首 {fd_first} → 末 {fd_last}  无泄漏(≤+10): {'PASS' if c2 else 'FAIL'}"); ok &= c2
        wms = [s["watermark"] for s in samples]
        c3 = all(b >= a for a, b in zip(wms, wms[1:])) and wms[-1] > wms[0]
        print(f"水位单调前进: {'PASS' if c3 else 'FAIL'}  {wms[0]} → {wms[-1]}"); ok &= c3
        c4 = m["errors"] == 0
        print(f"零同步错误: {'PASS' if c4 else 'FAIL'}  errors={m['errors']}"); ok &= c4
        c5 = mir_n == src_n
        print(f"最终一致: {'PASS' if c5 else 'FAIL'}  mirror={mir_n} source={src_n}"); ok &= c5
        c6 = m["outbox"] == 0
        print(f"outbox 排空: {'PASS' if c6 else 'FAIL'}  outbox={m['outbox']}"); ok &= c6
        log = open(LOG).read()
        c7 = "panic" not in log.lower()
        print(f"无 panic: {'PASS' if c7 else 'FAIL'}"); ok &= c7
        print(f"\n写入批次={write_count} DDL 次数={ddl_count} 时长={time.time()-t0:.0f}s")
        print("注意：这是缩短版 soak，不等同方案要求的 24h。")
        if args.steady_ddl:
            print("steady-ddl 模式：主表数据恒定、aux 表持续建删，RSS 判据可解释且覆盖 DDL 路径。")
        elif args.steady:
            print("steady 模式：数据量恒定、无 DDL，RSS 增长可直接归因为泄漏或缓冲累积。")
        else:
            print("混合模式：数据量持续增长，RSS 判据含数据/版本贡献，不可单独归因为泄漏。")
        json.dump({"samples": samples, "writes": write_count, "ddls": ddl_count,
                   "duration_s": round(time.time()-t0,1), "all_pass": bool(ok),
                   "final": {"mirror": mir_n, "source": src_n}},
                  open("/tmp/t7-soak-results.json", "w"), indent=2)
        return 0 if ok else 1
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except Exception:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())