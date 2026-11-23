#!/usr/bin/env python3
"""路由端到端：证明 HTTP 请求真的走了 DuckDB，且被拒的形状确实没走。

非空测靠 /metrics 的 routed_total 计数，不靠延迟推断。
"""
import json
import re
import subprocess
import sys
import time

import pymysql
import requests

USER = "http://127.0.0.1:18280"
ADMIN = "http://127.0.0.1:18281"
NS = "route_e2e"
HOST = {"Host": f"{NS}.db"}
MY = dict(host="127.0.0.1", port=12981, user="root", password="", autocommit=True)
results = []


def rec(n, ok, ev):
    results.append((n, ok, ev))
    print(f"[{'PASS' if ok else 'FAIL'}] {n}\n       {ev}", flush=True)


def pipe(sql):
    r = requests.post(f"{USER}/v2/pipeline",
                      json={"baton": None, "requests": [{"type": "execute", "stmt": {"sql": sql}}, {"type": "close"}]},
                      headers=HOST, timeout=60)
    return r.status_code, r.json()


def routed_total():
    t = requests.get(f"{USER}/metrics", timeout=10).text
    m = re.search(r'xsqld_routed_total\{ns="%s"\}\s+(\d+)' % NS, t)
    return int(m.group(1)) if m else 0


def values(body):
    try:
        res = body["results"][0]["response"]["result"]
        return [[c.get("value") for c in row] for row in res["rows"]], [c["name"] for c in res["cols"]]
    except Exception:
        return None, str(body)[:200]


def main():
    for _ in range(60):
        try:
            if requests.get(f"{USER}/health", timeout=2).status_code == 200:
                break
        except Exception:
            time.sleep(0.5)
    r = requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=30)
    rec("建命名空间", r.status_code in (200, 201, 400, 409), f"{r.status_code}")

    pipe("CREATE TABLE t(id INT PRIMARY KEY, uid INT, s VARCHAR(64))")
    vals = ",".join(f"({i},{i%50},'v{i}')" for i in range(1, 501))
    st, b = pipe(f"INSERT INTO t VALUES{vals}")
    rec("插入 500 行", st == 200, f"{st}")

    # 等同步完成（outbox 表由 worker 首轮创建，需容忍其尚不存在）
    n = -1
    for _ in range(80):
        try:
            c = pymysql.connect(**MY, database=NS); cu = c.cursor()
            cu.execute("SELECT COUNT(*) FROM xsqld_changelog"); n = cu.fetchone()[0]; c.close()
        except Exception:
            n = -1
            time.sleep(0.3)
            continue
        if n == 0:
            break
        time.sleep(0.3)
    rec("outbox 已清空（镜像已就绪）", n == 0, f"pending={n}")
    # 等 worker 发布 MirrorView：以"COUNT(*) 真的被路由"为就绪判据，不用固定 sleep
    for _ in range(60):
        b0 = routed_total()
        pipe("SELECT COUNT(*) FROM t")
        if routed_total() > b0:
            break
        time.sleep(0.5)

    base = routed_total()
    st, b = pipe("SELECT COUNT(*) FROM t")
    v, cols = values(b)
    after = routed_total()
    rec("COUNT(*) 被路由到 DuckDB（计数增加）", after > base, f"routed_total {base} -> {after}")
    rec("路由结果值正确", v == [["500"]], f"rows={v} cols={cols}")

    # 响应契约：列名必须与 SeekDB 一致，不能用 DuckDB 内部命名（count_star()）
    c = pymysql.connect(**MY, database=NS); cu = c.cursor()
    cu.execute("SELECT COUNT(*) FROM t"); cu.fetchall()
    src_cols = [d[0] for d in cu.description]
    cu.execute("SELECT uid, COUNT(*) FROM t GROUP BY uid ORDER BY uid LIMIT 5"); cu.fetchall()
    src_grp_cols = [d[0] for d in cu.description]
    c.close()
    st, b = pipe("SELECT COUNT(*) FROM t"); _, rcols = values(b)
    rec("COUNT 列名与 SeekDB 一致", rcols == src_cols, f"routed={rcols} seekdb={src_cols}")
    st, b = pipe("SELECT uid, COUNT(*) FROM t GROUP BY uid ORDER BY uid LIMIT 5"); _, gcols = values(b)
    rec("GROUP BY 列名与 SeekDB 一致", gcols == src_grp_cols, f"routed={gcols} seekdb={src_grp_cols}")

    # decltype 契约：用一条被等价闸拒绝但投影相同的 SQL 取 SeekDB 路径的 cols 做对照
    def full_cols(sql):
        r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
            {"type": "execute", "stmt": {"sql": sql}}, {"type": "close"}]}, headers=HOST, timeout=60)
        return r.json()["results"][0]["response"]["result"]["cols"]
    routed_cols = full_cols("SELECT COUNT(*) FROM t")
    seekdb_cols = full_cols("SELECT COUNT(*) FROM t WHERE s='zzz-no-match'")
    rec("COUNT 的 decltype 与 SeekDB 路径一致", routed_cols == seekdb_cols,
        f"routed={routed_cols} seekdb={seekdb_cols}")
    rg = full_cols("SELECT uid, COUNT(*) FROM t GROUP BY uid ORDER BY uid LIMIT 5")
    sg = full_cols("SELECT uid, COUNT(*) FROM t WHERE s='zzz-no-match' GROUP BY uid ORDER BY uid LIMIT 5")
    rec("GROUP BY 的 decltype 与 SeekDB 路径一致", rg == sg, f"routed={rg} seekdb={sg}")



    # GROUP BY 也应路由
    base = routed_total()
    st, b = pipe("SELECT uid, COUNT(*) FROM t GROUP BY uid ORDER BY uid LIMIT 5")
    v, _ = values(b)
    rec("GROUP BY 被路由", routed_total() > base, f"routed_total={routed_total()}")
    rec("GROUP BY 值正确", v == [["0", "10"], ["1", "10"], ["2", "10"], ["3", "10"], ["4", "10"]], f"{v}")

    # 被拒形状：MIN/MAX 单表（收益闸）、ci 文本比较（等价闸）、SUM
    for sql, why in [("SELECT MIN(id), MAX(id) FROM t", "收益闸:索引-backed MIN/MAX"),
                     ("SELECT COUNT(*) FROM t WHERE s='v1'", "等价闸:ci 文本比较"),
                     ("SELECT SUM(uid) FROM t", "等价闸:SUM")]:
        base = routed_total()
        st, b = pipe(sql)
        v, _ = values(b)
        rec(f"拒绝路由 {why}", routed_total() == base and st == 200,
            f"routed_total 保持 {base}, rows={v}")

    # 与 SeekDB 直读对照，确认路由答案与行存一致
    c = pymysql.connect(**MY, database=NS); cu = c.cursor()
    cu.execute("SELECT COUNT(*) FROM t"); src_count = cu.fetchone()[0]
    cu.execute("SELECT uid, COUNT(*) FROM t GROUP BY uid ORDER BY uid LIMIT 5"); src_grp = cu.fetchall()
    c.close()
    st, b = pipe("SELECT COUNT(*) FROM t"); v, _ = values(b)
    rec("路由 COUNT 与 SeekDB 直读一致", v == [[str(src_count)]], f"duckdb={v} seekdb={src_count}")
    st, b = pipe("SELECT uid, COUNT(*) FROM t GROUP BY uid ORDER BY uid LIMIT 5"); v, _ = values(b)
    rec("路由 GROUP BY 与 SeekDB 直读一致",
        v == [[str(a), str(b_)] for a, b_ in src_grp], f"duckdb={v} seekdb={src_grp}")

    # 事务内不路由
    r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
        {"type": "execute", "stmt": {"sql": "BEGIN"}},
        {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) FROM t"}}]}, headers=HOST, timeout=30)
    baton = r.json().get("baton")
    base = routed_total()
    requests.post(f"{USER}/v2/pipeline", json={"baton": baton, "requests": [
        {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) FROM t"}}, {"type": "close"}]},
        headers=HOST, timeout=30)
    rec("事务内不路由", routed_total() == base, f"routed_total 保持 {base}")

    # 分理由拒绝计数必须真的被填充（放在所有拒绝用例之后）
    mt = requests.get(f"{USER}/metrics", timeout=10).text
    reasons = dict(re.findall(r'xsqld_route_refused_total\{ns="%s",reason="([^"]+)"\}\s+(\d+)' % NS, mt))
    rec("分理由拒绝计数已填充且理由正确",
        all(k in reasons for k in ("collation", "not_profitable", "sum")),
        f"{reasons}")

    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], check=False)
    np = sum(1 for _, o, _ in results if o); nf = sum(1 for _, o, _ in results if not o)
    print(f"\nPASS={np} FAIL={nf}", flush=True)
    json.dump([{"check": n, "ok": o, "evidence": str(e)} for n, o, e in results],
              open("/tmp/route-e2e-results.json", "w"), ensure_ascii=False, indent=2)
    return 0 if nf == 0 else 1


if __name__ == "__main__":
    sys.exit(main())