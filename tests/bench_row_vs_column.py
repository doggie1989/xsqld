#!/usr/bin/env python3
"""T6 性能与资源：SeekDB 行存 vs DuckDB 列存镜像，同机同数据对称 A/B。

纪律：先比对比值再比时间；每轮交替引擎顺序抵消机器状态漂移；≥2 轮取中位数。
range_100 用修正后的 BETWEEN 500000 AND 500099（原 fixture 多写一个零，见 T6a）。
"""
import json
import statistics
import sys
import time

import duckdb
import pymysql

MY = dict(host="127.0.0.1", port=12981, user="root", password="", database="cg", autocommit=True)
ROUNDS = 3
WORKLOADS = [
    ("point_pk", "SELECT id,uid,amt,status,s FROM row_t WHERE id=5000000"),
    ("point_range_100", "SELECT id,amt FROM row_t WHERE id BETWEEN 500000 AND 500099"),
    ("wide_5000", "SELECT id,uid,amt,status,s FROM row_t WHERE id BETWEEN 2000000 AND 2004999"),
    ("narrow_agg_one_col", "SELECT SUM(amt) FROM row_t WHERE id BETWEEN 1 AND 5000000"),
    ("agg_full_scan", "SELECT COUNT(*), SUM(amt) FROM row_t"),
    ("agg_filter_2m", "SELECT COUNT(*), SUM(amt), AVG(amt) FROM row_t WHERE status=2"),
    ("group_by_uid_top", "SELECT uid, SUM(amt) s FROM row_t GROUP BY uid ORDER BY s DESC, uid LIMIT 10"),
]


def norm(v):
    import decimal, datetime
    if isinstance(v, decimal.Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat(sep=" ")
    return v


def rows_norm(rs):
    return [tuple(norm(x) for x in r) for r in rs]


def main():
    c = pymysql.connect(**MY); cu = c.cursor()
    cu.execute("SELECT COUNT(*) FROM row_t"); n = cu.fetchone()[0]
    print(f"row_t 行数 = {n}", flush=True)

    d = duckdb.connect()
    d.execute("LOAD mysql")
    d.execute("SET threads=2")
    d.execute("SET memory_limit='1GB'")
    d.execute("SET preserve_insertion_order=false")
    d.execute("ATTACH 'host=127.0.0.1 port=12981 user=root db=cg' AS src (TYPE mysql, READ_ONLY)")
    t0 = time.perf_counter()
    d.execute("CREATE OR REPLACE TABLE m_row AS FROM src.row_t")
    ctas = time.perf_counter() - t0
    cnt = d.execute("SELECT COUNT(*) FROM m_row").fetchone()[0]
    print(f"CTAS: {ctas:.2f}s, {cnt} 行, {cnt/ctas:,.0f} 行/秒", flush=True)
    assert cnt == n, "镜像行数必须与源一致"

    # 先验证值等价，再谈性能
    print("\n=== 值等价（先于计时）===", flush=True)
    alleq = True
    for name, sql in WORKLOADS:
        cu.execute(sql); a = rows_norm(cu.fetchall())
        b = rows_norm(d.execute(sql.replace("row_t", "m_row")).fetchall())
        eq = a == b
        alleq &= eq
        print(f"  [{'EQ ' if eq else 'DIV'}] {name}", flush=True)
        if not eq:
            print(f"      seekdb={str(a)[:140]}\n      duckdb={str(b)[:140]}", flush=True)
    if not alleq:
        print("存在值分歧，性能数字不可用于收益归因")
        return 1

    print(f"\n=== 对称 A/B（{ROUNDS} 轮，每轮交替引擎顺序）===", flush=True)
    samples = {name: {"seekdb": [], "duckdb": []} for name, _ in WORKLOADS}
    for r in range(ROUNDS):
        order = ["seekdb", "duckdb"] if r % 2 == 0 else ["duckdb", "seekdb"]
        for name, sql in WORKLOADS:
            for eng in order:
                ts = []
                for _ in range(3):
                    t = time.perf_counter()
                    if eng == "seekdb":
                        cu.execute(sql); cu.fetchall()
                    else:
                        d.execute(sql.replace("row_t", "m_row")).fetchall()
                    ts.append((time.perf_counter() - t) * 1000)
                samples[name][eng].append(statistics.median(ts))
        print(f"  轮 {r+1} 完成（顺序 {order}）", flush=True)

    print("\n=== 结果（各轮中位数的中位）===", flush=True)
    verdict = {}
    for name, _ in WORKLOADS:
        s = statistics.median(samples[name]["seekdb"])
        k = statistics.median(samples[name]["duckdb"])
        sp = s / k if k else None
        verdict[name] = {"seekdb_ms": round(s, 3), "duckdb_ms": round(k, 3), "speedup": round(sp, 2),
                         "seekdb_rounds": [round(x, 3) for x in samples[name]["seekdb"]],
                         "duckdb_rounds": [round(x, 3) for x in samples[name]["duckdb"]]}
        print(f"  {name:20s} SeekDB={s:9.3f}ms DuckDB={k:9.3f}ms 加速={sp:6.2f}x", flush=True)

    analytical = ["narrow_agg_one_col", "agg_full_scan", "agg_filter_2m", "group_by_uid_top"]
    point = ["point_pk", "point_range_100", "wide_5000"]
    ana_ok = all(verdict[x]["speedup"] >= 2.0 for x in analytical)
    pt_ok = all(verdict[x]["speedup"] >= 0.95 for x in point)
    print(f"\n分析负载 ≥2x: {'PASS' if ana_ok else 'FAIL'}  ({[verdict[x]['speedup'] for x in analytical]})")
    print(f"点查/宽结果退化 ≤5%: {'PASS' if pt_ok else 'FAIL'}  ({[verdict[x]['speedup'] for x in point]})")

    out = {"rows": n, "ctas_seconds": round(ctas, 3), "ctas_rows_per_s": round(cnt / ctas, 1),
           "rounds": ROUNDS, "verdict": verdict, "analytical_ge_2x": ana_ok, "point_within_5pct": pt_ok,
           "values_all_equal": alleq}
    with open("/tmp/t6-bench-results.json", "w") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("结果已存 /tmp/t6-bench-results.json", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())