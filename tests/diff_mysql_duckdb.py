#!/usr/bin/env python3
"""T3 差分 harness：SeekDB(行存) ↔ DuckDB(列存镜像) 逐类逐值等价判定。

这是路由白名单的准入证。核心安全性质：**被路由的类必须等价**。
harness 同时测两件事并断言前者蕴含后者：
  Phase A（xsqld 运行中）：每条 SQL 经 HTTP 发出，用 routed_total 计数差判定是否真被路由。
  Phase B（xsqld 停机后）：直接开镜像文件与 SeekDB 各跑一遍，归一化后逐值比对。

归一化区分"语义差异"与"表示差异"：MySQL 的 Decimal 与 DuckDB 的 int 若数值全等则视为等价，
但类型差异单独记为 representation note。collation 分组、NULL 排序、溢出回绕会表现为真实的值差异。

用法：python3 tests/diff_mysql_duckdb.py [--xsqld-bin PATH] [--keep]
需要本机 SeekDB 在 127.0.0.1:12981，且 venv 里有 pymysql/duckdb/requests。
"""
import argparse
import datetime
import decimal
import json
import os
import re
import signal
import subprocess
import sys
import time

import duckdb
import pymysql
import requests

NS = "diff_t3"
USER = "http://127.0.0.1:18290"
ADMIN = "http://127.0.0.1:18291"
HOST = {"Host": f"{NS}.db"}
MY = dict(host="127.0.0.1", port=12981, user="root", password="", autocommit=True)
MIRROR_DIR = "/tmp/xsqld-t3-mirrors"

SCHEMA = [
    "CREATE TABLE nums(id INT PRIMARY KEY, v INT, n INT)",
    "CREATE TABLE txt(id INT PRIMARY KEY, ci VARCHAR(64), bin_s VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)",
    "CREATE TABLE dec_t(id INT PRIMARY KEY, d20 DECIMAL(20,4), d38 DECIMAL(38,10), d65 DECIMAL(65,10))",
    "CREATE TABLE ub(id INT PRIMARY KEY, u BIGINT UNSIGNED)",
    "CREATE TABLE dt(id INT PRIMARY KEY, d DATE, ts DATETIME)",
    "CREATE TABLE bits(id INT PRIMARY KEY, b8 BIT(8), b1 BIT(1))",
    "CREATE TABLE empty_t(id INT PRIMARY KEY, v INT)",
    "CREATE TABLE allnull(id INT PRIMARY KEY, v INT)",
    "CREATE TABLE dim(id INT PRIMARY KEY, w INT)",
    "CREATE TABLE big(id INT PRIMARY KEY, v BIGINT)",
    "CREATE TABLE misc(id INT PRIMARY KEY, e ENUM('a','b','c'), st SET('x','y','z'), j JSON, bl BLOB, vb VARBINARY(64))",
    # SeekDB 实测只支持 binary 与 utf8mb4（SHOW CHARACTER SET），gbk/latin1 不存在，
    # 故非 utf8 文本用 binary 列覆盖；方案里的 gbk/latin1 项在本平台不适用。
    "CREATE TABLE nonutf(id INT PRIMARY KEY, raw VARCHAR(64) CHARACTER SET binary, lat VARCHAR(64))",
    "CREATE TABLE floats(id INT PRIMARY KEY, f FLOAT, d DOUBLE)",
    "CREATE TABLE vec(id INT PRIMARY KEY, v VECTOR(3))",
]

DATA = [
    "INSERT INTO nums VALUES(1,10,5),(2,20,NULL),(3,-7,1),(4,10,NULL),(5,0,9),(6,3,2)",
    # ci 列用默认 utf8mb4_general_ci；含大小写与尾空格，用于暴露 PAD SPACE 与大小写不敏感
    "INSERT INTO txt VALUES(1,'a','a'),(2,'A','A'),(3,'a ','a '),(4,NULL,NULL),(5,'b','b')",
    "INSERT INTO dec_t VALUES(1,1.2500,12345678901234567890.1234567890,12345678901234567890123456789012345.1234567890)",
    "INSERT INTO ub VALUES(1,18446744073709551615),(2,0)",
    "INSERT INTO dt VALUES(1,'2026-11-22','2026-11-22 15:30:45')",
    "INSERT INTO bits VALUES(1,b'10101010',b'1')",
    "INSERT INTO allnull VALUES(1,NULL),(2,NULL)",
    "INSERT INTO dim VALUES(1,100),(2,200),(3,300)",
    # 接近 i64 上界，用于暴露 SUM 溢出行为差异
    "INSERT INTO big VALUES(1,9223372036854775800),(2,100)",
    "INSERT INTO misc VALUES(1,'b','x,z','{\"k\":[1,2,3]}',X'DEADBEEF',X'010203')",
    "INSERT INTO misc VALUES(2,'a','y',NULL,NULL,NULL)",
    "INSERT INTO nonutf VALUES(1,'中文','cafe')",
    "INSERT INTO nonutf VALUES(2,X'E4B8ADE69687','naive')",
    "INSERT INTO floats VALUES(1,1.5,2.25),(2,-0.0,1e308),(3,3.402823466e38,1.7976931348623157e308)",
]

# (class, sql, totally_ordered)
CASES = [
    ("count_star", "SELECT COUNT(*) FROM nums", True),
    ("count_col_with_null", "SELECT COUNT(n) FROM nums", True),
    ("count_empty", "SELECT COUNT(*) FROM empty_t", True),
    ("sum_pure_int", "SELECT SUM(v) FROM nums", True),
    ("sum_all_null", "SELECT SUM(v) FROM allnull", True),
    ("sum_overflow_i64", "SELECT SUM(v) FROM big", True),
    ("min_max_int", "SELECT MIN(v), MAX(v) FROM nums", True),
    ("min_max_pk", "SELECT MIN(id), MAX(id) FROM nums", True),
    ("avg_int", "SELECT AVG(v) FROM nums", True),
    ("group_by_int", "SELECT v, COUNT(*) FROM nums GROUP BY v ORDER BY v", True),
    ("group_by_null_order_asc", "SELECT n, COUNT(*) FROM nums GROUP BY n ORDER BY n ASC", True),
    ("group_by_null_order_desc", "SELECT n, COUNT(*) FROM nums GROUP BY n ORDER BY n DESC", True),
    ("order_by_null_asc", "SELECT id, n FROM nums ORDER BY n ASC, id ASC", True),
    ("order_by_null_desc", "SELECT id, n FROM nums ORDER BY n DESC, id DESC", True),
    ("ci_text_eq", "SELECT COUNT(*) FROM txt WHERE ci='a'", True),
    ("ci_text_group", "SELECT ci, COUNT(*) FROM txt GROUP BY ci ORDER BY ci", True),
    ("ci_text_order", "SELECT id, ci FROM txt ORDER BY ci, id", True),
    ("ci_text_distinct", "SELECT DISTINCT ci FROM txt ORDER BY ci", True),
    ("ci_text_minmax", "SELECT MIN(ci), MAX(ci) FROM txt", True),
    ("ci_pad_space", "SELECT COUNT(*) FROM txt WHERE ci='a '", True),
    ("bin_text_eq", "SELECT COUNT(*) FROM txt WHERE bin_s='a'", True),
    ("bin_text_group", "SELECT bin_s, COUNT(*) FROM txt GROUP BY bin_s ORDER BY bin_s", True),
    ("bin_text_minmax", "SELECT MIN(bin_s), MAX(bin_s) FROM txt", True),
    ("int_division", "SELECT id, v/2 FROM nums ORDER BY id", True),
    ("int_div", "SELECT id, v DIV 2 FROM nums ORDER BY id", True),
    ("modulo_negative", "SELECT id, v % 3 FROM nums ORDER BY id", True),
    ("div_by_zero", "SELECT v/0 FROM nums WHERE id=1", True),
    ("decimal20_sum", "SELECT SUM(d20) FROM dec_t", True),
    ("decimal38_value", "SELECT d38 FROM dec_t WHERE id=1", True),
    ("decimal65_value", "SELECT d65 FROM dec_t WHERE id=1", True),
    ("unsigned_max", "SELECT u FROM ub WHERE id=1", True),
    ("unsigned_sum", "SELECT SUM(u) FROM ub", True),
    ("date_value", "SELECT d FROM dt WHERE id=1", True),
    ("datetime_value", "SELECT ts FROM dt WHERE id=1", True),
    ("bit8_value", "SELECT b8 FROM bits WHERE id=1", True),
    ("bit1_value", "SELECT b1 FROM bits WHERE id=1", True),
    ("join_count", "SELECT COUNT(*) FROM nums JOIN dim ON nums.id=dim.id", True),
    ("join_group", "SELECT dim.w, COUNT(*) FROM nums JOIN dim ON nums.id=dim.id GROUP BY dim.w ORDER BY dim.w", True),
    ("join_minmax", "SELECT MIN(dim.w), MAX(dim.w) FROM nums JOIN dim ON nums.id=dim.id", True),
    ("left_join", "SELECT COUNT(*) FROM nums LEFT JOIN dim ON nums.id=dim.id", True),
    ("where_between", "SELECT COUNT(*) FROM nums WHERE v BETWEEN 0 AND 20", True),
    ("where_in", "SELECT COUNT(*) FROM nums WHERE v IN (10,20)", True),
    ("where_is_null", "SELECT COUNT(*) FROM nums WHERE n IS NULL", True),
    ("limit_with_order", "SELECT id FROM nums ORDER BY id LIMIT 3", True),
    ("group_concat", "SELECT GROUP_CONCAT(v) FROM nums", True),
    ("cast_avg", "SELECT CAST(AVG(v) AS SIGNED) FROM nums", True),
    ("enum_value", "SELECT e FROM misc WHERE id=1", True),
    ("enum_group", "SELECT e, COUNT(*) FROM misc GROUP BY e ORDER BY e", True),
    ("set_value", "SELECT st FROM misc WHERE id=1", True),
    ("json_value", "SELECT j FROM misc WHERE id=1", True),
    ("blob_value", "SELECT bl FROM misc WHERE id=1", True),
    ("varbinary_value", "SELECT vb FROM misc WHERE id=1", True),
    ("blob_eq", "SELECT COUNT(*) FROM misc WHERE bl=X'DEADBEEF'", True),
    ("binary_col_value", "SELECT raw FROM nonutf WHERE id=1", True),
    ("binary_col_bytes", "SELECT raw FROM nonutf WHERE id=2", True),
    ("utf8_value", "SELECT lat FROM nonutf WHERE id=1", True),
    ("nonutf_eq", "SELECT COUNT(*) FROM nonutf WHERE lat='cafe'", True),
    ("float_value", "SELECT f FROM floats WHERE id=1", True),
    ("double_max", "SELECT d FROM floats WHERE id=3", True),
    ("float_sum", "SELECT SUM(f) FROM floats", True),
    ("double_neg_zero", "SELECT d FROM floats WHERE id=2", True),
    ("null_aggregates", "SELECT COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v) FROM allnull", True),
    ("empty_aggregates", "SELECT COUNT(*), SUM(v), MIN(v), MAX(v) FROM empty_t", True),
    ("group_concat_order", "SELECT GROUP_CONCAT(v ORDER BY v) FROM nums", True),
    ("having_refused", "SELECT v, COUNT(*) FROM nums GROUP BY v HAVING COUNT(*)>1", True),
    ("subquery_refused", "SELECT COUNT(*) FROM nums WHERE v IN (SELECT w FROM dim)", True),
    ("rename_shape", "SELECT id AS ident, v AS val FROM nums ORDER BY id LIMIT 3", True),
]


def canon(v):
    """语义归一化。返回 (kind, value)；表示差异另记 note。"""
    if v is None:
        return ("null", None)
    if isinstance(v, bool):
        return ("int", int(v))
    if isinstance(v, int):
        return ("int", v)
    if isinstance(v, decimal.Decimal):
        if v == v.to_integral_value():
            return ("int", int(v))
        return ("float", float(v))
    if isinstance(v, float):
        return ("float", v)
    if isinstance(v, (bytes, bytearray)):
        return ("blob", bytes(v).hex())
    if isinstance(v, (datetime.date, datetime.datetime, datetime.time)):
        return ("text", v.isoformat(sep=" ") if isinstance(v, datetime.datetime) else v.isoformat())
    if isinstance(v, datetime.timedelta):
        return ("text", str(v))
    return ("text", str(v))


def rows_canon(rows):
    return [tuple(canon(x) for x in r) for r in rows]


def compare(a, b, ordered):
    ra, rb = rows_canon(a), rows_canon(b)
    if not ordered:
        ra, rb = sorted(ra, key=repr), sorted(rb, key=repr)
    if len(ra) != len(rb):
        return False, f"row count {len(ra)} vs {len(rb)}"
    for i, (x, y) in enumerate(zip(ra, rb)):
        if len(x) != len(y):
            return False, f"row {i} width {len(x)} vs {len(y)}"
        for j, (p, q) in enumerate(zip(x, y)):
            if p != q:
                return False, f"row {i} col {j}: {p!r} vs {q!r}"
    return True, ""


def repr_notes(a, b):
    """记录类型表示差异（数值可能全等），供人工判断是否影响 Hrana 契约。"""
    notes = []
    for i, (x, y) in enumerate(zip(a, b)):
        for j, (p, q) in enumerate(zip(x, y)):
            tp, tq = type(p).__name__, type(q).__name__
            if tp != tq:
                notes.append(f"r{i}c{j}:{tp}->{tq}")
    return notes[:6]


def hrana_canon(v):
    """把 Hrana JSON 值归一化到与 canon() 相同的表示，便于跨路径比对。"""
    ty = v.get("type")
    if ty == "null":
        return ("null", None)
    if ty == "integer":
        return ("int", int(v["value"]))
    if ty == "float":
        return ("float", float(v["value"]))
    if ty == "text":
        return ("text", v["value"])
    if ty == "blob":
        import base64
        return ("blob", base64.b64decode(v["base64"]).hex())
    return ("?", json.dumps(v, sort_keys=True))


def hrana_rows(body):
    res = body["results"][0]["response"]["result"]
    return [tuple(hrana_canon(c) for c in row) for row in res["rows"]]


def compare_canon(ra, rb, ordered):
    if not ordered:
        ra, rb = sorted(ra, key=repr), sorted(rb, key=repr)
    if len(ra) != len(rb):
        return False, f"row count {len(ra)} vs {len(rb)}"
    for i, (x, y) in enumerate(zip(ra, rb)):
        if len(x) != len(y):
            return False, f"row {i} width {len(x)} vs {len(y)}"
        for j, (p, q) in enumerate(zip(x, y)):
            if p != q:
                return False, f"row {i} col {j}: {p!r} vs {q!r}"
    return True, ""


def pipe(sql):
    r = requests.post(f"{USER}/v2/pipeline", json={"baton": None, "requests": [
        {"type": "execute", "stmt": {"sql": sql}}, {"type": "close"}]}, headers=HOST, timeout=90)
    return r.status_code, r.json()


def routed_total():
    t = requests.get(f"{USER}/metrics", timeout=10).text
    m = re.search(r'xsqld_routed_total\{ns="%s"\}\s+(\d+)' % NS, t)
    return int(m.group(1)) if m else 0


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xsqld-bin", default="./target/debug/xsqld")
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()
    binpath = os.path.abspath(args.xsqld_bin)

    env = {k: v for k, v in os.environ.items()
           if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    env["RUST_LOG"] = "info"
    subprocess.run(["pkill", "-TERM", "-f", "target/debug/xsqld"], check=False)
    time.sleep(2)
    subprocess.run(["rm", "-rf", MIRROR_DIR], check=False)
    proc = subprocess.Popen(
        [binpath, "--mysql-host", "127.0.0.1", "--mysql-port", "12981", "--mysql-user", "root",
         "--http-listen-addr", "127.0.0.1:18290", "--admin-listen-addr", "127.0.0.1:18291",
         "--mirror-enabled", "--mirror-dir", MIRROR_DIR, "--mirror-interval-ms", "400",
         "--route-min-rows", "1"],
        stdout=open("/tmp/xsqld-t3.log", "w"), stderr=subprocess.STDOUT, env=env)
    try:
        if not wait_health():
            print("xsqld 未就绪")
            return 2
        requests.post(f"{ADMIN}/v1/namespaces/{NS}/create", json={}, timeout=30)
        c = pymysql.connect(**MY)
        cu = c.cursor()
        cu.execute(f"DROP DATABASE IF EXISTS {NS}")
        cu.execute(f"CREATE DATABASE {NS}")
        cu.execute(f"USE {NS}")
        for s in SCHEMA:
            cu.execute(s)
        cu.execute("SET SESSION sql_mode=''")
        data_failures = []
        for s in DATA:
            try:
                cu.execute(s)
            except Exception as e:
                data_failures.append((s[:70], str(e)[:120]))
        for f in data_failures:
            print("DATA 失败（显式记录，不静默跳过）:", f)
        try:
            cu.execute("INSERT INTO vec VALUES(1,'[1,2,3]')")
            CASES.append(("vector_value", "SELECT v FROM vec WHERE id=1", True))
        except Exception as e:
            print("VECTOR 插入失败，跳过该类:", str(e)[:120])
        # zero date 需要宽松 sql_mode
        try:
            cu.execute("INSERT INTO dt VALUES(2,'0000-00-00','0000-00-00 00:00:00')")
        except Exception as e:
            print("zero date 插入失败:", str(e)[:120])
        c.close()

        # 就绪判据必须是"真的有查询被路由"，不能用 outbox 为空：fixture 数据在装触发器之前就插入了，
        # outbox 从一开始就是空的，那样会在镜像建好之前就跑 Phase A，得到 0 条路由的空测结果。
        ready = False
        for _ in range(150):
            b0 = routed_total()
            pipe("SELECT COUNT(*) FROM nums")
            if routed_total() > b0:
                ready = True
                break
            time.sleep(0.4)
        if not ready:
            print("镜像未就绪：没有任何查询被路由，检查 /tmp/xsqld-t3.log")
            return 2
        print("镜像就绪（已确认有查询真的走了 DuckDB）")

        # Phase A：判定每条 SQL 是否真被路由，并留下 HTTP 实际响应（路由器改写后的 SQL 的结果）
        routed = {}
        http_rows = {}
        for name, sql, _ in CASES:
            b0 = routed_total()
            st, body = pipe(sql)
            routed[name] = (routed_total() > b0, st)
            try:
                http_rows[name] = hrana_rows(body)
            except Exception:
                http_rows[name] = None
        n_routed = sum(1 for v, _ in routed.values() if v)
        print(f"Phase A: {n_routed}/{len(CASES)} 条被路由")

        # Phase B：停机后直接比 SeekDB 与 DuckDB
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=30)
        time.sleep(1)

        d = duckdb.connect(f"{MIRROR_DIR}/{NS}.duckdb", read_only=True)
        c = pymysql.connect(**MY, database=NS); cu = c.cursor()
        verdicts = []
        unsafe = []
        for name, sql, ordered in CASES:
            msql = re.sub(r"\b(nums|txt|dec_t|ub|dt|bits|empty_t|allnull|dim|big|misc|nonutf|floats|vec)\b",
                          lambda m: "m_" + m.group(1), sql)
            try:
                cu.execute(sql); a = cu.fetchall()
                aerr = None
            except Exception as e:
                a, aerr = [], str(e)[:160]
            try:
                b = d.execute(msql).fetchall(); berr = None
            except Exception as e:
                b, berr = [], str(e)[:160]
            is_routed, st = routed[name]
            if is_routed:
                # 被路由的类必须用 HTTP 实际响应比对：那才是路由器改写后（含 NULLS 注入）真正上线的行为。
                # 拿原始 SQL 去 DuckDB 跑会测到路由器根本没发出的语句。
                hr = http_rows.get(name)
                if aerr:
                    eq, why = False, f"seekdb errored but routed answered: {aerr}"
                elif hr is None:
                    eq, why = False, "routed response could not be parsed"
                else:
                    eq, why = compare_canon(rows_canon(a), hr, ordered)
                basis = "http_routed_vs_seekdb"
            elif aerr or berr:
                if aerr and berr:
                    eq, why = True, "both errored (equivalent refusal)"
                else:
                    eq, why = False, f"seekdb_err={aerr} duckdb_err={berr}"
                basis = "raw_both"
            else:
                eq, why = compare(a, b, ordered)
                basis = "raw_duckdb_vs_seekdb"
            notes = repr_notes(a, b) if not (aerr or berr) else []
            verdicts.append({"class": name, "sql": sql, "routed": is_routed, "http_status": st,
                             "equivalent": eq, "why": why, "basis": basis, "repr_notes": notes,
                             "seekdb": str(a)[:200], "duckdb": str(b)[:200]})
            if is_routed and not eq:
                unsafe.append((name, why))
            mark = "ROUTED" if is_routed else "refused"
            print(f"  [{'EQ ' if eq else 'DIV'}] {mark:7s} {name:26s} {why[:90]}")
        c.close(); d.close()

        print("\n=== 安全性质：被路由 ⟹ 等价 ===")
        if unsafe:
            for n, w in unsafe:
                print(f"  !! 不安全: {n} :: {w}")
        else:
            print(f"  通过：{n_routed} 条被路由的类全部等价")
        div_refused = [v["class"] for v in verdicts if not v["equivalent"] and not v["routed"]]
        eq_refused = [v["class"] for v in verdicts if v["equivalent"] and not v["routed"]]
        print(f"  被拒且确实不等价（拒绝正确）: {len(div_refused)} -> {div_refused}")
        print(f"  被拒但其实等价（保守，可日后放宽）: {len(eq_refused)} -> {eq_refused}")

        out = {"verdicts": verdicts, "unsafe": unsafe, "n_routed": n_routed,
               "divergent_refused": div_refused, "equivalent_refused": eq_refused}
        with open("tests/fixtures/mysql-duckdb-verdicts.json", "w") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print("\n verdict 表已存 tests/fixtures/mysql-duckdb-verdicts.json")
        return 1 if unsafe else 0
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=20)
            except Exception:
                proc.kill()
        if not args.keep:
            subprocess.run(["rm", "-rf", MIRROR_DIR], check=False)


if __name__ == "__main__":
    sys.exit(main())