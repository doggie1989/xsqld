#!/usr/bin/env python3
"""Engine differential harness: the admission evidence for transparent row/column routing.

Runs the SAME SQL on SQLite (/v2/pipeline) and on the DuckDB mirror (/v2/analytics, tables rewritten to
`t_native`) and compares them value by value WITH type tags. A query class is only allowed into the router's
whitelist if this harness says its values are equivalent.

`cols` (name + decltype) is reported separately from values: analytics currently returns decltype=None for every
column, which the router is meant to fill from the source schema. That is a contract gap, not a value divergence,
and conflating the two would hide real semantic differences.

Usage:
  xsqld --db-path /tmp/diff --http-listen-addr 127.0.0.1:28150 --admin-listen-addr 127.0.0.1:28151 \
        --request-timeout-ms 600000 --idle-close-ms 0
  python3 tests/diff_engines.py [--xsqld 127.0.0.1:28150 --xsqld-admin 127.0.0.1:28151] [--json out.json]
"""

import argparse
import json
import re
import sys
import time

from compare import request as raw_request, ex


def request(addr, method, path, body=None, host=None):
    """compare.py's request returns raw bytes; every call here wants JSON."""
    status, data = raw_request(addr, method, path, body, host=host)
    try:
        return status, json.loads(data)
    except (ValueError, TypeError):
        return status, {"error": data.decode("utf-8", "replace")[:400]}

TABLES = ["nums", "mixed", "txt", "dim", "empty_t", "allnull", "halves"]
_RE = re.compile(r"\b(" + "|".join(sorted(TABLES, key=len, reverse=True)) + r")\b")


def native(sql):
    return _RE.sub(lambda m: m.group(1) + "_native", sql)


SCHEMA = [
    # pure integer column, plus a nullable one for NULL-order cases
    "CREATE TABLE nums(id INTEGER PRIMARY KEY, v INTEGER, n INTEGER)",
    # one column holding several storage classes, as SQLite's dynamic typing allows
    "CREATE TABLE mixed(id INTEGER PRIMARY KEY, v)",
    # text columns with the three collations that matter for GROUP BY / ORDER BY
    "CREATE TABLE txt(id INTEGER PRIMARY KEY, b TEXT COLLATE BINARY, nc TEXT COLLATE NOCASE, rt TEXT COLLATE RTRIM, bad TEXT)",
    # join dimension
    "CREATE TABLE dim(id INTEGER PRIMARY KEY, name TEXT COLLATE BINARY, w INTEGER)",
    "CREATE TABLE empty_t(id INTEGER PRIMARY KEY, v INTEGER)",
    "CREATE TABLE allnull(id INTEGER PRIMARY KEY, v INTEGER)",
    # an average that lands exactly on .5, so CAST rounding is actually exercised
    "CREATE TABLE halves(id INTEGER PRIMARY KEY, v INTEGER)",
]

DATA = [
    "INSERT INTO nums VALUES(1,10,5),(2,20,NULL),(3,-7,1),(4,10,NULL),(5,0,9),(6,3,2)",
    "INSERT INTO mixed VALUES(1,10),(2,'ten'),(3,1.5),(4,NULL),(5,X'0102'),(6,20)",
    # 'bad' holds text that is not valid UTF-8: the mirror stores it as a blob
    "INSERT INTO txt VALUES(1,'a','a','a ',CAST(X'FF' AS TEXT)),(2,'A','A','a',NULL),(3,'b','b','b ',NULL),(4,NULL,NULL,NULL,NULL)",
    "INSERT INTO dim VALUES(1,'one',100),(2,'two',200),(3,'three',300)",
    "INSERT INTO allnull VALUES(1,NULL),(2,NULL)",
    "INSERT INTO halves VALUES(1,1),(2,2)",
]


class Engines:
    def __init__(self, addr, admin, ns):
        self.addr, self.admin, self.ns = addr, admin, ns
        self.host = f"{ns}.localhost"

    def setup(self):
        request(self.admin, "POST", f"/v1/namespaces/{self.ns}/create", {})
        stmts = SCHEMA + DATA
        # overflow and boundary rows go in separately so a failure is attributable
        stmts += [
            "INSERT INTO nums VALUES(7,9223372036854775807,NULL)",
            "INSERT INTO nums VALUES(8,-9223372036854775808,NULL)",
        ]
        r = request(self.addr, "POST", "/v2/pipeline",
                    {"requests": [ex(s) for s in stmts] + [{"type": "close"}]}, host=self.host)
        for item in r[1]["results"]:
            if item["type"] == "error":
                raise RuntimeError(item["error"]["message"])
        # build the mirror
        r = request(self.addr, "POST", "/v2/analytics", {"sql": "SELECT COUNT(*) AS n FROM nums_native", "args": []}, host=self.host)
        if r[0] != 200:
            raise RuntimeError(f"mirror not built: {r}")

    def sqlite(self, sql):
        st, data = request(self.addr, "POST", "/v2/pipeline",
                           {"requests": [ex(sql), {"type": "close"}]}, host=self.host)
        if st != 200:
            return {"error": f"HTTP {st}: {data}"}
        first = data["results"][0]
        if first["type"] == "error":
            return {"error": first["error"]["message"]}
        return first["response"]["result"]

    def duckdb(self, sql):
        st, data = request(self.addr, "POST", "/v2/analytics",
                           {"sql": native(sql), "args": []}, host=self.host)
        if st != 200 or "error" in data:
            return {"error": f"HTTP {st}: {data.get('error', data)}"}
        return data["result"]


def canon(v):
    ty = v.get("type")
    if ty == "null":
        return ("null", None)
    if ty == "integer":
        return ("integer", int(v["value"]))
    if ty == "float":
        return ("float", float(v["value"]))
    if ty == "text":
        return ("text", v["value"])
    if ty == "blob":
        return ("blob", v["base64"])
    return ("?", json.dumps(v, sort_keys=True))


def values_equal(a, b):
    """Type-tagged comparison. integer 10 and float 10.0 are NOT the same answer."""
    if len(a) != len(b):
        return False, f"row count {len(a)} vs {len(b)}"
    for n, (ra, rb) in enumerate(zip(a, b)):
        if len(ra) != len(rb):
            return False, f"row {n} width {len(ra)} vs {len(rb)}"
        for m, (x, y) in enumerate(zip(ra, rb)):
            if x != y:
                return False, f"row {n} col {m}: {x} vs {y}"
    return True, ""


def cols_of(res):
    return [(c.get("name"), c.get("decltype")) for c in res.get("cols", [])]


# (class, sql, totally_ordered) — totally_ordered decides whether row ORDER is part of the verdict
CASES = [
    ("count_star", "SELECT COUNT(*) AS n FROM nums", True),
    ("sum_pure_int", "SELECT SUM(v) AS s FROM nums WHERE id<=6", True),
    ("sum_overflow", "SELECT SUM(v) AS s FROM nums", True),
    ("sum_all_null", "SELECT SUM(v) AS s FROM allnull", True),
    ("sum_empty", "SELECT SUM(v) AS s FROM empty_t", True),
    ("sum_mixed_classes", "SELECT SUM(v) AS s FROM mixed", True),
    ("min_max_int", "SELECT MIN(v) AS lo, MAX(v) AS hi FROM nums", True),
    ("min_max_mixed", "SELECT MIN(v) AS lo, MAX(v) AS hi FROM mixed", True),
    ("count_col_nulls", "SELECT COUNT(n) AS n FROM nums", True),
    ("group_by_int", "SELECT v, COUNT(*) AS n FROM nums WHERE id<=6 GROUP BY v ORDER BY v", True),
    ("group_by_binary_text", "SELECT b, COUNT(*) AS n FROM txt GROUP BY b ORDER BY b", True),
    ("group_by_nocase", "SELECT nc, COUNT(*) AS n FROM txt GROUP BY nc ORDER BY nc", True),
    ("group_by_rtrim", "SELECT rt, COUNT(*) AS n FROM txt GROUP BY rt ORDER BY rt", True),
    ("order_asc_nulls", "SELECT id, n FROM nums ORDER BY n ASC, id ASC", True),
    ("order_desc_nulls", "SELECT id, n FROM nums ORDER BY n DESC, id ASC", True),
    ("order_text_binary", "SELECT id, b FROM txt ORDER BY b ASC, id ASC", True),
    ("order_text_nocase", "SELECT id, nc FROM txt ORDER BY nc ASC, id ASC", True),
    ("limit_ties", "SELECT v FROM nums WHERE id<=6 ORDER BY v DESC LIMIT 3", False),
    ("limit_total_order", "SELECT v FROM nums WHERE id<=6 ORDER BY v DESC, id ASC LIMIT 3", True),
    ("inner_join_sum", "SELECT d.name, SUM(n.v) AS s FROM nums n JOIN dim d ON d.id=n.id WHERE n.id<=6 GROUP BY d.name ORDER BY d.name", True),
    ("inner_join_count", "SELECT d.name, COUNT(*) AS c FROM nums n JOIN dim d ON d.id=n.id GROUP BY d.name ORDER BY d.name", True),
    ("join_three_tables", "SELECT d.name, COUNT(*) AS c FROM nums n JOIN dim d ON d.id=n.id JOIN txt x ON x.id=n.id GROUP BY d.name ORDER BY d.name", True),
    ("avg", "SELECT AVG(v) AS a FROM nums WHERE id<=6", True),
    ("cast_avg_bigint", "SELECT CAST(AVG(v) AS BIGINT) AS a FROM nums WHERE id<=6", True),
    ("integer_division", "SELECT id, v/2 AS h FROM nums WHERE id<=6 ORDER BY id", True),
    ("non_utf8_text", "SELECT id, bad FROM txt ORDER BY id", True),
    ("typeof_projection", "SELECT id, typeof(v) AS ty FROM mixed ORDER BY id", True),
    ("predicate_text_eq", "SELECT id FROM txt WHERE b='a' ORDER BY id", True),
    ("predicate_nocase_eq", "SELECT id FROM txt WHERE nc='a' ORDER BY id", True),
    ("distinct", "SELECT DISTINCT v FROM nums WHERE id<=6 ORDER BY v", True),
    # the .5 average: a probe that never hits a half cannot prove CAST equivalence
    ("avg_half", "SELECT AVG(v) AS a FROM halves", True),
    ("cast_avg_half", "SELECT CAST(AVG(v) AS BIGINT) AS a FROM halves", True),
    ("min_max_pure_text", "SELECT MIN(b) AS lo, MAX(b) AS hi FROM txt", True),
    ("group_by_nullable_int", "SELECT n, COUNT(*) AS c FROM nums GROUP BY n ORDER BY n", True),
    ("sum_negative_only", "SELECT SUM(v) AS s FROM nums WHERE v<0", True),
    # positive control: does explicit NULLS injection actually remove the ordering divergence?
    ("order_asc_nulls_injected", "SELECT id, n FROM nums ORDER BY n ASC NULLS FIRST, id ASC", True),
    ("order_desc_nulls_injected", "SELECT id, n FROM nums ORDER BY n DESC NULLS LAST, id ASC", True),
    ("group_by_nullable_injected", "SELECT n, COUNT(*) AS c FROM nums GROUP BY n ORDER BY n ASC NULLS FIRST", True),
    ("group_by_text_injected", "SELECT b, COUNT(*) AS n FROM txt GROUP BY b ORDER BY b ASC NULLS FIRST", True),
    ("join_sum_pure_int", "SELECT d.name, SUM(n.v) AS s FROM nums n JOIN dim d ON d.id=n.id WHERE n.v>=0 AND n.id<=6 GROUP BY d.name ORDER BY d.name", True),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xsqld", default="127.0.0.1:28150")
    ap.add_argument("--xsqld-admin", default="127.0.0.1:28151")
    ap.add_argument("--ns", default=f"diff{int(time.time())}")
    ap.add_argument("--json")
    args = ap.parse_args()

    e = Engines(args.xsqld, args.xsqld_admin, args.ns)
    e.setup()
    print(f"fixture ready in namespace {args.ns}\n")
    print(f"{'class':<26} {'values':<12} {'cols':<8} detail")
    print("-" * 96)

    verdicts = []
    for name, sql, ordered in CASES:
        a, b = e.sqlite(sql), e.duckdb(sql)
        rec = {"class": name, "sql": sql, "totally_ordered": ordered}
        if "error" in a or "error" in b:
            rec["values"] = "ERROR"
            rec["detail"] = f"sqlite={a.get('error','-')[:60]} duckdb={b.get('error','-')[:60]}"
            rec["cols"] = "-"
        else:
            ra = [[canon(v) for v in row] for row in a["rows"]]
            rb = [[canon(v) for v in row] for row in b["rows"]]
            if not ordered:
                ra, rb = sorted(ra, key=repr), sorted(rb, key=repr)
            ok, why = values_equal(ra, rb)
            rec["values"] = "EQUIVALENT" if ok else "DIVERGENT"
            rec["detail"] = why
            ca, cb = cols_of(a), cols_of(b)
            rec["cols"] = "same" if ca == cb else "differs"
            rec["cols_sqlite"], rec["cols_duckdb"] = ca, cb
        verdicts.append(rec)
        print(f"{name:<26} {rec['values']:<12} {rec['cols']:<8} {rec['detail'][:44]}")

    eq = [v["class"] for v in verdicts if v["values"] == "EQUIVALENT"]
    dv = [v["class"] for v in verdicts if v["values"] == "DIVERGENT"]
    er = [v["class"] for v in verdicts if v["values"] == "ERROR"]
    print("-" * 96)
    print(f"EQUIVALENT ({len(eq)}): {', '.join(eq)}")
    print(f"DIVERGENT  ({len(dv)}): {', '.join(dv)}")
    print(f"ERROR      ({len(er)}): {', '.join(er)}")
    print("\nWhitelist admission: only EQUIVALENT classes may be routed; DIVERGENT/ERROR are permanently refused.")
    if args.json:
        with open(args.json, "w") as fh:
            json.dump(verdicts, fh, indent=2, ensure_ascii=False)
        print(f"verdict table written to {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())