#!/usr/bin/env python3
"""Acceptance for transparent routing: the same SELECT sent to /v2/pipeline on a server with routing ON must
produce the same response as on a server with routing OFF, field by field — and routing must actually have
happened, otherwise the comparison proves nothing.

Usage: two servers over identical data, one started with --route-enabled.
  python3 tests/compare_routing.py --off 127.0.0.1:28160 --on 127.0.0.1:28170 --ns rt
"""
import argparse
import json
import sys

from compare import request as raw_request, ex

# The shapes the gate admits (the profitable joins) plus shapes it must refuse. Both kinds have to come back
# identical: an admitted one because the engines agree, a refused one because SQLite answered it.
QUERIES = [
    ("join_sum_group_order", "SELECT d.title AS title, SUM(i.amount_cents) AS s FROM order_items i JOIN products d ON d.id=i.product_id GROUP BY d.title ORDER BY d.title"),
    ("join_count_group", "SELECT p.brand AS brand, COUNT(*) AS c FROM order_items i JOIN products p ON p.id=i.product_id GROUP BY p.brand ORDER BY p.brand"),
    ("three_table_join", "SELECT c.name AS cat, COUNT(*) AS n FROM order_items i JOIN products p ON p.id=i.product_id JOIN categories c ON c.id=p.category_id GROUP BY c.name ORDER BY c.name"),
    ("join_min_max", "SELECT p.brand AS brand, MIN(i.qty) AS lo, MAX(i.qty) AS hi FROM order_items i JOIN products p ON p.id=i.product_id GROUP BY p.brand ORDER BY p.brand"),
    ("big_scan_count", "SELECT COUNT(*) AS n FROM order_items"),
    ("big_scan_sum", "SELECT SUM(amount_cents) AS s FROM order_items"),
    ("nullable_order_asc", "SELECT id, paid_at FROM orders ORDER BY paid_at ASC, id ASC"),
    ("nullable_order_desc", "SELECT id, paid_at FROM orders ORDER BY paid_at DESC, id ASC"),
    ("limit_total_order", "SELECT id, total_cents FROM orders ORDER BY total_cents DESC, id ASC LIMIT 20"),
    ("predicate_range", "SELECT COUNT(*) AS n FROM order_items WHERE qty>1"),
    ("refused_avg", "SELECT AVG(total_cents) AS a FROM orders"),
    ("refused_arithmetic", "SELECT id, total_cents/2 AS h FROM orders ORDER BY id LIMIT 5"),
    ("refused_distinct", "SELECT DISTINCT status FROM orders ORDER BY status"),
    ("refused_limit_no_order", "SELECT id FROM orders LIMIT 5"),
    # a routed SUM that leaves i64: SQLite raises an overflow error, DuckDB widens. The runtime guard must fall
    # back so the client sees SQLite's error, not a wider number.
    ("overflow_sum_falls_back", "SELECT SUM(amount_cents) AS s FROM order_items"),
]


def call(addr, path, body, ns):
    status, data = raw_request(addr, "POST", path, body, host=f"{ns}.localhost")
    try:
        return status, json.loads(data)
    except (ValueError, TypeError):
        return status, {"error": data.decode("utf-8", "replace")[:300]}


def pipeline(addr, ns, sql):
    return call(addr, "/v2/pipeline", {"requests": [ex(sql), {"type": "close"}]}, ns)


def metrics(addr, ns):
    status, data = raw_request(addr, "GET", "/metrics", None, host=f"{ns}.localhost")
    out = {}
    for line in data.decode().splitlines():
        if line.startswith("xsqld_route_runs_total") and f'ns="{ns}"' in line:
            out["route_runs"] = int(line.split()[-1])
    return out


def result_of(resp):
    first = resp["results"][0]
    if first["type"] == "error":
        return {"error": first["error"]}
    return first["response"]["result"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--off", required=True)
    ap.add_argument("--on", required=True)
    ap.add_argument("--ns", default="rt")
    ap.add_argument("--json")
    args = ap.parse_args()

    print(f"{'query':<30} {'match':<8} detail")
    print("-" * 88)
    records = []
    for name, sql in QUERIES:
        sql = " ".join(sql.split())
        sa, ra = pipeline(args.off, args.ns, sql)
        sb, rb = pipeline(args.on, args.ns, sql)
        rec = {"name": name, "sql": sql}
        if sa != 200 or sb != 200:
            rec["match"] = "HTTP"
            rec["detail"] = f"off={sa} on={sb}"
        else:
            a, b = result_of(ra), result_of(rb)
            if "error" in a or "error" in b:
                # the same error on both sides is equivalent behaviour: SQLite refused the query and the router
                # let SQLite refuse it too (e.g. an overflowing SUM)
                if a.get("error") == b.get("error") and "error" in a:
                    rec["match"] = "same"
                    rec["detail"] = "same error: " + str(a["error"].get("message", ""))[:40]
                else:
                    rec["match"] = "ERROR"
                    rec["detail"] = f"off={a.get('error','-')} on={b.get('error','-')}"
            else:
                diffs = []
                if a["cols"] != b["cols"]:
                    diffs.append(f"cols {a['cols']} vs {b['cols']}")
                if a["rows"] != b["rows"]:
                    diffs.append(f"rows differ ({len(a['rows'])} vs {len(b['rows'])})")
                for field in ("rows_read", "affected_row_count", "last_insert_rowid", "replication_index"):
                    if a.get(field) != b.get(field):
                        diffs.append(f"{field} {a.get(field)} vs {b.get(field)}")
                rec["match"] = "same" if not diffs else "DIFFERS"
                rec["detail"] = "; ".join(diffs)[:60]
        records.append(rec)
        print(f"{name:<30} {rec['match']:<8} {rec['detail']}")

    m_off, m_on = metrics(args.off, args.ns), metrics(args.on, args.ns)
    print("-" * 88)
    print(f"route_runs: off={m_off.get('route_runs', 0)} on={m_on.get('route_runs', 0)}")
    same = sum(1 for r in records if r["match"] == "same")
    print(f"{same}/{len(records)} responses identical")
    routed = m_on.get("route_runs", 0)
    if routed == 0:
        print("VACUOUS: routing never fired, so identical responses prove nothing")
    if args.json:
        with open(args.json, "w") as fh:
            json.dump({"records": records, "route_runs_on": routed}, fh, indent=2)
    return 0 if same == len(records) and routed > 0 else 1


if __name__ == "__main__":
    sys.exit(main())