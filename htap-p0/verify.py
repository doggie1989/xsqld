import argparse, json, sys
import common as C


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--detail", action="store_true", help="also compare per-status counts")
    args = ap.parse_args()

    sc = C.sqlite_ro()
    dc = C.duckdb_connect(read_only=True)

    s_cnt, s_sum = sc.execute("SELECT count(*), COALESCE(sum(amount_cents),0) FROM orders").fetchone()
    d_cnt, d_sum = dc.execute("SELECT count(*), COALESCE(sum(amount_cents),0) FROM orders_native").fetchone()
    wm = C.get_watermark(dc)
    max_seq = sc.execute("SELECT COALESCE(MAX(seq),0) FROM xsqld_changelog").fetchone()[0]

    result = {
        "sqlite_count": s_cnt, "duckdb_count": d_cnt,
        "sqlite_sum": s_sum, "duckdb_sum": d_sum,
        "watermark": wm, "changelog_max_seq": max_seq,
        "pending": max_seq - wm,
        "count_match": s_cnt == d_cnt, "sum_match": s_sum == d_sum,
    }
    ok = result["count_match"] and result["sum_match"]

    if args.detail:
        s_status = dict(sc.execute("SELECT status, count(*) FROM orders GROUP BY status").fetchall())
        d_status = dict(dc.execute("SELECT status, count(*) FROM orders_native GROUP BY status").fetchall())
        result["status_match"] = s_status == d_status
        result["sqlite_status"] = s_status
        result["duckdb_status"] = d_status
        ok = ok and result["status_match"]

    result["consistent"] = ok
    print(json.dumps(result, indent=2))
    sc.close()
    dc.close()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()